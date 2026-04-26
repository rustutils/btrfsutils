//! # Logical-to-physical address mapping for btrfs filesystems
//!
//! Btrfs maps logical addresses to physical device offsets through chunk items
//! stored in the chunk tree. The superblock embeds a small subset of the chunk
//! tree (the system chunk array) to bootstrap access to the full chunk tree.
//!
//! This module provides a `ChunkTreeCache` that resolves logical addresses to
//! physical offsets, seeded from the `sys_chunk_array` and then populated from
//! the full chunk tree.

use crate::raw;
use bytes::Buf;
use std::{collections::BTreeMap, mem};
use uuid::Uuid;

fn get_uuid(buf: &mut &[u8]) -> Uuid {
    let bytes: [u8; 16] = buf[..16].try_into().unwrap();
    buf.advance(16);
    Uuid::from_bytes(bytes)
}

/// A single stripe in a chunk mapping, identifying a physical location on a device.
#[derive(Debug, Clone)]
pub struct Stripe {
    /// Device ID where this stripe resides.
    pub devid: u64,
    /// Physical byte offset on the device.
    pub offset: u64,
    /// UUID of the device.
    pub dev_uuid: Uuid,
}

/// A chunk mapping: maps a range of logical addresses to physical device locations.
#[derive(Debug, Clone)]
pub struct ChunkMapping {
    /// Starting logical byte address of this chunk.
    pub logical: u64,
    /// Length of this chunk in bytes.
    pub length: u64,
    /// Stripe length for striped profiles (RAID0/10/5/6).
    pub stripe_len: u64,
    /// Chunk type flags (DATA/METADATA/SYSTEM + RAID profile).
    pub chunk_type: u64,
    /// Number of stripes (device copies/segments).
    pub num_stripes: u16,
    /// Sub-stripes for RAID10.
    pub sub_stripes: u16,
    /// Physical device locations for each stripe.
    pub stripes: Vec<Stripe>,
}

/// RAID profile derived from a chunk's `chunk_type` flags.
///
/// `RAID1` covers all mirrored profiles (RAID1, RAID1C3, RAID1C4) since
/// they share the same routing math (every stripe gets the same bytes).
/// The number of mirrors is given by `ChunkMapping::num_stripes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkProfile {
    /// No replication, no striping. One stripe, written/read whole.
    Single,
    /// Two copies on the same device (mostly used for metadata on
    /// single-device filesystems). Both stripes get the same bytes.
    Dup,
    /// Striped across devices, no redundancy. Each row of `stripe_len`
    /// bytes lands on a single device; consecutive rows round-robin.
    Raid0,
    /// Mirrored across N devices (`num_stripes` = 2/3/4 for
    /// RAID1 / RAID1C3 / RAID1C4). Every stripe gets the same bytes.
    Raid1,
    /// Striped mirrors. `num_stripes / sub_stripes` data groups, each
    /// mirrored `sub_stripes`-ways (always 2 in practice).
    Raid10,
    /// Striped with parity (single parity device). Not yet handled.
    Raid5,
    /// Striped with double parity. Not yet handled.
    Raid6,
}

impl ChunkProfile {
    /// Decode the RAID profile bits of an on-disk `chunk_type` field.
    ///
    /// SINGLE is the absence of any profile bit. The data/metadata/system
    /// bits are ignored — only the RAID profile bits matter for routing.
    #[must_use]
    pub fn from_chunk_type(chunk_type: u64) -> Self {
        if chunk_type & u64::from(raw::BTRFS_BLOCK_GROUP_RAID0) != 0 {
            Self::Raid0
        } else if chunk_type & u64::from(raw::BTRFS_BLOCK_GROUP_RAID10) != 0 {
            Self::Raid10
        } else if chunk_type & u64::from(raw::BTRFS_BLOCK_GROUP_RAID5) != 0 {
            Self::Raid5
        } else if chunk_type & u64::from(raw::BTRFS_BLOCK_GROUP_RAID6) != 0 {
            Self::Raid6
        } else if chunk_type
            & u64::from(
                raw::BTRFS_BLOCK_GROUP_RAID1
                    | raw::BTRFS_BLOCK_GROUP_RAID1C3
                    | raw::BTRFS_BLOCK_GROUP_RAID1C4,
            )
            != 0
        {
            Self::Raid1
        } else if chunk_type & u64::from(raw::BTRFS_BLOCK_GROUP_DUP) != 0 {
            Self::Dup
        } else {
            Self::Single
        }
    }
}

impl ChunkMapping {
    /// Decode the chunk's RAID profile from its `chunk_type` field.
    #[must_use]
    pub fn profile(&self) -> ChunkProfile {
        ChunkProfile::from_chunk_type(self.chunk_type)
    }
}

/// One per-device write or read produced by [`ChunkTreeCache::plan_write`]
/// or [`ChunkTreeCache::plan_read`].
///
/// `buf_offset..buf_offset + len` is the slice of the caller's buffer
/// that goes to this device at `physical`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripePlacement {
    /// Device id where this slice goes.
    pub devid: u64,
    /// Physical byte offset on the device.
    pub physical: u64,
    /// Byte offset within the caller's buffer where this slice starts.
    pub buf_offset: usize,
    /// Number of bytes to write (or read).
    pub len: usize,
}

/// Cache of chunk tree mappings for resolving logical to physical addresses.
///
/// Keyed by logical start address. Uses a `BTreeMap` for efficient range lookups.
#[derive(Debug, Default)]
pub struct ChunkTreeCache {
    inner: BTreeMap<u64, ChunkMapping>,
}

impl ChunkTreeCache {
    /// Insert a chunk mapping into the cache.
    pub fn insert(&mut self, mapping: ChunkMapping) {
        self.inner.insert(mapping.logical, mapping);
    }

    /// Look up the chunk mapping that contains the given logical address.
    #[must_use]
    pub fn lookup(&self, logical: u64) -> Option<&ChunkMapping> {
        // Find the entry whose start is <= logical
        self.inner
            .range(..=logical)
            .next_back()
            .map(|(_, mapping)| mapping)
            .filter(|mapping| logical < mapping.logical + mapping.length)
    }

    /// Resolve a logical address to `(devid, physical)` for the first stripe.
    ///
    /// For read-only access the first stripe is sufficient on SINGLE, DUP,
    /// and any mirroring profile. RAID0/5/6/10 striping would need stripe
    /// index calculation, but for tree blocks (always `nodesize <= stripe_len`)
    /// the whole block lives in one stripe slot, so this works for the
    /// common case.
    ///
    /// Callers using a multi-device `BlockReader` look up the device handle
    /// by `devid`; single-device callers ignore it.
    #[must_use]
    pub fn resolve(&self, logical: u64) -> Option<(u64, u64)> {
        let mapping = self.lookup(logical)?;
        let offset_within_chunk = logical - mapping.logical;
        let stripe = &mapping.stripes[0];
        Some((stripe.devid, stripe.offset + offset_within_chunk))
    }

    /// Resolve a logical address to `(devid, physical)` for every stripe.
    ///
    /// For DUP, RAID1, RAID1C3, and RAID1C4, a single logical address maps
    /// to multiple physical copies. Write operations must update all copies
    /// to maintain consistency.
    ///
    /// **Use [`plan_write`](Self::plan_write) for actual write routing.**
    /// `resolve_all` ignores the chunk's RAID profile and assumes every
    /// stripe should receive the same bytes; that is correct for DUP /
    /// RAID1* but wrong for RAID0 (each row goes to one device only) and
    /// RAID10 (each row goes to one mirror pair, not all pairs). Kept
    /// for diagnostics and read-only callers that only need a list of
    /// stripe locations.
    #[must_use]
    pub fn resolve_all(&self, logical: u64) -> Option<Vec<(u64, u64)>> {
        let mapping = self.lookup(logical)?;
        let offset_within_chunk = logical - mapping.logical;
        Some(
            mapping
                .stripes
                .iter()
                .map(|s| (s.devid, s.offset + offset_within_chunk))
                .collect(),
        )
    }

    /// Plan the per-device writes needed to land `len` bytes at the
    /// logical address `logical`, accounting for the chunk's RAID
    /// profile and stripe length.
    ///
    /// Returns one [`StripePlacement`] per (row, mirror) the caller must
    /// write. The caller iterates the placements and writes
    /// `buf[p.buf_offset..p.buf_offset + p.len]` to device `p.devid`
    /// at byte offset `p.physical`.
    ///
    /// Per-profile fan-out for a single row:
    ///
    /// - SINGLE: one placement (column 0).
    /// - DUP / RAID1 / RAID1C3 / RAID1C4: `num_stripes` placements
    ///   (every stripe gets the same bytes).
    /// - RAID0: one placement (column = `stripe_nr % num_stripes`).
    /// - RAID10: `sub_stripes` placements (the mirror pair for the row).
    ///
    /// Buffers larger than `stripe_len - stripe_offset` span multiple
    /// rows; each row's placements are appended in order.
    ///
    /// Returns `None` if `logical` is unmapped, if `logical + len`
    /// exceeds the chunk, or if the chunk uses RAID5/RAID6 (not yet
    /// implemented — those need their own plan).
    #[must_use]
    pub fn plan_write(
        &self,
        logical: u64,
        len: usize,
    ) -> Option<Vec<StripePlacement>> {
        plan_io(self.lookup(logical)?, logical, len, /* read = */ false)
    }

    /// Plan the per-device reads needed to fetch `len` bytes from the
    /// logical address `logical`. Returns exactly one placement per row
    /// (the first stripe of each row) — the caller assembles the bytes
    /// in order.
    ///
    /// Like [`plan_write`](Self::plan_write), returns `None` for unmapped
    /// addresses, out-of-bounds requests, or RAID5/RAID6 chunks.
    #[must_use]
    pub fn plan_read(
        &self,
        logical: u64,
        len: usize,
    ) -> Option<Vec<StripePlacement>> {
        plan_io(self.lookup(logical)?, logical, len, /* read = */ true)
    }

    /// Return the number of cached chunk mappings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterate over all chunk mappings in logical address order.
    pub fn iter(&self) -> impl Iterator<Item = &ChunkMapping> {
        self.inner.values()
    }
}

/// Compute per-device placements for a logical-range I/O within `mapping`.
///
/// The shared core for [`ChunkTreeCache::plan_write`] and
/// [`ChunkTreeCache::plan_read`]: walks the request row by row, and for
/// each row picks the columns of `mapping.stripes` that own that row's
/// bytes per the chunk's RAID profile.
///
/// `read` is true for read planning (one placement per row, picking the
/// first column of the row's mirror group) and false for write planning
/// (every column of the row's mirror group).
fn plan_io(
    mapping: &ChunkMapping,
    logical: u64,
    len: usize,
    read: bool,
) -> Option<Vec<StripePlacement>> {
    if len == 0 {
        return Some(Vec::new());
    }
    // Bounds check: the entire request must fit in the chunk.
    let end = logical.checked_add(len as u64)?;
    if end > mapping.logical.checked_add(mapping.length)? {
        return None;
    }
    // RAID5/RAID6 routing isn't implemented; signal that to the caller.
    let profile = mapping.profile();
    if matches!(profile, ChunkProfile::Raid5 | ChunkProfile::Raid6) {
        return None;
    }

    let stripe_len = mapping.stripe_len;
    debug_assert!(stripe_len > 0, "chunk stripe_len must be non-zero");

    // Depending on the profile, only some columns of `mapping.stripes`
    // are independently addressable. SINGLE/DUP/RAID1*: 1 column (the
    // others are mirrors). RAID0: every column carries different rows.
    // RAID10: sub_stripes columns per group, num_stripes/sub_stripes
    // groups total.
    let factor: u64 = match profile {
        ChunkProfile::Single | ChunkProfile::Dup | ChunkProfile::Raid1 => 1,
        ChunkProfile::Raid0 => u64::from(mapping.num_stripes),
        ChunkProfile::Raid10 => {
            let sub = u64::from(mapping.sub_stripes.max(1));
            u64::from(mapping.num_stripes) / sub
        }
        ChunkProfile::Raid5 | ChunkProfile::Raid6 => unreachable!(),
    };
    debug_assert!(factor >= 1, "factor must be >= 1");

    let mut placements: Vec<StripePlacement> = Vec::new();
    let mut buf_offset: usize = 0;
    let mut cur = logical - mapping.logical; // offset within chunk
    let mut remaining = len;

    // Row column buffer reused per row: RAID1C4 (4 mirrors) is the
    // largest fan-out, so 4 entries is enough.
    let mut cols: [u16; 4] = [0; 4];

    while remaining > 0 {
        let stripe_nr = cur / stripe_len;
        let stripe_offset = cur % stripe_len;
        // How many bytes of this row are in our request.
        // `row_bytes` is bounded by `stripe_len` and `remaining` (a
        // usize) so it always fits in usize.
        let row_bytes =
            usize::try_from((stripe_len - stripe_offset).min(remaining as u64))
                .expect("row_bytes capped by remaining (usize)");

        // Per-row column selection: which columns of mapping.stripes
        // own this row's bytes.
        let n_cols = fill_row_columns(profile, mapping, stripe_nr, &mut cols);
        let cols_to_use = if read { &cols[..1] } else { &cols[..n_cols] };

        // Per-device offset within the device.
        let per_device_stripe_nr = stripe_nr / factor;
        let per_device_offset =
            per_device_stripe_nr * stripe_len + stripe_offset;

        for &col in cols_to_use {
            let stripe = &mapping.stripes[col as usize];
            placements.push(StripePlacement {
                devid: stripe.devid,
                physical: stripe.offset + per_device_offset,
                buf_offset,
                len: row_bytes,
            });
        }

        buf_offset += row_bytes;
        cur += row_bytes as u64;
        remaining -= row_bytes;
    }

    Some(placements)
}

/// Fill `cols` with the column indices of `mapping.stripes` that own
/// the row at `stripe_nr` for the given profile, and return how many
/// columns were written. `cols` must have room for at least
/// `mapping.num_stripes` entries (4 is enough for the largest mirror
/// fan-out: RAID1C4).
fn fill_row_columns(
    profile: ChunkProfile,
    mapping: &ChunkMapping,
    stripe_nr: u64,
    cols: &mut [u16; 4],
) -> usize {
    // num_stripes / sub_stripes / column indices all fit in u16 (the
    // profile fan-out is at most 4 — RAID1C4); the cast asserts here
    // are documentation that they cannot truncate in practice.
    match profile {
        ChunkProfile::Single => {
            cols[0] = 0;
            1
        }
        ChunkProfile::Dup | ChunkProfile::Raid1 => {
            let n = usize::from(mapping.num_stripes);
            debug_assert!(n <= cols.len(), "mirror count {n} exceeds 4");
            for (i, c) in cols.iter_mut().enumerate().take(n) {
                *c =
                    u16::try_from(i).expect("mirror count fits in u16 (max 4)");
            }
            n
        }
        ChunkProfile::Raid0 => {
            let col_u64 = stripe_nr % u64::from(mapping.num_stripes);
            cols[0] = u16::try_from(col_u64)
                .expect("col bounded by num_stripes (u16)");
            1
        }
        ChunkProfile::Raid10 => {
            let sub = mapping.sub_stripes.max(1);
            let factor = mapping.num_stripes / sub;
            let group_u64 = stripe_nr % u64::from(factor);
            let group = u16::try_from(group_u64)
                .expect("group bounded by factor (u16)");
            let base = group * sub;
            let n = usize::from(sub);
            for (s, c) in cols.iter_mut().enumerate().take(n) {
                *c = base
                    + u16::try_from(s)
                        .expect("sub_stripes fits in u16 (max 4)");
            }
            n
        }
        ChunkProfile::Raid5 | ChunkProfile::Raid6 => {
            // Caller must filter these out before calling.
            unreachable!()
        }
    }
}

/// Parse a chunk item (`btrfs_chunk` + stripes) from a raw byte buffer.
///
/// Returns the chunk mapping and the total number of bytes consumed.
/// `logical` is the logical start address from the key's offset field.
#[must_use]
pub fn parse_chunk_item(
    buf: &[u8],
    logical: u64,
) -> Option<(ChunkMapping, usize)> {
    let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();

    if buf.len() < chunk_base_size {
        return None;
    }

    let mut b = buf;
    let length = b.get_u64_le();
    b.advance(8); // owner
    let stripe_len = b.get_u64_le();
    let chunk_type = b.get_u64_le();
    b.advance(12); // io_align(4) + io_width(4) + sector_size(4)
    let num_stripes = b.get_u16_le();
    let sub_stripes = b.get_u16_le();

    let total_size = chunk_base_size + num_stripes as usize * stripe_size;
    if buf.len() < total_size {
        return None;
    }

    let mut stripes = Vec::with_capacity(num_stripes as usize);
    let mut b = &buf[chunk_base_size..];
    for _ in 0..num_stripes as usize {
        let devid = b.get_u64_le();
        let offset = b.get_u64_le();
        let dev_uuid = get_uuid(&mut b);
        stripes.push(Stripe {
            devid,
            offset,
            dev_uuid,
        });
    }

    let mapping = ChunkMapping {
        logical,
        length,
        stripe_len,
        chunk_type,
        num_stripes,
        sub_stripes,
        stripes,
    };

    Some((mapping, total_size))
}

/// Seed a `ChunkTreeCache` from the superblock's `sys_chunk_array`.
///
/// The `sys_chunk_array` contains a subset of chunk items needed to bootstrap
/// access to the full chunk tree (system profile chunks).
#[must_use]
pub fn seed_from_sys_chunk_array(array: &[u8], size: u32) -> ChunkTreeCache {
    let array = &array[..size as usize];
    let mut cache = ChunkTreeCache::default();

    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let mut offset = 0usize;

    while offset + disk_key_size <= array.len() {
        let mut b = &array[offset + 9..];
        let key_offset = b.get_u64_le();
        offset += disk_key_size;

        if let Some((mapping, consumed)) =
            parse_chunk_item(&array[offset..], key_offset)
        {
            cache.insert(mapping);
            offset += consumed;
        } else {
            break;
        }
    }

    cache
}

/// Serialize a [`ChunkMapping`] into the on-disk `btrfs_chunk` byte
/// layout (48-byte fixed header + `num_stripes * 32`-byte stripes).
///
/// `sector_size` is written into the chunk's `io_align`, `io_width`,
/// and `sector_size` fields (all three are conventionally equal to the
/// filesystem sector size).
#[must_use]
pub fn chunk_item_bytes(mapping: &ChunkMapping, sector_size: u32) -> Vec<u8> {
    use bytes::BufMut;

    let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();
    let total = chunk_base_size + mapping.num_stripes as usize * stripe_size;
    let mut buf: Vec<u8> = Vec::with_capacity(total);

    buf.put_u64_le(mapping.length);
    // Owner is always EXTENT_TREE objectid (2) for chunk records.
    buf.put_u64_le(u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID));
    buf.put_u64_le(mapping.stripe_len);
    buf.put_u64_le(mapping.chunk_type);
    buf.put_u32_le(sector_size); // io_align
    buf.put_u32_le(sector_size); // io_width
    buf.put_u32_le(sector_size); // sector_size
    buf.put_u16_le(mapping.num_stripes);
    buf.put_u16_le(mapping.sub_stripes);
    debug_assert_eq!(buf.len(), chunk_base_size);

    for stripe in &mapping.stripes {
        buf.put_u64_le(stripe.devid);
        buf.put_u64_le(stripe.offset);
        buf.extend_from_slice(stripe.dev_uuid.as_bytes());
    }
    debug_assert_eq!(buf.len(), total);
    buf
}

/// Walk the superblock's `sys_chunk_array` and return `true` if it
/// already contains a record whose `disk_key.offset` matches `bg_start`
/// (i.e. the system chunk starting at that logical address is already
/// part of the bootstrap snippet).
#[must_use]
pub fn sys_chunk_array_contains(
    array: &[u8],
    size: u32,
    bg_start: u64,
) -> bool {
    let array = &array[..size as usize];
    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let mut offset = 0usize;
    while offset + disk_key_size <= array.len() {
        // disk_key layout: u64 objectid | u8 type | u64 offset.
        let mut b = &array[offset + 9..];
        let key_offset = b.get_u64_le();
        offset += disk_key_size;
        if key_offset == bg_start {
            return true;
        }
        let Some((_, consumed)) =
            parse_chunk_item(&array[offset..], key_offset)
        else {
            return false;
        };
        offset += consumed;
    }
    false
}

/// Append a single `(disk_key, btrfs_chunk)` record to the
/// superblock's `sys_chunk_array` byte buffer.
///
/// Writes the 17-byte `btrfs_disk_key` followed by `chunk_bytes`
/// (already serialized via [`chunk_item_bytes`]) starting at offset
/// `*size`. On success, `*size` is bumped by the record size and
/// `Ok(new_size)` is returned. Returns `Err` if the record would
/// overflow the 2048-byte `sys_chunk_array`.
///
/// `bg_start` is the chunk's logical start address; it becomes the
/// `offset` field of the `BTRFS_FIRST_CHUNK_TREE_OBJECTID / CHUNK_ITEM`
/// disk key.
///
/// # Errors
///
/// Returns an error if the record does not fit in the remaining
/// `sys_chunk_array` space.
///
/// # Panics
///
/// Panics in debug builds if the new array size cannot be represented
/// in a `u32`. In practice this never happens because callers cap the
/// buffer at 2048 bytes.
pub fn sys_chunk_array_append(
    array: &mut [u8],
    size: &mut u32,
    bg_start: u64,
    chunk_bytes: &[u8],
) -> Result<u32, &'static str> {
    use bytes::BufMut;

    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let record_size = disk_key_size + chunk_bytes.len();
    let cur = *size as usize;
    if cur + record_size > array.len() {
        return Err("sys_chunk_array overflow");
    }

    // disk_key: objectid=BTRFS_FIRST_CHUNK_TREE_OBJECTID(256),
    //           type=BTRFS_CHUNK_ITEM_KEY(228),
    //           offset=bg_start.
    #[allow(clippy::cast_possible_truncation)]
    let chunk_item_type = raw::BTRFS_CHUNK_ITEM_KEY as u8;
    let mut header = [0u8; 17];
    {
        let mut w = &mut header[..];
        w.put_u64_le(u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID));
        w.put_u8(chunk_item_type);
        w.put_u64_le(bg_start);
    }
    array[cur..cur + 17].copy_from_slice(&header);
    array[cur + 17..cur + record_size].copy_from_slice(chunk_bytes);

    let new_size = u32::try_from(cur + record_size).unwrap();
    *size = new_size;
    Ok(new_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mapping(logical: u64, length: u64, physical: u64) -> ChunkMapping {
        ChunkMapping {
            logical,
            length,
            stripe_len: 65536,
            chunk_type: 0,
            num_stripes: 1,
            sub_stripes: 0,
            stripes: vec![Stripe {
                devid: 1,
                offset: physical,
                dev_uuid: Uuid::nil(),
            }],
        }
    }

    /// Build a chunk mapping with arbitrary stripes. Each entry is
    /// `(devid, physical_offset)`.
    fn make_multi_stripe_mapping(
        logical: u64,
        length: u64,
        stripes: &[(u64, u64)],
    ) -> ChunkMapping {
        ChunkMapping {
            logical,
            length,
            stripe_len: 65536,
            chunk_type: 0,
            num_stripes: stripes.len() as u16,
            sub_stripes: 0,
            stripes: stripes
                .iter()
                .map(|&(devid, offset)| Stripe {
                    devid,
                    offset,
                    dev_uuid: Uuid::nil(),
                })
                .collect(),
        }
    }

    #[test]
    fn empty_cache() {
        let cache = ChunkTreeCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.resolve(0), None);
    }

    #[test]
    fn single_mapping() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(1000, 500, 2000));
        assert_eq!(cache.len(), 1);

        assert_eq!(cache.resolve(1000), Some((1, 2000)));
        assert_eq!(cache.resolve(1100), Some((1, 2100)));
        assert_eq!(cache.resolve(1499), Some((1, 2499)));
        assert_eq!(cache.resolve(1500), None); // past end
        assert_eq!(cache.resolve(999), None); // before start
    }

    #[test]
    fn multiple_mappings() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(0, 1000, 5000));
        cache.insert(make_mapping(1000, 1000, 6000));
        cache.insert(make_mapping(5000, 2000, 10000));

        assert_eq!(cache.resolve(0), Some((1, 5000)));
        assert_eq!(cache.resolve(500), Some((1, 5500)));
        assert_eq!(cache.resolve(1000), Some((1, 6000)));
        assert_eq!(cache.resolve(1999), Some((1, 6999)));
        assert_eq!(cache.resolve(2000), None); // gap
        assert_eq!(cache.resolve(5000), Some((1, 10000)));
        assert_eq!(cache.resolve(6999), Some((1, 11999)));
        assert_eq!(cache.resolve(7000), None);
    }

    #[test]
    fn lookup_returns_mapping() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(1000, 500, 2000));

        let m = cache.lookup(1100).unwrap();
        assert_eq!(m.logical, 1000);
        assert_eq!(m.length, 500);
        assert!(cache.lookup(500).is_none());
    }

    #[test]
    fn resolve_returns_first_stripe_only() {
        // For a multi-stripe mapping, the single-result `resolve` always
        // picks stripe[0]'s (devid, physical). Useful for read paths
        // where any mirror is fine; write paths use `resolve_all`.
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (2, 9000)],
        ));
        assert_eq!(cache.resolve(1000), Some((1, 2000)));
        assert_eq!(cache.resolve(1100), Some((1, 2100)));
        assert_eq!(cache.resolve(1499), Some((1, 2499)));
    }

    #[test]
    fn resolve_all_dup_returns_two_offsets_same_devid() {
        // DUP profile: 2 stripes, both on devid 1, at distinct physical
        // offsets. The on-device write path writes to both copies via
        // the same handle, but the cache must report both placements.
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (1, 50000)],
        ));
        assert_eq!(cache.resolve_all(1000), Some(vec![(1, 2000), (1, 50000)]),);
        // Within-chunk offset propagates to every stripe's physical.
        assert_eq!(cache.resolve_all(1100), Some(vec![(1, 2100), (1, 50100)]),);
    }

    #[test]
    fn resolve_all_raid1_returns_two_devids() {
        // RAID1: stripes on different devices. `write_block` must
        // route each placement to its own device handle.
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (2, 9000)],
        ));
        assert_eq!(cache.resolve_all(1000), Some(vec![(1, 2000), (2, 9000)]),);
        assert_eq!(cache.resolve_all(1250), Some(vec![(1, 2250), (2, 9250)]),);
    }

    #[test]
    fn resolve_all_raid1c3_returns_three_stripes() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (2, 9000), (3, 0x10_0000)],
        ));
        let placements = cache.resolve_all(1100).unwrap();
        assert_eq!(placements.len(), 3);
        assert_eq!(placements[0], (1, 2100));
        assert_eq!(placements[1], (2, 9100));
        assert_eq!(placements[2], (3, 0x10_0000 + 100));
    }

    #[test]
    fn resolve_all_raid1c4_returns_four_stripes() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (2, 9000), (3, 0x10_0000), (4, 0x20_0000)],
        ));
        let placements = cache.resolve_all(1100).unwrap();
        assert_eq!(placements.len(), 4);
        assert_eq!(placements[0], (1, 2100));
        assert_eq!(placements[3], (4, 0x20_0000 + 100));
    }

    #[test]
    fn resolve_all_returns_none_outside_chunks() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            1000,
            500,
            &[(1, 2000), (2, 9000)],
        ));
        assert_eq!(cache.resolve_all(999), None);
        assert_eq!(cache.resolve_all(1500), None);
    }

    #[test]
    fn resolve_all_with_non_dense_devids() {
        // btrfs allows device removal that leaves devid gaps. Stripes
        // on devids {1, 5} should resolve correctly.
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_multi_stripe_mapping(
            0,
            1000,
            &[(1, 100), (5, 999_000)],
        ));
        assert_eq!(cache.resolve_all(42), Some(vec![(1, 142), (5, 999_042)]),);
    }

    #[test]
    fn seed_from_empty_array() {
        let array = [0u8; 2048];
        let cache = seed_from_sys_chunk_array(&array, 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn parse_chunk_item_basic() {
        let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
        let stripe_size = mem::size_of::<raw::btrfs_stripe>();
        let total = chunk_base_size + stripe_size;
        let mut buf = vec![0u8; total];

        // length
        buf[0..8].copy_from_slice(&1000u64.to_le_bytes());
        // owner
        buf[8..16].copy_from_slice(&2u64.to_le_bytes());
        // stripe_len
        buf[16..24].copy_from_slice(&65536u64.to_le_bytes());
        // type
        buf[24..32].copy_from_slice(&1u64.to_le_bytes());
        // num_stripes
        buf[44..46].copy_from_slice(&1u16.to_le_bytes());

        // stripe: devid=1, offset=5000
        buf[chunk_base_size..chunk_base_size + 8]
            .copy_from_slice(&1u64.to_le_bytes());
        buf[chunk_base_size + 8..chunk_base_size + 16]
            .copy_from_slice(&5000u64.to_le_bytes());

        let (mapping, consumed) = parse_chunk_item(&buf, 0).unwrap();
        assert_eq!(consumed, total);
        assert_eq!(mapping.logical, 0);
        assert_eq!(mapping.length, 1000);
        assert_eq!(mapping.num_stripes, 1);
        assert_eq!(mapping.stripes[0].devid, 1);
        assert_eq!(mapping.stripes[0].offset, 5000);
    }

    fn sample_mapping(
        logical: u64,
        length: u64,
        physical: u64,
    ) -> ChunkMapping {
        ChunkMapping {
            logical,
            length,
            stripe_len: 65536,
            // SYSTEM | SINGLE
            chunk_type: u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
            num_stripes: 1,
            sub_stripes: 1,
            stripes: vec![Stripe {
                devid: 1,
                offset: physical,
                dev_uuid: Uuid::from_bytes([0xAB; 16]),
            }],
        }
    }

    #[test]
    fn chunk_item_bytes_round_trips_via_parser() {
        let m = sample_mapping(0x100_0000, 0x40_0000, 0x200_0000);
        let bytes = chunk_item_bytes(&m, 4096);
        let (parsed, consumed) = parse_chunk_item(&bytes, m.logical).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.logical, m.logical);
        assert_eq!(parsed.length, m.length);
        assert_eq!(parsed.stripe_len, m.stripe_len);
        assert_eq!(parsed.chunk_type, m.chunk_type);
        assert_eq!(parsed.num_stripes, 1);
        assert_eq!(parsed.sub_stripes, 1);
        assert_eq!(parsed.stripes[0].devid, 1);
        assert_eq!(parsed.stripes[0].offset, 0x200_0000);
        assert_eq!(parsed.stripes[0].dev_uuid, m.stripes[0].dev_uuid);
    }

    #[test]
    fn sys_chunk_array_append_then_contains_and_seed() {
        let mut buf = [0u8; 2048];
        let mut size: u32 = 0;
        let m1 = sample_mapping(0x100_0000, 0x40_0000, 0x200_0000);
        let m2 = sample_mapping(0x500_0000, 0x40_0000, 0x600_0000);

        let bytes1 = chunk_item_bytes(&m1, 4096);
        sys_chunk_array_append(&mut buf, &mut size, m1.logical, &bytes1)
            .unwrap();
        assert!(sys_chunk_array_contains(&buf, size, m1.logical));
        assert!(!sys_chunk_array_contains(&buf, size, m2.logical));

        let bytes2 = chunk_item_bytes(&m2, 4096);
        sys_chunk_array_append(&mut buf, &mut size, m2.logical, &bytes2)
            .unwrap();
        assert!(sys_chunk_array_contains(&buf, size, m2.logical));

        // Seeding from the array should yield both mappings.
        let cache = seed_from_sys_chunk_array(&buf, size);
        assert_eq!(cache.len(), 2);
        assert!(cache.lookup(m1.logical).is_some());
        assert!(cache.lookup(m2.logical).is_some());
    }

    #[test]
    fn sys_chunk_array_append_overflow() {
        // Tiny array that fits exactly one record (97 bytes for a
        // single-stripe chunk).
        let mut buf = [0u8; 100];
        let mut size: u32 = 0;
        let m = sample_mapping(0, 0x40_0000, 0);
        let bytes = chunk_item_bytes(&m, 4096);
        sys_chunk_array_append(&mut buf, &mut size, m.logical, &bytes).unwrap();
        // Second append must fail.
        let m2 = sample_mapping(0x100_0000, 0x40_0000, 0x200_0000);
        let bytes2 = chunk_item_bytes(&m2, 4096);
        assert!(
            sys_chunk_array_append(&mut buf, &mut size, m2.logical, &bytes2)
                .is_err()
        );
    }

    #[test]
    fn parse_chunk_item_too_short() {
        let buf = [0u8; 10];
        assert!(parse_chunk_item(&buf, 0).is_none());
    }

    // --- ChunkProfile decoding ---

    #[test]
    fn profile_from_chunk_type_basic() {
        use ChunkProfile::*;
        // SINGLE: no profile bit set (only DATA/SYSTEM/METADATA bits).
        assert_eq!(
            ChunkProfile::from_chunk_type(u64::from(
                raw::BTRFS_BLOCK_GROUP_DATA
            )),
            Single
        );
        // DUP, RAID0, RAID1, RAID10, RAID5, RAID6.
        let cases = [
            (raw::BTRFS_BLOCK_GROUP_DUP, Dup),
            (raw::BTRFS_BLOCK_GROUP_RAID0, Raid0),
            (raw::BTRFS_BLOCK_GROUP_RAID1, Raid1),
            (raw::BTRFS_BLOCK_GROUP_RAID10, Raid10),
            (raw::BTRFS_BLOCK_GROUP_RAID5, Raid5),
            (raw::BTRFS_BLOCK_GROUP_RAID6, Raid6),
        ];
        for (bit, expected) in cases {
            let ct = u64::from(bit) | u64::from(raw::BTRFS_BLOCK_GROUP_DATA);
            assert_eq!(ChunkProfile::from_chunk_type(ct), expected);
        }
    }

    #[test]
    fn profile_from_chunk_type_raid1c3_and_c4() {
        let c3 = u64::from(raw::BTRFS_BLOCK_GROUP_RAID1C3);
        let c4 = u64::from(raw::BTRFS_BLOCK_GROUP_RAID1C4);
        assert_eq!(ChunkProfile::from_chunk_type(c3), ChunkProfile::Raid1);
        assert_eq!(ChunkProfile::from_chunk_type(c4), ChunkProfile::Raid1);
    }

    // --- plan_write / plan_read shared helpers ---

    /// Build a chunk mapping at logical 0 with the given profile and
    /// stripe layout.
    fn make_chunk(
        chunk_type_bit: u32,
        num_stripes: u16,
        sub_stripes: u16,
        stripe_len: u64,
        length: u64,
        stripes: &[(u64, u64)],
    ) -> ChunkMapping {
        ChunkMapping {
            logical: 0,
            length,
            stripe_len,
            chunk_type: u64::from(chunk_type_bit)
                | u64::from(raw::BTRFS_BLOCK_GROUP_DATA),
            num_stripes,
            sub_stripes,
            stripes: stripes
                .iter()
                .map(|&(devid, offset)| Stripe {
                    devid,
                    offset,
                    dev_uuid: Uuid::nil(),
                })
                .collect(),
        }
    }

    fn cache_with(mapping: ChunkMapping) -> ChunkTreeCache {
        let mut c = ChunkTreeCache::default();
        c.insert(mapping);
        c
    }

    // --- SINGLE ---

    #[test]
    fn plan_write_single_one_row() {
        let m = make_chunk(0, 1, 1, 65536, 1 << 20, &[(1, 0x1000)]);
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 4096).unwrap();
        assert_eq!(
            placements,
            vec![StripePlacement {
                devid: 1,
                physical: 0x1000,
                buf_offset: 0,
                len: 4096,
            }]
        );
    }

    #[test]
    fn plan_write_single_spans_multiple_rows() {
        // SINGLE with a 1 MiB chunk and stripe_len 64 KiB. A 96 KiB
        // write starting at offset 32 KiB spans two rows. Both go to
        // the same column, but the per-device offset advances as the
        // row index increases.
        let m = make_chunk(0, 1, 1, 65536, 1 << 20, &[(1, 0x10000)]);
        let cache = cache_with(m);
        let placements = cache.plan_write(32 * 1024, 96 * 1024).unwrap();
        assert_eq!(placements.len(), 2);
        // Row 0: offset 32K, 32K bytes (rest of stripe 0).
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000 + 32 * 1024);
        assert_eq!(placements[0].buf_offset, 0);
        assert_eq!(placements[0].len, 32 * 1024);
        // Row 1: 64K bytes, starting at the next stripe (stripe_nr=1, factor=1
        // so per_device_stripe_nr=1; physical = base + 64K).
        assert_eq!(placements[1].devid, 1);
        assert_eq!(placements[1].physical, 0x10000 + 65536);
        assert_eq!(placements[1].buf_offset, 32 * 1024);
        assert_eq!(placements[1].len, 64 * 1024);
    }

    // --- DUP / RAID1 mirroring ---

    #[test]
    fn plan_write_dup_writes_both_copies_same_buf_slice() {
        // DUP: 2 stripes both on devid 1 at distinct physicals. Same
        // buf_offset/len for each placement (identical bytes).
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_DUP,
            2,
            1,
            65536,
            1 << 20,
            &[(1, 0x1000), (1, 0x2_0000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(4096, 16384).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x1000 + 4096);
        assert_eq!(placements[0].buf_offset, 0);
        assert_eq!(placements[0].len, 16384);
        assert_eq!(placements[1].devid, 1);
        assert_eq!(placements[1].physical, 0x2_0000 + 4096);
        assert_eq!(placements[1].buf_offset, 0);
        assert_eq!(placements[1].len, 16384);
    }

    #[test]
    fn plan_write_raid1_writes_all_mirrors() {
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID1,
            2,
            1,
            65536,
            1 << 20,
            &[(1, 0x1000), (2, 0x2000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 8192).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[1].devid, 2);
        for p in &placements {
            assert_eq!(p.buf_offset, 0);
            assert_eq!(p.len, 8192);
        }
    }

    #[test]
    fn plan_write_raid1c3_writes_three_mirrors() {
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID1C3,
            3,
            1,
            65536,
            1 << 20,
            &[(1, 0x1000), (2, 0x2000), (3, 0x3000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 8192).unwrap();
        assert_eq!(placements.len(), 3);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[1].devid, 2);
        assert_eq!(placements[2].devid, 3);
    }

    #[test]
    fn plan_write_raid1c4_writes_four_mirrors() {
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID1C4,
            4,
            1,
            65536,
            1 << 20,
            &[(1, 0x1000), (2, 0x2000), (3, 0x3000), (4, 0x4000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 8192).unwrap();
        assert_eq!(placements.len(), 4);
        assert_eq!(placements[3].devid, 4);
    }

    // --- RAID0 striping ---

    #[test]
    fn plan_write_raid0_routes_first_row_to_column_zero() {
        // 2 devices, stripe_len=64K. Write at logical 0 of length 4K
        // goes to stripe[0].
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            2 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 4096).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000);
        assert_eq!(placements[0].len, 4096);
    }

    #[test]
    fn plan_write_raid0_second_row_routes_to_second_device() {
        // Same chunk, write at logical = STRIPE_LEN (row 1) goes to stripe[1].
        // The per-device stripe number is row 1 / factor(2) = 0; physical
        // = base + 0 = base.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            2 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(65536, 4096).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].devid, 2);
        assert_eq!(placements[0].physical, 0x20000);
    }

    #[test]
    fn plan_write_raid0_third_row_wraps_to_first_device() {
        // Row 2 (logical = 2 * STRIPE_LEN) wraps back to stripe[0],
        // but the per-device stripe number is 1 (advances on the device).
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            2 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(2 * 65536, 4096).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000 + 65536);
    }

    #[test]
    fn plan_write_raid0_spans_multiple_rows_round_robins_devices() {
        // 3 devices, stripe_len=64K. Write 192K starting at logical 0:
        // row 0 -> dev 1, row 1 -> dev 2, row 2 -> dev 3.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            3,
            1,
            65536,
            6 << 20,
            &[(1, 0x10000), (2, 0x20000), (3, 0x30000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 192 * 1024).unwrap();
        assert_eq!(placements.len(), 3);
        for (i, p) in placements.iter().enumerate() {
            assert_eq!(p.devid, (i + 1) as u64);
            assert_eq!(p.buf_offset, i * 65536);
            assert_eq!(p.len, 65536);
        }
        // All three rows are stripe_nr 0, 1, 2; per_device_stripe_nr = 0.
        for p in &placements {
            // Each device's base physical is 0x{i+1}_0000; per-device
            // offset within is 0.
            assert_eq!(p.physical & 0xFFFF, 0);
        }
    }

    #[test]
    fn plan_write_raid0_partial_first_row_then_full_then_partial() {
        // 2 devices, stripe_len 64K. Start mid-row: logical = 32K, len = 96K.
        // Row 0: dev 1, 32K..64K (32K bytes), buf 0..32K.
        // Row 1: dev 2, 0..64K (64K bytes), buf 32K..96K.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            2 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(32 * 1024, 96 * 1024).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000 + 32 * 1024);
        assert_eq!(placements[0].buf_offset, 0);
        assert_eq!(placements[0].len, 32 * 1024);
        assert_eq!(placements[1].devid, 2);
        assert_eq!(placements[1].physical, 0x20000);
        assert_eq!(placements[1].buf_offset, 32 * 1024);
        assert_eq!(placements[1].len, 64 * 1024);
    }

    // --- RAID10 striped mirrors ---

    #[test]
    fn plan_write_raid10_first_row_writes_pair_zero() {
        // 4 stripes, sub_stripes=2: 2 mirror pairs.
        // Row 0 -> pair 0 = stripes[0,1] = devs (1, 2).
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID10,
            4,
            2,
            65536,
            4 << 20,
            &[(1, 0x10000), (2, 0x20000), (3, 0x30000), (4, 0x40000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 4096).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000);
        assert_eq!(placements[1].devid, 2);
        assert_eq!(placements[1].physical, 0x20000);
        for p in &placements {
            assert_eq!(p.buf_offset, 0);
            assert_eq!(p.len, 4096);
        }
    }

    #[test]
    fn plan_write_raid10_second_row_writes_pair_one() {
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID10,
            4,
            2,
            65536,
            4 << 20,
            &[(1, 0x10000), (2, 0x20000), (3, 0x30000), (4, 0x40000)],
        );
        let cache = cache_with(m);
        // Row 1 (logical = STRIPE_LEN) -> pair 1 = stripes[2,3] = devs (3, 4).
        let placements = cache.plan_write(65536, 4096).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 3);
        assert_eq!(placements[1].devid, 4);
    }

    #[test]
    fn plan_write_raid10_wraps_after_factor_rows() {
        // 4-stripe RAID10: factor = 4/2 = 2. Row 2 wraps back to pair 0
        // but advances per-device offset.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID10,
            4,
            2,
            65536,
            4 << 20,
            &[(1, 0x10000), (2, 0x20000), (3, 0x30000), (4, 0x40000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(2 * 65536, 4096).unwrap();
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].devid, 1);
        assert_eq!(placements[0].physical, 0x10000 + 65536);
        assert_eq!(placements[1].devid, 2);
        assert_eq!(placements[1].physical, 0x20000 + 65536);
    }

    // --- plan_read ---

    #[test]
    fn plan_read_picks_first_mirror_per_row() {
        // RAID1 read: only one placement (the first mirror).
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID1,
            2,
            1,
            65536,
            1 << 20,
            &[(1, 0x1000), (2, 0x2000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_read(0, 8192).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].devid, 1);
    }

    #[test]
    fn plan_read_raid10_picks_first_mirror_per_row() {
        // RAID10 spanning two rows: 2 placements, each on one device only.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID10,
            4,
            2,
            65536,
            4 << 20,
            &[(1, 0x10000), (2, 0x20000), (3, 0x30000), (4, 0x40000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_read(0, 128 * 1024).unwrap();
        assert_eq!(placements.len(), 2);
        // Row 0 mirror pair (1,2) -> first = dev 1.
        assert_eq!(placements[0].devid, 1);
        // Row 1 mirror pair (3,4) -> first = dev 3.
        assert_eq!(placements[1].devid, 3);
    }

    #[test]
    fn plan_read_raid0_matches_plan_write() {
        // Read planning for a striped profile is identical to write
        // planning (RAID0 has only one column per row anyway).
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            2 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let r = cache.plan_read(32 * 1024, 96 * 1024).unwrap();
        let w = cache.plan_write(32 * 1024, 96 * 1024).unwrap();
        assert_eq!(r, w);
    }

    // --- Bounds and error cases ---

    #[test]
    fn plan_write_out_of_chunk_returns_none() {
        let m = make_chunk(0, 1, 1, 65536, 1 << 20, &[(1, 0)]);
        let cache = cache_with(m);
        // Request straddles the chunk end.
        assert!(cache.plan_write((1 << 20) - 4096, 8192).is_none());
        // Request entirely past the chunk.
        assert!(cache.plan_write(2 << 20, 4096).is_none());
        // Empty request still succeeds (zero placements).
        let p = cache.plan_write(0, 0).unwrap();
        assert!(p.is_empty());
    }

    #[test]
    fn plan_write_unmapped_returns_none() {
        let cache = ChunkTreeCache::default();
        assert!(cache.plan_write(0, 4096).is_none());
    }

    #[test]
    fn plan_write_raid5_returns_none() {
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID5,
            3,
            1,
            65536,
            3 << 20,
            &[(1, 0), (2, 0), (3, 0)],
        );
        let cache = cache_with(m);
        // RAID5 not implemented — must fail explicitly so callers know.
        assert!(cache.plan_write(0, 4096).is_none());
    }

    #[test]
    fn plan_write_buf_offsets_cover_request_exactly() {
        // For a striped (no-mirror) profile, the sum of placement
        // lengths should equal the request length and the buf_offsets
        // should tile [0, len) without gaps.
        let m = make_chunk(
            raw::BTRFS_BLOCK_GROUP_RAID0,
            2,
            1,
            65536,
            4 << 20,
            &[(1, 0x10000), (2, 0x20000)],
        );
        let cache = cache_with(m);
        let placements = cache.plan_write(0, 256 * 1024).unwrap();
        let total: usize = placements.iter().map(|p| p.len).sum();
        assert_eq!(total, 256 * 1024);
        // Check buf_offsets tile contiguously.
        let mut next = 0;
        for p in &placements {
            assert_eq!(p.buf_offset, next);
            next += p.len;
        }
        assert_eq!(next, 256 * 1024);
    }
}

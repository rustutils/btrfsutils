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
    /// index calculation, but for tree blocks (always nodesize ≤ stripe_len)
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
}

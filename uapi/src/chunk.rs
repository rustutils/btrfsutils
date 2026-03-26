//! # Per-device physical allocation data from the chunk tree
//!
//! Walks the chunk tree to determine how many bytes each device has allocated,
//! broken down by block-group profile flags.  This is the data source for the
//! per-device breakdown in `btrfs filesystem usage`.
//!
//! Also exposes the full per-stripe chunk list used by `inspect-internal
//! list-chunks`, including the bytes-used figure from the extent tree.
//!
//! Requires `CAP_SYS_ADMIN`.

use crate::{
    field_size,
    raw::{
        BTRFS_BLOCK_GROUP_ITEM_KEY, BTRFS_CHUNK_ITEM_KEY, BTRFS_CHUNK_TREE_OBJECTID,
        BTRFS_EXTENT_TREE_OBJECTID, BTRFS_FIRST_CHUNK_TREE_OBJECTID, btrfs_block_group_item,
        btrfs_chunk, btrfs_stripe,
    },
    space::BlockGroupFlags,
    tree_search::{SearchKey, tree_search},
};
use std::os::unix::io::BorrowedFd;

/// Physical allocation of one block-group profile on one device, as read
/// from the chunk tree.
///
/// `bytes` is the sum of `stripe_len` over all chunk stripes that land on
/// `devid` and share the same `flags`.  This is the physical space the device
/// contributes to that profile, not the logical (usable) space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceAllocation {
    /// btrfs device ID.
    pub devid: u64,
    /// Combined block-group type and profile flags, e.g.
    /// `BlockGroupFlags::DATA | BlockGroupFlags::RAID1`.
    pub flags: BlockGroupFlags,
    /// Physical bytes allocated on this device for chunks with these flags.
    pub bytes: u64,
}

const CHUNK_LENGTH_OFF: usize = std::mem::offset_of!(btrfs_chunk, length);
const CHUNK_STRIPE_LEN_OFF: usize = std::mem::offset_of!(btrfs_chunk, stripe_len);
const CHUNK_TYPE_OFF: usize = std::mem::offset_of!(btrfs_chunk, type_);
const CHUNK_NUM_STRIPES_OFF: usize = std::mem::offset_of!(btrfs_chunk, num_stripes);
const CHUNK_FIRST_STRIPE_OFF: usize = std::mem::offset_of!(btrfs_chunk, stripe);

const STRIPE_SIZE: usize = std::mem::size_of::<btrfs_stripe>();
const STRIPE_DEVID_OFF: usize = std::mem::offset_of!(btrfs_stripe, devid);
const STRIPE_OFFSET_OFF: usize = std::mem::offset_of!(btrfs_stripe, offset);

// Minimum item length: the btrfs_chunk struct with exactly one stripe.
const CHUNK_MIN_LEN: usize = CHUNK_FIRST_STRIPE_OFF + STRIPE_SIZE; // 80

/// One physical chunk stripe as seen in the chunk tree, with usage data from
/// the extent tree.
///
/// For striped profiles (RAID0, RAID10, …) each logical chunk maps to
/// multiple stripes on different devices; each stripe yields one `ChunkEntry`.
/// For non-striped profiles (single, DUP) there is one `ChunkEntry` per chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkEntry {
    /// btrfs device ID that holds this stripe.
    pub devid: u64,
    /// Physical byte offset of this stripe on the device.
    pub physical_start: u64,
    /// Logical byte offset of the chunk within the filesystem address space.
    pub logical_start: u64,
    /// Logical length of the chunk in bytes (shared across all stripes of
    /// the same chunk).
    pub length: u64,
    /// Combined block-group type and profile flags.
    pub flags: BlockGroupFlags,
    /// Bytes currently used within this chunk, as reported by the extent tree.
    /// `0` if the block-group item could not be read.
    pub used: u64,
}

/// Walk the chunk tree of the filesystem referred to by `fd` and return the
/// physical allocation of each block-group profile on each device.
///
/// The result may contain multiple entries with the same `devid` when a
/// device participates in chunks of different profiles (e.g. both
/// `DATA|SINGLE` and `METADATA|DUP`).  Entries with the same `(devid, flags)`
/// pair are merged — there will be at most one entry per unique pair.
///
/// Internally, each `BTRFS_CHUNK_ITEM_KEY` payload is a packed `btrfs_chunk`
/// struct followed by `num_stripes - 1` additional `btrfs_stripe` structs.
/// The `stripe_len` field of each stripe is accumulated per `(devid, flags)`
/// to produce the physical byte counts in the returned list.
pub fn device_chunk_allocations(fd: BorrowedFd) -> nix::Result<Vec<DeviceAllocation>> {
    let mut allocs: Vec<DeviceAllocation> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_type(
            BTRFS_CHUNK_TREE_OBJECTID as u64,
            BTRFS_CHUNK_ITEM_KEY as u32,
        ),
        |_hdr, data| {
            if let Some((stripe_len, flags, stripes)) = parse_chunk(data) {
                for devid in stripes {
                    accumulate(&mut allocs, devid, flags, stripe_len);
                }
            }
            Ok(())
        },
    )?;

    Ok(allocs)
}

/// Walk the chunk tree and return one [`ChunkEntry`] per stripe, including
/// bytes-used from the extent tree.
///
/// The returned list is in chunk-tree order (ascending logical offset); call
/// sites are responsible for any further sorting.  For each logical chunk the
/// `used` field is populated by a single extent-tree lookup; if that lookup
/// fails the field is set to `0` rather than propagating an error.
///
/// Requires `CAP_SYS_ADMIN`.
pub fn chunk_list(fd: BorrowedFd) -> nix::Result<Vec<ChunkEntry>> {
    let mut entries: Vec<ChunkEntry> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            BTRFS_CHUNK_TREE_OBJECTID as u64,
            BTRFS_CHUNK_ITEM_KEY as u32,
            BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        ),
        |hdr, data| {
            if let Some(stripes) = parse_chunk_stripes(data) {
                let logical_start = hdr.offset;
                let length = read_le_u64(data, CHUNK_LENGTH_OFF);
                let type_bits = read_le_u64(data, CHUNK_TYPE_OFF);
                let flags = BlockGroupFlags::from_bits_truncate(type_bits);
                let used = block_group_used(fd, logical_start).unwrap_or(0);
                for (devid, physical_start) in stripes {
                    entries.push(ChunkEntry {
                        devid,
                        physical_start,
                        logical_start,
                        length,
                        flags,
                        used,
                    });
                }
            }
            Ok(())
        },
    )?;

    Ok(entries)
}

/// Look up the bytes-used counter for the block group at `logical_start` by
/// searching for `BTRFS_BLOCK_GROUP_ITEM_KEY` in the extent tree.
///
/// Returns `None` if the block group item is not found or cannot be read.
fn block_group_used(fd: BorrowedFd, logical_start: u64) -> Option<u64> {
    let mut used: Option<u64> = None;
    tree_search(
        fd,
        SearchKey {
            tree_id: BTRFS_EXTENT_TREE_OBJECTID as u64,
            min_objectid: logical_start,
            max_objectid: logical_start,
            min_type: BTRFS_BLOCK_GROUP_ITEM_KEY,
            max_type: BTRFS_BLOCK_GROUP_ITEM_KEY,
            min_offset: 0,
            max_offset: u64::MAX,
            min_transid: 0,
            max_transid: u64::MAX,
        },
        |_hdr, data| {
            let used_off = std::mem::offset_of!(btrfs_block_group_item, used);
            if data.len() >= used_off + field_size!(btrfs_block_group_item, used) {
                used = Some(read_le_u64(data, used_off));
            }
            Ok(())
        },
    )
    .ok()?;
    used
}

/// Parse a raw chunk item payload.
///
/// Returns `(stripe_len, flags, devids)` on success, or `None` if the buffer
/// is too small to be a valid chunk item.
fn parse_chunk(data: &[u8]) -> Option<(u64, BlockGroupFlags, impl Iterator<Item = u64> + '_)> {
    if data.len() < CHUNK_MIN_LEN {
        return None;
    }

    let stripe_len = read_le_u64(data, CHUNK_STRIPE_LEN_OFF);
    let type_bits = read_le_u64(data, CHUNK_TYPE_OFF);
    let num_stripes = read_le_u16(data, CHUNK_NUM_STRIPES_OFF) as usize;
    let _length = read_le_u64(data, CHUNK_LENGTH_OFF);

    // Sanity-check: the item must be large enough to hold all stripes.
    let expected_len = CHUNK_FIRST_STRIPE_OFF + num_stripes * STRIPE_SIZE;
    if data.len() < expected_len || num_stripes == 0 {
        return None;
    }

    let flags = BlockGroupFlags::from_bits_truncate(type_bits);

    let devids = (0..num_stripes).map(move |i| {
        let stripe_off = CHUNK_FIRST_STRIPE_OFF + i * STRIPE_SIZE;
        read_le_u64(data, stripe_off + STRIPE_DEVID_OFF)
    });

    Some((stripe_len, flags, devids))
}

/// Parse a raw chunk item payload and return an iterator of `(devid,
/// physical_start)` pairs for each stripe.
///
/// Returns `None` if the buffer is too small to be a valid chunk item.
fn parse_chunk_stripes(data: &[u8]) -> Option<impl Iterator<Item = (u64, u64)> + '_> {
    if data.len() < CHUNK_MIN_LEN {
        return None;
    }

    let num_stripes = read_le_u16(data, CHUNK_NUM_STRIPES_OFF) as usize;
    let expected_len = CHUNK_FIRST_STRIPE_OFF + num_stripes * STRIPE_SIZE;
    if data.len() < expected_len || num_stripes == 0 {
        return None;
    }

    let iter = (0..num_stripes).map(move |i| {
        let stripe_off = CHUNK_FIRST_STRIPE_OFF + i * STRIPE_SIZE;
        let devid = read_le_u64(data, stripe_off + STRIPE_DEVID_OFF);
        let physical_start = read_le_u64(data, stripe_off + STRIPE_OFFSET_OFF);
        (devid, physical_start)
    });

    Some(iter)
}

/// Add `stripe_len` bytes to the `(devid, flags)` entry, creating it if
/// it does not yet exist.
fn accumulate(allocs: &mut Vec<DeviceAllocation>, devid: u64, flags: BlockGroupFlags, bytes: u64) {
    if let Some(entry) = allocs
        .iter_mut()
        .find(|a| a.devid == devid && a.flags == flags)
    {
        entry.bytes += bytes;
    } else {
        allocs.push(DeviceAllocation {
            devid,
            flags,
            bytes,
        });
    }
}

fn read_le_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_le_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid single-stripe chunk item buffer.
    fn build_chunk_buf(
        length: u64,
        stripe_len: u64,
        type_bits: u64,
        num_stripes: u16,
        stripes: &[(u64, u64)], // (devid, offset) per stripe
    ) -> Vec<u8> {
        let total = CHUNK_FIRST_STRIPE_OFF + stripes.len() * STRIPE_SIZE;
        let mut buf = vec![0u8; total];
        buf[CHUNK_LENGTH_OFF..CHUNK_LENGTH_OFF + 8].copy_from_slice(&length.to_le_bytes());
        buf[CHUNK_STRIPE_LEN_OFF..CHUNK_STRIPE_LEN_OFF + 8]
            .copy_from_slice(&stripe_len.to_le_bytes());
        buf[CHUNK_TYPE_OFF..CHUNK_TYPE_OFF + 8].copy_from_slice(&type_bits.to_le_bytes());
        buf[CHUNK_NUM_STRIPES_OFF..CHUNK_NUM_STRIPES_OFF + 2]
            .copy_from_slice(&num_stripes.to_le_bytes());
        for (i, &(devid, offset)) in stripes.iter().enumerate() {
            let s = CHUNK_FIRST_STRIPE_OFF + i * STRIPE_SIZE;
            buf[s + STRIPE_DEVID_OFF..s + STRIPE_DEVID_OFF + 8]
                .copy_from_slice(&devid.to_le_bytes());
            buf[s + STRIPE_OFFSET_OFF..s + STRIPE_OFFSET_OFF + 8]
                .copy_from_slice(&offset.to_le_bytes());
        }
        buf
    }

    // --- read_le_u64 / read_le_u16 ---

    #[test]
    fn read_le_u64_basic() {
        let buf = 0x0102030405060708u64.to_le_bytes();
        assert_eq!(read_le_u64(&buf, 0), 0x0102030405060708);
    }

    #[test]
    fn read_le_u16_basic() {
        let buf = 0x0102u16.to_le_bytes();
        assert_eq!(read_le_u16(&buf, 0), 0x0102);
    }

    // --- parse_chunk ---

    #[test]
    fn parse_chunk_single_stripe() {
        let data_flags = BlockGroupFlags::DATA.bits();
        let buf = build_chunk_buf(1024 * 1024, 65536, data_flags, 1, &[(1, 0)]);
        let (stripe_len, flags, devids) = parse_chunk(&buf).unwrap();
        assert_eq!(stripe_len, 65536);
        assert_eq!(flags, BlockGroupFlags::DATA);
        let devids: Vec<u64> = devids.collect();
        assert_eq!(devids, vec![1]);
    }

    #[test]
    fn parse_chunk_two_stripes() {
        let flags_bits = (BlockGroupFlags::DATA | BlockGroupFlags::RAID1).bits();
        let buf = build_chunk_buf(1 << 30, 1 << 30, flags_bits, 2, &[(1, 0), (2, 4096)]);
        let (_, flags, devids) = parse_chunk(&buf).unwrap();
        assert_eq!(flags, BlockGroupFlags::DATA | BlockGroupFlags::RAID1);
        let devids: Vec<u64> = devids.collect();
        assert_eq!(devids, vec![1, 2]);
    }

    #[test]
    fn parse_chunk_too_short() {
        let buf = vec![0u8; CHUNK_MIN_LEN - 1];
        assert!(parse_chunk(&buf).is_none());
    }

    #[test]
    fn parse_chunk_zero_stripes() {
        // num_stripes = 0 is invalid
        let buf = build_chunk_buf(1024, 1024, 0, 0, &[]);
        // buf is only CHUNK_FIRST_STRIPE_OFF bytes, but num_stripes says 0
        // which means expected_len = CHUNK_FIRST_STRIPE_OFF + 0*STRIPE_SIZE
        // but the function also checks num_stripes == 0
        let mut padded = vec![0u8; CHUNK_MIN_LEN];
        padded[..buf.len().min(CHUNK_MIN_LEN)]
            .copy_from_slice(&buf[..buf.len().min(CHUNK_MIN_LEN)]);
        padded[CHUNK_NUM_STRIPES_OFF..CHUNK_NUM_STRIPES_OFF + 2]
            .copy_from_slice(&0u16.to_le_bytes());
        assert!(parse_chunk(&padded).is_none());
    }

    #[test]
    fn parse_chunk_claims_more_stripes_than_fit() {
        // num_stripes says 5 but buffer only has room for 1
        let buf = build_chunk_buf(1024, 1024, 0, 5, &[(1, 0)]);
        assert!(parse_chunk(&buf).is_none());
    }

    // --- parse_chunk_stripes ---

    #[test]
    fn parse_chunk_stripes_returns_devid_and_offset() {
        let buf = build_chunk_buf(1 << 20, 1 << 20, 0, 2, &[(3, 8192), (7, 16384)]);
        let stripes: Vec<(u64, u64)> = parse_chunk_stripes(&buf).unwrap().collect();
        assert_eq!(stripes, vec![(3, 8192), (7, 16384)]);
    }

    #[test]
    fn parse_chunk_stripes_too_short() {
        let buf = vec![0u8; 10];
        assert!(parse_chunk_stripes(&buf).is_none());
    }

    // --- accumulate ---

    #[test]
    fn accumulate_new_entry() {
        let mut allocs = Vec::new();
        accumulate(&mut allocs, 1, BlockGroupFlags::DATA, 1000);
        assert_eq!(allocs.len(), 1);
        assert_eq!(allocs[0].devid, 1);
        assert_eq!(allocs[0].bytes, 1000);
    }

    #[test]
    fn accumulate_merge_same_devid_flags() {
        let mut allocs = Vec::new();
        accumulate(&mut allocs, 1, BlockGroupFlags::DATA, 1000);
        accumulate(&mut allocs, 1, BlockGroupFlags::DATA, 2000);
        assert_eq!(allocs.len(), 1);
        assert_eq!(allocs[0].bytes, 3000);
    }

    #[test]
    fn accumulate_separate_different_flags() {
        let mut allocs = Vec::new();
        accumulate(&mut allocs, 1, BlockGroupFlags::DATA, 1000);
        accumulate(&mut allocs, 1, BlockGroupFlags::METADATA, 2000);
        assert_eq!(allocs.len(), 2);
    }

    #[test]
    fn accumulate_separate_different_devids() {
        let mut allocs = Vec::new();
        accumulate(&mut allocs, 1, BlockGroupFlags::DATA, 1000);
        accumulate(&mut allocs, 2, BlockGroupFlags::DATA, 2000);
        assert_eq!(allocs.len(), 2);
    }
}

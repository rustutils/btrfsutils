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
    raw::{
        BTRFS_BLOCK_GROUP_ITEM_KEY, BTRFS_CHUNK_ITEM_KEY,
        BTRFS_CHUNK_TREE_OBJECTID, BTRFS_EXTENT_TREE_OBJECTID,
        BTRFS_FIRST_CHUNK_TREE_OBJECTID,
    },
    space::BlockGroupFlags,
    tree_search::{SearchKey, tree_search},
};
use btrfs_disk::items::ChunkItem;
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
///
/// # Errors
///
/// Returns `Err` if the tree search ioctl fails.
pub fn device_chunk_allocations(
    fd: BorrowedFd,
) -> nix::Result<Vec<DeviceAllocation>> {
    let mut allocs: Vec<DeviceAllocation> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_type(
            u64::from(BTRFS_CHUNK_TREE_OBJECTID),
            BTRFS_CHUNK_ITEM_KEY,
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
///
/// # Errors
///
/// Returns `Err` if the tree search ioctl fails.
pub fn chunk_list(fd: BorrowedFd) -> nix::Result<Vec<ChunkEntry>> {
    let mut entries: Vec<ChunkEntry> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            u64::from(BTRFS_CHUNK_TREE_OBJECTID),
            BTRFS_CHUNK_ITEM_KEY,
            u64::from(BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        ),
        |hdr, data| {
            if let Some(chunk) = ChunkItem::parse(data) {
                let logical_start = hdr.offset;
                let flags = BlockGroupFlags::from_bits_truncate(
                    chunk.chunk_type.bits(),
                );
                let used = block_group_used(fd, logical_start).unwrap_or(0);
                for stripe in &chunk.stripes {
                    entries.push(ChunkEntry {
                        devid: stripe.devid,
                        physical_start: stripe.offset,
                        logical_start,
                        length: chunk.length,
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
            tree_id: u64::from(BTRFS_EXTENT_TREE_OBJECTID),
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
            if let Some(bg) = btrfs_disk::items::BlockGroupItem::parse(data) {
                used = Some(bg.used);
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
fn parse_chunk(data: &[u8]) -> Option<(u64, BlockGroupFlags, Vec<u64>)> {
    let chunk = ChunkItem::parse(data)?;
    let flags = BlockGroupFlags::from_bits_truncate(chunk.chunk_type.bits());
    let devids: Vec<u64> = chunk.stripes.iter().map(|s| s.devid).collect();
    Some((chunk.stripe_len, flags, devids))
}

/// Add `stripe_len` bytes to the `(devid, flags)` entry, creating it if
/// it does not yet exist.
fn accumulate(
    allocs: &mut Vec<DeviceAllocation>,
    devid: u64,
    flags: BlockGroupFlags,
    bytes: u64,
) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid chunk item buffer matching the on-disk layout
    /// that `ChunkItem::parse` expects (sequential LE fields).
    fn build_chunk_buf(
        length: u64,
        stripe_len: u64,
        type_bits: u64,
        num_stripes: u16,
        stripes: &[(u64, u64)], // (devid, offset) per stripe
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // owner
        buf.extend_from_slice(&stripe_len.to_le_bytes());
        buf.extend_from_slice(&type_bits.to_le_bytes()); // chunk_type
        buf.extend_from_slice(&4096u32.to_le_bytes()); // io_align
        buf.extend_from_slice(&4096u32.to_le_bytes()); // io_width
        buf.extend_from_slice(&4096u32.to_le_bytes()); // sector_size
        buf.extend_from_slice(&num_stripes.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // sub_stripes
        for &(devid, offset) in stripes {
            buf.extend_from_slice(&devid.to_le_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&[0u8; 16]); // dev_uuid
        }
        buf
    }

    #[test]
    fn parse_chunk_single_stripe() {
        let data_flags = BlockGroupFlags::DATA.bits();
        let buf = build_chunk_buf(1024 * 1024, 65536, data_flags, 1, &[(1, 0)]);
        let (stripe_len, flags, devids) = parse_chunk(&buf).unwrap();
        assert_eq!(stripe_len, 65536);
        assert_eq!(flags, BlockGroupFlags::DATA);
        assert_eq!(devids, vec![1]);
    }

    #[test]
    fn parse_chunk_two_stripes() {
        let flags_bits =
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1).bits();
        let buf = build_chunk_buf(
            1 << 30,
            1 << 30,
            flags_bits,
            2,
            &[(1, 0), (2, 4096)],
        );
        let (_, flags, devids) = parse_chunk(&buf).unwrap();
        assert_eq!(flags, BlockGroupFlags::DATA | BlockGroupFlags::RAID1);
        assert_eq!(devids, vec![1, 2]);
    }

    #[test]
    fn parse_chunk_too_short() {
        let buf = vec![0u8; 10];
        assert!(parse_chunk(&buf).is_none());
    }

    #[test]
    fn parse_chunk_claims_more_stripes_than_fit() {
        // num_stripes says 5 but buffer only has room for 1
        let buf = build_chunk_buf(1024, 1024, 0, 5, &[(1, 0)]);
        // ChunkItem::parse will parse only as many stripes as fit
        let result = parse_chunk(&buf);
        assert!(result.is_some());
        let (_, _, devids) = result.unwrap();
        assert_eq!(devids.len(), 1);
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

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

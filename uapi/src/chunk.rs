//! Per-device physical allocation data from the chunk tree.
//!
//! Walks `BTRFS_CHUNK_ITEM_KEY` entries in the chunk tree and aggregates the
//! physical bytes each device has allocated, broken down by block-group
//! profile flags.  This is the data source for the per-device breakdown in
//! `btrfs filesystem usage`.
//!
//! # Chunk layout in the search buffer
//!
//! A `CHUNK_ITEM_KEY` item payload is a packed `btrfs_chunk` struct (80 bytes)
//! immediately followed by `num_stripes - 1` additional `btrfs_stripe` structs
//! (32 bytes each).  The first stripe is embedded as the last field of
//! `btrfs_chunk`.  All multi-byte fields are **little-endian**.
//!
//! ```text
//! btrfs_chunk (80 bytes):
//!   [0..8]   length       LE u64  — logical size of the chunk
//!   [8..16]  owner        LE u64
//!   [16..24] stripe_len   LE u64  — physical size of each stripe on-device
//!   [24..32] type_        LE u64  — BlockGroupFlags (type + profile bits)
//!   [32..36] io_align     LE u32
//!   [36..40] io_width     LE u32
//!   [40..44] sector_size  LE u32
//!   [44..46] num_stripes  LE u16
//!   [46..48] sub_stripes  LE u16
//!   [48..80] stripe[0]    btrfs_stripe (32 bytes)
//!
//! btrfs_stripe (32 bytes):
//!   [0..8]   devid        LE u64
//!   [8..16]  offset       LE u64  — physical start on the device
//!   [16..32] dev_uuid     [u8; 16]
//! ```

use std::os::unix::io::BorrowedFd;

use crate::{
    raw::{BTRFS_CHUNK_ITEM_KEY, BTRFS_CHUNK_TREE_OBJECTID},
    space::BlockGroupFlags,
    tree_search::{SearchKey, tree_search},
};

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

const CHUNK_LENGTH_OFF: usize = 0;
const CHUNK_STRIPE_LEN_OFF: usize = 16;
const CHUNK_TYPE_OFF: usize = 24;
const CHUNK_NUM_STRIPES_OFF: usize = 44;
const CHUNK_FIRST_STRIPE_OFF: usize = 48;

const STRIPE_SIZE: usize = 32;
const STRIPE_DEVID_OFF: usize = 0;

// Minimum item length: the btrfs_chunk struct with exactly one stripe.
const CHUNK_MIN_LEN: usize = CHUNK_FIRST_STRIPE_OFF + STRIPE_SIZE; // 80

/// Walk the chunk tree of the filesystem referred to by `fd` and return the
/// physical allocation of each block-group profile on each device.
///
/// The result may contain multiple entries with the same `devid` when a
/// device participates in chunks of different profiles (e.g. both
/// `DATA|SINGLE` and `METADATA|DUP`).  Entries with the same `(devid, flags)`
/// pair are merged — there will be at most one entry per unique pair.
///
/// Requires `CAP_SYS_ADMIN`.
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

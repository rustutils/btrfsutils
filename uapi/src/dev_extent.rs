//! # Device extent tree: per-device physical extent layout
//!
//! Walks the device tree (`BTRFS_DEV_TREE_OBJECTID`) to enumerate
//! `BTRFS_DEV_EXTENT_KEY` items for a given device, then computes the
//! minimum size to which the device can be shrunk.

use crate::{
    field_size,
    raw::{BTRFS_DEV_EXTENT_KEY, BTRFS_DEV_TREE_OBJECTID, btrfs_dev_extent},
    tree_search::{SearchKey, tree_search},
};
use std::os::unix::io::BorrowedFd;

const DEV_EXTENT_LENGTH_OFF: usize = std::mem::offset_of!(btrfs_dev_extent, length);

const SZ_1M: u64 = 1024 * 1024;
const SZ_32M: u64 = 32 * 1024 * 1024;

/// Number of superblock mirror copies btrfs maintains.
const BTRFS_SUPER_MIRROR_MAX: usize = 3;

/// Return the byte offset of superblock mirror `i`.
///
/// Mirror 0 is at 64 KiB, mirror 1 at 64 MiB, mirror 2 at 256 GiB.
fn sb_offset(i: usize) -> u64 {
    match i {
        0 => 64 * 1024,
        _ => 1u64 << (20 + 10 * (i as u64)),
    }
}

/// A contiguous physical byte range on a device (inclusive end).
#[derive(Debug, Clone, Copy)]
struct Extent {
    start: u64,
    /// Inclusive end byte.
    end: u64,
}

/// Compute the minimum size to which device `devid` can be shrunk.
///
/// Walks the device tree for all `DEV_EXTENT_KEY` items belonging to
/// `devid`, sums their lengths, then adjusts for extents that sit beyond
/// the sum by checking whether they can be relocated into holes closer to
/// the start of the device. The algorithm matches `btrfs inspect-internal
/// min-dev-size` from btrfs-progs.
///
/// Requires `CAP_SYS_ADMIN`.
pub fn min_dev_size(fd: BorrowedFd, devid: u64) -> nix::Result<u64> {
    let mut min_size: u64 = SZ_1M;
    let mut extents: Vec<Extent> = Vec::new();
    let mut holes: Vec<Extent> = Vec::new();
    let mut last_pos: Option<u64> = None;

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            BTRFS_DEV_TREE_OBJECTID as u64,
            BTRFS_DEV_EXTENT_KEY,
            devid,
            devid,
        ),
        |hdr, data| {
            if data.len() < DEV_EXTENT_LENGTH_OFF + field_size!(btrfs_dev_extent, length) {
                return Ok(());
            }
            let phys_start = hdr.offset;
            let len = read_le_u64(data, DEV_EXTENT_LENGTH_OFF);

            min_size += len;

            // Extents are prepended (descending end offset) so that the
            // adjustment pass processes the highest-addressed extent first.
            extents.push(Extent {
                start: phys_start,
                end: phys_start + len - 1,
            });

            if let Some(prev_end) = last_pos {
                if prev_end != phys_start {
                    holes.push(Extent {
                        start: prev_end,
                        end: phys_start - 1,
                    });
                }
            }

            last_pos = Some(phys_start + len);
            Ok(())
        },
    )?;

    // Sort extents by descending end offset for the adjustment pass.
    extents.sort_by(|a, b| b.end.cmp(&a.end));

    adjust_min_size(&mut extents, &mut holes, &mut min_size);

    Ok(min_size)
}

/// Check whether a byte range `[start, end]` contains a superblock mirror.
fn hole_includes_sb_mirror(start: u64, end: u64) -> bool {
    (0..BTRFS_SUPER_MIRROR_MAX).any(|i| {
        let bytenr = sb_offset(i);
        bytenr >= start && bytenr <= end
    })
}

/// Adjust `min_size` downward by relocating tail extents into holes.
///
/// Processes extents in descending order of end offset. If an extent sits
/// beyond the current `min_size`, try to find a hole large enough to
/// relocate it. If no hole fits, the device cannot be shrunk past that
/// extent and `min_size` is set to its end + 1.
///
/// Adds scratch space (largest relocated extent + 32 MiB for a potential
/// system chunk allocation) when any relocation is needed.
fn adjust_min_size(extents: &mut Vec<Extent>, holes: &mut Vec<Extent>, min_size: &mut u64) {
    let mut scratch_space: u64 = 0;

    while let Some(&ext) = extents.first() {
        if ext.end < *min_size {
            break;
        }

        let extent_len = ext.end - ext.start + 1;

        // Find the first hole large enough to hold this extent.
        let hole_idx = holes.iter().position(|h| {
            let hole_len = h.end - h.start + 1;
            hole_len >= extent_len
        });

        let Some(idx) = hole_idx else {
            *min_size = ext.end + 1;
            break;
        };

        // If the target hole contains a superblock mirror location,
        // pessimistically assume we need one more extent worth of space.
        if hole_includes_sb_mirror(holes[idx].start, holes[idx].start + extent_len - 1) {
            *min_size += extent_len;
        }

        // Shrink or remove the hole.
        let hole_len = holes[idx].end - holes[idx].start + 1;
        if hole_len > extent_len {
            holes[idx].start += extent_len;
        } else {
            holes.remove(idx);
        }

        extents.remove(0);

        if extent_len > scratch_space {
            scratch_space = extent_len;
        }
    }

    if scratch_space > 0 {
        *min_size += scratch_space;
        // Chunk allocation may require a new system chunk (up to 32 MiB).
        *min_size += SZ_32M;
    }
}

fn read_le_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

//! # Block group space usage: how much space each chunk type allocates and uses
//!
//! Reports the allocated and used byte counts for each combination of block
//! group type (data, metadata, system) and RAID profile.  This is the data
//! underlying the `btrfs filesystem df` command.

use crate::raw::{
    btrfs_ioc_space_info, btrfs_ioctl_space_args, btrfs_ioctl_space_info,
};
pub use btrfs_disk::items::BlockGroupFlags;
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

/// Space usage information for one block group type/profile combination.
///
/// Returned by [`space_info`]. The `flags` field describes the chunk type and
/// RAID profile; `total_bytes` and `used_bytes` are the allocated and in-use
/// byte counts respectively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceInfo {
    /// Block group type and RAID profile flags for this space category.
    pub flags: BlockGroupFlags,
    /// Total bytes allocated to block groups of this type/profile.
    pub total_bytes: u64,
    /// Bytes actually in use within those block groups.
    pub used_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_bits_preserves_known_flags() {
        let raw = BlockGroupFlags::DATA.bits() | BlockGroupFlags::RAID1.bits();
        let flags = BlockGroupFlags::from_bits_truncate(raw);
        assert!(flags.contains(BlockGroupFlags::DATA));
        assert!(flags.contains(BlockGroupFlags::RAID1));
    }
}

impl From<btrfs_ioctl_space_info> for SpaceInfo {
    fn from(raw: btrfs_ioctl_space_info) -> Self {
        Self {
            flags: BlockGroupFlags::from_bits_truncate(raw.flags),
            total_bytes: raw.total_bytes,
            used_bytes: raw.used_bytes,
        }
    }
}

/// Query space usage by block group type for the filesystem referred to by
/// `fd`.
///
/// Returns one [`SpaceInfo`] entry per block group type/profile combination.
///
/// Uses a two-phase ioctl call: the first call with `space_slots = 0`
/// retrieves the entry count, and the second call retrieves all entries.
/// The entry count can change between calls if the kernel allocates new
/// block groups concurrently; this is benign (the kernel fills at most
/// `space_slots` entries and the second call will simply return fewer
/// than expected).
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
#[allow(clippy::cast_possible_truncation)] // space count always fits in usize
pub fn space_info(fd: BorrowedFd) -> nix::Result<Vec<SpaceInfo>> {
    // Phase 1: query with space_slots = 0 to discover the number of entries.
    let mut args: btrfs_ioctl_space_args = unsafe { mem::zeroed() };
    unsafe { btrfs_ioc_space_info(fd.as_raw_fd(), &raw mut args) }?;
    let count = args.total_spaces as usize;

    if count == 0 {
        return Ok(Vec::new());
    }

    // Phase 2: allocate a buffer large enough to hold the header plus all
    // entries, then call again with space_slots set to the count.
    //
    // We use Vec<u64> rather than Vec<u8> to guarantee 8-byte alignment,
    // matching the alignment requirement of btrfs_ioctl_space_args.
    let base_size = mem::size_of::<btrfs_ioctl_space_args>();
    let info_size = mem::size_of::<btrfs_ioctl_space_info>();
    let total_bytes = base_size + count * info_size;
    let num_u64s = total_bytes.div_ceil(mem::size_of::<u64>());
    let mut buf = vec![0u64; num_u64s];

    // SAFETY: buf is correctly sized and aligned for btrfs_ioctl_space_args.
    // We write space_slots before the ioctl and read spaces[] only after the
    // ioctl has populated them, keeping everything within the allocation.
    unsafe {
        let args_ptr = buf.as_mut_ptr().cast::<btrfs_ioctl_space_args>();
        (*args_ptr).space_slots = count as u64;
        btrfs_ioc_space_info(fd.as_raw_fd(), &raw mut *args_ptr)?;
        Ok((*args_ptr)
            .spaces
            .as_slice(count)
            .iter()
            .copied()
            .map(SpaceInfo::from)
            .collect())
    }
}

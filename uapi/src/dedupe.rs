//! # Extent deduplication: comparing and deduplicating file extents
//!
//! Wraps `BTRFS_IOC_FILE_EXTENT_SAME` to request that the kernel compare a
//! byte range in a source file against ranges in one or more destination files.
//! Where the data is identical, the destination extents are replaced with
//! references to the source extent, saving space.

use crate::raw::{
    BTRFS_SAME_DATA_DIFFERS, btrfs_ioc_file_extent_same, btrfs_ioctl_same_args,
    btrfs_ioctl_same_extent_info,
};
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

/// A destination file and offset to deduplicate against the source range.
#[derive(Debug, Clone)]
pub struct DedupeTarget {
    /// File descriptor of the destination file (passed as raw fd).
    pub fd: BorrowedFd<'static>,
    /// Byte offset in the destination file to compare from.
    pub logical_offset: u64,
}

/// Result of a single dedupe comparison against one destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeResult {
    /// Deduplication succeeded; the given number of bytes were deduped.
    Deduped(u64),
    /// The data differs between source and destination.
    DataDiffers,
    /// The kernel returned an error for this particular destination.
    Error(i32),
}

/// Deduplicate a source range against one or more destination ranges.
///
/// Compares `length` bytes starting at `src_offset` in the file referred to
/// by `src_fd` against each target. Where data matches, the destination
/// extents are replaced with shared references to the source extent.
///
/// Returns one [`DedupeResult`] per target, in the same order.
///
/// Errors (ioctl-level): EINVAL if `src_offset` or `length` are not
/// sector-aligned, or if `targets` is empty. EPERM if the destination
/// files are not writable.
pub fn file_extent_same(
    src_fd: BorrowedFd<'_>,
    src_offset: u64,
    length: u64,
    targets: &[DedupeTarget],
) -> nix::Result<Vec<DedupeResult>> {
    let count = targets.len();

    // Flexible array member pattern: allocate header + count info entries.
    let base_size = mem::size_of::<btrfs_ioctl_same_args>();
    let info_size = mem::size_of::<btrfs_ioctl_same_extent_info>();
    let total_bytes = base_size + count * info_size;
    let num_u64s = total_bytes.div_ceil(mem::size_of::<u64>());
    let mut buf = vec![0u64; num_u64s];

    // SAFETY: buf is correctly sized and aligned for btrfs_ioctl_same_args.
    // We populate the header and info entries before calling the ioctl, and
    // read the results only after a successful return.
    unsafe {
        let args_ptr = buf.as_mut_ptr() as *mut btrfs_ioctl_same_args;
        (*args_ptr).logical_offset = src_offset;
        (*args_ptr).length = length;
        (*args_ptr).dest_count = count as u16;

        let info_slice = (*args_ptr).info.as_mut_slice(count);
        for (i, target) in targets.iter().enumerate() {
            info_slice[i].fd = target.fd.as_raw_fd() as i64;
            info_slice[i].logical_offset = target.logical_offset;
        }

        btrfs_ioc_file_extent_same(src_fd.as_raw_fd(), &mut *args_ptr)?;

        let info_slice = (*args_ptr).info.as_slice(count);
        Ok(info_slice
            .iter()
            .map(|info| {
                if info.status == 0 {
                    DedupeResult::Deduped(info.bytes_deduped)
                } else if info.status == BTRFS_SAME_DATA_DIFFERS as i32 {
                    DedupeResult::DataDiffers
                } else {
                    DedupeResult::Error(info.status)
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_result_deduped_debug() {
        let r = DedupeResult::Deduped(4096);
        assert_eq!(format!("{r:?}"), "Deduped(4096)");
    }

    #[test]
    fn dedupe_result_data_differs_debug() {
        let r = DedupeResult::DataDiffers;
        assert_eq!(format!("{r:?}"), "DataDiffers");
    }

    #[test]
    fn dedupe_result_error_debug() {
        let r = DedupeResult::Error(-22);
        assert_eq!(format!("{r:?}"), "Error(-22)");
    }

    #[test]
    fn dedupe_result_equality() {
        assert_eq!(DedupeResult::Deduped(100), DedupeResult::Deduped(100));
        assert_ne!(DedupeResult::Deduped(100), DedupeResult::Deduped(200));
        assert_eq!(DedupeResult::DataDiffers, DedupeResult::DataDiffers);
        assert_ne!(DedupeResult::DataDiffers, DedupeResult::Deduped(0));
        assert_eq!(DedupeResult::Error(-1), DedupeResult::Error(-1));
        assert_ne!(DedupeResult::Error(-1), DedupeResult::Error(-2));
    }

    #[test]
    fn allocation_sizing() {
        // Verify the flexible array member allocation produces enough space.
        let base_size = mem::size_of::<btrfs_ioctl_same_args>();
        let info_size = mem::size_of::<btrfs_ioctl_same_extent_info>();

        for count in [0, 1, 2, 5, 16, 255] {
            let total_bytes = base_size + count * info_size;
            let num_u64s = total_bytes.div_ceil(mem::size_of::<u64>());
            let allocated = num_u64s * mem::size_of::<u64>();
            assert!(
                allocated >= total_bytes,
                "count={count}: allocated {allocated} < needed {total_bytes}"
            );
        }
    }

    #[test]
    fn btrfs_same_data_differs_value() {
        // Sanity check: the constant should be 1 per the kernel header.
        assert_eq!(BTRFS_SAME_DATA_DIFFERS, 1);
    }
}

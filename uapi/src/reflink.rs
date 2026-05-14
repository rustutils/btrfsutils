//! # Lightweight file copy via `BTRFS_IOC_CLONE_RANGE`
//!
//! Reflinks a byte range from one file to another: the destination
//! gains an extent reference to the source's data, no bytes are
//! copied, and subsequent modifications are copy-on-write.
//! Equivalent to the standard VFS `FICLONERANGE` ioctl (the btrfs
//! and VFS encodings happen to share the same magic/number).

use crate::raw::{btrfs_ioc_clone_range, btrfs_ioctl_clone_range_args};
use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

/// Reflink a range of bytes from `src` to `dst`.
///
/// A `length` of zero is a sentinel meaning "from `src_offset` to
/// end-of-source-file" — this matches the kernel's documented
/// behaviour. The source and destination can be the same file.
///
/// # Errors
///
/// Common errors: `EINVAL` if offsets or length are not block-
/// aligned (filesystem-specific; btrfs requires sector alignment
/// for non-tail extents), `EXDEV` if the files live on different
/// filesystems, `EPERM` if the destination is not writable, or
/// `EOPNOTSUPP` if the filesystem doesn't support reflinks.
pub fn clone_range(
    src: BorrowedFd<'_>,
    src_offset: u64,
    length: u64,
    dst: BorrowedFd<'_>,
    dst_offset: u64,
) -> nix::Result<()> {
    let mut args = btrfs_ioctl_clone_range_args {
        src_fd: i64::from(src.as_raw_fd()),
        src_offset,
        src_length: length,
        dest_offset: dst_offset,
    };
    // SAFETY: `args` is fully initialised and lives for the duration
    // of the ioctl. The kernel reads from it via copy_from_user; we
    // do not read any output back.
    unsafe { btrfs_ioc_clone_range(dst.as_raw_fd(), &raw mut args) }?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn args_struct_size_matches_kernel() {
        // s64 src_fd + 3 x u64 = 32 bytes — locked in by the kernel
        // ABI; any drift here means the bindgen-generated layout is
        // no longer compatible with the ioctl.
        assert_eq!(size_of::<btrfs_ioctl_clone_range_args>(), 32);
    }
}

//! # Filesystem sync: flushing all pending writes to disk
//!
//! Forces the kernel to commit all dirty btrfs metadata and data to stable
//! storage.  Equivalent to calling `sync(2)` scoped to a single btrfs
//! filesystem rather than all mounted filesystems.

use crate::raw::btrfs_ioc_sync;
use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

/// Force a sync on the btrfs filesystem referred to by `fd`.
///
/// Equivalent to `ioctl(fd, BTRFS_IOC_SYNC)`.
pub fn sync(fd: BorrowedFd) -> nix::Result<()> {
    unsafe { btrfs_ioc_sync(fd.as_raw_fd()) }?;
    Ok(())
}

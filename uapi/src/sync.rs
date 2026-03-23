//! Safe wrapper for the btrfs sync ioctl.

use crate::raw::btrfs_ioc_sync;
use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

/// Force a sync on the btrfs filesystem referred to by `fd`.
///
/// Equivalent to `ioctl(fd, BTRFS_IOC_SYNC)`.
pub fn sync(fd: BorrowedFd) -> nix::Result<()> {
    unsafe { btrfs_ioc_sync(fd.as_raw_fd()) }?;
    Ok(())
}

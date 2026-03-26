//! # Filesystem sync: flushing all pending writes to disk
//!
//! Forces the kernel to commit all dirty btrfs metadata and data to stable
//! storage.  Equivalent to calling `sync(2)` scoped to a single btrfs
//! filesystem rather than all mounted filesystems.

use crate::raw::{btrfs_ioc_start_sync, btrfs_ioc_sync, btrfs_ioc_wait_sync};
use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

/// Force a sync on the btrfs filesystem referred to by `fd` and wait for it
/// to complete.
pub fn sync(fd: BorrowedFd) -> nix::Result<()> {
    unsafe { btrfs_ioc_sync(fd.as_raw_fd()) }?;
    Ok(())
}

/// Asynchronously start a sync on the btrfs filesystem referred to by `fd`.
///
/// Returns the transaction ID of the initiated sync, which can be passed to
/// `wait_sync` to block until it completes.
pub fn start_sync(fd: BorrowedFd) -> nix::Result<u64> {
    let mut transid: u64 = 0;
    unsafe { btrfs_ioc_start_sync(fd.as_raw_fd(), &mut transid) }?;
    Ok(transid)
}

/// Wait for a previously started transaction to complete.
///
/// `transid` is the transaction ID returned by `start_sync`. Pass zero to
/// wait for the current transaction.
pub fn wait_sync(fd: BorrowedFd, transid: u64) -> nix::Result<()> {
    unsafe { btrfs_ioc_wait_sync(fd.as_raw_fd(), &transid) }?;
    Ok(())
}

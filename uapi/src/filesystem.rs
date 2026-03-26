//! # Filesystem-level operations: metadata, sync, and label
//!
//! Operations that apply to a btrfs filesystem as a whole rather than to any
//! individual device or subvolume: querying filesystem info (UUID, device count,
//! node size), syncing pending writes to disk, and reading/writing the
//! human-readable label.

use crate::raw::{
    BTRFS_FS_INFO_FLAG_GENERATION, btrfs_ioc_fs_info, btrfs_ioc_get_fslabel,
    btrfs_ioc_set_fslabel, btrfs_ioc_start_sync, btrfs_ioc_sync, btrfs_ioc_wait_sync,
    btrfs_ioctl_fs_info_args,
};
use nix::libc::c_char;
use std::{
    ffi::{CStr, CString},
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};
use uuid::Uuid;

/// Information about a mounted btrfs filesystem, as returned by
/// `BTRFS_IOC_FS_INFO`.
#[derive(Debug, Clone)]
pub struct FsInfo {
    /// Filesystem UUID.
    pub uuid: Uuid,
    /// Number of devices in the filesystem.
    pub num_devices: u64,
    /// Highest device ID in the filesystem.
    pub max_id: u64,
    /// B-tree node size in bytes.
    pub nodesize: u32,
    /// Sector size in bytes.
    pub sectorsize: u32,
    /// Generation number of the filesystem.
    pub generation: u64,
}

/// Query information about the btrfs filesystem referred to by `fd`.
pub fn fs_info(fd: BorrowedFd) -> nix::Result<FsInfo> {
    let mut raw: btrfs_ioctl_fs_info_args = unsafe { mem::zeroed() };
    raw.flags = BTRFS_FS_INFO_FLAG_GENERATION as u64;
    unsafe { btrfs_ioc_fs_info(fd.as_raw_fd(), &mut raw) }?;

    Ok(FsInfo {
        uuid: Uuid::from_bytes(raw.fsid),
        num_devices: raw.num_devices,
        max_id: raw.max_id,
        nodesize: raw.nodesize,
        sectorsize: raw.sectorsize,
        generation: raw.generation,
    })
}

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

/// Maximum label length including the null terminator (BTRFS_LABEL_SIZE).
const BTRFS_LABEL_SIZE: usize = crate::raw::BTRFS_LABEL_SIZE as usize;

/// Read the label of the btrfs filesystem referred to by `fd`.
///
/// Returns the label as a [`CString`]. An empty string means no label is set.
pub fn label_get(fd: BorrowedFd) -> nix::Result<CString> {
    let mut buf = [0i8; BTRFS_LABEL_SIZE];
    unsafe { btrfs_ioc_get_fslabel(fd.as_raw_fd(), &mut buf) }?;
    let cstr = unsafe { CStr::from_ptr(buf.as_ptr()) };
    // CStr::to_owned() copies the bytes into a freshly allocated CString,
    // which is safe to return after `buf` goes out of scope.
    Ok(cstr.to_owned())
}

/// Set the label of the btrfs filesystem referred to by `fd`.
///
/// The label must be shorter than 256 bytes (not counting the null terminator).
/// Further validation (e.g. rejecting labels that contain `/`) is left to the
/// kernel.
pub fn label_set(fd: BorrowedFd, label: &CStr) -> nix::Result<()> {
    let bytes = label.to_bytes();
    if bytes.len() >= BTRFS_LABEL_SIZE {
        return Err(nix::errno::Errno::EINVAL);
    }
    let mut buf = [0i8; BTRFS_LABEL_SIZE];
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
    unsafe { btrfs_ioc_set_fslabel(fd.as_raw_fd(), &buf) }?;
    Ok(())
}

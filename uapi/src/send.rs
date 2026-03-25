//! # Send stream: generating an incremental or full send stream from a subvolume
//!
//! The kernel generates a binary stream representing the contents of a read-only
//! subvolume (or the delta between a parent and child snapshot). The stream is
//! written to a pipe; the caller reads from the other end and writes it to a
//! file or stdout for later consumption by `btrfs receive`.

use crate::raw::{
    self, btrfs_ioc_send, btrfs_ioctl_send_args,
};
use bitflags::bitflags;
use nix::libc::c_int;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};

bitflags! {
    /// Flags for the send ioctl.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendFlags: u64 {
        /// Do not include file data in the stream (metadata only).
        const NO_FILE_DATA = raw::BTRFS_SEND_FLAG_NO_FILE_DATA as u64;
        /// Omit the stream header (for multi-subvolume sends).
        const OMIT_STREAM_HEADER = raw::BTRFS_SEND_FLAG_OMIT_STREAM_HEADER as u64;
        /// Omit the end-cmd marker (for multi-subvolume sends).
        const OMIT_END_CMD = raw::BTRFS_SEND_FLAG_OMIT_END_CMD as u64;
        /// Request a specific protocol version (set the version field).
        const VERSION = raw::BTRFS_SEND_FLAG_VERSION as u64;
        /// Send compressed data directly without decompressing.
        const COMPRESSED = raw::BTRFS_SEND_FLAG_COMPRESSED as u64;
    }
}

/// Invoke `BTRFS_IOC_SEND` on the given subvolume.
///
/// The kernel writes the send stream to `send_fd` (the write end of a pipe).
/// The caller is responsible for reading from the read end of the pipe,
/// typically in a separate thread.
///
/// `clone_sources` is a list of root IDs that the kernel may reference for
/// clone operations in the stream. `parent_root` is the root ID of the parent
/// snapshot for incremental sends, or `0` for a full send.
pub fn send(
    subvol_fd: BorrowedFd<'_>,
    send_fd: RawFd,
    parent_root: u64,
    clone_sources: &mut [u64],
    flags: SendFlags,
    version: u32,
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_send_args = unsafe { std::mem::zeroed() };
    args.send_fd = send_fd as i64;
    args.parent_root = parent_root;
    args.clone_sources_count = clone_sources.len() as u64;
    args.clone_sources = if clone_sources.is_empty() {
        std::ptr::null_mut()
    } else {
        clone_sources.as_mut_ptr()
    };
    args.flags = flags.bits();
    args.version = version;

    // SAFETY: args is fully initialized, clone_sources points to valid memory
    // that outlives the ioctl call, and subvol_fd is a valid borrowed fd.
    unsafe {
        btrfs_ioc_send(subvol_fd.as_raw_fd() as c_int, &args)?;
    }

    Ok(())
}

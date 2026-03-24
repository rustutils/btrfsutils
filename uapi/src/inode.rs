//! Inode-related operations for querying filesystem metadata.

use nix::libc::c_int;
use std::os::fd::{AsRawFd, BorrowedFd};

use crate::raw::{BTRFS_FIRST_FREE_OBJECTID, btrfs_ioc_ino_lookup};

/// Look up the tree ID (root ID) of the subvolume containing the given file or directory.
///
/// For a file or directory, returns the containing tree root ID.
/// For a subvolume, returns its own tree ID.
///
/// # Arguments
///
/// * `fd` - File descriptor to a file or directory on the btrfs filesystem
///
/// # Returns
///
/// The tree ID (root ID) of the containing subvolume
///
/// # Errors
///
/// Returns an error if the ioctl fails (e.g., file descriptor is not on a btrfs filesystem)
pub fn lookup_path_rootid(fd: BorrowedFd<'_>) -> nix::Result<u64> {
    let mut args = crate::raw::btrfs_ioctl_ino_lookup_args {
        treeid: 0,
        objectid: BTRFS_FIRST_FREE_OBJECTID as u64,
        ..unsafe { std::mem::zeroed() }
    };

    unsafe {
        btrfs_ioc_ino_lookup(fd.as_raw_fd() as c_int, &mut args)?;
    }

    Ok(args.treeid)
}

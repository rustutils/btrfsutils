//! Safe wrappers for the btrfs filesystem label ioctls.

use std::ffi::{CStr, CString};
use std::os::fd::AsRawFd;
use std::os::unix::io::BorrowedFd;

use nix::libc::c_char;

use crate::raw::{btrfs_ioc_get_fslabel, btrfs_ioc_set_fslabel};

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

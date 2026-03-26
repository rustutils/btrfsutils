//! # Block device ioctls: BLKGETSIZE64, BLKDISCARD
//!
//! Standard Linux block device ioctls that are not btrfs-specific but are
//! needed for device preparation (e.g. querying device size, issuing TRIM).

use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

// From linux/fs.h:
// #define BLKGETSIZE64 _IOR(0x12, 114, size_t)
// #define BLKDISCARD   _IO(0x12, 119)
nix::ioctl_read!(blk_getsize64, 0x12, 114, u64);
nix::ioctl_write_ptr!(blk_discard, 0x12, 119, [u64; 2]);

/// Get the size of a block device in bytes.
pub fn device_size(fd: BorrowedFd) -> nix::Result<u64> {
    let mut size: u64 = 0;
    unsafe { blk_getsize64(fd.as_raw_fd(), &mut size) }?;
    Ok(size)
}

/// Issue a BLKDISCARD (TRIM) on the given byte range of a block device.
///
/// Tells the device that the specified range is no longer in use and its
/// contents can be discarded. This is typically done before repurposing a
/// device (e.g. as a replace target).
pub fn discard_range(
    fd: BorrowedFd,
    offset: u64,
    length: u64,
) -> nix::Result<()> {
    let range: [u64; 2] = [offset, length];
    unsafe { blk_discard(fd.as_raw_fd(), &range) }?;
    Ok(())
}

/// Issue a BLKDISCARD on the entire block device.
///
/// Returns the number of bytes discarded (the device size), or an error.
/// Silently ignores EOPNOTSUPP (device does not support discard).
pub fn discard_whole_device(fd: BorrowedFd) -> nix::Result<u64> {
    let size = device_size(fd)?;
    if size == 0 {
        return Ok(0);
    }
    match discard_range(fd, 0, size) {
        Ok(()) => Ok(size),
        Err(nix::errno::Errno::EOPNOTSUPP) => Ok(0),
        Err(e) => Err(e),
    }
}

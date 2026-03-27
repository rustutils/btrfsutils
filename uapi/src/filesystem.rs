//! # Filesystem-level operations: metadata, sync, label, and resize
//!
//! Operations that apply to a btrfs filesystem as a whole rather than to any
//! individual device or subvolume: querying filesystem info (UUID, device count,
//! node size), syncing pending writes to disk, reading/writing the
//! human-readable label, and resizing a device within the filesystem.

use crate::raw::{
    BTRFS_FS_INFO_FLAG_GENERATION, BTRFS_LABEL_SIZE, btrfs_ioc_fs_info,
    btrfs_ioc_get_fslabel, btrfs_ioc_resize, btrfs_ioc_set_fslabel,
    btrfs_ioc_start_sync, btrfs_ioc_sync, btrfs_ioc_wait_sync,
    btrfs_ioctl_fs_info_args, btrfs_ioctl_vol_args,
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
pub struct FilesystemInfo {
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
pub fn filesystem_info(fd: BorrowedFd) -> nix::Result<FilesystemInfo> {
    let mut raw: btrfs_ioctl_fs_info_args = unsafe { mem::zeroed() };
    raw.flags = BTRFS_FS_INFO_FLAG_GENERATION as u64;
    unsafe { btrfs_ioc_fs_info(fd.as_raw_fd(), &mut raw) }?;

    Ok(FilesystemInfo {
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

/// Read the label of the btrfs filesystem referred to by `fd`.
///
/// Returns the label as a [`CString`]. An empty string means no label is set.
pub fn label_get(fd: BorrowedFd) -> nix::Result<CString> {
    let mut buf = [0i8; BTRFS_LABEL_SIZE as usize];
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
///
/// Errors: EINVAL if the label is 256 bytes or longer (checked before the
/// ioctl).  EPERM without `CAP_SYS_ADMIN`.
pub fn label_set(fd: BorrowedFd, label: &CStr) -> nix::Result<()> {
    let bytes = label.to_bytes();
    if bytes.len() >= BTRFS_LABEL_SIZE as usize {
        return Err(nix::errno::Errno::EINVAL);
    }
    let mut buf = [0i8; BTRFS_LABEL_SIZE as usize];
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
    unsafe { btrfs_ioc_set_fslabel(fd.as_raw_fd(), &buf) }?;
    Ok(())
}

/// The target size for a resize operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResizeAmount {
    /// Cancel an in-progress resize.
    Cancel,
    /// Grow the device to its maximum available size.
    Max,
    /// Set the device to exactly this many bytes.
    Set(u64),
    /// Add this many bytes to the current device size.
    Add(u64),
    /// Subtract this many bytes from the current device size.
    Sub(u64),
}

impl std::fmt::Display for ResizeAmount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancel => f.write_str("cancel"),
            Self::Max => f.write_str("max"),
            Self::Set(n) => write!(f, "{n}"),
            Self::Add(n) => write!(f, "+{n}"),
            Self::Sub(n) => write!(f, "-{n}"),
        }
    }
}

/// Arguments for a resize operation.
///
/// `devid` selects which device within the filesystem to resize. When `None`,
/// the kernel defaults to device ID 1 (the first device).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResizeArgs {
    pub devid: Option<u64>,
    pub amount: ResizeAmount,
}

impl ResizeArgs {
    pub fn new(amount: ResizeAmount) -> Self {
        Self {
            devid: None,
            amount,
        }
    }

    pub fn with_devid(mut self, devid: u64) -> Self {
        self.devid = Some(devid);
        self
    }

    /// Format into the string that `BTRFS_IOC_RESIZE` expects in
    /// `btrfs_ioctl_vol_args.name`: `[<devid>:]<amount>`.
    fn format_name(&self) -> String {
        let amount = self.amount.to_string();
        match self.devid {
            Some(devid) => format!("{devid}:{amount}"),
            None => amount,
        }
    }
}

/// Resize a device within the btrfs filesystem referred to by `fd`.
///
/// `fd` must be an open file descriptor to a directory on the mounted
/// filesystem. Use [`ResizeArgs`] to specify the target device and amount.
pub fn resize(fd: BorrowedFd, args: ResizeArgs) -> nix::Result<()> {
    let name = args.format_name();
    let name_bytes = name.as_bytes();

    // BTRFS_PATH_NAME_MAX is 4087; the name field is [c_char; 4088].
    // A well-formed resize string (devid + colon + u64 digits) is at most
    // ~23 characters, so this can only fail if the caller constructs a
    // pathological devid.
    if name_bytes.len() >= 4088 {
        return Err(nix::errno::Errno::EINVAL);
    }

    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    for (i, &b) in name_bytes.iter().enumerate() {
        raw.name[i] = b as c_char;
    }

    unsafe { btrfs_ioc_resize(fd.as_raw_fd(), &raw) }?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ResizeAmount::to_string ---

    #[test]
    fn resize_amount_cancel() {
        assert_eq!(ResizeAmount::Cancel.to_string(), "cancel");
    }

    #[test]
    fn resize_amount_max() {
        assert_eq!(ResizeAmount::Max.to_string(), "max");
    }

    #[test]
    fn resize_amount_set() {
        assert_eq!(ResizeAmount::Set(1073741824).to_string(), "1073741824");
    }

    #[test]
    fn resize_amount_add() {
        assert_eq!(ResizeAmount::Add(512000000).to_string(), "+512000000");
    }

    #[test]
    fn resize_amount_sub() {
        assert_eq!(ResizeAmount::Sub(256000000).to_string(), "-256000000");
    }

    // --- ResizeArgs builder + format_name ---

    #[test]
    fn resize_args_no_devid() {
        let args = ResizeArgs::new(ResizeAmount::Max);
        assert!(args.devid.is_none());
        assert_eq!(args.format_name(), "max");
    }

    #[test]
    fn resize_args_with_devid() {
        let args = ResizeArgs::new(ResizeAmount::Add(1024)).with_devid(2);
        assert_eq!(args.devid, Some(2));
        assert_eq!(args.format_name(), "2:+1024");
    }

    #[test]
    fn resize_args_set_with_devid() {
        let args = ResizeArgs::new(ResizeAmount::Set(999)).with_devid(1);
        assert_eq!(args.format_name(), "1:999");
    }
}

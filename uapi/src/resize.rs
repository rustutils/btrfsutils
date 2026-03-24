//! Device resizing — growing or shrinking a device within a mounted filesystem.
//!
//! Resizing adjusts how much of a block device's capacity btrfs uses, without
//! unmounting.  A device can be grown up to its physical size, shrunk to the
//! minimum space currently occupied, or set to an explicit byte count.

use crate::raw::{btrfs_ioc_resize, btrfs_ioctl_vol_args};
use nix::libc::c_char;
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

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

impl ResizeAmount {
    fn to_string(&self) -> String {
        match self {
            Self::Cancel => "cancel".to_owned(),
            Self::Max => "max".to_owned(),
            Self::Set(n) => n.to_string(),
            Self::Add(n) => format!("+{n}"),
            Self::Sub(n) => format!("-{n}"),
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

    unsafe { btrfs_ioc_resize(fd.as_raw_fd(), &mut raw) }?;
    Ok(())
}

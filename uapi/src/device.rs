use crate::{
    filesystem::FsInfo,
    raw::{btrfs_ioc_dev_info, btrfs_ioctl_dev_info_args},
};
use nix::errno::Errno;
use std::{
    ffi::CStr,
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};
use uuid::Uuid;

/// Information about a single device within a btrfs filesystem, as returned
/// by `BTRFS_IOC_DEV_INFO`.
#[derive(Debug, Clone)]
pub struct DevInfo {
    /// Device ID.
    pub devid: u64,
    /// Device UUID.
    pub uuid: Uuid,
    /// Number of bytes used on this device.
    pub bytes_used: u64,
    /// Total size of this device in bytes.
    pub total_bytes: u64,
    /// Path to the block device, e.g. `/dev/sda`.
    pub path: String,
}

/// Query information about the device with the given `devid` on the filesystem
/// referred to by `fd`.
///
/// Returns `None` if no device with that ID exists (`ENODEV`).
pub fn dev_info(fd: BorrowedFd, devid: u64) -> nix::Result<Option<DevInfo>> {
    let mut raw: btrfs_ioctl_dev_info_args = unsafe { mem::zeroed() };
    raw.devid = devid;

    match unsafe { btrfs_ioc_dev_info(fd.as_raw_fd(), &mut raw) } {
        Err(Errno::ENODEV) => return Ok(None),
        Err(e) => return Err(e),
        Ok(_) => {}
    }

    let path = unsafe { CStr::from_ptr(raw.path.as_ptr() as *const _) }
        .to_string_lossy()
        .into_owned();

    Ok(Some(DevInfo {
        devid: raw.devid,
        uuid: Uuid::from_bytes(raw.uuid),
        bytes_used: raw.bytes_used,
        total_bytes: raw.total_bytes,
        path,
    }))
}

/// Query information about all devices in the filesystem referred to by `fd`,
/// using the device count from a previously obtained [`FsInfo`].
///
/// Iterates devids `1..=max_id`, skipping any that return `ENODEV` (holes in
/// the devid space are normal when devices have been removed).
pub fn all_dev_info(fd: BorrowedFd, fs_info: &FsInfo) -> nix::Result<Vec<DevInfo>> {
    let mut devices = Vec::with_capacity(fs_info.num_devices as usize);
    for devid in 1..=fs_info.max_id {
        if let Some(info) = dev_info(fd, devid)? {
            devices.push(info);
        }
    }
    Ok(devices)
}

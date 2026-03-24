//! Device management — adding, removing, and querying block devices in a filesystem.
//!
//! Covers adding and removing devices from a mounted filesystem, scanning a
//! device to register it with the kernel, querying per-device I/O error
//! statistics, and checking whether all devices of a multi-device filesystem
//! are present and ready.
//!
//! Most operations require `CAP_SYS_ADMIN`.

use crate::{
    filesystem::FsInfo,
    raw::{
        BTRFS_DEV_STATS_RESET, BTRFS_DEVICE_SPEC_BY_ID,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_CORRUPTION_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_FLUSH_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_GENERATION_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_READ_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_VALUES_MAX,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_WRITE_ERRS, btrfs_ioc_add_dev, btrfs_ioc_dev_info,
        btrfs_ioc_devices_ready, btrfs_ioc_forget_dev, btrfs_ioc_get_dev_stats, btrfs_ioc_rm_dev,
        btrfs_ioc_rm_dev_v2, btrfs_ioc_scan_dev, btrfs_ioctl_dev_info_args,
        btrfs_ioctl_get_dev_stats, btrfs_ioctl_vol_args, btrfs_ioctl_vol_args_v2,
    },
};
use nix::errno::Errno;
use nix::libc::c_char;
use std::{
    ffi::CStr,
    fs::OpenOptions,
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

/// Specifies a device for operations that can address by either path or ID.
#[derive(Debug, Clone)]
pub enum DeviceSpec<'a> {
    /// A block device path (e.g. `/dev/sdb`), or the special strings
    /// `"missing"` or `"cancel"` accepted by the remove ioctl.
    Path(&'a CStr),
    /// A btrfs device ID as reported by `BTRFS_IOC_DEV_INFO`.
    Id(u64),
}

/// Per-device I/O error statistics, as returned by `BTRFS_IOC_GET_DEV_STATS`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DevStats {
    /// Device ID these stats belong to.
    pub devid: u64,
    /// Number of write I/O errors (EIO/EREMOTEIO from lower layers).
    pub write_errs: u64,
    /// Number of read I/O errors (EIO/EREMOTEIO from lower layers).
    pub read_errs: u64,
    /// Number of flush I/O errors (EIO/EREMOTEIO from lower layers).
    pub flush_errs: u64,
    /// Number of checksum or bytenr corruption errors detected on read.
    pub corruption_errs: u64,
    /// Number of generation errors (blocks not written where expected).
    pub generation_errs: u64,
}

impl DevStats {
    /// Sum of all error counters.
    pub fn total_errs(&self) -> u64 {
        self.write_errs
            + self.read_errs
            + self.flush_errs
            + self.corruption_errs
            + self.generation_errs
    }

    /// Returns `true` if every counter is zero.
    pub fn is_clean(&self) -> bool {
        self.total_errs() == 0
    }
}

/// Copy the bytes of `path` (without the nul terminator) into `name`,
/// returning `ENAMETOOLONG` if the path (including the terminator that the
/// kernel expects to already be present via zeroing) does not fit.
fn copy_path_to_name(name: &mut [c_char], path: &CStr) -> nix::Result<()> {
    let bytes = path.to_bytes(); // excludes nul terminator
    if bytes.len() >= name.len() {
        return Err(Errno::ENAMETOOLONG);
    }
    for (i, &b) in bytes.iter().enumerate() {
        name[i] = b as c_char;
    }
    // The remainder of `name` is already zeroed by the caller (mem::zeroed).
    Ok(())
}

/// Open `/dev/btrfs-control` for read+write, mapping any `std::io::Error` to
/// the appropriate `nix::errno::Errno`.
fn open_control() -> nix::Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/btrfs-control")
        .map_err(|e| Errno::from_raw(e.raw_os_error().unwrap_or(nix::libc::ENODEV)))
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

/// Add a device to the btrfs filesystem referred to by `fd`.
///
/// `path` must be the path to an unmounted block device. The kernel requires
/// `CAP_SYS_ADMIN`.
pub fn device_add(fd: BorrowedFd, path: &CStr) -> nix::Result<()> {
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_add_dev(fd.as_raw_fd(), &raw) }?;
    Ok(())
}

/// Remove a device from the btrfs filesystem referred to by `fd`.
///
/// The device can be specified either by path or by its btrfs device ID via
/// [`DeviceSpec`]. Uses `BTRFS_IOC_RM_DEV_V2` and falls back to the older
/// `BTRFS_IOC_RM_DEV` ioctl on kernels that do not support the v2 variant
/// (only possible when removing by path). The kernel requires `CAP_SYS_ADMIN`.
pub fn device_remove(fd: BorrowedFd, spec: DeviceSpec) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };

    match spec {
        DeviceSpec::Id(devid) => {
            args.flags = BTRFS_DEVICE_SPEC_BY_ID as u64;
            // SAFETY: devid is the active union member when BTRFS_DEVICE_SPEC_BY_ID is set.
            args.__bindgen_anon_2.devid = devid;
            unsafe { btrfs_ioc_rm_dev_v2(fd.as_raw_fd(), &args) }?;
        }
        DeviceSpec::Path(path) => {
            // SAFETY: name is the active union member when flags == 0.
            unsafe { copy_path_to_name(&mut args.__bindgen_anon_2.name, path) }?;
            match unsafe { btrfs_ioc_rm_dev_v2(fd.as_raw_fd(), &args) } {
                Ok(_) => {}
                // Fall back to the old single-arg ioctl on kernels that either
                // don't know about v2 (ENOTTY) or don't recognise our flags (EOPNOTSUPP).
                Err(Errno::ENOTTY) | Err(Errno::EOPNOTSUPP) => {
                    let mut old: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
                    copy_path_to_name(&mut old.name, path)?;
                    unsafe { btrfs_ioc_rm_dev(fd.as_raw_fd(), &old) }?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}

/// Register a block device with the kernel's btrfs device scanner so that
/// multi-device filesystems containing it can be mounted.
///
/// Opens `/dev/btrfs-control` and issues `BTRFS_IOC_SCAN_DEV`. `path` must
/// be the path to a block device that contains a btrfs filesystem member.
pub fn device_scan(path: &CStr) -> nix::Result<()> {
    let ctl = open_control()?;
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_scan_dev(ctl.as_raw_fd(), &raw) }?;
    Ok(())
}

/// Unregister a device (or all stale devices) from the kernel's btrfs device
/// scanner.
///
/// Opens `/dev/btrfs-control` and issues `BTRFS_IOC_FORGET_DEV`. If `path`
/// is `None`, all devices that are not part of a currently mounted filesystem
/// are unregistered. If `path` is `Some`, only that specific device path is
/// unregistered.
pub fn device_forget(path: Option<&CStr>) -> nix::Result<()> {
    let ctl = open_control()?;
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    if let Some(p) = path {
        copy_path_to_name(&mut raw.name, p)?;
    }
    unsafe { btrfs_ioc_forget_dev(ctl.as_raw_fd(), &raw) }?;
    Ok(())
}

/// Check whether all member devices of the filesystem that contains `path`
/// are available and the filesystem is ready to mount.
///
/// Opens `/dev/btrfs-control` and issues `BTRFS_IOC_DEVICES_READY`. `path`
/// must be the path to one of the block devices belonging to the filesystem.
/// Returns `Ok(())` when all devices are present; returns an error (typically
/// `ENOENT` or `ENXIO`) if the set is incomplete.
pub fn device_ready(path: &CStr) -> nix::Result<()> {
    let ctl = open_control()?;
    // BTRFS_IOC_DEVICES_READY is declared _IOR but the kernel reads the device
    // path from args.name, so we pass a mut pointer as ioctl_read! requires.
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_devices_ready(ctl.as_raw_fd(), &mut raw) }?;
    Ok(())
}

/// Query I/O error statistics for the device identified by `devid` within the
/// filesystem referred to by `fd`.
///
/// If `reset` is `true`, the kernel atomically returns the current values and
/// then resets all counters to zero. The kernel requires `CAP_SYS_ADMIN`.
pub fn dev_stats(fd: BorrowedFd, devid: u64, reset: bool) -> nix::Result<DevStats> {
    let mut raw: btrfs_ioctl_get_dev_stats = unsafe { mem::zeroed() };
    raw.devid = devid;
    raw.nr_items = btrfs_dev_stat_values_BTRFS_DEV_STAT_VALUES_MAX as u64;
    if reset {
        raw.flags = BTRFS_DEV_STATS_RESET as u64;
    }

    unsafe { btrfs_ioc_get_dev_stats(fd.as_raw_fd(), &mut raw) }?;

    Ok(DevStats {
        devid,
        write_errs: raw.values[btrfs_dev_stat_values_BTRFS_DEV_STAT_WRITE_ERRS as usize],
        read_errs: raw.values[btrfs_dev_stat_values_BTRFS_DEV_STAT_READ_ERRS as usize],
        flush_errs: raw.values[btrfs_dev_stat_values_BTRFS_DEV_STAT_FLUSH_ERRS as usize],
        corruption_errs: raw.values[btrfs_dev_stat_values_BTRFS_DEV_STAT_CORRUPTION_ERRS as usize],
        generation_errs: raw.values[btrfs_dev_stat_values_BTRFS_DEV_STAT_GENERATION_ERRS as usize],
    })
}

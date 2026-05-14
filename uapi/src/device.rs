//! # Device management: adding, removing, querying, and extent layout
//!
//! Covers adding and removing devices from a mounted filesystem, scanning a
//! device to register it with the kernel, querying per-device I/O error
//! statistics, checking whether all devices of a multi-device filesystem
//! are present and ready, and computing minimum device sizes from the
//! device extent tree.
//!
//! Most operations require `CAP_SYS_ADMIN`.

use crate::{
    filesystem::FilesystemInfo,
    raw::{
        BTRFS_DEV_EXTENT_KEY, BTRFS_DEV_STATS_RESET, BTRFS_DEV_TREE_OBJECTID,
        BTRFS_DEVICE_SPEC_BY_ID,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_CORRUPTION_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_FLUSH_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_GENERATION_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_READ_ERRS,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_VALUES_MAX,
        btrfs_dev_stat_values_BTRFS_DEV_STAT_WRITE_ERRS, btrfs_ioc_add_dev,
        btrfs_ioc_dev_info, btrfs_ioc_devices_ready, btrfs_ioc_forget_dev,
        btrfs_ioc_get_dev_stats, btrfs_ioc_rm_dev, btrfs_ioc_rm_dev_v2,
        btrfs_ioc_scan_dev, btrfs_ioctl_dev_info_args,
        btrfs_ioctl_get_dev_stats, btrfs_ioctl_vol_args,
        btrfs_ioctl_vol_args_v2,
    },
    tree_search::{SearchFilter, tree_search},
};
use nix::{errno::Errno, libc::c_char};
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
pub struct DeviceInfo {
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
pub struct DeviceStats {
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

impl DeviceStats {
    /// Sum of all error counters.
    #[must_use]
    pub fn total_errs(&self) -> u64 {
        self.write_errs
            + self.read_errs
            + self.flush_errs
            + self.corruption_errs
            + self.generation_errs
    }

    /// Returns `true` if every counter is zero.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.total_errs() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_stats_default_is_clean() {
        let stats = DeviceStats::default();
        assert!(stats.is_clean());
        assert_eq!(stats.total_errs(), 0);
    }

    #[test]
    fn dev_stats_total_errs() {
        let stats = DeviceStats {
            devid: 1,
            write_errs: 1,
            read_errs: 2,
            flush_errs: 3,
            corruption_errs: 4,
            generation_errs: 5,
        };
        assert_eq!(stats.total_errs(), 15);
        assert!(!stats.is_clean());
    }

    #[test]
    fn dev_stats_single_error_not_clean() {
        let stats = DeviceStats {
            corruption_errs: 1,
            ..DeviceStats::default()
        };
        assert!(!stats.is_clean());
        assert_eq!(stats.total_errs(), 1);
    }
}

/// Copy the bytes of `path` (without the nul terminator) into `name`,
/// returning `ENAMETOOLONG` if the path (including the terminator that the
/// kernel expects to already be present via zeroing) does not fit.
#[allow(clippy::cast_possible_wrap)] // ASCII bytes always fit in c_char
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
        .map_err(|e| {
            Errno::from_raw(e.raw_os_error().unwrap_or(nix::libc::ENODEV))
        })
}

/// Query information about the device with the given `devid` on the filesystem
/// referred to by `fd`.
///
/// Returns `None` if no device with that ID exists (`ENODEV`).
///
/// # Errors
///
/// Returns `Err` if the ioctl fails (other than `ENODEV`).
pub fn device_info(
    fd: BorrowedFd,
    devid: u64,
) -> nix::Result<Option<DeviceInfo>> {
    let mut raw: btrfs_ioctl_dev_info_args = unsafe { mem::zeroed() };
    raw.devid = devid;

    match unsafe { btrfs_ioc_dev_info(fd.as_raw_fd(), &raw mut raw) } {
        Err(Errno::ENODEV) => return Ok(None),
        Err(e) => return Err(e),
        Ok(_) => {}
    }

    let path = unsafe { CStr::from_ptr(raw.path.as_ptr().cast()) }
        .to_string_lossy()
        .into_owned();

    Ok(Some(DeviceInfo {
        devid: raw.devid,
        uuid: Uuid::from_bytes(raw.uuid),
        bytes_used: raw.bytes_used,
        total_bytes: raw.total_bytes,
        path,
    }))
}

/// Query information about all devices in the filesystem referred to by `fd`,
/// using the device count from a previously obtained [`FilesystemInfo`].
///
/// Iterates devids `1..=max_id`, skipping any that return `ENODEV` (holes in
/// the devid space are normal when devices have been removed).
///
/// # Errors
///
/// Returns `Err` if any device info ioctl fails.
pub fn device_info_all(
    fd: BorrowedFd,
    fs_info: &FilesystemInfo,
) -> nix::Result<Vec<DeviceInfo>> {
    #[allow(clippy::cast_possible_truncation)]
    // device count always fits in usize
    let mut devices = Vec::with_capacity(fs_info.num_devices as usize);
    for devid in 1..=fs_info.max_id {
        if let Some(info) = device_info(fd, devid)? {
            devices.push(info);
        }
    }
    Ok(devices)
}

/// Add a device to the btrfs filesystem referred to by `fd`.
///
/// `path` must be the path to an unmounted block device. The kernel requires
/// `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn device_add(fd: BorrowedFd, path: &CStr) -> nix::Result<()> {
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_add_dev(fd.as_raw_fd(), &raw const raw) }?;
    Ok(())
}

/// Remove a device from the btrfs filesystem referred to by `fd`.
///
/// The device can be specified either by path or by its btrfs device ID via
/// [`DeviceSpec`]. Uses `BTRFS_IOC_RM_DEV_V2` and falls back to the older
/// `BTRFS_IOC_RM_DEV` ioctl on kernels that do not support the v2 variant
/// (only possible when removing by path). The kernel requires `CAP_SYS_ADMIN`.
///
/// Errors: ENOTTY or EOPNOTSUPP from `RM_DEV_V2` triggers an automatic
/// fallback to the v1 ioctl (path-based removal only; by-ID removal
/// requires v2 and will propagate the error).  `EBUSY` if the device holds
/// the only copy of some data and cannot be removed.
///
/// # Errors
///
/// Returns `Err` if the remove ioctl fails.
pub fn device_remove(fd: BorrowedFd, spec: &DeviceSpec<'_>) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };

    match *spec {
        DeviceSpec::Id(devid) => {
            args.flags = u64::from(BTRFS_DEVICE_SPEC_BY_ID);
            // SAFETY: devid is the active union member when BTRFS_DEVICE_SPEC_BY_ID is set.
            args.__bindgen_anon_2.devid = devid;
            unsafe { btrfs_ioc_rm_dev_v2(fd.as_raw_fd(), &raw const args) }?;
        }
        DeviceSpec::Path(path) => {
            // SAFETY: name is the active union member when flags == 0.
            unsafe {
                copy_path_to_name(&mut args.__bindgen_anon_2.name, path)
            }?;
            match unsafe {
                btrfs_ioc_rm_dev_v2(fd.as_raw_fd(), &raw const args)
            } {
                Ok(_) => {}
                // Fall back to the old single-arg ioctl on kernels that either
                // don't know about v2 (ENOTTY) or don't recognise our flags (EOPNOTSUPP).
                Err(Errno::ENOTTY | Errno::EOPNOTSUPP) => {
                    let mut old: btrfs_ioctl_vol_args =
                        unsafe { mem::zeroed() };
                    copy_path_to_name(&mut old.name, path)?;
                    unsafe {
                        btrfs_ioc_rm_dev(fd.as_raw_fd(), &raw const old)
                    }?;
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
///
/// # Errors
///
/// Returns `Err` if opening `/dev/btrfs-control` or the ioctl fails.
pub fn device_scan(path: &CStr) -> nix::Result<()> {
    let ctl = open_control()?;
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_scan_dev(ctl.as_raw_fd(), &raw const raw) }?;
    Ok(())
}

/// Unregister a device (or all stale devices) from the kernel's btrfs device
/// scanner.
///
/// Opens `/dev/btrfs-control` and issues `BTRFS_IOC_FORGET_DEV`. If `path`
/// is `None`, all devices that are not part of a currently mounted filesystem
/// are unregistered. If `path` is `Some`, only that specific device path is
/// unregistered.
///
/// # Errors
///
/// Returns `Err` if opening `/dev/btrfs-control` or the ioctl fails.
pub fn device_forget(path: Option<&CStr>) -> nix::Result<()> {
    let ctl = open_control()?;
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    if let Some(p) = path {
        copy_path_to_name(&mut raw.name, p)?;
    }
    unsafe { btrfs_ioc_forget_dev(ctl.as_raw_fd(), &raw const raw) }?;
    Ok(())
}

/// Check whether all member devices of the filesystem that contains `path`
/// are available and the filesystem is ready to mount.
///
/// Opens `/dev/btrfs-control` and issues `BTRFS_IOC_DEVICES_READY`. `path`
/// must be the path to one of the block devices belonging to the filesystem.
/// Returns `Ok(())` when all devices are present; returns an error (typically
/// `ENOENT` or `ENXIO`) if the set is incomplete.
///
/// # Errors
///
/// Returns `Err` if some devices are missing or the ioctl fails.
pub fn device_ready(path: &CStr) -> nix::Result<()> {
    let ctl = open_control()?;
    // BTRFS_IOC_DEVICES_READY is declared _IOR but the kernel reads the device
    // path from args.name, so we pass a mut pointer as ioctl_read! requires.
    let mut raw: btrfs_ioctl_vol_args = unsafe { mem::zeroed() };
    copy_path_to_name(&mut raw.name, path)?;
    unsafe { btrfs_ioc_devices_ready(ctl.as_raw_fd(), &raw mut raw) }?;
    Ok(())
}

/// Query I/O error statistics for the device identified by `devid` within the
/// filesystem referred to by `fd`.
///
/// If `reset` is `true`, the kernel atomically returns the current values and
/// then resets all counters to zero. The kernel requires `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn device_stats(
    fd: BorrowedFd,
    devid: u64,
    reset: bool,
) -> nix::Result<DeviceStats> {
    let mut raw: btrfs_ioctl_get_dev_stats = unsafe { mem::zeroed() };
    raw.devid = devid;
    raw.nr_items = u64::from(btrfs_dev_stat_values_BTRFS_DEV_STAT_VALUES_MAX);
    if reset {
        raw.flags = u64::from(BTRFS_DEV_STATS_RESET);
    }

    unsafe { btrfs_ioc_get_dev_stats(fd.as_raw_fd(), &raw mut raw) }?;

    Ok(DeviceStats {
        devid,
        write_errs: raw.values
            [btrfs_dev_stat_values_BTRFS_DEV_STAT_WRITE_ERRS as usize],
        read_errs: raw.values
            [btrfs_dev_stat_values_BTRFS_DEV_STAT_READ_ERRS as usize],
        flush_errs: raw.values
            [btrfs_dev_stat_values_BTRFS_DEV_STAT_FLUSH_ERRS as usize],
        corruption_errs: raw.values
            [btrfs_dev_stat_values_BTRFS_DEV_STAT_CORRUPTION_ERRS as usize],
        generation_errs: raw.values
            [btrfs_dev_stat_values_BTRFS_DEV_STAT_GENERATION_ERRS as usize],
    })
}

const SZ_1M: u64 = 1024 * 1024;
const SZ_32M: u64 = 32 * 1024 * 1024;

/// Number of superblock mirror copies btrfs maintains.
const BTRFS_SUPER_MIRROR_MAX: usize = 3;

/// Return the byte offset of superblock mirror `i`.
///
/// Mirror 0 is at 64 KiB, mirror 1 at 64 MiB, mirror 2 at 256 GiB.
fn sb_offset(i: usize) -> u64 {
    match i {
        0 => 64 * 1024,
        _ => 1u64 << (20 + 10 * (i as u64)),
    }
}

/// A contiguous physical byte range on a device (inclusive end).
#[derive(Debug, Clone, Copy)]
struct Extent {
    start: u64,
    /// Inclusive end byte.
    end: u64,
}

/// Compute the minimum size to which device `devid` can be shrunk.
///
/// Walks the device tree for all `DEV_EXTENT_KEY` items belonging to
/// `devid`, sums their lengths, then adjusts for extents that sit beyond
/// the sum by checking whether they can be relocated into holes closer to
/// the start of the device. The algorithm matches `btrfs inspect-internal
/// min-dev-size` from btrfs-progs.
///
/// Requires `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the tree search ioctl fails.
pub fn device_min_size(fd: BorrowedFd, devid: u64) -> nix::Result<u64> {
    let mut dev_extents: Vec<(u64, u64)> = Vec::new();

    tree_search(
        fd,
        SearchFilter::for_objectid_range(
            u64::from(BTRFS_DEV_TREE_OBJECTID),
            BTRFS_DEV_EXTENT_KEY,
            devid,
            devid,
        ),
        |hdr, data| {
            let Some(de) = btrfs_disk::items::DeviceExtent::parse(data) else {
                return Ok(());
            };
            dev_extents.push((hdr.offset, de.length));
            Ok(())
        },
    )?;

    Ok(compute_min_size(&dev_extents))
}

/// Compute the minimum device size from a list of device extents.
///
/// Each entry is `(physical_start, length)`. The list must be sorted by
/// ascending `physical_start` (as returned by the device tree).
///
/// The algorithm sums all extent lengths (plus 1 MiB base), then tries to
/// relocate tail extents into holes to reduce the total. Matches the
/// btrfs-progs `min-dev-size` logic.
#[must_use]
pub fn compute_min_size(dev_extents: &[(u64, u64)]) -> u64 {
    let mut min_size: u64 = SZ_1M;
    let mut extents: Vec<Extent> = Vec::new();
    let mut holes: Vec<Extent> = Vec::new();
    let mut last_pos: Option<u64> = None;

    for &(phys_start, len) in dev_extents {
        min_size += len;

        extents.push(Extent {
            start: phys_start,
            end: phys_start + len - 1,
        });

        if let Some(prev_end) = last_pos
            && prev_end != phys_start
        {
            holes.push(Extent {
                start: prev_end,
                end: phys_start - 1,
            });
        }

        last_pos = Some(phys_start + len);
    }

    // Sort extents by descending end offset for the adjustment pass.
    extents.sort_by_key(|e| std::cmp::Reverse(e.end));

    adjust_min_size(&mut extents, &mut holes, &mut min_size);

    min_size
}

/// Check whether a byte range `[start, end]` contains a superblock mirror.
fn hole_includes_sb_mirror(start: u64, end: u64) -> bool {
    (0..BTRFS_SUPER_MIRROR_MAX).any(|i| {
        let bytenr = sb_offset(i);
        bytenr >= start && bytenr <= end
    })
}

/// Adjust `min_size` downward by relocating tail extents into holes.
///
/// Processes extents in descending order of end offset. If an extent sits
/// beyond the current `min_size`, try to find a hole large enough to
/// relocate it. If no hole fits, the device cannot be shrunk past that
/// extent and `min_size` is set to its end + 1.
///
/// Adds scratch space (largest relocated extent + 32 MiB for a potential
/// system chunk allocation) when any relocation is needed.
fn adjust_min_size(
    extents: &mut Vec<Extent>,
    holes: &mut Vec<Extent>,
    min_size: &mut u64,
) {
    let mut scratch_space: u64 = 0;

    while let Some(&ext) = extents.first() {
        if ext.end < *min_size {
            break;
        }

        let extent_len = ext.end - ext.start + 1;

        // Find the first hole large enough to hold this extent.
        let hole_idx = holes.iter().position(|h| {
            let hole_len = h.end - h.start + 1;
            hole_len >= extent_len
        });

        let Some(idx) = hole_idx else {
            *min_size = ext.end + 1;
            break;
        };

        // If the target hole contains a superblock mirror location,
        // pessimistically assume we need one more extent worth of space.
        if hole_includes_sb_mirror(
            holes[idx].start,
            holes[idx].start + extent_len - 1,
        ) {
            *min_size += extent_len;
        }

        // Shrink or remove the hole.
        let hole_len = holes[idx].end - holes[idx].start + 1;
        if hole_len > extent_len {
            holes[idx].start += extent_len;
        } else {
            holes.remove(idx);
        }

        extents.remove(0);

        if extent_len > scratch_space {
            scratch_space = extent_len;
        }
    }

    if scratch_space > 0 {
        *min_size += scratch_space;
        // Chunk allocation may require a new system chunk (up to 32 MiB).
        *min_size += SZ_32M;
    }
}

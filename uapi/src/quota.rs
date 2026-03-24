//! Filesystem quota — enabling, disabling, and rescanning quota accounting.
//!
//! Quota accounting tracks disk usage per subvolume via qgroups.  It must be
//! explicitly enabled before any qgroup limits or usage data are available.
//! Once enabled, usage numbers are maintained incrementally by the kernel; a
//! rescan rebuilds them from scratch if they become inconsistent.
//!
//! Quota status (whether quotas are on, which mode, inconsistency flag) is
//! read from sysfs via [`crate::sysfs::SysfsBtrfs::quota_status`].

use std::{mem, os::fd::AsRawFd, os::unix::io::BorrowedFd};

use crate::raw::{
    BTRFS_QUOTA_CTL_DISABLE, BTRFS_QUOTA_CTL_ENABLE, BTRFS_QUOTA_CTL_ENABLE_SIMPLE_QUOTA,
    btrfs_ioc_quota_ctl, btrfs_ioc_quota_rescan, btrfs_ioc_quota_rescan_status,
    btrfs_ioc_quota_rescan_wait, btrfs_ioctl_quota_ctl_args, btrfs_ioctl_quota_rescan_args,
};

/// Enable quota accounting on the filesystem referred to by `fd`.
///
/// When `simple` is `true`, uses `BTRFS_QUOTA_CTL_ENABLE_SIMPLE_QUOTA`, which
/// accounts for extent ownership by lifetime rather than backref walks. This is
/// faster but less precise than full qgroup accounting.
pub fn quota_enable(fd: BorrowedFd, simple: bool) -> nix::Result<()> {
    let cmd = if simple {
        BTRFS_QUOTA_CTL_ENABLE_SIMPLE_QUOTA as u64
    } else {
        BTRFS_QUOTA_CTL_ENABLE as u64
    };
    let mut args: btrfs_ioctl_quota_ctl_args = unsafe { mem::zeroed() };
    args.cmd = cmd;
    unsafe { btrfs_ioc_quota_ctl(fd.as_raw_fd(), &mut args) }?;
    Ok(())
}

/// Disable quota accounting on the filesystem referred to by `fd`.
pub fn quota_disable(fd: BorrowedFd) -> nix::Result<()> {
    let mut args: btrfs_ioctl_quota_ctl_args = unsafe { mem::zeroed() };
    args.cmd = BTRFS_QUOTA_CTL_DISABLE as u64;
    unsafe { btrfs_ioc_quota_ctl(fd.as_raw_fd(), &mut args) }?;
    Ok(())
}

/// Start a quota rescan on the filesystem referred to by `fd`.
///
/// Returns immediately after kicking off the background scan. Use
/// [`quota_rescan_wait`] to block until it finishes. If a rescan is already
/// in progress the kernel returns `EINPROGRESS`; callers that are about to
/// wait anyway can treat that as a non-error.
pub fn quota_rescan(fd: BorrowedFd) -> nix::Result<()> {
    let args: btrfs_ioctl_quota_rescan_args = unsafe { mem::zeroed() };
    unsafe { btrfs_ioc_quota_rescan(fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Block until the quota rescan currently running on the filesystem referred
/// to by `fd` completes. Returns immediately if no rescan is in progress.
pub fn quota_rescan_wait(fd: BorrowedFd) -> nix::Result<()> {
    unsafe { btrfs_ioc_quota_rescan_wait(fd.as_raw_fd()) }?;
    Ok(())
}

/// Status of an in-progress (or absent) quota rescan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRescanStatus {
    /// Whether a rescan is currently running.
    pub running: bool,
    /// Object ID of the most recently scanned tree item. Only meaningful
    /// when `running` is `true`.
    pub progress: u64,
}

/// Query the status of the quota rescan on the filesystem referred to by `fd`.
pub fn quota_rescan_status(fd: BorrowedFd) -> nix::Result<QuotaRescanStatus> {
    let mut args: btrfs_ioctl_quota_rescan_args = unsafe { mem::zeroed() };
    unsafe { btrfs_ioc_quota_rescan_status(fd.as_raw_fd(), &mut args) }?;
    Ok(QuotaRescanStatus {
        running: args.flags != 0,
        progress: args.progress,
    })
}

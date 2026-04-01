//! # Device replacement: replacing a device with another while the filesystem is online
//!
//! A replace operation copies all data from a source device to a target device,
//! then swaps the target into the filesystem in place of the source. The
//! filesystem remains mounted and usable throughout.
//!
//! Requires `CAP_SYS_ADMIN`.

use crate::raw::{
    BTRFS_IOCTL_DEV_REPLACE_CMD_CANCEL, BTRFS_IOCTL_DEV_REPLACE_CMD_START,
    BTRFS_IOCTL_DEV_REPLACE_CMD_STATUS,
    BTRFS_IOCTL_DEV_REPLACE_CONT_READING_FROM_SRCDEV_MODE_ALWAYS,
    BTRFS_IOCTL_DEV_REPLACE_CONT_READING_FROM_SRCDEV_MODE_AVOID,
    BTRFS_IOCTL_DEV_REPLACE_RESULT_ALREADY_STARTED,
    BTRFS_IOCTL_DEV_REPLACE_RESULT_NO_ERROR,
    BTRFS_IOCTL_DEV_REPLACE_RESULT_NOT_STARTED,
    BTRFS_IOCTL_DEV_REPLACE_RESULT_SCRUB_INPROGRESS,
    BTRFS_IOCTL_DEV_REPLACE_STATE_CANCELED,
    BTRFS_IOCTL_DEV_REPLACE_STATE_FINISHED,
    BTRFS_IOCTL_DEV_REPLACE_STATE_NEVER_STARTED,
    BTRFS_IOCTL_DEV_REPLACE_STATE_STARTED,
    BTRFS_IOCTL_DEV_REPLACE_STATE_SUSPENDED, btrfs_ioc_dev_replace,
    btrfs_ioctl_dev_replace_args,
};
use nix::errno::Errno;
use std::{
    ffi::CStr,
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Current state of a device replace operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceState {
    NeverStarted,
    Started,
    Finished,
    Canceled,
    Suspended,
}

impl ReplaceState {
    fn from_raw(val: u64) -> Option<ReplaceState> {
        match val {
            x if x
                == u64::from(BTRFS_IOCTL_DEV_REPLACE_STATE_NEVER_STARTED) =>
            {
                Some(ReplaceState::NeverStarted)
            }
            x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_STATE_STARTED) => {
                Some(ReplaceState::Started)
            }
            x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_STATE_FINISHED) => {
                Some(ReplaceState::Finished)
            }
            x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_STATE_CANCELED) => {
                Some(ReplaceState::Canceled)
            }
            x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_STATE_SUSPENDED) => {
                Some(ReplaceState::Suspended)
            }
            _ => None,
        }
    }
}

/// Status of a device replace operation, as returned by the status query.
#[derive(Debug, Clone)]
pub struct ReplaceStatus {
    /// Current state of the replace operation.
    pub state: ReplaceState,
    /// Progress in tenths of a percent (0..=1000).
    pub progress_1000: u64,
    /// Time the replace operation was started.
    pub time_started: Option<SystemTime>,
    /// Time the replace operation stopped (finished, canceled, or suspended).
    pub time_stopped: Option<SystemTime>,
    /// Number of write errors encountered during the replace.
    pub num_write_errors: u64,
    /// Number of uncorrectable read errors encountered during the replace.
    pub num_uncorrectable_read_errors: u64,
}

fn epoch_to_systemtime(secs: u64) -> Option<SystemTime> {
    if secs == 0 {
        None
    } else {
        Some(UNIX_EPOCH + Duration::from_secs(secs))
    }
}

/// How to identify the source device for a replace operation.
pub enum ReplaceSource<'a> {
    /// Source device identified by its btrfs device ID.
    DevId(u64),
    /// Source device identified by its block device path.
    Path(&'a CStr),
}

/// Query the status of a device replace operation on the filesystem referred
/// to by `fd`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn replace_status(fd: BorrowedFd) -> nix::Result<ReplaceStatus> {
    let mut args: btrfs_ioctl_dev_replace_args = unsafe { mem::zeroed() };
    args.cmd = u64::from(BTRFS_IOCTL_DEV_REPLACE_CMD_STATUS);

    unsafe { btrfs_ioc_dev_replace(fd.as_raw_fd(), &raw mut args) }?;

    // SAFETY: we issued CMD_STATUS so the status union member is active.
    let status = unsafe { &args.__bindgen_anon_1.status };
    let state =
        ReplaceState::from_raw(status.replace_state).ok_or(Errno::EINVAL)?;

    Ok(ReplaceStatus {
        state,
        progress_1000: status.progress_1000,
        time_started: epoch_to_systemtime(status.time_started),
        time_stopped: epoch_to_systemtime(status.time_stopped),
        num_write_errors: status.num_write_errors,
        num_uncorrectable_read_errors: status.num_uncorrectable_read_errors,
    })
}

/// Result of a replace start attempt that the kernel rejected at the
/// application level (ioctl succeeded but the `result` field indicates a
/// problem).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceStartError {
    /// A replace operation is already in progress.
    AlreadyStarted,
    /// A scrub is in progress and must finish before replace can start.
    ScrubInProgress,
}

impl std::fmt::Display for ReplaceStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplaceStartError::AlreadyStarted => {
                write!(f, "a device replace operation is already in progress")
            }
            ReplaceStartError::ScrubInProgress => {
                write!(f, "a scrub is in progress; cancel it first")
            }
        }
    }
}

impl std::error::Error for ReplaceStartError {}

/// Start a device replace operation, copying all data from `source` to the
/// target device at `tgtdev_path`.
///
/// When `avoid_srcdev` is true, the kernel will only read from the source
/// device when no other zero-defect mirror is available (useful for replacing
/// a device with known read errors).
///
/// Returns a two-level Result: the outer `nix::Result` covers ioctl-level
/// failures (EPERM, EINVAL, etc.), while the inner `Result` covers
/// application-level rejections reported by the kernel in the `result` field.
/// `Ok(Ok(()))` means the replace started successfully.
/// `Ok(Err(AlreadyStarted))` means another replace is in progress.
/// `Ok(Err(ScrubInProgress))` means a scrub must finish or be cancelled first.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails. `ENAMETOOLONG` if device paths exceed
/// the kernel buffer size.
pub fn replace_start(
    fd: BorrowedFd,
    source: &ReplaceSource<'_>,
    tgtdev_path: &CStr,
    avoid_srcdev: bool,
) -> nix::Result<Result<(), ReplaceStartError>> {
    let mut args: btrfs_ioctl_dev_replace_args = unsafe { mem::zeroed() };
    args.cmd = u64::from(BTRFS_IOCTL_DEV_REPLACE_CMD_START);

    // SAFETY: we are filling in the start union member before issuing CMD_START.
    let start = unsafe { &mut args.__bindgen_anon_1.start };

    match *source {
        ReplaceSource::DevId(devid) => {
            start.srcdevid = devid;
        }
        ReplaceSource::Path(path) => {
            start.srcdevid = 0;
            let bytes = path.to_bytes();
            if bytes.len() >= start.srcdev_name.len() {
                return Err(Errno::ENAMETOOLONG);
            }
            start.srcdev_name[..bytes.len()].copy_from_slice(bytes);
        }
    }

    let tgt_bytes = tgtdev_path.to_bytes();
    if tgt_bytes.len() >= start.tgtdev_name.len() {
        return Err(Errno::ENAMETOOLONG);
    }
    start.tgtdev_name[..tgt_bytes.len()].copy_from_slice(tgt_bytes);

    start.cont_reading_from_srcdev_mode = if avoid_srcdev {
        u64::from(BTRFS_IOCTL_DEV_REPLACE_CONT_READING_FROM_SRCDEV_MODE_AVOID)
    } else {
        u64::from(BTRFS_IOCTL_DEV_REPLACE_CONT_READING_FROM_SRCDEV_MODE_ALWAYS)
    };

    unsafe { btrfs_ioc_dev_replace(fd.as_raw_fd(), &raw mut args) }?;

    match args.result {
        x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_RESULT_NO_ERROR) => {
            Ok(Ok(()))
        }
        x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_RESULT_ALREADY_STARTED) => {
            Ok(Err(ReplaceStartError::AlreadyStarted))
        }
        x if x
            == u64::from(BTRFS_IOCTL_DEV_REPLACE_RESULT_SCRUB_INPROGRESS) =>
        {
            Ok(Err(ReplaceStartError::ScrubInProgress))
        }
        _ => Err(Errno::EINVAL),
    }
}

/// Cancel a running device replace operation on the filesystem referred to
/// by `fd`.
///
/// Returns `Ok(true)` if the replace was successfully cancelled, or
/// `Ok(false)` if no replace operation was in progress.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn replace_cancel(fd: BorrowedFd) -> nix::Result<bool> {
    let mut args: btrfs_ioctl_dev_replace_args = unsafe { mem::zeroed() };
    args.cmd = u64::from(BTRFS_IOCTL_DEV_REPLACE_CMD_CANCEL);

    unsafe { btrfs_ioc_dev_replace(fd.as_raw_fd(), &raw mut args) }?;

    match args.result {
        x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_RESULT_NO_ERROR) => {
            Ok(true)
        }
        x if x == u64::from(BTRFS_IOCTL_DEV_REPLACE_RESULT_NOT_STARTED) => {
            Ok(false)
        }
        _ => Err(Errno::EINVAL),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- epoch_to_systemtime ---

    #[test]
    fn epoch_zero_is_none() {
        assert!(epoch_to_systemtime(0).is_none());
    }

    #[test]
    fn epoch_nonzero_is_some() {
        let t = epoch_to_systemtime(1700000000).unwrap();
        assert_eq!(t, UNIX_EPOCH + Duration::from_secs(1700000000));
    }

    // --- ReplaceState::from_raw ---

    #[test]
    fn replace_state_from_raw_all_variants() {
        assert!(matches!(
            ReplaceState::from_raw(
                BTRFS_IOCTL_DEV_REPLACE_STATE_NEVER_STARTED as u64
            ),
            Some(ReplaceState::NeverStarted)
        ));
        assert!(matches!(
            ReplaceState::from_raw(
                BTRFS_IOCTL_DEV_REPLACE_STATE_STARTED as u64
            ),
            Some(ReplaceState::Started)
        ));
        assert!(matches!(
            ReplaceState::from_raw(
                BTRFS_IOCTL_DEV_REPLACE_STATE_FINISHED as u64
            ),
            Some(ReplaceState::Finished)
        ));
        assert!(matches!(
            ReplaceState::from_raw(
                BTRFS_IOCTL_DEV_REPLACE_STATE_CANCELED as u64
            ),
            Some(ReplaceState::Canceled)
        ));
        assert!(matches!(
            ReplaceState::from_raw(
                BTRFS_IOCTL_DEV_REPLACE_STATE_SUSPENDED as u64
            ),
            Some(ReplaceState::Suspended)
        ));
    }

    #[test]
    fn replace_state_from_raw_unknown() {
        assert!(ReplaceState::from_raw(9999).is_none());
    }

    // --- ReplaceStartError Display ---

    #[test]
    fn replace_start_error_display() {
        assert_eq!(
            format!("{}", ReplaceStartError::AlreadyStarted),
            "a device replace operation is already in progress"
        );
        assert_eq!(
            format!("{}", ReplaceStartError::ScrubInProgress),
            "a scrub is in progress; cancel it first"
        );
    }
}

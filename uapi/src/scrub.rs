//! # Data integrity scrubbing: verifying and repairing filesystem checksums
//!
//! A scrub reads every data and metadata block on the filesystem, verifies it
//! against its stored checksum, and repairs any errors it finds using redundant
//! copies where available (e.g. RAID profiles).  Scrubbing is the primary way
//! to proactively detect silent data corruption.
//!
//! Requires `CAP_SYS_ADMIN`.

use crate::raw::{
    BTRFS_SCRUB_READONLY, btrfs_ioc_scrub, btrfs_ioc_scrub_cancel,
    btrfs_ioc_scrub_progress, btrfs_ioctl_scrub_args,
};
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

/// Progress counters for a scrub operation, as returned by `BTRFS_IOC_SCRUB`
/// or `BTRFS_IOC_SCRUB_PROGRESS`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScrubProgress {
    /// Number of data extents scrubbed.
    pub data_extents_scrubbed: u64,
    /// Number of tree (metadata) extents scrubbed.
    pub tree_extents_scrubbed: u64,
    /// Number of data bytes scrubbed.
    pub data_bytes_scrubbed: u64,
    /// Number of tree (metadata) bytes scrubbed.
    pub tree_bytes_scrubbed: u64,
    /// Number of read errors encountered.
    pub read_errors: u64,
    /// Number of checksum errors.
    pub csum_errors: u64,
    /// Number of metadata verification errors.
    pub verify_errors: u64,
    /// Number of data blocks with no checksum.
    pub no_csum: u64,
    /// Number of checksums with no corresponding data extent.
    pub csum_discards: u64,
    /// Number of bad superblock copies encountered.
    pub super_errors: u64,
    /// Number of internal memory allocation errors.
    pub malloc_errors: u64,
    /// Number of errors that could not be corrected.
    pub uncorrectable_errors: u64,
    /// Number of errors that were successfully corrected.
    pub corrected_errors: u64,
    /// Last physical byte address scrubbed (useful for resuming).
    pub last_physical: u64,
    /// Number of transient read errors (re-read succeeded).
    pub unverified_errors: u64,
}

impl ScrubProgress {
    /// Total number of hard errors (read, super, verify, checksum).
    pub fn error_count(&self) -> u64 {
        self.read_errors
            + self.super_errors
            + self.verify_errors
            + self.csum_errors
    }

    /// Total bytes scrubbed (data + tree).
    pub fn bytes_scrubbed(&self) -> u64 {
        self.data_bytes_scrubbed + self.tree_bytes_scrubbed
    }

    /// Returns `true` if no errors of any kind were found.
    pub fn is_clean(&self) -> bool {
        self.error_count() == 0
            && self.corrected_errors == 0
            && self.uncorrectable_errors == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_progress_default_is_clean() {
        let p = ScrubProgress::default();
        assert!(p.is_clean());
        assert_eq!(p.error_count(), 0);
        assert_eq!(p.bytes_scrubbed(), 0);
    }

    #[test]
    fn scrub_progress_error_count() {
        let p = ScrubProgress {
            read_errors: 1,
            super_errors: 2,
            verify_errors: 3,
            csum_errors: 4,
            ..ScrubProgress::default()
        };
        assert_eq!(p.error_count(), 10);
        assert!(!p.is_clean());
    }

    #[test]
    fn scrub_progress_bytes_scrubbed() {
        let p = ScrubProgress {
            data_bytes_scrubbed: 1000,
            tree_bytes_scrubbed: 500,
            ..ScrubProgress::default()
        };
        assert_eq!(p.bytes_scrubbed(), 1500);
    }

    #[test]
    fn scrub_progress_corrected_errors_not_clean() {
        let p = ScrubProgress {
            corrected_errors: 1,
            ..ScrubProgress::default()
        };
        assert!(!p.is_clean());
        assert_eq!(p.error_count(), 0); // error_count doesn't include corrected
    }

    #[test]
    fn scrub_progress_uncorrectable_errors_not_clean() {
        let p = ScrubProgress {
            uncorrectable_errors: 1,
            ..ScrubProgress::default()
        };
        assert!(!p.is_clean());
    }
}

fn from_raw(raw: &btrfs_ioctl_scrub_args) -> ScrubProgress {
    let p = &raw.progress;
    ScrubProgress {
        data_extents_scrubbed: p.data_extents_scrubbed,
        tree_extents_scrubbed: p.tree_extents_scrubbed,
        data_bytes_scrubbed: p.data_bytes_scrubbed,
        tree_bytes_scrubbed: p.tree_bytes_scrubbed,
        read_errors: p.read_errors,
        csum_errors: p.csum_errors,
        verify_errors: p.verify_errors,
        no_csum: p.no_csum,
        csum_discards: p.csum_discards,
        super_errors: p.super_errors,
        malloc_errors: p.malloc_errors,
        uncorrectable_errors: p.uncorrectable_errors,
        corrected_errors: p.corrected_errors,
        last_physical: p.last_physical,
        unverified_errors: p.unverified_errors,
    }
}

/// Start a scrub on the device identified by `devid` within the filesystem
/// referred to by `fd`.
///
/// This call **blocks** until the scrub completes or is cancelled. On
/// completion the final [`ScrubProgress`] counters are returned.
///
/// Set `readonly` to `true` to check for errors without attempting repairs.
pub fn scrub_start(
    fd: BorrowedFd,
    devid: u64,
    readonly: bool,
) -> nix::Result<ScrubProgress> {
    let mut args: btrfs_ioctl_scrub_args = unsafe { mem::zeroed() };
    args.devid = devid;
    args.start = 0;
    args.end = u64::MAX;
    if readonly {
        args.flags = BTRFS_SCRUB_READONLY as u64;
    }
    unsafe { btrfs_ioc_scrub(fd.as_raw_fd(), &mut args) }?;
    Ok(from_raw(&args))
}

/// Cancel the scrub currently running on the filesystem referred to by `fd`.
pub fn scrub_cancel(fd: BorrowedFd) -> nix::Result<()> {
    unsafe { btrfs_ioc_scrub_cancel(fd.as_raw_fd()) }?;
    Ok(())
}

/// Query the progress of the scrub currently running on the device identified
/// by `devid` within the filesystem referred to by `fd`.
///
/// Returns `None` if no scrub is running on that device (`ENOTCONN`).
pub fn scrub_progress(
    fd: BorrowedFd,
    devid: u64,
) -> nix::Result<Option<ScrubProgress>> {
    let mut args: btrfs_ioctl_scrub_args = unsafe { mem::zeroed() };
    args.devid = devid;
    args.start = 0;
    args.end = u64::MAX;

    match unsafe { btrfs_ioc_scrub_progress(fd.as_raw_fd(), &mut args) } {
        Err(nix::errno::Errno::ENOTCONN) => Ok(None),
        Err(e) => Err(e),
        Ok(_) => Ok(Some(from_raw(&args))),
    }
}

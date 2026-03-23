//! Safe high-level wrappers for the btrfs balance ioctls.
//!
//! This module provides three public functions:
//! - [`balance`] — start or resume a balance operation
//! - [`balance_ctl`] — pause or cancel an in-progress balance
//! - [`balance_progress`] — query the current balance state and progress

use std::os::fd::AsRawFd;
use std::os::unix::io::BorrowedFd;

use bitflags::bitflags;
use nix::libc::c_int;

use crate::ioctls::*;

// ---------------------------------------------------------------------------
// Flag types
// ---------------------------------------------------------------------------

bitflags! {
    /// Top-level flags for a balance operation (`btrfs_ioctl_balance_args.flags`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BalanceFlags: u64 {
        /// Balance data chunks.
        const DATA     = BTRFS_BALANCE_DATA as u64;
        /// Balance system chunks.
        const SYSTEM   = BTRFS_BALANCE_SYSTEM as u64;
        /// Balance metadata chunks.
        const METADATA = BTRFS_BALANCE_METADATA as u64;
        /// Force a balance even if the device is busy.
        const FORCE    = BTRFS_BALANCE_FORCE as u64;
        /// Resume a previously paused balance.
        const RESUME   = BTRFS_BALANCE_RESUME as u64;
    }
}

bitflags! {
    /// Per-chunk-type filter flags (`btrfs_balance_args.flags`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BalanceArgsFlags: u64 {
        /// Filter by chunk profiles.
        const PROFILES       = BTRFS_BALANCE_ARGS_PROFILES as u64;
        /// Filter by usage (single value, 0..N percent).
        const USAGE          = BTRFS_BALANCE_ARGS_USAGE as u64;
        /// Filter by usage range (min..max percent).
        const USAGE_RANGE    = BTRFS_BALANCE_ARGS_USAGE_RANGE as u64;
        /// Filter by device ID.
        const DEVID          = BTRFS_BALANCE_ARGS_DEVID as u64;
        /// Filter by physical byte range on device.
        const DRANGE         = BTRFS_BALANCE_ARGS_DRANGE as u64;
        /// Filter by virtual byte range.
        const VRANGE         = BTRFS_BALANCE_ARGS_VRANGE as u64;
        /// Limit number of chunks processed (single value).
        const LIMIT          = BTRFS_BALANCE_ARGS_LIMIT as u64;
        /// Limit number of chunks processed (min..max range).
        const LIMIT_RANGE    = BTRFS_BALANCE_ARGS_LIMIT_RANGE as u64;
        /// Filter by stripe count range.
        const STRIPES_RANGE  = BTRFS_BALANCE_ARGS_STRIPES_RANGE as u64;
        /// Convert chunks to a different profile.
        const CONVERT        = BTRFS_BALANCE_ARGS_CONVERT as u64;
        /// Soft convert: skip chunks already on the target profile.
        const SOFT           = BTRFS_BALANCE_ARGS_SOFT as u64;
    }
}

bitflags! {
    /// State flags returned by the kernel (`btrfs_ioctl_balance_args.state`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BalanceState: u64 {
        /// A balance is currently running.
        const RUNNING    = BTRFS_BALANCE_STATE_RUNNING as u64;
        /// A pause has been requested.
        const PAUSE_REQ  = BTRFS_BALANCE_STATE_PAUSE_REQ as u64;
        /// A cancellation has been requested.
        const CANCEL_REQ = BTRFS_BALANCE_STATE_CANCEL_REQ as u64;
    }
}

// ---------------------------------------------------------------------------
// BalanceArgs builder
// ---------------------------------------------------------------------------

/// Per-type filter arguments for a balance operation, corresponding to
/// `btrfs_balance_args`.
///
/// Construct one with [`BalanceArgs::new`] and chain the setter methods to
/// enable filters. Each setter automatically sets the corresponding flag bit.
#[derive(Clone)]
pub struct BalanceArgs {
    raw: btrfs_balance_args,
}

impl std::fmt::Debug for BalanceArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // btrfs_balance_args is __attribute__((packed)), so we must copy fields
        // to locals before taking references to them.
        let flags = self.raw.flags;
        let profiles = self.raw.profiles;
        let devid = self.raw.devid;
        let pstart = self.raw.pstart;
        let pend = self.raw.pend;
        let vstart = self.raw.vstart;
        let vend = self.raw.vend;
        let target = self.raw.target;
        let stripes_min = self.raw.stripes_min;
        let stripes_max = self.raw.stripes_max;
        f.debug_struct("BalanceArgs")
            .field("flags", &flags)
            .field("profiles", &profiles)
            .field("devid", &devid)
            .field("pstart", &pstart)
            .field("pend", &pend)
            .field("vstart", &vstart)
            .field("vend", &vend)
            .field("target", &target)
            .field("stripes_min", &stripes_min)
            .field("stripes_max", &stripes_max)
            .finish()
    }
}

impl Default for BalanceArgs {
    fn default() -> Self {
        Self {
            raw: unsafe { std::mem::zeroed() },
        }
    }
}

impl BalanceArgs {
    /// Create a new `BalanceArgs` with no filters enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by chunk profile bitmask. The value is a bitmask of
    /// `BTRFS_BLOCK_GROUP_*` profile flags.
    pub fn profiles(mut self, profiles: u64) -> Self {
        self.raw.profiles = profiles;
        self.raw.flags |= BalanceArgsFlags::PROFILES.bits();
        self
    }

    /// Filter chunks whose usage is below `percent` (0..100).
    pub fn usage(mut self, percent: u64) -> Self {
        // SAFETY: the union field `usage` and `usage_min`/`usage_max` overlap;
        // setting `usage` covers the whole 8-byte field.
        self.raw.__bindgen_anon_1.usage = percent;
        self.raw.flags |= BalanceArgsFlags::USAGE.bits();
        self
    }

    /// Filter chunks whose usage falls in `min..=max` percent.
    pub fn usage_range(mut self, min: u32, max: u32) -> Self {
        self.raw.__bindgen_anon_1.__bindgen_anon_1.usage_min = min;
        self.raw.__bindgen_anon_1.__bindgen_anon_1.usage_max = max;
        self.raw.flags |= BalanceArgsFlags::USAGE_RANGE.bits();
        self
    }

    /// Filter chunks that reside on the given device ID.
    pub fn devid(mut self, devid: u64) -> Self {
        self.raw.devid = devid;
        self.raw.flags |= BalanceArgsFlags::DEVID.bits();
        self
    }

    /// Filter chunks whose physical range on-disk overlaps `start..end`.
    pub fn drange(mut self, start: u64, end: u64) -> Self {
        self.raw.pstart = start;
        self.raw.pend = end;
        self.raw.flags |= BalanceArgsFlags::DRANGE.bits();
        self
    }

    /// Filter chunks whose virtual address range overlaps `start..end`.
    pub fn vrange(mut self, start: u64, end: u64) -> Self {
        self.raw.vstart = start;
        self.raw.vend = end;
        self.raw.flags |= BalanceArgsFlags::VRANGE.bits();
        self
    }

    /// Process at most `limit` chunks.
    pub fn limit(mut self, limit: u64) -> Self {
        self.raw.__bindgen_anon_2.limit = limit;
        self.raw.flags |= BalanceArgsFlags::LIMIT.bits();
        self
    }

    /// Process between `min` and `max` chunks.
    pub fn limit_range(mut self, min: u32, max: u32) -> Self {
        self.raw.__bindgen_anon_2.__bindgen_anon_1.limit_min = min;
        self.raw.__bindgen_anon_2.__bindgen_anon_1.limit_max = max;
        self.raw.flags |= BalanceArgsFlags::LIMIT_RANGE.bits();
        self
    }

    /// Filter chunks that span between `min` and `max` stripes.
    pub fn stripes_range(mut self, min: u32, max: u32) -> Self {
        self.raw.stripes_min = min;
        self.raw.stripes_max = max;
        self.raw.flags |= BalanceArgsFlags::STRIPES_RANGE.bits();
        self
    }

    /// Convert balanced chunks to the given profile.
    pub fn convert(mut self, profile: u64) -> Self {
        self.raw.target = profile;
        self.raw.flags |= BalanceArgsFlags::CONVERT.bits();
        self
    }

    /// When converting, skip chunks already on the target profile.
    pub fn soft(mut self) -> Self {
        self.raw.flags |= BalanceArgsFlags::SOFT.bits();
        self
    }
}

// ---------------------------------------------------------------------------
// BalanceProgress
// ---------------------------------------------------------------------------

/// Progress counters returned by the kernel for an in-progress balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BalanceProgress {
    /// Estimated number of chunks that will be relocated.
    pub expected: u64,
    /// Number of chunks considered so far.
    pub considered: u64,
    /// Number of chunks relocated so far.
    pub completed: u64,
}

// ---------------------------------------------------------------------------
// BalanceCtl
// ---------------------------------------------------------------------------

/// Control command for [`balance_ctl`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceCtl {
    /// Pause the running balance at the next safe point.
    Pause,
    /// Cancel the running balance.
    Cancel,
}

impl BalanceCtl {
    fn as_raw(self) -> c_int {
        match self {
            BalanceCtl::Pause => BTRFS_BALANCE_CTL_PAUSE as c_int,
            BalanceCtl::Cancel => BTRFS_BALANCE_CTL_CANCEL as c_int,
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start or resume a balance operation on the filesystem referred to by `fd`.
///
/// `flags` controls which chunk types are balanced and whether to force or
/// resume. `data`, `meta`, and `sys` are optional per-type filter arguments;
/// pass `None` to use defaults for that type.
///
/// On success, returns the progress counters as reported by the kernel after
/// the operation completes (or is paused/interrupted).
pub fn balance(
    fd: BorrowedFd,
    flags: BalanceFlags,
    data: Option<BalanceArgs>,
    meta: Option<BalanceArgs>,
    sys: Option<BalanceArgs>,
) -> nix::Result<BalanceProgress> {
    let mut args: btrfs_ioctl_balance_args = unsafe { std::mem::zeroed() };

    args.flags = flags.bits();

    if let Some(a) = data {
        args.data = a.raw;
    }
    if let Some(a) = meta {
        args.meta = a.raw;
    }
    if let Some(a) = sys {
        args.sys = a.raw;
    }

    unsafe {
        btrfs_ioc_balance_v2(fd.as_raw_fd(), &mut args)?;
    }

    Ok(BalanceProgress {
        expected: args.stat.expected,
        considered: args.stat.considered,
        completed: args.stat.completed,
    })
}

/// Send a control command to a running balance operation on the filesystem
/// referred to by `fd`.
///
/// Use [`BalanceCtl::Pause`] to pause or [`BalanceCtl::Cancel`] to cancel.
pub fn balance_ctl(fd: BorrowedFd, cmd: BalanceCtl) -> nix::Result<()> {
    unsafe {
        btrfs_ioc_balance_ctl(fd.as_raw_fd(), cmd.as_raw() as u64)?;
    }
    Ok(())
}

/// Query the current balance state and progress on the filesystem referred to
/// by `fd`.
///
/// Returns a [`BalanceState`] bitflags value indicating whether a balance is
/// running, paused, or being cancelled, along with a [`BalanceProgress`] with
/// the current counters.
pub fn balance_progress(fd: BorrowedFd) -> nix::Result<(BalanceState, BalanceProgress)> {
    let mut args: btrfs_ioctl_balance_args = unsafe { std::mem::zeroed() };

    unsafe {
        btrfs_ioc_balance_progress(fd.as_raw_fd(), &mut args)?;
    }

    let state = BalanceState::from_bits_truncate(args.state);
    let progress = BalanceProgress {
        expected: args.stat.expected,
        considered: args.stat.considered,
        completed: args.stat.completed,
    };

    Ok((state, progress))
}

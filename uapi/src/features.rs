//! # Feature flags: querying and setting filesystem feature flags
//!
//! Wraps `BTRFS_IOC_GET_FEATURES` and `BTRFS_IOC_GET_SUPPORTED_FEATURES`
//! to query the active and supported feature flags of a mounted btrfs
//! filesystem.

use crate::raw::{
    btrfs_ioc_get_features, btrfs_ioc_get_supported_features,
    btrfs_ioctl_feature_flags,
};
use bitflags::bitflags;
use nix::libc::c_int;
use std::os::fd::{AsRawFd, BorrowedFd};

bitflags! {
    /// Compatible read-only feature flags (`compat_ro_flags`).
    ///
    /// These features are backward-compatible for read operations: a kernel
    /// that does not understand a compat_ro flag can still mount the
    /// filesystem read-only.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CompatRoFlags: u64 {
        const FREE_SPACE_TREE =
            crate::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64;
        const FREE_SPACE_TREE_VALID =
            crate::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID as u64;
        const VERITY =
            crate::raw::BTRFS_FEATURE_COMPAT_RO_VERITY as u64;
        const BLOCK_GROUP_TREE =
            crate::raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE as u64;
    }
}

bitflags! {
    /// Incompatible feature flags (`incompat_flags`).
    ///
    /// A filesystem with an incompat flag set cannot be mounted by a kernel
    /// that does not understand that flag.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IncompatFlags: u64 {
        const MIXED_BACKREF =
            crate::raw::BTRFS_FEATURE_INCOMPAT_MIXED_BACKREF as u64;
        const DEFAULT_SUBVOL =
            crate::raw::BTRFS_FEATURE_INCOMPAT_DEFAULT_SUBVOL as u64;
        const MIXED_GROUPS =
            crate::raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS as u64;
        const COMPRESS_LZO =
            crate::raw::BTRFS_FEATURE_INCOMPAT_COMPRESS_LZO as u64;
        const COMPRESS_ZSTD =
            crate::raw::BTRFS_FEATURE_INCOMPAT_COMPRESS_ZSTD as u64;
        const BIG_METADATA =
            crate::raw::BTRFS_FEATURE_INCOMPAT_BIG_METADATA as u64;
        const EXTENDED_IREF =
            crate::raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64;
        const RAID56 =
            crate::raw::BTRFS_FEATURE_INCOMPAT_RAID56 as u64;
        const SKINNY_METADATA =
            crate::raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64;
        const NO_HOLES =
            crate::raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64;
        const METADATA_UUID =
            crate::raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64;
        const RAID1C34 =
            crate::raw::BTRFS_FEATURE_INCOMPAT_RAID1C34 as u64;
        const ZONED =
            crate::raw::BTRFS_FEATURE_INCOMPAT_ZONED as u64;
        const EXTENT_TREE_V2 =
            crate::raw::BTRFS_FEATURE_INCOMPAT_EXTENT_TREE_V2 as u64;
        const RAID_STRIPE_TREE =
            crate::raw::BTRFS_FEATURE_INCOMPAT_RAID_STRIPE_TREE as u64;
        const SIMPLE_QUOTA =
            crate::raw::BTRFS_FEATURE_INCOMPAT_SIMPLE_QUOTA as u64;
        const REMAP_TREE =
            crate::raw::BTRFS_FEATURE_INCOMPAT_REMAP_TREE as u64;
    }
}

/// The set of feature flags active on a mounted filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureFlags {
    /// Compatible read-only feature flags.
    pub compat_ro: CompatRoFlags,
    /// Incompatible feature flags.
    pub incompat: IncompatFlags,
}

/// The feature flags supported by the running kernel.
///
/// Each category has three sets: `supported` (kernel understands the flag),
/// `safe_set` (can be enabled at runtime), and `safe_clear` (can be disabled
/// at runtime).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedFeatures {
    /// Compat_ro flags the kernel understands.
    pub compat_ro_supported: CompatRoFlags,
    /// Compat_ro flags that can be enabled at runtime.
    pub compat_ro_safe_set: CompatRoFlags,
    /// Compat_ro flags that can be disabled at runtime.
    pub compat_ro_safe_clear: CompatRoFlags,
    /// Incompat flags the kernel understands.
    pub incompat_supported: IncompatFlags,
    /// Incompat flags that can be enabled at runtime.
    pub incompat_safe_set: IncompatFlags,
    /// Incompat flags that can be disabled at runtime.
    pub incompat_safe_clear: IncompatFlags,
}

fn parse_feature_flags(raw: &btrfs_ioctl_feature_flags) -> FeatureFlags {
    FeatureFlags {
        compat_ro: CompatRoFlags::from_bits_truncate(raw.compat_ro_flags),
        incompat: IncompatFlags::from_bits_truncate(raw.incompat_flags),
    }
}

/// Query the feature flags currently active on the filesystem.
pub fn get_features(fd: BorrowedFd<'_>) -> nix::Result<FeatureFlags> {
    let mut flags: btrfs_ioctl_feature_flags = unsafe { std::mem::zeroed() };
    unsafe { btrfs_ioc_get_features(fd.as_raw_fd() as c_int, &mut flags) }?;
    Ok(parse_feature_flags(&flags))
}

/// Query the feature flags supported by the running kernel.
///
/// Returns three sets per category (compat_ro, incompat): which flags the
/// kernel understands, which can be enabled at runtime, and which can be
/// disabled at runtime.
pub fn get_supported_features(
    fd: BorrowedFd<'_>,
) -> nix::Result<SupportedFeatures> {
    let mut buf: [btrfs_ioctl_feature_flags; 3] = unsafe { std::mem::zeroed() };
    unsafe {
        btrfs_ioc_get_supported_features(fd.as_raw_fd() as c_int, &mut buf)
    }?;

    Ok(SupportedFeatures {
        compat_ro_supported: CompatRoFlags::from_bits_truncate(
            buf[0].compat_ro_flags,
        ),
        compat_ro_safe_set: CompatRoFlags::from_bits_truncate(
            buf[1].compat_ro_flags,
        ),
        compat_ro_safe_clear: CompatRoFlags::from_bits_truncate(
            buf[2].compat_ro_flags,
        ),
        incompat_supported: IncompatFlags::from_bits_truncate(
            buf[0].incompat_flags,
        ),
        incompat_safe_set: IncompatFlags::from_bits_truncate(
            buf[1].incompat_flags,
        ),
        incompat_safe_clear: IncompatFlags::from_bits_truncate(
            buf[2].incompat_flags,
        ),
    })
}

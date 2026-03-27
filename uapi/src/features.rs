//! # Feature flags: querying and setting filesystem feature flags
//!
//! Wraps `BTRFS_IOC_GET_FEATURES`, `BTRFS_IOC_GET_SUPPORTED_FEATURES`,
//! and `BTRFS_IOC_SET_FEATURES` to query and modify the feature flags of
//! a mounted btrfs filesystem.

use crate::raw::{
    btrfs_ioc_get_features, btrfs_ioc_get_supported_features,
    btrfs_ioc_set_features, btrfs_ioctl_feature_flags,
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

/// Set or clear feature flags on the filesystem.
///
/// The `flags` argument specifies the desired values, and `mask` specifies
/// which bits to change. Only bits set in `mask` are modified: they are set
/// to the corresponding value in `flags`. Bits not in the mask are left
/// unchanged.
///
/// Use `get_supported_features` first to check which flags can be safely
/// set or cleared at runtime.
///
/// Requires `CAP_SYS_ADMIN`. Returns `EPERM` without it.
pub fn set_features(
    fd: BorrowedFd<'_>,
    flags: &FeatureFlags,
    mask: &FeatureFlags,
) -> nix::Result<()> {
    let mut buf: [btrfs_ioctl_feature_flags; 2] = unsafe { std::mem::zeroed() };
    buf[0].compat_ro_flags = flags.compat_ro.bits();
    buf[0].incompat_flags = flags.incompat.bits();
    buf[1].compat_ro_flags = mask.compat_ro.bits();
    buf[1].incompat_flags = mask.incompat.bits();
    unsafe { btrfs_ioc_set_features(fd.as_raw_fd() as c_int, &buf) }?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_feature_flags_empty() {
        let raw = btrfs_ioctl_feature_flags {
            compat_flags: 0,
            compat_ro_flags: 0,
            incompat_flags: 0,
        };
        let flags = parse_feature_flags(&raw);
        assert!(flags.compat_ro.is_empty());
        assert!(flags.incompat.is_empty());
    }

    #[test]
    fn parse_feature_flags_known_bits() {
        let raw = btrfs_ioctl_feature_flags {
            compat_flags: 0,
            compat_ro_flags: crate::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE
                as u64,
            incompat_flags: crate::raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64
                | crate::raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64,
        };
        let flags = parse_feature_flags(&raw);
        assert!(flags.compat_ro.contains(CompatRoFlags::FREE_SPACE_TREE));
        assert!(flags.incompat.contains(IncompatFlags::NO_HOLES));
        assert!(flags.incompat.contains(IncompatFlags::SKINNY_METADATA));
    }

    #[test]
    fn parse_feature_flags_truncates_unknown() {
        let raw = btrfs_ioctl_feature_flags {
            compat_flags: 0,
            compat_ro_flags: 0xFFFF_FFFF_FFFF_FFFF,
            incompat_flags: 0xFFFF_FFFF_FFFF_FFFF,
        };
        let flags = parse_feature_flags(&raw);
        // Should not panic; unknown bits are silently dropped.
        assert!(!flags.compat_ro.is_empty());
        assert!(!flags.incompat.is_empty());
    }

    #[test]
    fn feature_flags_equality() {
        let a = FeatureFlags {
            compat_ro: CompatRoFlags::FREE_SPACE_TREE,
            incompat: IncompatFlags::NO_HOLES,
        };
        let b = FeatureFlags {
            compat_ro: CompatRoFlags::FREE_SPACE_TREE,
            incompat: IncompatFlags::NO_HOLES,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn feature_flags_inequality() {
        let a = FeatureFlags {
            compat_ro: CompatRoFlags::FREE_SPACE_TREE,
            incompat: IncompatFlags::NO_HOLES,
        };
        let b = FeatureFlags {
            compat_ro: CompatRoFlags::empty(),
            incompat: IncompatFlags::NO_HOLES,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn compat_ro_flags_debug() {
        let flags = CompatRoFlags::FREE_SPACE_TREE
            | CompatRoFlags::FREE_SPACE_TREE_VALID;
        let s = format!("{flags:?}");
        assert!(s.contains("FREE_SPACE_TREE"), "debug: {s}");
        assert!(s.contains("FREE_SPACE_TREE_VALID"), "debug: {s}");
    }

    #[test]
    fn incompat_flags_debug() {
        let flags = IncompatFlags::NO_HOLES | IncompatFlags::ZONED;
        let s = format!("{flags:?}");
        assert!(s.contains("NO_HOLES"), "debug: {s}");
        assert!(s.contains("ZONED"), "debug: {s}");
    }

    #[test]
    fn supported_features_struct_fields() {
        let supported = SupportedFeatures {
            compat_ro_supported: CompatRoFlags::FREE_SPACE_TREE,
            compat_ro_safe_set: CompatRoFlags::empty(),
            compat_ro_safe_clear: CompatRoFlags::empty(),
            incompat_supported: IncompatFlags::NO_HOLES,
            incompat_safe_set: IncompatFlags::empty(),
            incompat_safe_clear: IncompatFlags::empty(),
        };
        assert!(
            supported
                .compat_ro_supported
                .contains(CompatRoFlags::FREE_SPACE_TREE)
        );
        assert!(
            supported
                .incompat_supported
                .contains(IncompatFlags::NO_HOLES)
        );
        assert!(supported.incompat_safe_set.is_empty());
    }
}

use crate::common::single_mount;
use btrfs_uapi::features::{
    CompatRoFlags, FeatureFlags, IncompatFlags, get_features,
    get_supported_features, set_features,
};

/// get_features should return the active feature flags for a filesystem.
/// A freshly-created filesystem should have at least some incompat flags set.
#[test]
#[ignore = "requires elevated privileges"]
fn features_get() {
    let (_td, mnt) = single_mount();

    let flags = get_features(mnt.fd()).expect("get_features failed");

    // A modern mkfs always sets NO_HOLES and EXTENDED_IREF at minimum.
    assert!(
        flags.incompat.contains(IncompatFlags::NO_HOLES)
            || flags.incompat.contains(IncompatFlags::EXTENDED_IREF)
            || !flags.incompat.is_empty(),
        "expected at least some incompat flags on a new filesystem, got {:?}",
        flags.incompat,
    );
}

/// get_supported_features should return the kernel's supported feature set.
/// The supported set should be a superset of what's active on the filesystem.
#[test]
#[ignore = "requires elevated privileges"]
fn features_get_supported() {
    let (_td, mnt) = single_mount();

    let active = get_features(mnt.fd()).expect("get_features failed");
    let supported = get_supported_features(mnt.fd())
        .expect("get_supported_features failed");

    // Every active flag must be in the supported set.
    assert!(
        supported.incompat_supported.contains(active.incompat),
        "active incompat {:?} should be subset of supported {:?}",
        active.incompat,
        supported.incompat_supported,
    );
    assert!(
        supported.compat_ro_supported.contains(active.compat_ro),
        "active compat_ro {:?} should be subset of supported {:?}",
        active.compat_ro,
        supported.compat_ro_supported,
    );
}

/// set_features should be able to set a safe_set flag and read it back.
#[test]
#[ignore = "requires elevated privileges"]
fn features_set_and_get_roundtrip() {
    let (_td, mnt) = single_mount();

    let supported = get_supported_features(mnt.fd())
        .expect("get_supported_features failed");
    let before = get_features(mnt.fd()).expect("get_features failed");

    // Find a compat_ro flag that is safe to set and not already active.
    let settable = supported.compat_ro_safe_set & !before.compat_ro;
    if settable.is_empty() {
        // All safe_set flags are already active; try clearing one instead.
        let clearable = supported.compat_ro_safe_clear & before.compat_ro;
        if clearable.is_empty() {
            // Nothing we can toggle — skip rather than fail.
            eprintln!("no toggleable compat_ro flags available, skipping");
            return;
        }
        // Clear the first clearable flag.
        let flag = CompatRoFlags::from_bits_truncate(
            clearable.bits() & clearable.bits().wrapping_neg(),
        );
        let desired = FeatureFlags {
            compat_ro: before.compat_ro & !flag,
            incompat: before.incompat,
        };
        let mask = FeatureFlags {
            compat_ro: flag,
            incompat: IncompatFlags::empty(),
        };
        set_features(mnt.fd(), &desired, &mask)
            .expect("set_features (clear) failed");

        let after = get_features(mnt.fd()).expect("get_features failed");
        assert!(
            !after.compat_ro.contains(flag),
            "flag {flag:?} should have been cleared, got {:?}",
            after.compat_ro,
        );
    } else {
        // Set the first settable flag.
        let flag = CompatRoFlags::from_bits_truncate(
            settable.bits() & settable.bits().wrapping_neg(),
        );
        let desired = FeatureFlags {
            compat_ro: before.compat_ro | flag,
            incompat: before.incompat,
        };
        let mask = FeatureFlags {
            compat_ro: flag,
            incompat: IncompatFlags::empty(),
        };
        set_features(mnt.fd(), &desired, &mask)
            .expect("set_features (set) failed");

        let after = get_features(mnt.fd()).expect("get_features failed");
        assert!(
            after.compat_ro.contains(flag),
            "flag {flag:?} should have been set, got {:?}",
            after.compat_ro,
        );
    }
}

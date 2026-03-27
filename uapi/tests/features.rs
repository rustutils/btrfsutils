use crate::common::single_mount;
use btrfs_uapi::features::{
    IncompatFlags, get_features, get_supported_features,
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

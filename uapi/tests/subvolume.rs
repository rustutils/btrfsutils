use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    subvolume::{
        SubvolumeFlags, snapshot_create, subvolume_create, subvolume_default_get,
        subvolume_default_set, subvolume_delete, subvolume_flags_get, subvolume_flags_set,
        subvolume_info, subvolume_list,
    },
    filesystem::sync,
};
use std::{ffi::CStr, fs::File, os::unix::io::AsFd};

/// Creating a subvolume, querying its info, and deleting it should work.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_info_delete() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    // subvolume_info should return valid metadata.
    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    assert!(
        info.id > 255,
        "subvolume ID should be > 255, got {}",
        info.id
    );
    assert!(!info.uuid.is_nil(), "subvolume UUID should not be nil");
    drop(subvol_dir);

    // Delete the subvolume.
    subvolume_delete(mnt.fd(), name).expect("subvolume_delete failed");

    // Opening the subvolume should now fail.
    assert!(
        File::open(mnt.path().join("test-subvol")).is_err(),
        "opening deleted subvolume should fail",
    );
}

/// Snapshotting a subvolume should produce an independent copy: modifying the
/// original should not affect the snapshot.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_snapshot() {
    let (_td, mnt) = single_mount();

    let origin_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), origin_name, &[]).expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    // Create a snapshot.
    let snap_name = CStr::from_bytes_with_nul(b"snap1\0").unwrap();
    let origin_dir = File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, false, &[])
        .expect("snapshot_create failed");
    drop(origin_dir);

    // Snapshot should have the same data.
    crate::common::verify_test_data(&mnt.path().join("snap1"), "data.bin", 1_000_000);

    // Modify the original — snapshot should retain the old content.
    std::fs::write(mnt.path().join("origin").join("data.bin"), b"overwritten")
        .expect("overwrite failed");

    // Snapshot should still have the original data.
    crate::common::verify_test_data(&mnt.path().join("snap1"), "data.bin", 1_000_000);
}

/// A readonly snapshot should have the RDONLY flag and refuse writes.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_readonly_snapshot() {
    let (_td, mnt) = single_mount();

    let origin_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), origin_name, &[]).expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    let snap_name = CStr::from_bytes_with_nul(b"ro-snap\0").unwrap();
    let origin_dir = File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, true, &[]).expect("snapshot_create failed");
    drop(origin_dir);

    let snap_dir = File::open(mnt.path().join("ro-snap")).expect("open snap failed");
    let flags = subvolume_flags_get(snap_dir.as_fd()).expect("subvolume_flags_get failed");
    assert!(
        flags.contains(SubvolumeFlags::RDONLY),
        "readonly snapshot should have RDONLY flag, got {flags:?}",
    );

    // Writing to the snapshot should fail.
    let write_result = File::create(mnt.path().join("ro-snap").join("new-file.txt"));
    assert!(
        write_result.is_err(),
        "writing to readonly snapshot should fail"
    );
}

/// subvolume_list should return all subvolumes with correct names.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_test() {
    let (_td, mnt) = single_mount();

    for name in [
        CStr::from_bytes_with_nul(b"alpha\0").unwrap(),
        CStr::from_bytes_with_nul(b"beta\0").unwrap(),
        CStr::from_bytes_with_nul(b"gamma\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    let list = subvolume_list(mnt.fd()).expect("subvolume_list failed");

    // Should find all three subvolumes.
    for name in ["alpha", "beta", "gamma"] {
        assert!(
            list.iter().any(|item| item.name == name),
            "subvolume_list should contain '{name}': {:?}",
            list.iter().map(|i| &i.name).collect::<Vec<_>>(),
        );
    }

    // Every item should have a valid root_id.
    for item in &list {
        assert!(item.root_id > 255, "root_id should be > 255: {item:?}");
    }
}

/// subvolume_flags_get and subvolume_flags_set should round-trip, and setting
/// RDONLY should prevent writes.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_flags_get_set() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open failed");

    // Initially should not be readonly.
    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get failed");
    assert!(
        !flags.contains(SubvolumeFlags::RDONLY),
        "new subvolume should not be readonly",
    );

    // Set readonly.
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::RDONLY)
        .expect("flags_set RDONLY failed");

    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get after set failed");
    assert!(
        flags.contains(SubvolumeFlags::RDONLY),
        "should be readonly now"
    );

    // Writing should fail.
    assert!(
        File::create(mnt.path().join("test-subvol").join("file.txt")).is_err(),
        "writing to readonly subvolume should fail",
    );

    // Clear readonly.
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::empty())
        .expect("flags_set empty failed");

    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get after clear failed");
    assert!(
        !flags.contains(SubvolumeFlags::RDONLY),
        "should not be readonly after clearing"
    );

    // Writing should work again.
    File::create(mnt.path().join("test-subvol").join("file.txt"))
        .expect("writing should work after clearing readonly");
}

/// subvolume_default_get should return 5 initially, and subvolume_default_set
/// should change the default subvolume.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_default_get_set() {
    let (_td, mnt) = single_mount();

    let default = subvolume_default_get(mnt.fd()).expect("default_get failed");
    assert_eq!(default, 5, "initial default should be FS_TREE_OBJECTID (5)");

    // Create a subvolume and set it as default.
    let name = CStr::from_bytes_with_nul(b"new-default\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("new-default")).expect("open failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    drop(subvol_dir);

    subvolume_default_set(mnt.fd(), info.id).expect("default_set failed");

    let new_default = subvolume_default_get(mnt.fd()).expect("default_get after set failed");
    assert_eq!(new_default, info.id, "default should be the new subvolume");

    // Reset back to 5.
    subvolume_default_set(mnt.fd(), 5).expect("default_set back to 5 failed");
    let reset = subvolume_default_get(mnt.fd()).expect("default_get after reset failed");
    assert_eq!(reset, 5, "default should be back to 5");
}

/// subvolume_list with nested subvolumes should show correct paths.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_nested() {
    let (_td, mnt) = single_mount();

    // Create A.
    let a_name = CStr::from_bytes_with_nul(b"A\0").unwrap();
    subvolume_create(mnt.fd(), a_name, &[]).expect("create A failed");

    // Create B inside A.
    let a_dir = File::open(mnt.path().join("A")).expect("open A failed");
    let b_name = CStr::from_bytes_with_nul(b"B\0").unwrap();
    subvolume_create(a_dir.as_fd(), b_name, &[]).expect("create B failed");
    drop(a_dir);

    // Create C inside A/B.
    let b_dir = File::open(mnt.path().join("A").join("B")).expect("open B failed");
    let c_name = CStr::from_bytes_with_nul(b"C\0").unwrap();
    subvolume_create(b_dir.as_fd(), c_name, &[]).expect("create C failed");
    drop(b_dir);

    sync(mnt.fd()).unwrap();

    let list = subvolume_list(mnt.fd()).expect("subvolume_list failed");

    assert!(
        list.iter().any(|i| i.name == "A"),
        "should find 'A': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
    );
    assert!(
        list.iter().any(|i| i.name == "A/B"),
        "should find 'A/B': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
    );
    assert!(
        list.iter().any(|i| i.name == "A/B/C"),
        "should find 'A/B/C': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
    );
}

use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    fiemap::file_extents,
    filesystem::sync,
    subvolume::{snapshot_create, subvolume_create},
};
use std::{ffi::CStr, fs::File, os::unix::io::AsFd, process::Command};

/// file_extents on a regular file should report non-zero total bytes and zero
/// shared bytes when no snapshots or reflinks exist.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_basic() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    let file = File::open(mnt.path().join("data.bin")).expect("open failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert_eq!(
        info.shared_bytes, 0,
        "shared_bytes should be 0 without snapshots: {info:?}"
    );
}

/// After snapshotting a subvolume, file_extents should report shared bytes for
/// files that exist in both the original and the snapshot.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_shared_after_snapshot() {
    let (_td, mnt) = single_mount();

    // Create a subvolume and write data into it.
    let subvol_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), subvol_name, &[])
        .expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    // Snapshot the subvolume.
    let snap_name = CStr::from_bytes_with_nul(b"snap\0").unwrap();
    let origin_dir =
        File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, false, &[])
        .expect("snapshot_create failed");
    sync(mnt.fd()).unwrap();

    // The file in the original subvolume should now have shared extents.
    let file = File::open(mnt.path().join("origin").join("data.bin"))
        .expect("open data failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert!(
        info.shared_bytes > 0,
        "shared_bytes should be > 0 after snapshot: {info:?}",
    );
}

/// After reflinking a file, file_extents should report shared bytes.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_shared_after_reflink() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "original.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    // cp --reflink creates a CoW copy that shares all extents.
    let original = mnt.path().join("original.bin");
    let reflinked = mnt.path().join("reflinked.bin");
    let output = Command::new("cp")
        .args(["--reflink=always"])
        .arg(&original)
        .arg(&reflinked)
        .output()
        .expect("failed to run cp");
    assert!(
        output.status.success(),
        "cp --reflink failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    sync(mnt.fd()).unwrap();

    let file = File::open(&original).expect("open original failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert!(
        info.shared_bytes > 0,
        "shared_bytes should be > 0 after reflink: {info:?}",
    );
}

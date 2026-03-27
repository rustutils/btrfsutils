use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    fiemap::file_extents,
    filesystem::sync,
    inode::{
        ino_lookup_user, ino_paths, logical_ino, lookup_path_rootid,
        subvolid_resolve,
    },
    subvolume::{subvolume_create, subvolume_info},
};
use std::{ffi::CStr, fs::File, os::unix::io::AsFd};

/// lookup_path_rootid on the mount root should return FS_TREE_OBJECTID (5),
/// and on a subvolume should return that subvolume's ID.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_lookup_path_rootid() {
    let (_td, mnt) = single_mount();

    let root_id = lookup_path_rootid(mnt.fd())
        .expect("lookup_path_rootid on mount failed");
    assert_eq!(
        root_id, 5,
        "mount root should have tree ID 5 (FS_TREE_OBJECTID)"
    );

    // Create a subvolume and check its root ID.
    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir =
        File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let subvol_root_id = lookup_path_rootid(subvol_dir.as_fd())
        .expect("lookup_path_rootid on subvol failed");

    // The subvolume's root ID should be different from FS_TREE and > 255.
    assert_ne!(
        subvol_root_id, 5,
        "subvolume should have a different root ID"
    );
    assert!(
        subvol_root_id > 255,
        "subvolume root ID should be > 255, got {subvol_root_id}"
    );

    // It should match what subvolume_info reports.
    let info =
        subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    assert_eq!(
        subvol_root_id, info.id,
        "root ID should match subvolume_info.id"
    );
}

/// ino_paths should resolve an inode to its filesystem path(s), including
/// hardlinks.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_ino_paths() {
    use std::os::unix::fs::MetadataExt;

    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "file.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    let meta = std::fs::metadata(mnt.path().join("file.bin"))
        .expect("metadata failed");
    let inum = meta.ino();

    let paths = ino_paths(mnt.fd(), inum).expect("ino_paths failed");
    assert!(
        paths.iter().any(|p| p.contains("file.bin")),
        "should find file.bin in paths: {paths:?}",
    );

    // Create a hardlink and check that both paths appear.
    std::fs::hard_link(
        mnt.path().join("file.bin"),
        mnt.path().join("link.bin"),
    )
    .expect("hard_link failed");
    sync(mnt.fd()).unwrap();

    let paths2 =
        ino_paths(mnt.fd(), inum).expect("ino_paths after hardlink failed");
    assert_eq!(
        paths2.len(),
        2,
        "should have 2 paths after hardlink: {paths2:?}"
    );
    assert!(
        paths2.iter().any(|p| p.contains("file.bin")),
        "should find file.bin: {paths2:?}",
    );
    assert!(
        paths2.iter().any(|p| p.contains("link.bin")),
        "should find link.bin: {paths2:?}",
    );
}

/// logical_ino should resolve a btrfs logical address back to the inode that
/// references it. We use a reflinked file so that fiemap reports shared extents
/// with physical (= btrfs logical) offsets.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_logical_ino() {
    use std::os::unix::fs::MetadataExt;

    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    // Reflink so fiemap reports shared extents (which include physical offsets).
    let output = std::process::Command::new("cp")
        .args(["--reflink=always"])
        .arg(mnt.path().join("data.bin"))
        .arg(mnt.path().join("copy.bin"))
        .output()
        .expect("failed to run cp");
    assert!(output.status.success(), "cp --reflink failed");
    sync(mnt.fd()).unwrap();

    let file = File::open(mnt.path().join("data.bin")).expect("open failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");
    assert!(
        !info.shared_extents.is_empty(),
        "should have shared extents after reflink"
    );

    // The physical start from fiemap is the btrfs logical address.
    let logical_addr = info.shared_extents[0].0;

    let results = logical_ino(mnt.fd(), logical_addr, false, None)
        .expect("logical_ino failed");
    assert!(
        !results.is_empty(),
        "logical_ino should return at least one result"
    );

    let inum = std::fs::metadata(mnt.path().join("data.bin"))
        .unwrap()
        .ino();
    assert!(
        results.iter().any(|r| r.inode == inum),
        "logical_ino should find our file's inode {inum}: {results:?}",
    );
}

/// ino_lookup_user should resolve a subvolume's name and parent path
/// without requiring CAP_SYS_ADMIN.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_ino_lookup_user() {
    use btrfs_uapi::raw::BTRFS_FIRST_FREE_OBJECTID;

    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"lookup-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("lookup-subvol"))
        .expect("open subvol failed");
    let info =
        subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");

    let result =
        ino_lookup_user(mnt.fd(), info.id, BTRFS_FIRST_FREE_OBJECTID as u64)
            .expect("ino_lookup_user failed");

    assert_eq!(
        result.name, "lookup-subvol",
        "subvolume name should match, got '{}'",
        result.name,
    );
}

/// ino_lookup_user should resolve the path when a subvolume is inside a
/// subdirectory.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_ino_lookup_user_nested_dir() {
    use btrfs_uapi::raw::BTRFS_FIRST_FREE_OBJECTID;

    let (_td, mnt) = single_mount();

    std::fs::create_dir(mnt.path().join("parent-dir")).expect("mkdir failed");
    let name = CStr::from_bytes_with_nul(b"parent-dir/nested\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("parent-dir/nested"))
        .expect("open subvol failed");
    let info =
        subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");

    let result =
        ino_lookup_user(mnt.fd(), info.id, BTRFS_FIRST_FREE_OBJECTID as u64)
            .expect("ino_lookup_user failed");

    assert_eq!(result.name, "nested");
    assert!(
        result.path.contains("parent-dir"),
        "path should contain 'parent-dir', got '{}'",
        result.path,
    );
}

/// subvolid_resolve should return the path of a subvolume given its ID.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_subvolid_resolve() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"my-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir =
        File::open(mnt.path().join("my-subvol")).expect("open subvol failed");
    let info =
        subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");

    let resolved =
        subvolid_resolve(mnt.fd(), info.id).expect("subvolid_resolve failed");
    assert!(
        resolved.contains("my-subvol"),
        "resolved path should contain 'my-subvol', got '{resolved}'",
    );
}

use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    filesystem::sync,
    space::{BlockGroupFlags, space_info},
};

/// space_info should return at least Data and Metadata entries with
/// reasonable values.
#[test]
#[ignore = "requires elevated privileges"]
fn space_info_basics() {
    let (_td, mnt) = single_mount();

    let spaces = space_info(mnt.fd()).expect("space_info failed");
    assert!(
        spaces.len() >= 2,
        "should have at least 2 block group types, got {}",
        spaces.len()
    );

    let has_data = spaces
        .iter()
        .any(|s| s.flags.contains(BlockGroupFlags::DATA));
    let has_meta = spaces
        .iter()
        .any(|s| s.flags.contains(BlockGroupFlags::METADATA));
    assert!(has_data, "should have a Data entry");
    assert!(has_meta, "should have a Metadata entry");

    for s in &spaces {
        assert!(s.total_bytes > 0, "total_bytes should be > 0: {s:?}");
    }
}

/// Writing data should increase the Data used_bytes in space_info.
#[test]
#[ignore = "requires elevated privileges"]
fn space_info_after_write() {
    let (_td, mnt) = single_mount();

    let before = space_info(mnt.fd()).expect("space_info before failed");
    let data_used_before: u64 = before
        .iter()
        .filter(|s| s.flags.contains(BlockGroupFlags::DATA))
        .map(|s| s.used_bytes)
        .sum();

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    let after = space_info(mnt.fd()).expect("space_info after failed");
    let data_used_after: u64 = after
        .iter()
        .filter(|s| s.flags.contains(BlockGroupFlags::DATA))
        .map(|s| s.used_bytes)
        .sum();

    assert!(
        data_used_after > data_used_before,
        "data used_bytes should increase after writing: before={data_used_before}, after={data_used_after}",
    );
}

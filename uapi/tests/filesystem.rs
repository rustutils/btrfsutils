use crate::common::{BackingFile, LoopbackDevice, Mount, single_mount, write_test_data};
use btrfs_uapi::{
    device::{device_add, device_info, device_min_size},
    filesystem::{filesystem_info, label_get, label_set, sync, ResizeAmount, ResizeArgs, resize},
};
use std::ffi::{CStr, CString};

/// filesystem_info on a fresh filesystem should return a valid UUID, correct device
/// count, and reasonable node/sector sizes.
#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_info_basics() {
    let (_td, mnt) = single_mount();

    let info = filesystem_info(mnt.fd()).expect("filesystem_info failed");
    assert!(!info.uuid.is_nil(), "uuid should not be nil");
    assert_eq!(info.num_devices, 1);
    assert_eq!(info.max_id, 1);
    assert!(
        info.sectorsize == 4096 || info.sectorsize == 16384,
        "sectorsize should be 4096 or 16384, got {}",
        info.sectorsize,
    );
    assert!(
        info.nodesize == 4096 || info.nodesize == 16384,
        "nodesize should be 4096 or 16384, got {}",
        info.nodesize,
    );
}

/// filesystem_info should reflect a newly added device.
#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_info_after_add() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let info1 = filesystem_info(mnt.fd()).expect("filesystem_info failed");
    assert_eq!(info1.num_devices, 1);
    assert_eq!(info1.max_id, 1);

    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    let info2 = filesystem_info(mnt.fd()).expect("filesystem_info after add failed");
    assert_eq!(info2.num_devices, 2);
    assert_eq!(info2.max_id, 2);
    assert_eq!(
        info1.uuid, info2.uuid,
        "uuid should not change after adding a device"
    );
}

/// sync should succeed without error (smoke test).
#[test]
#[ignore = "requires elevated privileges"]
fn sync_basic() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 1_000_000);
    sync(mnt.fd()).expect("sync failed");

    // A second sync should also succeed.
    sync(mnt.fd()).expect("second sync failed");
}

/// label_get and label_set should round-trip correctly.
#[test]
#[ignore = "requires elevated privileges"]
fn label_get_set() {
    let (_td, mnt) = single_mount();

    // Fresh filesystem should have an empty label.
    let initial = label_get(mnt.fd()).expect("label_get failed");
    assert!(
        initial.to_bytes().is_empty(),
        "initial label should be empty, got {initial:?}"
    );

    let test_label = CStr::from_bytes_with_nul(b"test-label\0").unwrap();
    label_set(mnt.fd(), test_label).expect("label_set failed");

    let got = label_get(mnt.fd()).expect("label_get after set failed");
    assert_eq!(got.as_c_str(), test_label, "label should round-trip");
}

/// Setting a label at exactly 255 bytes should work, and 256 bytes should fail.
#[test]
#[ignore = "requires elevated privileges"]
fn label_max_length() {
    let (_td, mnt) = single_mount();

    // 255 bytes is the max (BTRFS_LABEL_SIZE is 256 including nul).
    let max_label = "a".repeat(255);
    let max_cstr = CString::new(max_label.clone()).unwrap();
    label_set(mnt.fd(), &max_cstr).expect("label_set with 255 bytes should succeed");

    let got = label_get(mnt.fd()).expect("label_get failed");
    assert_eq!(got.to_bytes().len(), 255);

    // 256 bytes should fail.
    let too_long = "b".repeat(256);
    let too_long_cstr = CString::new(too_long).unwrap();
    let err = label_set(mnt.fd(), &too_long_cstr);
    assert!(err.is_err(), "label_set with 256 bytes should fail");
}

/// Clearing a label by setting it to empty should work.
#[test]
#[ignore = "requires elevated privileges"]
fn label_clear() {
    let (_td, mnt) = single_mount();

    let test_label = CStr::from_bytes_with_nul(b"some-label\0").unwrap();
    label_set(mnt.fd(), test_label).expect("label_set failed");

    let got = label_get(mnt.fd()).expect("label_get failed");
    assert_eq!(got.as_c_str(), test_label);

    let empty = CStr::from_bytes_with_nul(b"\0").unwrap();
    label_set(mnt.fd(), empty).expect("label_set empty failed");

    let cleared = label_get(mnt.fd()).expect("label_get after clear failed");
    assert!(
        cleared.to_bytes().is_empty(),
        "label should be empty after clearing, got {cleared:?}"
    );
}

/// Growing a filesystem by enlarging the backing device and calling resize
/// should increase the available space.
#[test]
#[ignore = "requires elevated privileges"]
fn resize_grow() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 200_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    let dev_before = device_info(mnt.fd(), 1).unwrap().unwrap();

    // Grow the backing file and tell the loop device.
    mnt.loopback().backing_file().resize(400_000_000);
    mnt.loopback().refresh_size();

    // Tell btrfs to use the new space.
    resize(mnt.fd(), ResizeArgs::new(ResizeAmount::Max).with_devid(1)).expect("resize grow failed");

    let dev_after = device_info(mnt.fd(), 1).unwrap().unwrap();

    assert!(
        dev_after.total_bytes > dev_before.total_bytes,
        "device total_bytes should increase: before={}, after={}",
        dev_before.total_bytes,
        dev_after.total_bytes,
    );
}

/// Shrinking a filesystem to just above the minimum should succeed and data
/// should remain intact.
#[test]
#[ignore = "requires elevated privileges"]
fn resize_shrink() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 500_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    let dev = device_info(mnt.fd(), 1).unwrap().unwrap();
    let min = device_min_size(mnt.fd(), 1).expect("device_min_size failed");

    // Shrink to halfway between min and current size.
    let target = min + (dev.total_bytes - min) / 2;
    assert!(
        target < dev.total_bytes,
        "target {target} should be less than current size {}",
        dev.total_bytes,
    );
    resize(
        mnt.fd(),
        ResizeArgs::new(ResizeAmount::Set(target)).with_devid(1),
    )
    .expect("resize shrink failed");

    crate::common::verify_test_data(mnt.path(), "data.bin", 50_000_000);
}

/// Attempting to shrink below the minimum used space should fail.
#[test]
#[ignore = "requires elevated privileges"]
fn resize_shrink_below_minimum_fails() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 500_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    write_test_data(mnt.path(), "data.bin", 200_000_000);
    sync(mnt.fd()).unwrap();

    // Try to shrink to 1MB — way below what's used.
    let result = resize(
        mnt.fd(),
        ResizeArgs::new(ResizeAmount::Set(1_000_000)).with_devid(1),
    );
    assert!(
        result.is_err(),
        "resize to 1MB should fail when data is present"
    );
}

use crate::common::{BackingFile, LoopbackDevice, Mount, write_test_data};
use btrfs_uapi::{
    dev_extent::min_dev_size,
    device::device_info,
    resize::{ResizeAmount, ResizeArgs, resize},
    filesystem::sync,
};

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
    let min = min_dev_size(mnt.fd(), 1).expect("min_dev_size failed");

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

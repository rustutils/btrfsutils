use crate::common::{BackingFile, LoopbackDevice, Mount, write_test_data};
use btrfs_uapi::{dev_extent::min_dev_size, device::device_add, sync::sync};
use std::ffi::CString;

/// min_dev_size should return a sensible value and increase after writing data.
#[test]
#[ignore = "requires elevated privileges"]
fn dev_extent_min_size_single() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 500_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    write_test_data(mnt.path(), "data1.bin", 100_000_000);
    sync(mnt.fd()).unwrap();

    let min1 = min_dev_size(mnt.fd(), 1).expect("min_dev_size failed");
    assert!(min1 > 0, "min_dev_size should be > 0");
    assert!(min1 <= 500_000_000, "min_dev_size should be <= device size");

    write_test_data(mnt.path(), "data2.bin", 200_000_000);
    sync(mnt.fd()).unwrap();

    let min2 = min_dev_size(mnt.fd(), 1).expect("min_dev_size after more data failed");
    assert!(
        min2 >= min1,
        "min_dev_size should not decrease after writing more data: before={min1}, after={min2}",
    );
}

/// min_dev_size on both devices of a two-device filesystem should return
/// sensible values.
#[test]
#[ignore = "requires elevated privileges"]
fn dev_extent_min_size_multi() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let f2 = BackingFile::new(td.path(), "d2.img", 400_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    let min1 = min_dev_size(mnt.fd(), 1).expect("min_dev_size dev 1 failed");
    let min2 = min_dev_size(mnt.fd(), 2).expect("min_dev_size dev 2 failed");

    assert!(min1 > 0, "min_dev_size for dev 1 should be > 0");
    assert!(min2 > 0, "min_dev_size for dev 2 should be > 0");
    assert!(
        min1 <= 300_000_000,
        "min_dev_size for dev 1 should be <= its size: {min1}",
    );
    assert!(
        min2 <= 400_000_000,
        "min_dev_size for dev 2 should be <= its size: {min2}",
    );
}

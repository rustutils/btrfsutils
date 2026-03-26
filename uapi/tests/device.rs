use crate::common::{BackingFile, LoopbackDevice, Mount, single_mount, write_test_data};
use btrfs_uapi::{
    balance::{BalanceFlags, balance},
    device::{
        DeviceSpec, device_add, device_info, device_info_all, device_ready, device_remove,
        device_scan, device_stats,
    },
    filesystem::fs_info,
    filesystem::sync,
};
use std::ffi::CString;

/// Adding and removing a device should change the device count accordingly.
#[test]
#[ignore = "requires elevated privileges"]
fn device_add_remove() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let info_before = fs_info(mnt.fd()).expect("fs_info failed");
    assert_eq!(info_before.num_devices, 1);

    // Add second device.
    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    let info_after_add = fs_info(mnt.fd()).expect("fs_info after add failed");
    assert_eq!(info_after_add.num_devices, 2);

    let devs = device_info_all(mnt.fd(), &info_after_add).expect("device_info_all failed");
    assert_eq!(devs.len(), 2);

    // Balance to single profile so all data is on one device, allowing removal.
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(mnt.fd(), flags, None, None, None).expect("balance failed");

    // Remove second device by path.
    device_remove(mnt.fd(), DeviceSpec::Path(&dev2_cpath)).expect("device_remove failed");

    let info_after_remove = fs_info(mnt.fd()).expect("fs_info after remove failed");
    assert_eq!(info_after_remove.num_devices, 1);
}

/// device_info should return valid info and device_stats should report zero
/// errors on a healthy filesystem.
#[test]
#[ignore = "requires elevated privileges"]
fn device_info_and_stats() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let info = device_info(mnt.fd(), 1)
        .expect("device_info failed")
        .expect("device 1 should exist");
    assert_eq!(info.devid, 1);
    assert!(info.total_bytes > 0, "total_bytes should be > 0");
    assert!(!info.path.is_empty(), "device path should not be empty");

    let stats = device_stats(mnt.fd(), 1, false).expect("device_stats failed");
    assert!(
        stats.is_clean(),
        "healthy filesystem should have zero errors: {stats:?}"
    );

    // Reset and re-read — should still be zero.
    let _reset = device_stats(mnt.fd(), 1, true).expect("device_stats reset failed");
    let after_reset = device_stats(mnt.fd(), 1, false).expect("device_stats after reset failed");
    assert!(
        after_reset.is_clean(),
        "stats after reset should be zero: {after_reset:?}"
    );
}

/// device_scan and device_ready should succeed on a formatted loop device.
#[test]
#[ignore = "requires elevated privileges"]
fn device_scan_and_ready() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 300_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);

    let dev_cpath = CString::new(lo.path().to_str().unwrap()).unwrap();
    device_scan(&dev_cpath).expect("device_scan failed");
    device_ready(&dev_cpath).expect("device_ready failed");
}

/// Removing a device by its devid should work.
#[test]
#[ignore = "requires elevated privileges"]
fn device_remove_by_devid() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    // Balance so data is on device 1, then remove device 2 by ID.
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(mnt.fd(), flags, None, None, None).expect("balance failed");

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let devs = device_info_all(mnt.fd(), &info).expect("device_info_all failed");
    let dev2 = devs
        .iter()
        .find(|d| d.devid != 1)
        .expect("should have a second device");

    device_remove(mnt.fd(), DeviceSpec::Id(dev2.devid)).expect("device_remove by id failed");

    let info_after = fs_info(mnt.fd()).expect("fs_info after remove failed");
    assert_eq!(info_after.num_devices, 1);
}

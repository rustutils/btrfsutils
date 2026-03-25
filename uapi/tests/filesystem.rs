use crate::common::{BackingFile, LoopbackDevice, Mount, single_mount};
use btrfs_uapi::{device::device_add, filesystem::fs_info};
use std::ffi::CString;

/// fs_info on a fresh filesystem should return a valid UUID, correct device
/// count, and reasonable node/sector sizes.
#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_info_basics() {
    let (_td, mnt) = single_mount();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
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

/// fs_info should reflect a newly added device.
#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_info_after_add() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let info1 = fs_info(mnt.fd()).expect("fs_info failed");
    assert_eq!(info1.num_devices, 1);
    assert_eq!(info1.max_id, 1);

    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    let info2 = fs_info(mnt.fd()).expect("fs_info after add failed");
    assert_eq!(info2.num_devices, 2);
    assert_eq!(info2.max_id, 2);
    assert_eq!(
        info1.uuid, info2.uuid,
        "uuid should not change after adding a device"
    );
}

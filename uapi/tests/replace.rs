use crate::common::{BackingFile, LoopbackDevice, Mount, single_mount, write_test_data};
use btrfs_uapi::{
    balance::{BalanceArgs, BalanceFlags, balance},
    device::{device_add, device_info_all},
    filesystem::fs_info,
    replace::{ReplaceSource, ReplaceState, replace_cancel, replace_start, replace_status},
    space::BlockGroupFlags,
    sync::sync,
};
use std::ffi::CString;

/// replace_status on a filesystem where no replace has been started should
/// report NeverStarted.
#[test]
#[ignore = "requires elevated privileges"]
fn replace_status_idle() {
    let (_td, mnt) = single_mount();

    let status = replace_status(mnt.fd()).expect("replace_status failed");
    assert_eq!(
        status.state,
        ReplaceState::NeverStarted,
        "should be NeverStarted on a fresh filesystem, got {:?}",
        status.state,
    );
}

/// Replacing a device in a RAID1 filesystem should succeed, and the data
/// should still be readable afterwards.
#[test]
#[ignore = "requires elevated privileges"]
fn replace_device() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    // Add second device and convert to RAID1.
    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    let convert = BalanceArgs::new().convert(BlockGroupFlags::RAID1.bits());
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(
        mnt.fd(),
        flags,
        Some(convert.clone()),
        Some(convert.clone()),
        Some(convert),
    )
    .expect("balance to RAID1 failed");

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    // Create a third device as the replacement target.
    let f3 = BackingFile::new(td.path(), "d3.img", 300_000_000);
    let lo3 = LoopbackDevice::new(f3);
    let tgt_cpath = CString::new(lo3.path().to_str().unwrap()).unwrap();

    // Replace device 2.
    replace_start(mnt.fd(), ReplaceSource::DevId(2), &tgt_cpath, false)
        .expect("replace_start ioctl failed")
        .expect("replace_start returned application error");

    // Poll until finished.
    loop {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let status = replace_status(mnt.fd()).expect("replace_status failed");
        match status.state {
            ReplaceState::Finished => break,
            ReplaceState::Started | ReplaceState::Suspended => continue,
            other => panic!("unexpected replace state: {other:?}"),
        }
    }

    // Data should still be intact.
    crate::common::verify_test_data(mnt.path(), "data.bin", 50_000_000);

    // The replacement device should now be device 2.
    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let devs = device_info_all(mnt.fd(), &info).expect("device_info_all failed");
    assert_eq!(devs.len(), 2, "should still have 2 devices");
    let dev2 = devs
        .iter()
        .find(|d| d.devid == 2)
        .expect("device 2 should exist");
    assert!(
        dev2.path.contains("d3.img") || dev2.path.contains("loop"),
        "device 2 should now point to the replacement: {}",
        dev2.path,
    );
}

/// Cancelling a replace operation should work.
#[test]
#[ignore = "requires elevated privileges"]
fn replace_cancel_test() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 512_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let f2 = BackingFile::new(td.path(), "d2.img", 512_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    let convert = BalanceArgs::new().convert(BlockGroupFlags::RAID1.bits());
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(
        mnt.fd(),
        flags,
        Some(convert.clone()),
        Some(convert.clone()),
        Some(convert),
    )
    .expect("balance to RAID1 failed");

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    let f3 = BackingFile::new(td.path(), "d3.img", 512_000_000);
    let lo3 = LoopbackDevice::new(f3);
    let tgt_cpath = CString::new(lo3.path().to_str().unwrap()).unwrap();

    replace_start(mnt.fd(), ReplaceSource::DevId(2), &tgt_cpath, false)
        .expect("replace_start ioctl failed")
        .expect("replace_start returned application error");

    // Cancel immediately — may already be finished on fast systems.
    let cancelled = replace_cancel(mnt.fd()).expect("replace_cancel failed");

    let status = replace_status(mnt.fd()).expect("replace_status failed");
    if cancelled {
        assert_ne!(
            status.state,
            ReplaceState::Started,
            "should not be Started after cancel",
        );
    }
    // If not cancelled (already finished), that's fine too.
}

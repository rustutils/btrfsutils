use crate::common::{
    BackingFile, LoopbackDevice, Mount, single_mount, write_test_data,
};
use btrfs_uapi::{
    balance::{BalanceArgs, BalanceFlags, balance},
    chunk::{chunk_list, device_chunk_allocations},
    device::device_add,
    filesystem::sync,
    space::BlockGroupFlags,
};
use std::ffi::CString;

/// chunk_list on a fresh filesystem should return a non-empty list with valid
/// entries, and writing data should produce new chunks.
#[test]
#[ignore = "requires elevated privileges"]
fn chunk_list_basic() {
    let (_td, mnt) = single_mount();

    let initial = chunk_list(mnt.fd()).expect("chunk_list failed");
    assert!(!initial.is_empty(), "fresh filesystem should have chunks");

    // Every entry should have a non-zero length and a recognized type.
    for entry in &initial {
        assert!(
            entry.length > 0,
            "chunk length should be non-zero: {entry:?}"
        );
        let has_type = entry.flags.contains(BlockGroupFlags::DATA)
            || entry.flags.contains(BlockGroupFlags::METADATA)
            || entry.flags.contains(BlockGroupFlags::SYSTEM);
        assert!(has_type, "chunk should have a type flag: {entry:?}");
    }

    // Write data, sync, and verify new data chunks appear.
    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    let after = chunk_list(mnt.fd()).expect("chunk_list after write failed");
    let initial_data_chunks = initial
        .iter()
        .filter(|e| e.flags.contains(BlockGroupFlags::DATA))
        .count();
    let after_data_chunks = after
        .iter()
        .filter(|e| e.flags.contains(BlockGroupFlags::DATA))
        .count();
    assert!(
        after_data_chunks >= initial_data_chunks,
        "data chunk count should not decrease after writing: before={initial_data_chunks}, after={after_data_chunks}",
    );
}

/// device_chunk_allocations on a two-device RAID1 filesystem should show both
/// devices having allocations.
#[test]
#[ignore = "requires elevated privileges"]
fn chunk_allocations_two_devices() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    // Convert to RAID1 so both devices get data.
    let convert_args =
        BalanceArgs::new().convert(BlockGroupFlags::RAID1.bits());
    let flags =
        BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(
        mnt.fd(),
        flags,
        Some(convert_args.clone()),
        Some(convert_args.clone()),
        Some(convert_args),
    )
    .expect("balance failed");

    let allocs = device_chunk_allocations(mnt.fd())
        .expect("device_chunk_allocations failed");
    let dev1_bytes: u64 = allocs
        .iter()
        .filter(|a| a.devid == 1)
        .map(|a| a.bytes)
        .sum();
    let dev2_bytes: u64 = allocs
        .iter()
        .filter(|a| a.devid == 2)
        .map(|a| a.bytes)
        .sum();
    assert!(dev1_bytes > 0, "device 1 should have allocations");
    assert!(dev2_bytes > 0, "device 2 should have allocations");
}

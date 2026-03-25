//! Integration tests for the ioctls.
//!
//! These tests require a mounted btrfs filesystem and root privileges. They are
//! skipped automatically unless the test is run with `--include-ignored`.
//!
//! To run:
//!   just test-privileged

mod common;

use common::{
    BackingFile, LoopbackDevice, Mount, write_compressible_data, write_test_data,
};

use btrfs_uapi::balance::{
    BalanceArgs, BalanceCtl, BalanceFlags, BalanceState, balance, balance_ctl, balance_progress,
};
use btrfs_uapi::chunk::{chunk_list, device_chunk_allocations};
use btrfs_uapi::defrag::{CompressSpec, CompressType, DefragRangeArgs, defrag_range};
use btrfs_uapi::dev_extent::min_dev_size;
use btrfs_uapi::device::{device_add, device_info_all, device_remove, DeviceSpec};
use btrfs_uapi::fiemap::file_extents;
use btrfs_uapi::filesystem::fs_info;
use btrfs_uapi::space::{BlockGroupFlags, space_info};
use btrfs_uapi::sync::sync;
use nix::errno::Errno;
use std::ffi::CString;
use std::fs::File;
use std::os::unix::io::AsFd;

/// Create a single-device 512MB btrfs filesystem. Returns the tempdir (must be
/// kept alive) and the mount.
fn single_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs();
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    (td, mnt)
}

/// Querying balance progress on an idle filesystem should indicate that no
/// balance is running (ENOTCONN).
#[test]
#[ignore = "requires elevated privileges"]
fn balance_progress_idle() {
    let (_td, mnt) = single_mount();

    match balance_progress(mnt.fd()) {
        Err(e) if e == Errno::ENOTCONN => {
            // Expected: no balance is running.
        }
        Err(e) => panic!("unexpected error from balance_progress: {e}"),
        Ok((state, progress)) => {
            // Some kernels return Ok with a zeroed state instead of ENOTCONN
            // when nothing has ever run. Accept that too.
            assert!(
                !state.contains(BalanceState::RUNNING),
                "expected no running balance, got state={state:?} progress={progress:?}"
            );
        }
    }
}

/// Pausing when no balance is running should return ENOTCONN.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_pause_not_running() {
    let (_td, mnt) = single_mount();

    match balance_ctl(mnt.fd(), BalanceCtl::Pause) {
        Err(e) if e == Errno::ENOTCONN => { /* expected */ }
        Err(e) => panic!("unexpected error from balance_ctl(Pause): {e}"),
        Ok(()) => panic!("expected ENOTCONN, but pause succeeded"),
    }
}

/// Cancelling when no balance is running should return ENOTCONN.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_cancel_not_running() {
    let (_td, mnt) = single_mount();

    match balance_ctl(mnt.fd(), BalanceCtl::Cancel) {
        Err(e) if e == Errno::ENOTCONN => { /* expected */ }
        Err(e) => panic!("unexpected error from balance_ctl(Cancel): {e}"),
        Ok(()) => panic!("expected ENOTCONN, but cancel succeeded"),
    }
}

/// Running a full balance on a freshly created filesystem should succeed and
/// report sane progress counters.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_full_completes() {
    let (_td, mnt) = single_mount();

    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    let progress = balance(mnt.fd(), flags, None, None, None).expect("balance failed");

    // On a fresh empty filesystem, completed should equal considered.
    assert_eq!(
        progress.completed, progress.considered,
        "expected all considered chunks to be completed: {progress:?}"
    );
}

/// A balance that is cancelled mid-run should not return an error from our
/// wrapper — ECANCELED is treated as a graceful stop.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_cancel_in_flight() {
    let (_td, mnt) = single_mount();

    // Kick off a balance in a background thread.
    let mount_path = mnt.path().to_path_buf();
    let balance_thread = std::thread::spawn(move || {
        let file = File::open(&mount_path).expect("failed to open mount in thread");
        let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
        balance(file.as_fd(), flags, None, None, None)
    });

    // Give the kernel a moment to start the balance before cancelling.
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Cancel may return ENOTCONN if the balance already finished on a small
    // filesystem — that's fine.
    match balance_ctl(mnt.fd(), BalanceCtl::Cancel) {
        Ok(()) | Err(Errno::ENOTCONN) => {}
        Err(e) => panic!("unexpected error from balance_ctl(Cancel): {e}"),
    }

    // The balance thread should complete without an error regardless.
    let result = balance_thread.join().expect("balance thread panicked");
    match result {
        Ok(_) | Err(Errno::ECANCELED) => {}
        Err(e) => panic!("balance returned unexpected error: {e}"),
    }
}

/// Adding a second device and converting to RAID1 should result in both devices
/// holding data, and space_info reporting RAID1 profiles.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_convert_raid1() {
    let td = tempfile::tempdir().unwrap();
    let f1 = BackingFile::new(td.path(), "d1.img", 300_000_000);
    f1.mkfs();
    let lo1 = LoopbackDevice::new(f1);
    let mnt = Mount::new(lo1, td.path());

    write_test_data(mnt.path(), "data.bin", 50_000_000);
    sync(mnt.fd()).unwrap();

    // Add a second device.
    let f2 = BackingFile::new(td.path(), "d2.img", 300_000_000);
    let lo2 = LoopbackDevice::new(f2);
    let dev2_cpath = CString::new(lo2.path().to_str().unwrap()).unwrap();
    device_add(mnt.fd(), &dev2_cpath).expect("device_add failed");

    // Convert data and metadata to RAID1.
    let convert_args = BalanceArgs::new().convert(BlockGroupFlags::RAID1.bits());
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(
        mnt.fd(),
        flags,
        Some(convert_args.clone()),
        Some(convert_args.clone()),
        Some(convert_args),
    )
    .expect("balance failed");

    // space_info should now show RAID1 for data and metadata.
    let spaces = space_info(mnt.fd()).expect("space_info failed");
    let data_entry = spaces
        .iter()
        .find(|s| s.flags.contains(BlockGroupFlags::DATA))
        .expect("no data entry in space_info");
    assert!(
        data_entry.flags.contains(BlockGroupFlags::RAID1),
        "data should be RAID1 after conversion, got {data_entry:?}",
    );

    // Both devices should have chunk allocations.
    let allocs = device_chunk_allocations(mnt.fd()).expect("device_chunk_allocations failed");
    let dev1_bytes: u64 = allocs.iter().filter(|a| a.devid == 1).map(|a| a.bytes).sum();
    let dev2_bytes: u64 = allocs.iter().filter(|a| a.devid == 2).map(|a| a.bytes).sum();
    assert!(dev1_bytes > 0, "device 1 should have allocations");
    assert!(dev2_bytes > 0, "device 2 should have allocations");
}

/// Pausing and resuming a balance in flight should work.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_pause_resume() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 512_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    // Write enough data to give balance some work.
    write_test_data(mnt.path(), "data.bin", 200_000_000);
    sync(mnt.fd()).unwrap();

    // Start balance in a background thread.
    let mount_path = mnt.path().to_path_buf();
    let balance_thread = std::thread::spawn(move || {
        let file = File::open(&mount_path).expect("failed to open mount in thread");
        let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
        balance(file.as_fd(), flags, None, None, None)
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    // Try to pause. The balance may already have finished on fast systems.
    match balance_ctl(mnt.fd(), BalanceCtl::Pause) {
        Ok(()) => {
            // Verify the paused state.
            match balance_progress(mnt.fd()) {
                Ok((state, _)) => {
                    assert!(
                        state.contains(BalanceState::PAUSE_REQ)
                            || !state.contains(BalanceState::RUNNING),
                        "expected paused state, got {state:?}",
                    );
                }
                Err(Errno::ENOTCONN) => {
                    // Balance finished between our pause and progress query.
                }
                Err(e) => panic!("unexpected error from balance_progress: {e}"),
            }

            // Resume — balance() with RESUME flag re-starts a paused balance.
            // May return ENOTCONN/ECANCELED if balance finished while paused.
            let resume_fd = File::open(&mnt.path()).expect("failed to open mount");
            let flags = BalanceFlags::DATA
                | BalanceFlags::METADATA
                | BalanceFlags::SYSTEM
                | BalanceFlags::RESUME;
            match balance(resume_fd.as_fd(), flags, None, None, None) {
                Ok(_) | Err(Errno::ENOTCONN) | Err(Errno::ECANCELED) => {}
                Err(e) => panic!("unexpected error resuming balance: {e}"),
            }
        }
        Err(Errno::ENOTCONN) => {
            // Balance already finished — that's fine.
        }
        Err(e) => panic!("unexpected error from balance_ctl(Pause): {e}"),
    }

    let result = balance_thread.join().expect("balance thread panicked");
    match result {
        Ok(_) | Err(Errno::ECANCELED) => {}
        Err(e) => panic!("balance returned unexpected error: {e}"),
    }
}

/// Cancelling a balance in flight using balance_ctl(Cancel) should stop it.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_cancel_with_data() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 512_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    write_test_data(mnt.path(), "data.bin", 200_000_000);
    sync(mnt.fd()).unwrap();

    let mount_path = mnt.path().to_path_buf();
    let balance_thread = std::thread::spawn(move || {
        let file = File::open(&mount_path).expect("failed to open mount in thread");
        let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
        balance(file.as_fd(), flags, None, None, None)
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    match balance_ctl(mnt.fd(), BalanceCtl::Cancel) {
        Ok(()) | Err(Errno::ENOTCONN) => {}
        Err(e) => panic!("unexpected error from balance_ctl(Cancel): {e}"),
    }

    let result = balance_thread.join().expect("balance thread panicked");
    match result {
        Ok(_) | Err(Errno::ECANCELED) => {}
        Err(e) => panic!("balance returned unexpected error: {e}"),
    }
}

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
        assert!(entry.length > 0, "chunk length should be non-zero: {entry:?}");
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
    let convert_args = BalanceArgs::new().convert(BlockGroupFlags::RAID1.bits());
    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    balance(
        mnt.fd(),
        flags,
        Some(convert_args.clone()),
        Some(convert_args.clone()),
        Some(convert_args),
    )
    .expect("balance failed");

    let allocs = device_chunk_allocations(mnt.fd()).expect("device_chunk_allocations failed");
    let dev1_bytes: u64 = allocs.iter().filter(|a| a.devid == 1).map(|a| a.bytes).sum();
    let dev2_bytes: u64 = allocs.iter().filter(|a| a.devid == 2).map(|a| a.bytes).sum();
    assert!(dev1_bytes > 0, "device 1 should have allocations");
    assert!(dev2_bytes > 0, "device 2 should have allocations");
}

/// Defragmenting with compression should reduce the on-disk size of a
/// compressible file, and the file content should remain intact.
#[test]
#[ignore = "requires elevated privileges"]
fn defrag_compress() {
    let (_td, mnt) = single_mount();

    write_compressible_data(mnt.path(), "zeros.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .open(mnt.path().join("zeros.bin"))
        .expect("failed to open test file");

    let before = file_extents(file.as_fd()).expect("file_extents before defrag failed");

    defrag_range(
        file.as_fd(),
        &DefragRangeArgs::new().compress(CompressSpec {
            compress_type: CompressType::Zlib,
            level: None,
        }),
    )
    .expect("defrag_range failed");

    // Re-query after defrag. Drop and reopen to flush any caching.
    drop(file);
    sync(mnt.fd()).unwrap();

    let file = File::open(mnt.path().join("zeros.bin")).expect("reopen failed");
    let after = file_extents(file.as_fd()).expect("file_extents after defrag failed");

    // All-zeros data should compress very well.
    assert!(
        after.total_bytes < before.total_bytes,
        "on-disk size should decrease after compressing: before={}, after={}",
        before.total_bytes,
        after.total_bytes,
    );

    // Content should still be intact (all zeros).
    let data = std::fs::read(mnt.path().join("zeros.bin")).expect("read failed");
    assert_eq!(data.len(), 10_000_000);
    assert!(data.iter().all(|&b| b == 0), "data should still be all zeros");
}

/// Defragmenting a fragmented file (without compression) should not corrupt
/// the data.
#[test]
#[ignore = "requires elevated privileges"]
fn defrag_no_compression() {
    let (_td, mnt) = single_mount();

    // Write many small chunks with fsync between each to force fragmentation.
    let path = mnt.path().join("fragmented.bin");
    {
        use std::io::Write;
        let mut file = File::create(&path).expect("create failed");
        for i in 0..200u32 {
            let chunk = [i as u8; 4096];
            file.write_all(&chunk).unwrap();
            file.sync_all().unwrap();
        }
    }

    let file = File::options()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open failed");

    defrag_range(file.as_fd(), &DefragRangeArgs::new()).expect("defrag_range failed");
    drop(file);
    sync(mnt.fd()).unwrap();

    // Verify data integrity after defrag.
    let data = std::fs::read(&path).expect("read failed");
    assert_eq!(data.len(), 200 * 4096);
    for (i, chunk) in data.chunks(4096).enumerate() {
        assert!(
            chunk.iter().all(|&b| b == i as u8),
            "data mismatch in chunk {i}",
        );
    }
}

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

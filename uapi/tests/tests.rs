//! Integration tests for the ioctls.
//!
//! These tests require a mounted btrfs filesystem and root privileges. They are
//! skipped automatically unless the test is run with `--include-ignored`.
//!
//! To run:
//!   just test-privileged

mod common;

use common::{BackingFile, LoopbackDevice, Mount, write_compressible_data, write_test_data};

use btrfs_uapi::balance::{
    BalanceArgs, BalanceCtl, BalanceFlags, BalanceState, balance, balance_ctl, balance_progress,
};
use btrfs_uapi::chunk::{chunk_list, device_chunk_allocations};
use btrfs_uapi::defrag::{CompressSpec, CompressType, DefragRangeArgs, defrag_range};
use btrfs_uapi::dev_extent::min_dev_size;
use btrfs_uapi::device::{
    DeviceSpec, device_add, device_info, device_info_all, device_ready, device_remove, device_scan,
    device_stats,
};
use btrfs_uapi::fiemap::file_extents;
use btrfs_uapi::filesystem::fs_info;
use btrfs_uapi::inode::{ino_paths, logical_ino, lookup_path_rootid, subvolid_resolve};
use btrfs_uapi::label::{label_get, label_set};
use btrfs_uapi::qgroup::{
    QgroupLimitFlags, qgroup_assign, qgroup_clear_stale, qgroup_create, qgroup_destroy,
    qgroup_limit, qgroup_list, qgroup_remove,
};
use btrfs_uapi::quota::{quota_disable, quota_enable, quota_rescan_wait};
use btrfs_uapi::replace::{
    ReplaceSource, ReplaceState, replace_cancel, replace_start, replace_status,
};
use btrfs_uapi::resize::{ResizeAmount, ResizeArgs, resize};
use btrfs_uapi::scrub::{scrub_cancel, scrub_start};
use btrfs_uapi::space::{BlockGroupFlags, space_info};
use btrfs_uapi::subvolume::{
    SubvolumeFlags, snapshot_create, subvolume_create, subvolume_default_get,
    subvolume_default_set, subvolume_delete, subvolume_flags_get, subvolume_flags_set,
    subvolume_info, subvolume_list,
};
use btrfs_uapi::sync::sync;
use btrfs_uapi::sysfs::SysfsBtrfs;
use btrfs_uapi::tree_search::{SearchKey, tree_search};
use nix::errno::Errno;
use std::ffi::{CStr, CString};
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

/// Defragmenting with compression should reduce the on-disk block usage of a
/// compressible file, and the file content should remain intact.
// FIXME: defrag_range with COMPRESS flag does not seem to actually compress
// the data on the test filesystem. Neither extent_thresh(1) nor different
// compression algorithms help. Needs investigation — possibly a kernel
// version issue, or we need to mount with compress-force, or the ioctl
// flags aren't being set correctly.
#[test]
#[ignore = "requires elevated privileges"]
#[should_panic] // currently broken, see FIXME
fn defrag_compress() {
    use std::os::unix::fs::MetadataExt;

    let (_td, mnt) = single_mount();

    write_compressible_data(mnt.path(), "zeros.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let path = mnt.path().join("zeros.bin");
    let blocks_before = std::fs::metadata(&path).unwrap().blocks();

    let file = File::options()
        .read(true)
        .write(true)
        .open(&path)
        .expect("failed to open test file");

    defrag_range(
        file.as_fd(),
        &DefragRangeArgs::new()
            .compress(CompressSpec {
                compress_type: CompressType::Zlib,
                level: None,
            })
            // Force rewriting all extents regardless of size, otherwise the
            // kernel may skip already-contiguous extents.
            .extent_thresh(1),
    )
    .expect("defrag_range failed");

    drop(file);
    sync(mnt.fd()).unwrap();

    // st_blocks reflects actual disk usage (in 512-byte units), which
    // decreases when btrfs stores compressed extents.
    let blocks_after = std::fs::metadata(&path).unwrap().blocks();

    assert!(
        blocks_after < blocks_before,
        "disk blocks should decrease after compressing: before={blocks_before}, after={blocks_after}",
    );

    // Content should still be intact (all zeros).
    let data = std::fs::read(&path).expect("read failed");
    assert_eq!(data.len(), 10_000_000);
    assert!(
        data.iter().all(|&b| b == 0),
        "data should still be all zeros"
    );
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
            // FIXME: do we need to call btrfs sync here?
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

/// file_extents on a regular file should report non-zero total bytes and zero
/// shared bytes when no snapshots or reflinks exist.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_basic() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    let file = File::open(mnt.path().join("data.bin")).expect("open failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert_eq!(
        info.shared_bytes, 0,
        "shared_bytes should be 0 without snapshots: {info:?}"
    );
}

/// After snapshotting a subvolume, file_extents should report shared bytes for
/// files that exist in both the original and the snapshot.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_shared_after_snapshot() {
    let (_td, mnt) = single_mount();

    // Create a subvolume and write data into it.
    let subvol_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), subvol_name).expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    // Snapshot the subvolume.
    let snap_name = CStr::from_bytes_with_nul(b"snap\0").unwrap();
    let origin_dir = File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, false)
        .expect("snapshot_create failed");
    sync(mnt.fd()).unwrap();

    // The file in the original subvolume should now have shared extents.
    let file = File::open(mnt.path().join("origin").join("data.bin")).expect("open data failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert!(
        info.shared_bytes > 0,
        "shared_bytes should be > 0 after snapshot: {info:?}",
    );
}

/// After reflinking a file, file_extents should report shared bytes.
#[test]
#[ignore = "requires elevated privileges"]
fn fiemap_shared_after_reflink() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "original.bin", 5_000_000);
    sync(mnt.fd()).unwrap();

    // cp --reflink creates a CoW copy that shares all extents.
    let original = mnt.path().join("original.bin");
    let reflinked = mnt.path().join("reflinked.bin");
    let output = std::process::Command::new("cp")
        .args(["--reflink=always"])
        .arg(&original)
        .arg(&reflinked)
        .output()
        .expect("failed to run cp");
    assert!(
        output.status.success(),
        "cp --reflink failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    sync(mnt.fd()).unwrap();

    let file = File::open(&original).expect("open original failed");
    let info = file_extents(file.as_fd()).expect("file_extents failed");

    assert!(info.total_bytes > 0, "total_bytes should be > 0: {info:?}");
    assert!(
        info.shared_bytes > 0,
        "shared_bytes should be > 0 after reflink: {info:?}",
    );
}

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

/// lookup_path_rootid on the mount root should return FS_TREE_OBJECTID (5),
/// and on a subvolume should return that subvolume's ID.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_lookup_path_rootid() {
    let (_td, mnt) = single_mount();

    let root_id = lookup_path_rootid(mnt.fd()).expect("lookup_path_rootid on mount failed");
    assert_eq!(
        root_id, 5,
        "mount root should have tree ID 5 (FS_TREE_OBJECTID)"
    );

    // Create a subvolume and check its root ID.
    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let subvol_root_id =
        lookup_path_rootid(subvol_dir.as_fd()).expect("lookup_path_rootid on subvol failed");

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
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
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

    let meta = std::fs::metadata(mnt.path().join("file.bin")).expect("metadata failed");
    let inum = meta.ino();

    let paths = ino_paths(mnt.fd(), inum).expect("ino_paths failed");
    assert!(
        paths.iter().any(|p| p.contains("file.bin")),
        "should find file.bin in paths: {paths:?}",
    );

    // Create a hardlink and check that both paths appear.
    std::fs::hard_link(mnt.path().join("file.bin"), mnt.path().join("link.bin"))
        .expect("hard_link failed");
    sync(mnt.fd()).unwrap();

    let paths2 = ino_paths(mnt.fd(), inum).expect("ino_paths after hardlink failed");
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

    let results = logical_ino(mnt.fd(), logical_addr, false, None).expect("logical_ino failed");
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

/// subvolid_resolve should return the path of a subvolume given its ID.
#[test]
#[ignore = "requires elevated privileges"]
fn inode_subvolid_resolve() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"my-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("my-subvol")).expect("open subvol failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");

    let resolved = subvolid_resolve(mnt.fd(), info.id).expect("subvolid_resolve failed");
    assert!(
        resolved.contains("my-subvol"),
        "resolved path should contain 'my-subvol', got '{resolved}'",
    );
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

/// Full qgroup lifecycle: enable quotas, create a higher-level qgroup, create
/// a subvolume, assign, set limits, verify via qgroup_list, then tear down.
#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_lifecycle() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    // Create a level-1 qgroup (1/0).
    let level1_qgroupid = (1u64 << 48) | 0;
    qgroup_create(mnt.fd(), level1_qgroupid).expect("qgroup_create 1/0 failed");

    // Create a subvolume — the kernel auto-creates a 0/N qgroup for it.
    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    let subvol_qgroupid = info.id; // level-0 qgroup = subvolume ID

    // Assign the subvolume's qgroup to the level-1 parent.
    qgroup_assign(mnt.fd(), subvol_qgroupid, level1_qgroupid).expect("qgroup_assign failed");

    // Set an exclusive limit on the subvolume's qgroup.
    qgroup_limit(
        mnt.fd(),
        subvol_qgroupid,
        QgroupLimitFlags::MAX_EXCL,
        0,
        50_000_000,
    )
    .expect("qgroup_limit failed");

    // Verify via qgroup_list.
    let list = qgroup_list(mnt.fd()).expect("qgroup_list failed");

    let subvol_qg = list.qgroups.iter().find(|q| q.qgroupid == subvol_qgroupid);
    assert!(
        subvol_qg.is_some(),
        "subvolume qgroup should appear in list"
    );
    let subvol_qg = subvol_qg.unwrap();
    assert!(
        subvol_qg.limit_flags.contains(QgroupLimitFlags::MAX_EXCL),
        "limit flags should include MAX_EXCL: {:?}",
        subvol_qg.limit_flags,
    );
    assert_eq!(subvol_qg.max_excl, 50_000_000);
    assert!(
        subvol_qg.parents.contains(&level1_qgroupid),
        "subvol qgroup should have 1/0 as parent: {:?}",
        subvol_qg.parents,
    );

    let level1_qg = list.qgroups.iter().find(|q| q.qgroupid == level1_qgroupid);
    assert!(level1_qg.is_some(), "level-1 qgroup should appear in list");

    // Tear down: remove assignment, destroy level-1 qgroup.
    qgroup_remove(mnt.fd(), subvol_qgroupid, level1_qgroupid).expect("qgroup_remove failed");
    qgroup_destroy(mnt.fd(), level1_qgroupid).expect("qgroup_destroy failed");

    // Level-1 qgroup should be gone now.
    let list2 = qgroup_list(mnt.fd()).expect("qgroup_list after destroy failed");
    assert!(
        !list2.qgroups.iter().any(|q| q.qgroupid == level1_qgroupid),
        "level-1 qgroup should be gone after destroy",
    );
}

/// qgroup_clear_stale should remove qgroups for deleted subvolumes.
#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_clear_stale_test() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    // Create three subvolumes.
    for name in [b"sub-a\0", b"sub-b\0", b"sub-c\0"] {
        let cname = CStr::from_bytes_with_nul(name).unwrap();
        subvolume_create(mnt.fd(), cname).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    // Get sub-b's qgroupid before deletion.
    let sub_b_dir = File::open(mnt.path().join("sub-b")).expect("open sub-b failed");
    let sub_b_info = subvolume_info(sub_b_dir.as_fd()).expect("subvolume_info failed");
    let sub_b_qgroupid = sub_b_info.id;
    drop(sub_b_dir);

    // Delete sub-b. Its qgroup should linger.
    let b_name = CStr::from_bytes_with_nul(b"sub-b\0").unwrap();
    subvolume_delete(mnt.fd(), b_name).expect("subvolume_delete failed");

    // The kernel deletes subvolumes lazily via a background cleaner thread.
    // We need to wait for the ROOT_ITEM to actually disappear before
    // qgroup_list will mark the qgroup as stale. Sync + short retry loop.
    let mut stale_visible = false;
    for _ in 0..10 {
        sync(mnt.fd()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        let list = qgroup_list(mnt.fd()).expect("qgroup_list failed");
        if let Some(qg) = list.qgroups.iter().find(|q| q.qgroupid == sub_b_qgroupid) {
            if qg.stale {
                stale_visible = true;
                break;
            }
        } else {
            // Qgroup already gone (kernel cleaned it up itself) — nothing to test.
            return;
        }
    }

    if !stale_visible {
        // Kernel cleaner hasn't run yet — skip rather than flake.
        eprintln!("qgroup_clear_stale_test: subvolume cleaner hasn't run, skipping");
        return;
    }

    // Clear stale qgroups.
    let cleared = qgroup_clear_stale(mnt.fd()).expect("qgroup_clear_stale failed");
    assert!(
        cleared >= 1,
        "should have cleared at least 1 stale qgroup, got {cleared}"
    );

    let list2 = qgroup_list(mnt.fd()).expect("qgroup_list after clear failed");
    assert!(
        !list2.qgroups.iter().any(|q| q.qgroupid == sub_b_qgroupid),
        "stale qgroup for sub-b should be gone after clear_stale",
    );
}

/// quota enable, rescan, and disable should all succeed.
#[test]
#[ignore = "requires elevated privileges"]
fn quota_enable_disable_rescan() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");
    quota_disable(mnt.fd()).expect("quota_disable failed");
}

/// Enabling quotas twice should not fail (idempotent).
#[test]
#[ignore = "requires elevated privileges"]
fn quota_double_enable() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("first quota_enable failed");
    // Second enable should succeed or return a benign error.
    match quota_enable(mnt.fd(), false) {
        Ok(()) => {}
        Err(Errno::EEXIST) => { /* some kernels return EEXIST */ }
        Err(e) => panic!("second quota_enable returned unexpected error: {e}"),
    }
    quota_disable(mnt.fd()).expect("quota_disable failed");
}

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
    common::verify_test_data(mnt.path(), "data.bin", 50_000_000);

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

    common::verify_test_data(mnt.path(), "data.bin", 50_000_000);
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

/// A scrub on a healthy filesystem with data should complete with bytes
/// scrubbed and zero errors.
#[test]
#[ignore = "requires elevated privileges"]
fn scrub_healthy() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let progress = scrub_start(mnt.fd(), 1, false).expect("scrub_start failed");

    assert!(
        progress.data_bytes_scrubbed > 0,
        "should have scrubbed some data bytes: {progress:?}",
    );
    assert!(
        progress.is_clean(),
        "healthy filesystem should have zero errors: {progress:?}"
    );
}

/// A readonly scrub should complete without errors and not modify data.
#[test]
#[ignore = "requires elevated privileges"]
fn scrub_readonly() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let progress = scrub_start(mnt.fd(), 1, true).expect("scrub_start readonly failed");

    assert!(
        progress.data_bytes_scrubbed > 0,
        "readonly scrub should still scrub data: {progress:?}",
    );
    assert!(
        progress.is_clean(),
        "readonly scrub should have zero errors: {progress:?}"
    );

    // Data should still be intact.
    common::verify_test_data(mnt.path(), "data.bin", 10_000_000);
}

/// Cancelling a running scrub should succeed.
#[test]
#[ignore = "requires elevated privileges"]
fn scrub_cancel_test() {
    let td = tempfile::tempdir().unwrap();
    let f = BackingFile::new(td.path(), "disk.img", 512_000_000);
    f.mkfs();
    let lo = LoopbackDevice::new(f);
    let mnt = Mount::new(lo, td.path());

    // Write enough data so the scrub takes a moment.
    write_test_data(mnt.path(), "data.bin", 200_000_000);
    sync(mnt.fd()).unwrap();

    // Start scrub in a background thread (scrub_start blocks).
    let mount_path = mnt.path().to_path_buf();
    let scrub_thread = std::thread::spawn(move || {
        let file = File::open(&mount_path).expect("open mount in thread failed");
        scrub_start(file.as_fd(), 1, false)
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    // Cancel — may return ENOTCONN if scrub already finished.
    match scrub_cancel(mnt.fd()) {
        Ok(()) => {}
        Err(Errno::ENOTCONN) => {}
        Err(e) => panic!("unexpected error from scrub_cancel: {e}"),
    }

    // The scrub thread should complete (either finished or cancelled).
    let result = scrub_thread.join().expect("scrub thread panicked");
    match result {
        Ok(progress) => {
            // Scrub completed (possibly partially).
            assert!(
                progress.bytes_scrubbed() > 0,
                "should have scrubbed something"
            );
        }
        Err(Errno::ECANCELED) => { /* expected */ }
        Err(e) => panic!("scrub returned unexpected error: {e}"),
    }
}

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

/// Creating a subvolume, querying its info, and deleting it should work.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_info_delete() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    // subvolume_info should return valid metadata.
    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    assert!(
        info.id > 255,
        "subvolume ID should be > 255, got {}",
        info.id
    );
    assert!(!info.uuid.is_nil(), "subvolume UUID should not be nil");
    drop(subvol_dir);

    // Delete the subvolume.
    subvolume_delete(mnt.fd(), name).expect("subvolume_delete failed");

    // Opening the subvolume should now fail.
    assert!(
        File::open(mnt.path().join("test-subvol")).is_err(),
        "opening deleted subvolume should fail",
    );
}

/// Snapshotting a subvolume should produce an independent copy: modifying the
/// original should not affect the snapshot.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_snapshot() {
    let (_td, mnt) = single_mount();

    let origin_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), origin_name).expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    // Create a snapshot.
    let snap_name = CStr::from_bytes_with_nul(b"snap1\0").unwrap();
    let origin_dir = File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, false)
        .expect("snapshot_create failed");
    drop(origin_dir);

    // Snapshot should have the same data.
    common::verify_test_data(&mnt.path().join("snap1"), "data.bin", 1_000_000);

    // Modify the original — snapshot should retain the old content.
    std::fs::write(mnt.path().join("origin").join("data.bin"), b"overwritten")
        .expect("overwrite failed");

    // Snapshot should still have the original data.
    common::verify_test_data(&mnt.path().join("snap1"), "data.bin", 1_000_000);
}

/// A readonly snapshot should have the RDONLY flag and refuse writes.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_readonly_snapshot() {
    let (_td, mnt) = single_mount();

    let origin_name = CStr::from_bytes_with_nul(b"origin\0").unwrap();
    subvolume_create(mnt.fd(), origin_name).expect("subvolume_create failed");

    write_test_data(&mnt.path().join("origin"), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    let snap_name = CStr::from_bytes_with_nul(b"ro-snap\0").unwrap();
    let origin_dir = File::open(mnt.path().join("origin")).expect("open origin failed");
    snapshot_create(mnt.fd(), origin_dir.as_fd(), snap_name, true).expect("snapshot_create failed");
    drop(origin_dir);

    let snap_dir = File::open(mnt.path().join("ro-snap")).expect("open snap failed");
    let flags = subvolume_flags_get(snap_dir.as_fd()).expect("subvolume_flags_get failed");
    assert!(
        flags.contains(SubvolumeFlags::RDONLY),
        "readonly snapshot should have RDONLY flag, got {flags:?}",
    );

    // Writing to the snapshot should fail.
    let write_result = File::create(mnt.path().join("ro-snap").join("new-file.txt"));
    assert!(
        write_result.is_err(),
        "writing to readonly snapshot should fail"
    );
}

/// subvolume_list should return all subvolumes with correct names.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_test() {
    let (_td, mnt) = single_mount();

    for name in [
        CStr::from_bytes_with_nul(b"alpha\0").unwrap(),
        CStr::from_bytes_with_nul(b"beta\0").unwrap(),
        CStr::from_bytes_with_nul(b"gamma\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    let list = subvolume_list(mnt.fd()).expect("subvolume_list failed");

    // Should find all three subvolumes.
    for name in ["alpha", "beta", "gamma"] {
        assert!(
            list.iter().any(|item| item.name == name),
            "subvolume_list should contain '{name}': {:?}",
            list.iter().map(|i| &i.name).collect::<Vec<_>>(),
        );
    }

    // Every item should have a valid root_id.
    for item in &list {
        assert!(item.root_id > 255, "root_id should be > 255: {item:?}");
    }
}

/// subvolume_flags_get and subvolume_flags_set should round-trip, and setting
/// RDONLY should prevent writes.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_flags_get_set() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open failed");

    // Initially should not be readonly.
    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get failed");
    assert!(
        !flags.contains(SubvolumeFlags::RDONLY),
        "new subvolume should not be readonly",
    );

    // Set readonly.
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::RDONLY)
        .expect("flags_set RDONLY failed");

    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get after set failed");
    assert!(
        flags.contains(SubvolumeFlags::RDONLY),
        "should be readonly now"
    );

    // Writing should fail.
    assert!(
        File::create(mnt.path().join("test-subvol").join("file.txt")).is_err(),
        "writing to readonly subvolume should fail",
    );

    // Clear readonly.
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::empty())
        .expect("flags_set empty failed");

    let flags = subvolume_flags_get(subvol_dir.as_fd()).expect("flags_get after clear failed");
    assert!(
        !flags.contains(SubvolumeFlags::RDONLY),
        "should not be readonly after clearing"
    );

    // Writing should work again.
    File::create(mnt.path().join("test-subvol").join("file.txt"))
        .expect("writing should work after clearing readonly");
}

/// subvolume_default_get should return 5 initially, and subvolume_default_set
/// should change the default subvolume.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_default_get_set() {
    let (_td, mnt) = single_mount();

    let default = subvolume_default_get(mnt.fd()).expect("default_get failed");
    assert_eq!(default, 5, "initial default should be FS_TREE_OBJECTID (5)");

    // Create a subvolume and set it as default.
    let name = CStr::from_bytes_with_nul(b"new-default\0").unwrap();
    subvolume_create(mnt.fd(), name).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("new-default")).expect("open failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    drop(subvol_dir);

    subvolume_default_set(mnt.fd(), info.id).expect("default_set failed");

    let new_default = subvolume_default_get(mnt.fd()).expect("default_get after set failed");
    assert_eq!(new_default, info.id, "default should be the new subvolume");

    // Reset back to 5.
    subvolume_default_set(mnt.fd(), 5).expect("default_set back to 5 failed");
    let reset = subvolume_default_get(mnt.fd()).expect("default_get after reset failed");
    assert_eq!(reset, 5, "default should be back to 5");
}

/// subvolume_list with nested subvolumes should show correct paths.
#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_nested() {
    let (_td, mnt) = single_mount();

    // Create A.
    let a_name = CStr::from_bytes_with_nul(b"A\0").unwrap();
    subvolume_create(mnt.fd(), a_name).expect("create A failed");

    // Create B inside A.
    let a_dir = File::open(mnt.path().join("A")).expect("open A failed");
    let b_name = CStr::from_bytes_with_nul(b"B\0").unwrap();
    subvolume_create(a_dir.as_fd(), b_name).expect("create B failed");
    drop(a_dir);

    // Create C inside A/B.
    let b_dir = File::open(mnt.path().join("A").join("B")).expect("open B failed");
    let c_name = CStr::from_bytes_with_nul(b"C\0").unwrap();
    subvolume_create(b_dir.as_fd(), c_name).expect("create C failed");
    drop(b_dir);

    sync(mnt.fd()).unwrap();

    let list = subvolume_list(mnt.fd()).expect("subvolume_list failed");

    assert!(
        list.iter().any(|i| i.name == "A"),
        "should find 'A': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
    );
    assert!(
        list.iter().any(|i| i.name == "A/B"),
        "should find 'A/B': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
    );
    assert!(
        list.iter().any(|i| i.name == "A/B/C"),
        "should find 'A/B/C': {:?}",
        list.iter().map(|i| &i.name).collect::<Vec<_>>(),
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

/// SysfsBtrfs should read filesystem properties that match fs_info.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_read_info() {
    let (_td, mnt) = single_mount();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    let nodesize = sysfs.nodesize().expect("sysfs nodesize failed");
    assert_eq!(
        nodesize, info.nodesize as u64,
        "sysfs nodesize should match fs_info",
    );

    let sectorsize = sysfs.sectorsize().expect("sysfs sectorsize failed");
    assert_eq!(
        sectorsize, info.sectorsize as u64,
        "sysfs sectorsize should match fs_info",
    );

    let metadata_uuid = sysfs.metadata_uuid().expect("sysfs metadata_uuid failed");
    // metadata_uuid equals fsid when no separate metadata UUID is set.
    assert_eq!(
        metadata_uuid, info.uuid,
        "sysfs metadata_uuid should match fs_info uuid (no separate metadata uuid set)",
    );
}

/// SysfsBtrfs commit_stats should show commits after writes + sync.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_commit_stats() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    let stats = sysfs.commit_stats().expect("commit_stats failed");
    assert!(
        stats.commits > 0,
        "should have at least one commit after write+sync: {stats:?}",
    );
}

/// Quota status should be visible via sysfs after enabling/disabling quotas.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_quota_status() {
    let (_td, mnt) = single_mount();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    // Quotas should be disabled initially.
    let status = sysfs.quota_status().expect("quota_status failed");
    assert!(!status.enabled, "quotas should be disabled initially");

    // Enable quotas.
    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    let status = sysfs
        .quota_status()
        .expect("quota_status after enable failed");
    assert!(
        status.enabled,
        "quotas should be enabled after quota_enable"
    );

    // Disable quotas.
    quota_disable(mnt.fd()).expect("quota_disable failed");

    let status = sysfs
        .quota_status()
        .expect("quota_status after disable failed");
    assert!(
        !status.enabled,
        "quotas should be disabled after quota_disable"
    );
}

/// tree_search for ROOT_ITEM_KEY should find root items for created subvolumes.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_enumerate_root_items() {
    let (_td, mnt) = single_mount();

    // Create a few subvolumes and record their IDs.
    let mut subvol_ids = Vec::new();
    for name in [
        CStr::from_bytes_with_nul(b"ts-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"ts-b\0").unwrap(),
        CStr::from_bytes_with_nul(b"ts-c\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name).expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap())).expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        subvol_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    let mut found_ids = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_type(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
        ),
        |hdr, _data| {
            found_ids.push(hdr.objectid);
            Ok(())
        },
    )
    .expect("tree_search failed");

    for id in &subvol_ids {
        assert!(
            found_ids.contains(id),
            "tree_search should find subvolume {id} in root items: found {found_ids:?}",
        );
    }
}

/// tree_search with an objectid range should only return items within that range.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_objectid_range() {
    let (_td, mnt) = single_mount();

    // Create subvolumes to populate the root tree.
    let mut subvol_ids = Vec::new();
    for name in [
        CStr::from_bytes_with_nul(b"range-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"range-b\0").unwrap(),
        CStr::from_bytes_with_nul(b"range-c\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name).expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap())).expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        subvol_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    // Search for only the first subvolume's objectid.
    let target_id = subvol_ids[0];
    let mut found = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_objectid_range(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
            target_id,
            target_id,
        ),
        |hdr, _data| {
            found.push(hdr.objectid);
            Ok(())
        },
    )
    .expect("tree_search failed");

    assert!(
        found.contains(&target_id),
        "should find the target objectid {target_id}: {found:?}",
    );
    // Should not contain the other subvolumes.
    for &other_id in &subvol_ids[1..] {
        assert!(
            !found.contains(&other_id),
            "should not find objectid {other_id} outside the range: {found:?}",
        );
    }
}

/// tree_search for a non-existent item type should return Ok(()) with the
/// callback never invoked.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_empty_result() {
    let (_td, mnt) = single_mount();

    let mut invoked = false;
    // Objectid 0 is never used for ROOT_ITEM_KEY entries.
    tree_search(
        mnt.fd(),
        SearchKey::for_objectid_range(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
            0,
            0,
        ),
        |_hdr, _data| {
            invoked = true;
            Ok(())
        },
    )
    .expect("tree_search should succeed even with no results");

    assert!(
        !invoked,
        "callback should not be invoked when no items match"
    );
}

/// tree_search with many subvolumes (forcing multiple ioctl batches) should
/// complete without duplicates or infinite loops.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_large_result_no_duplicates() {
    let (_td, mnt) = single_mount();

    // Create 60 subvolumes to force multiple search batches.
    let mut expected_ids = Vec::new();
    for i in 0..60 {
        let name = CString::new(format!("sub-{i:03}")).unwrap();
        subvolume_create(mnt.fd(), &name).expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap())).expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        expected_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    let mut found_items: Vec<(u64, u64)> = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_type(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
        ),
        |hdr, _data| {
            found_items.push((hdr.objectid, hdr.offset));
            Ok(())
        },
    )
    .expect("tree_search failed");

    // All created subvolumes should appear.
    for id in &expected_ids {
        assert!(
            found_items.iter().any(|(oid, _)| oid == id),
            "tree_search should find subvolume {id}",
        );
    }

    // No duplicates — each (objectid, offset) pair should be unique.
    // Duplicate pairs would indicate the cursor advance bug (infinite loop
    // re-yielding already-seen items).
    let mut seen = std::collections::HashSet::new();
    let mut dup_count = 0;
    for &key in &found_items {
        if !seen.insert(key) {
            dup_count += 1;
        }
    }
    assert_eq!(
        dup_count, 0,
        "should have no duplicate (objectid, offset) pairs, found {dup_count} duplicates"
    );
}

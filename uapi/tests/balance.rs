use crate::common::{
    BackingFile, LoopbackDevice, Mount, single_mount, write_test_data,
};
use btrfs_uapi::{
    balance::{
        BalanceArgs, BalanceCtl, BalanceFlags, BalanceState, balance,
        balance_ctl, balance_progress,
    },
    chunk::device_chunk_allocations,
    device::device_add,
    filesystem::sync,
    space::{BlockGroupFlags, space_info},
};
use nix::errno::Errno;
use std::{ffi::CString, fs::File, os::unix::io::AsFd};

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

    let flags =
        BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    let progress =
        balance(mnt.fd(), flags, None, None, None).expect("balance failed");

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
        let file =
            File::open(&mount_path).expect("failed to open mount in thread");
        let flags =
            BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
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
        let file =
            File::open(&mount_path).expect("failed to open mount in thread");
        let flags =
            BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
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
            let resume_fd =
                File::open(&mnt.path()).expect("failed to open mount");
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
        let file =
            File::open(&mount_path).expect("failed to open mount in thread");
        let flags =
            BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
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

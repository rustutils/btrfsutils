use crate::common::{
    BackingFile, LoopbackDevice, Mount, single_mount, write_test_data,
};
use btrfs_uapi::{
    filesystem::sync,
    scrub::{scrub_cancel, scrub_start},
};
use nix::errno::Errno;
use std::{fs::File, os::unix::io::AsFd};

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

    let progress =
        scrub_start(mnt.fd(), 1, true).expect("scrub_start readonly failed");

    assert!(
        progress.data_bytes_scrubbed > 0,
        "readonly scrub should still scrub data: {progress:?}",
    );
    assert!(
        progress.is_clean(),
        "readonly scrub should have zero errors: {progress:?}"
    );

    // Data should still be intact.
    crate::common::verify_test_data(mnt.path(), "data.bin", 10_000_000);
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
        let file =
            File::open(&mount_path).expect("open mount in thread failed");
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

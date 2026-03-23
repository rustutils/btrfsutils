//! Integration tests for the ioctls.
//!
//! These tests require a mounted btrfs filesystem and root privileges. They are
//! skipped automatically unless the test is run with `--include-ignored`.
//!
//! To run:
//!   just test-privileged

use btrfs_uapi::balance::{
    BalanceCtl, BalanceFlags, BalanceState, balance, balance_ctl, balance_progress,
};
use nix::errno::Errno;
use std::{
    fs::{self, File},
    os::unix::io::AsFd,
    path::PathBuf,
    process::Command,
};

/// Expands to the name of the function it is invoked from.
///
/// Works by defining a dummy nested function and using `type_name_of_val` to
/// recover the fully-qualified path, then extracting the last component.
macro_rules! test_name {
    () => {{
        fn f() {}
        let full = std::any::type_name_of_val(&f);
        // full is something like "balance::my_test::{{closure}}::f" or
        // "balance::my_test::f"; strip the trailing "::f" and any
        // "::{{closure}}" suffix, then take the last path component.
        let trimmed = full
            .trim_end_matches("::f")
            .trim_end_matches("::{{closure}}");
        trimmed.rsplit("::").next().unwrap_or(trimmed)
    }};
}

/// A temporary btrfs filesystem backed by a loop-mounted image file.
///
/// Created with [`BtrfsFixture::create`]. The filesystem is unmounted and the
/// image file is deleted when this value is dropped.
struct BtrfsFixture {
    /// Path to the mounted filesystem.
    pub mount: PathBuf,
    /// Path to the backing image file.
    image: PathBuf,
    /// Loop device path (e.g. `/dev/loop0`).
    loop_dev: String,
}

impl BtrfsFixture {
    /// Create a temporary btrfs filesystem image, attach it to a loop device,
    /// and mount it under a temporary directory.
    ///
    /// `name` is used as part of the path so that parallel tests don't
    /// collide. Prefer passing `test_name!()` at the call site.
    fn create(name: &str) -> Self {
        let base = PathBuf::from("/tmp/btrfs-progrs-tests");
        fs::create_dir_all(&base).expect("failed to create /tmp/btrfs-progrs-tests");
        let id = format!("{}-{}", std::process::id(), name);
        let image = base.join(format!("{id}.img"));
        let mount = base.join(id);

        // 512 MiB — large enough that balance has something to do.
        Self::run("truncate", &["-s", "512M", image.to_str().unwrap()]);
        Self::run("mkfs.btrfs", &["-f", image.to_str().unwrap()]);

        fs::create_dir_all(&mount).expect("failed to create mount directory");

        // Attach to a loop device.
        let output = Command::new("losetup")
            .args(["--find", "--show", image.to_str().unwrap()])
            .output()
            .expect("failed to run losetup");
        assert!(
            output.status.success(),
            "losetup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let loop_dev = String::from_utf8(output.stdout)
            .expect("losetup output is not UTF-8")
            .trim()
            .to_string();

        Self::run(
            "mount",
            &["-t", "btrfs", &loop_dev, mount.to_str().unwrap()],
        );

        Self {
            mount,
            image,
            loop_dev,
        }
    }

    /// Open the mount root as a file descriptor suitable for ioctls.
    fn open(&self) -> File {
        File::open(&self.mount).expect("failed to open btrfs mount")
    }

    fn run(cmd: &str, args: &[&str]) {
        println!("running {cmd} {args:?}");
        let status = Command::new(cmd)
            .args(args)
            .status()
            .unwrap_or_else(|e| panic!("failed to run {cmd}: {e}"));
        assert!(status.success(), "{cmd} exited with status {status}");
    }
}

impl Drop for BtrfsFixture {
    fn drop(&mut self) {
        // Best-effort cleanup — don't panic in drop.
        let _ = Command::new("umount").arg(&self.mount).status();
        let _ = Command::new("losetup")
            .args(["-d", &self.loop_dev])
            .status();
        let _ = fs::remove_file(&self.image);
        let _ = fs::remove_dir(&self.mount);
    }
}

/// Create a fresh [`BtrfsFixture`] named after the calling test function.
macro_rules! fixture {
    () => {
        BtrfsFixture::create(test_name!())
    };
}

/// Querying balance progress on an idle filesystem should indicate that no
/// balance is running (ENOTCONN).
#[test]
#[ignore = "requires elevated privileges"]
fn balance_progress_idle() {
    let fixture = fixture!();
    let file = fixture.open();

    match balance_progress(file.as_fd()) {
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
    let fixture = fixture!();
    let file = fixture.open();

    match balance_ctl(file.as_fd(), BalanceCtl::Pause) {
        Err(e) if e == Errno::ENOTCONN => { /* expected */ }
        Err(e) => panic!("unexpected error from balance_ctl(Pause): {e}"),
        Ok(()) => panic!("expected ENOTCONN, but pause succeeded"),
    }
}

/// Cancelling when no balance is running should return ENOTCONN.
#[test]
#[ignore = "requires elevated privileges"]
fn balance_cancel_not_running() {
    let fixture = fixture!();
    let file = fixture.open();

    match balance_ctl(file.as_fd(), BalanceCtl::Cancel) {
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
    let fixture = fixture!();
    let file = fixture.open();

    let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
    let progress = balance(file.as_fd(), flags, None, None, None).expect("balance failed");

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
    let fixture = fixture!();

    // Kick off a balance in a background thread.
    let mount = fixture.mount.clone();
    let balance_thread = std::thread::spawn(move || {
        let file = File::open(&mount).expect("failed to open mount in thread");
        let flags = BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
        balance(file.as_fd(), flags, None, None, None)
    });

    // Give the kernel a moment to start the balance before cancelling.
    std::thread::sleep(std::time::Duration::from_millis(200));

    let file = fixture.open();
    // Cancel may return ENOTCONN if the balance already finished on a small
    // filesystem — that's fine.
    match balance_ctl(file.as_fd(), BalanceCtl::Cancel) {
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

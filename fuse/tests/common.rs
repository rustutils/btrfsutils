//! Shared helpers for the FUSE mount integration tests.
//!
//! Each test gets its own [`MountedFuse`] instance — fresh mountpoint,
//! fresh `btrfs-fuse` child process — so test isolation is preserved
//! while parallel test execution still works. The fixture image is
//! built once per test process via `OnceLock` and shared.
//!
//! These tests are unprivileged: `fusermount` is setuid root on every
//! distro the project supports, which is enough to mount/unmount
//! FUSE filesystems as a normal user. If `fusermount` is unavailable
//! on a CI runner, mark the test `#[ignore]` rather than reaching for
//! sudo.

#![allow(dead_code)] // each test file uses a subset of the helpers

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

/// How long [`MountedFuse::mount`] waits for the mountpoint to become
/// readable before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Polling interval while waiting for mount readiness.
const READY_POLL: Duration = Duration::from_millis(50);

/// Build a source directory under `base` with a known set of files,
/// then format `base/test.img` as btrfs using `mkfs.btrfs --rootdir`.
///
/// Mirrors `fs/tests/basic.rs::build_fixture` deliberately: the two
/// suites should be free to evolve independently for now.
fn build_fixture(base: &Path) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();

    fs::write(src.join("hello.txt"), b"hello, world\n").unwrap();
    fs::write(src.join("empty.txt"), b"").unwrap();
    fs::write(src.join("large.bin"), vec![0x42u8; 100_000]).unwrap();

    let sub = src.join("subdir");
    fs::create_dir(&sub).unwrap();
    fs::write(sub.join("nested.txt"), b"nested content\n").unwrap();

    std::os::unix::fs::symlink("hello.txt", src.join("link")).unwrap();

    // user.* xattrs may be rejected by the underlying tmpfs; the
    // xattr test handles a missing xattr gracefully.
    let _ = Command::new("setfattr")
        .args([
            "-n",
            "user.greeting",
            "-v",
            "hi",
            src.join("hello.txt").to_str().unwrap(),
        ])
        .status();

    let img = base.join("test.img");
    File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    btrfs_test_utils::run(
        "mkfs.btrfs",
        &[
            "-f",
            "--rootdir",
            src.to_str().unwrap(),
            img.to_str().unwrap(),
        ],
    );
    img
}

/// Path to the per-process shared fixture image, built on first use.
fn fixture_path() -> &'static Path {
    static INIT: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let (_td, path) = INIT.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_fixture(td.path());
        (td, img)
    });
    path
}

/// RAII guard around a running `btrfs-fuse` child process and its
/// mountpoint. Constructed via [`MountedFuse::mount`]; on `Drop` it
/// unmounts (lazy, so a still-busy mount doesn't wedge cleanup) and
/// reaps the child.
pub struct MountedFuse {
    /// Tempdir holding the mountpoint. Kept alive so it isn't deleted
    /// before unmount.
    _tempdir: tempfile::TempDir,
    mountpoint: PathBuf,
    child: Option<Child>,
}

impl MountedFuse {
    /// Mount the fixture image and wait for the kernel to surface its
    /// contents. Panics on timeout or spawn failure.
    pub fn mount() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mountpoint = tempdir.path().to_path_buf();
        let bin = env!("CARGO_BIN_EXE_btrfs-fuse");
        let child = Command::new(bin)
            .arg(fixture_path())
            .arg(&mountpoint)
            .arg("-f")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn btrfs-fuse binary");

        let mut this = Self {
            _tempdir: tempdir,
            mountpoint,
            child: Some(child),
        };
        this.wait_until_ready();
        this
    }

    /// The mountpoint where the fuse filesystem is reachable.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.mountpoint
    }

    /// Poll the mountpoint until it lists the expected fixture
    /// contents, or panic after [`READY_TIMEOUT`]. Polling
    /// `read_dir` doubles as a liveness probe — if the child died
    /// during bootstrap we surface the failure here rather than later
    /// in an opaque test assertion.
    fn wait_until_ready(&mut self) {
        let start = Instant::now();
        loop {
            if let Some(child) = self.child.as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!(
                        "btrfs-fuse exited before mount was ready: {status}"
                    );
                }
            }
            if let Ok(entries) = fs::read_dir(&self.mountpoint) {
                if entries.flatten().any(|e| e.file_name() == "hello.txt") {
                    return;
                }
            }
            if start.elapsed() > READY_TIMEOUT {
                panic!(
                    "fuse mount at {} did not become ready within {:?}",
                    self.mountpoint.display(),
                    READY_TIMEOUT,
                );
            }
            thread::sleep(READY_POLL);
        }
    }
}

impl Drop for MountedFuse {
    fn drop(&mut self) {
        // `-z` (lazy) detaches even if the mount is still busy with
        // FDs from a panicked test thread. Without it, a panic
        // mid-test could leave the mountpoint stuck and break
        // cleanup of the tempdir.
        let _ = Command::new("fusermount")
            .args(["-u", "-z"])
            .arg(&self.mountpoint)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
        }
    }
}

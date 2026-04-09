//! Shared RAII test harness for btrfs integration tests.
//!
//! Each struct consumes and owns the previous layer. Drop cleans up from the
//! inside out: `Mount` unmounts, then its inner `LoopbackDevice` detaches,
//! then the `BackingFile` removes the image file.
//!
//! ```text
//! Mount  owns  LoopbackDevice  owns  BackingFile
//!   │              │                      │
//!  umount      losetup -d             rm file
//! ```
//!
//! All operations shell out to standard Linux tools (`losetup`, `mount`,
//! `umount`, `mkfs.btrfs`, `gunzip`) so this crate has no dependency on any
//! of the workspace library crates. It is intended for use as a
//! `[dev-dependencies]` entry only — production code must not link against
//! it. [`find_our_mkfs`] locates the workspace's `btrfs-mkfs` binary for
//! callers that want to use it instead of the system `mkfs.btrfs`.
//!
//! # Binary path parameters
//!
//! Helpers that need to invoke tools built out of this workspace (for
//! example `btrfs-mkfs --rootdir`) take the binary path as an explicit
//! `&Path` parameter. That is because `env!("CARGO_BIN_EXE_<name>")` only
//! resolves inside the crate that declares the `[[bin]]`, so the caller
//! must look the binary up and pass it in.

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use std::{
    fs::{self, File},
    io::Write,
    mem::ManuallyDrop,
    os::unix::io::{AsFd, BorrowedFd},
    path::{Path, PathBuf},
    process::Command,
};

/// Fixed UUID used by [`deterministic_mount`] for reproducible test output.
pub const TEST_UUID: &str = "deadbeef-dead-beef-dead-beefdeadbeef";

/// Fixed label used by [`deterministic_mount`] for reproducible test output.
pub const TEST_LABEL: &str = "test-fs";

/// Locate the `btrfs-mkfs` binary in the same target directory as the
/// running test binary.
///
/// The test binary lives at `target/{profile}/deps/test_name-hash`. The
/// `btrfs-mkfs` binary lives at `target/{profile}/btrfs-mkfs`. We walk
/// up from the test binary to find it.
///
/// # Panics
///
/// Panics if the binary cannot be found (e.g. `cargo build -p btrfs-mkfs`
/// was not run).
#[must_use]
pub fn find_our_mkfs() -> PathBuf {
    let exe =
        std::env::current_exe().expect("cannot determine test binary path");
    // exe = target/debug/deps/test_name-hash
    let target_dir = exe
        .parent() // target/debug/deps/
        .and_then(Path::parent) // target/debug/
        .expect("cannot determine target directory from test binary path");
    let mkfs = target_dir.join("btrfs-mkfs");
    assert!(
        mkfs.exists(),
        "btrfs-mkfs not found at {}; run `cargo build -p btrfs-mkfs` first",
        mkfs.display()
    );
    mkfs
}

/// A file created via `set_len` (fallocate). Drop removes the file.
pub struct BackingFile {
    path: PathBuf,
}

impl BackingFile {
    #[must_use]
    pub fn new(dir: &Path, name: &str, size: u64) -> Self {
        let path = dir.join(name);
        let file = File::create(&path).unwrap_or_else(|e| {
            panic!("failed to create {}: {e}", path.display())
        });
        file.set_len(size).unwrap_or_else(|e| {
            panic!("failed to set length of {}: {e}", path.display())
        });
        Self { path }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Grow or shrink the backing file. Call [`LoopbackDevice::refresh_size`]
    /// afterwards if a loop device is attached.
    pub fn resize(&self, new_size: u64) {
        let file =
            File::options()
                .write(true)
                .open(&self.path)
                .unwrap_or_else(|e| {
                    panic!("failed to open {}: {e}", self.path.display())
                });
        file.set_len(new_size).unwrap_or_else(|e| {
            panic!("failed to resize {}: {e}", self.path.display())
        });
    }

    /// Format this file as a btrfs filesystem using `mkfs.btrfs`.
    pub fn mkfs(&self) {
        run("mkfs.btrfs", &["-f", self.path.to_str().unwrap()]);
    }

    /// Format with extra options using `mkfs.btrfs`.
    pub fn mkfs_with_args(&self, extra: &[&str]) {
        let mut args: Vec<&str> = vec!["-f"];
        args.extend_from_slice(extra);
        args.push(self.path.to_str().unwrap());
        run("mkfs.btrfs", &args);
    }

    /// Format this file using our `btrfs-mkfs` (located via
    /// [`find_our_mkfs`]).
    pub fn mkfs_ours(&self) {
        let mkfs = find_our_mkfs();
        run(
            mkfs.to_str().unwrap(),
            &["-f", "-q", self.path.to_str().unwrap()],
        );
    }

    /// Format with extra options using our `btrfs-mkfs`.
    pub fn mkfs_ours_with_args(&self, extra: &[&str]) {
        let mkfs = find_our_mkfs();
        let mut args: Vec<&str> = vec!["-f", "-q"];
        args.extend_from_slice(extra);
        args.push(self.path.to_str().unwrap());
        run(mkfs.to_str().unwrap(), &args);
    }

    /// Run our `btrfs-mkfs --rootdir` on this file.
    ///
    /// `mkfs_bin` is the path to the `btrfs-mkfs` binary, which the caller
    /// must locate (typically via [`find_our_mkfs`] or
    /// `env!("CARGO_BIN_EXE_btrfs")`).
    pub fn mkfs_rootdir(
        &self,
        mkfs_bin: &Path,
        rootdir: &Path,
        extra_args: &[&str],
    ) {
        let mut args: Vec<&str> =
            vec!["-f", "--rootdir", rootdir.to_str().unwrap()];
        args.extend_from_slice(extra_args);
        args.push(self.path.to_str().unwrap());
        run(mkfs_bin.to_str().unwrap(), &args);
    }

    /// Format with a fixed UUID and label for deterministic output
    /// using `mkfs.btrfs`.
    pub fn mkfs_with_options(&self, uuid: &str, label: &str) {
        run(
            "mkfs.btrfs",
            &[
                "-f",
                "--uuid",
                uuid,
                "--label",
                label,
                self.path.to_str().unwrap(),
            ],
        );
    }
}

impl Drop for BackingFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// A loop device attached to a file. Optionally owns a [`BackingFile`].
/// Drop detaches with `losetup -d`, then the inner `BackingFile` (if any)
/// removes the image file.
pub struct LoopbackDevice {
    dev_path: PathBuf,
    inner: Option<BackingFile>,
}

impl LoopbackDevice {
    /// Attach a loop device to a backing file, consuming it. Call
    /// [`BackingFile::mkfs`] before this if the file should be formatted.
    #[must_use]
    pub fn new(file: BackingFile) -> Self {
        let dev_path = Self::losetup(file.path());
        Self {
            dev_path,
            inner: Some(file),
        }
    }

    /// Attach a loop device to an existing file without taking ownership.
    /// The file will not be deleted on drop — only the loop device is detached.
    #[must_use]
    pub fn attach_existing(path: &Path) -> Self {
        let dev_path = Self::losetup(path);
        Self {
            dev_path,
            inner: None,
        }
    }

    fn losetup(path: &Path) -> PathBuf {
        let output = Command::new("losetup")
            .args(["--find", "--show", path.to_str().unwrap()])
            .output()
            .expect("failed to run losetup");
        assert!(
            output.status.success(),
            "losetup failed: {}",
            String::from_utf8_lossy(&output.stderr),
        );
        PathBuf::from(
            String::from_utf8(output.stdout)
                .expect("losetup output is not UTF-8")
                .trim(),
        )
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.dev_path
    }

    #[must_use]
    pub fn backing_file(&self) -> Option<&BackingFile> {
        self.inner.as_ref()
    }

    /// Tell the kernel to re-read the size of the backing file. Call this
    /// after [`BackingFile::resize`] to make the loop device reflect the new
    /// size.
    pub fn refresh_size(&self) {
        run(
            "losetup",
            &["--set-capacity", self.dev_path.to_str().unwrap()],
        );
    }
}

impl Drop for LoopbackDevice {
    fn drop(&mut self) {
        let _ = Command::new("losetup")
            .args(["-d", self.dev_path.to_str().unwrap()])
            .status();
    }
}

/// A mounted btrfs filesystem. Owns the [`LoopbackDevice`]. Keeps an open fd
/// for ioctl use. Drop closes the fd, unmounts, then the inner
/// `LoopbackDevice` detaches.
pub struct Mount {
    mountpoint: PathBuf,
    file: ManuallyDrop<File>,
    dev: LoopbackDevice,
}

impl Mount {
    /// Creates `base_dir/mnt` and mounts the loop device there, consuming it.
    #[must_use]
    pub fn new(dev: LoopbackDevice, base_dir: &Path) -> Self {
        Self::with_options(dev, base_dir, &[])
    }

    /// Creates `base_dir/mnt` and mounts with additional `-o` options.
    #[must_use]
    pub fn with_options(
        dev: LoopbackDevice,
        base_dir: &Path,
        extra_opts: &[&str],
    ) -> Self {
        let mountpoint = base_dir.join("mnt");
        fs::create_dir_all(&mountpoint).unwrap_or_else(|e| {
            panic!("failed to create {}: {e}", mountpoint.display())
        });
        let mut args = vec!["-t", "btrfs"];
        if !extra_opts.is_empty() {
            args.push("-o");
            // Join multiple options with commas.
            let opts = extra_opts.join(",");
            // Leak the string so it lives long enough — this is test code.
            args.push(Box::leak(opts.into_boxed_str()));
        }
        args.push(dev.path().to_str().unwrap());
        args.push(mountpoint.to_str().unwrap());
        run("mount", &args);
        let file = File::open(&mountpoint).expect("failed to open mount");
        Self {
            mountpoint,
            file: ManuallyDrop::new(file),
            dev,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.mountpoint
    }

    /// A borrowed fd suitable for btrfs ioctls.
    #[must_use]
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }

    #[must_use]
    pub fn loopback(&self) -> &LoopbackDevice {
        &self.dev
    }

    /// Unmount and return the underlying loop device for reuse.
    #[must_use]
    pub fn into_loopback(self) -> LoopbackDevice {
        // Close the fd first.
        let mut this = ManuallyDrop::new(self);
        // SAFETY: we never use this.file again.
        unsafe { ManuallyDrop::drop(&mut this.file) };

        let output = Command::new("umount")
            .arg(&this.mountpoint)
            .output()
            .expect("failed to run umount");
        assert!(
            output.status.success(),
            "umount {} failed: {}",
            this.mountpoint.display(),
            String::from_utf8_lossy(&output.stderr),
        );
        let _ = fs::remove_dir(&this.mountpoint);

        // SAFETY: we take dev out before the ManuallyDrop prevents Drop.
        // The ManuallyDrop around `this` prevents Mount::drop from running
        // (which would double-umount and double-drop the file).
        unsafe { std::ptr::read(&raw const this.dev) }
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        // SAFETY: we never use self.file again after this.
        unsafe { ManuallyDrop::drop(&mut self.file) };

        let output = Command::new("umount")
            .arg(&self.mountpoint)
            .output()
            .expect("failed to run umount");
        assert!(
            output.status.success(),
            "umount {} failed: {}",
            self.mountpoint.display(),
            String::from_utf8_lossy(&output.stderr),
        );
        let _ = fs::remove_dir(&self.mountpoint);
    }
}

/// Run a command and panic if it fails. Used for `losetup`, `mount`,
/// `mkfs.btrfs`, etc.
pub fn run(cmd: &str, args: &[&str]) {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run {cmd}: {e}"));
    assert!(
        output.status.success(),
        "{cmd} {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Write a file filled with a deterministic byte pattern. The pattern uses
/// `byte = position % 251` (prime modulus avoids alignment artifacts).
pub fn write_test_data(dir: &Path, name: &str, size: usize) {
    let path = dir.join(name);
    let mut file = File::create(&path)
        .unwrap_or_else(|e| panic!("failed to create {}: {e}", path.display()));
    let chunk_size = 64 * 1024;
    let mut buf = vec![0u8; chunk_size];
    let mut written = 0;
    while written < size {
        let n = chunk_size.min(size - written);
        for (i, b) in buf[..n].iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            {
                *b = ((written + i) % 251) as u8;
            }
        }
        file.write_all(&buf[..n]).unwrap();
        written += n;
    }
    file.sync_all().unwrap();
}

/// Read the file back and assert that every byte matches the pattern written
/// by [`write_test_data`].
pub fn verify_test_data(dir: &Path, name: &str, size: usize) {
    let path = dir.join(name);
    let data = fs::read(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    assert_eq!(
        data.len(),
        size,
        "file size mismatch for {}",
        path.display()
    );
    for (i, &b) in data.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let expected = (i % 251) as u8;
        assert_eq!(b, expected, "data mismatch at byte {i}");
    }
}

/// Write highly compressible data (all zeros).
pub fn write_compressible_data(dir: &Path, name: &str, size: usize) {
    let path = dir.join(name);
    let mut file = File::create(&path)
        .unwrap_or_else(|e| panic!("failed to create {}: {e}", path.display()));
    let chunk_size = 64 * 1024;
    let buf = vec![0u8; chunk_size];
    let mut written = 0;
    while written < size {
        let n = chunk_size.min(size - written);
        file.write_all(&buf[..n]).unwrap();
        written += n;
    }
    file.sync_all().unwrap();
}

/// Create a single-device 512MB btrfs filesystem. Returns the tempdir (must be
/// kept alive) and the mount.
#[must_use]
pub fn single_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs();
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    (td, mnt)
}

/// Like [`single_mount`], but with a fixed UUID and label so that command
/// output is deterministic and suitable for snapshot testing.
#[must_use]
pub fn deterministic_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_with_options(TEST_UUID, TEST_LABEL);
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    (td, mnt)
}

/// Decompress a gzipped image into `cache_dir/cache_name`, atomically. If the
/// cached file already exists it is reused. Returns the path to the cached
/// decompressed image.
///
/// The cache lives outside the crate source tree (typically under
/// `target/test-fixtures/`) so it survives across test runs but is cleaned
/// by `cargo clean`.
#[must_use]
pub fn cache_gzipped_image(
    gz_path: &Path,
    cache_dir: &Path,
    cache_name: &str,
) -> PathBuf {
    let cached = cache_dir.join(cache_name);
    if cached.exists() {
        return cached;
    }

    fs::create_dir_all(cache_dir).unwrap_or_else(|e| {
        panic!("failed to create {}: {e}", cache_dir.display())
    });

    // Decompress to a temp file, then rename atomically to avoid races when
    // multiple tests check cached.exists() concurrently.
    let tmp = cache_dir.join(format!("{cache_name}.tmp"));
    let status = Command::new("gunzip")
        .args(["-k", "-c"])
        .arg(gz_path)
        .stdout(File::create(&tmp).unwrap_or_else(|e| {
            panic!("failed to create {}: {e}", tmp.display())
        }))
        .status()
        .expect("failed to run gunzip");
    assert!(status.success(), "gunzip failed for {}", gz_path.display());
    let _ = fs::rename(&tmp, &cached);

    cached
}

/// Mount an existing image file read-only via a loop device. The file is not
/// copied or owned; the loop device is detached on drop but the underlying
/// file is left in place. Suitable for mounting shared fixture images that
/// may be used concurrently by many tests.
#[must_use]
pub fn mount_existing_readonly(
    image_path: &Path,
) -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let lo = LoopbackDevice::attach_existing(image_path);

    let mountpoint = td.path().join("mnt");
    fs::create_dir_all(&mountpoint).unwrap();
    run(
        "mount",
        &[
            "-t",
            "btrfs",
            "-o",
            "ro",
            lo.path().to_str().unwrap(),
            mountpoint.to_str().unwrap(),
        ],
    );
    let file = File::open(&mountpoint).expect("failed to open mount");
    let mnt = Mount {
        mountpoint,
        file: ManuallyDrop::new(file),
        dev: lo,
    };
    (td, mnt)
}

/// Expands to the name of the function it is invoked from. Useful for
/// naming snapshots after the test that produced them.
#[macro_export]
macro_rules! test_name {
    () => {{
        fn f() {}
        let full = std::any::type_name_of_val(&f);
        let trimmed = full
            .trim_end_matches("::f")
            .trim_end_matches("::{{closure}}");
        trimmed.rsplit("::").next().unwrap_or(trimmed)
    }};
}

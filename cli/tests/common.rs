#![allow(dead_code)]
//! RAII test helpers for btrfs integration tests.
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

use std::{
    fs::{self, File},
    io::Write,
    mem::ManuallyDrop,
    os::unix::io::{AsFd, BorrowedFd},
    path::{Path, PathBuf},
    process::Command,
};

/// A file created via `set_len` (fallocate). Drop removes the file.
pub struct BackingFile {
    path: PathBuf,
}

impl BackingFile {
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

    /// Run `mkfs.btrfs -f` on this file.
    pub fn mkfs(&self) {
        run("mkfs.btrfs", &["-f", self.path.to_str().unwrap()]);
    }

    /// Run `mkfs.btrfs -f` with a fixed UUID and label for deterministic output.
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
    _inner: Option<BackingFile>,
}

impl LoopbackDevice {
    /// Attach a loop device to a backing file, consuming it. Call
    /// [`BackingFile::mkfs`] before this if the file should be formatted.
    pub fn new(file: BackingFile) -> Self {
        let dev_path = Self::losetup(file.path());
        Self {
            dev_path,
            _inner: Some(file),
        }
    }

    /// Attach a loop device to an existing file without taking ownership.
    /// The file will not be deleted on drop — only the loop device is detached.
    pub fn attach_existing(path: &Path) -> Self {
        let dev_path = Self::losetup(path);
        Self {
            dev_path,
            _inner: None,
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

    pub fn path(&self) -> &Path {
        &self.dev_path
    }

    pub fn backing_file(&self) -> Option<&BackingFile> {
        self._inner.as_ref()
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
    pub fn new(dev: LoopbackDevice, base_dir: &Path) -> Self {
        Self::with_options(dev, base_dir, &[])
    }

    /// Creates `base_dir/mnt` and mounts with additional `-o` options.
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

    pub fn path(&self) -> &Path {
        &self.mountpoint
    }

    /// A borrowed fd suitable for btrfs ioctls.
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }

    pub fn loopback(&self) -> &LoopbackDevice {
        &self.dev
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

fn run(cmd: &str, args: &[&str]) {
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
            *b = ((written + i) % 251) as u8;
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
        assert_eq!(b, (i % 251) as u8, "data mismatch at byte {i}");
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
pub fn single_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs();
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    (td, mnt)
}

/// Fixed UUID used by [`deterministic_mount`] for reproducible test output.
pub const TEST_UUID: &str = "deadbeef-dead-beef-dead-beefdeadbeef";
/// Fixed label used by [`deterministic_mount`] for reproducible test output.
pub const TEST_LABEL: &str = "test-fs";

/// Like [`single_mount`], but with a fixed UUID and label so that command
/// output is deterministic and suitable for snapshot testing.
pub fn deterministic_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_with_options(TEST_UUID, TEST_LABEL);
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    (td, mnt)
}

/// Return the path to the cached decompressed fixture image, extracting it
/// on first use. The cache lives at `target/test-fixtures/test-fs.img` so it
/// survives across test runs but is cleaned by `cargo clean`.
fn cached_fixture_image() -> PathBuf {
    let cache_dir =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/test-fixtures");
    let cached = cache_dir.join("test-fs.img");

    if !cached.exists() {
        fs::create_dir_all(&cache_dir).unwrap_or_else(|e| {
            panic!("failed to create {}: {e}", cache_dir.display())
        });
        let gz_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/commands/fixture.img.gz");
        let status = Command::new("gunzip")
            .args(["-k", "-c"])
            .arg(&gz_path)
            .stdout(File::create(&cached).unwrap_or_else(|e| {
                panic!("failed to create {}: {e}", cached.display())
            }))
            .status()
            .expect("failed to run gunzip");
        assert!(status.success(), "gunzip failed");
    }

    cached
}

/// Mount the pre-built fixture image read-only. The decompressed image is
/// cached in `target/test-fixtures/` so only the first test pays the gunzip
/// cost. Each test attaches its own loopback device directly to the shared
/// cached file — no copy needed since we mount read-only.
pub fn fixture_mount() -> (tempfile::TempDir, Mount) {
    let td = tempfile::tempdir().unwrap();
    let cached = cached_fixture_image();

    let lo = LoopbackDevice::attach_existing(&cached);

    // Mount read-only to preserve the fixture.
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

/// Expands to the name of the function it is invoked from.
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

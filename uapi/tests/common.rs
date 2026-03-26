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
}

impl Drop for BackingFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// A loop device attached to a file. Owns the [`BackingFile`]. Drop detaches
/// with `losetup -d`, then the inner `BackingFile` removes the image file.
pub struct LoopbackDevice {
    dev_path: PathBuf,
    inner: BackingFile,
}

impl LoopbackDevice {
    /// Attach a loop device to a backing file, consuming it. Call
    /// [`BackingFile::mkfs`] before this if the file should be formatted.
    pub fn new(file: BackingFile) -> Self {
        let output = Command::new("losetup")
            .args(["--find", "--show", file.path().to_str().unwrap()])
            .output()
            .expect("failed to run losetup");
        assert!(
            output.status.success(),
            "losetup failed: {}",
            String::from_utf8_lossy(&output.stderr),
        );
        let dev = String::from_utf8(output.stdout)
            .expect("losetup output is not UTF-8")
            .trim()
            .to_string();
        Self {
            dev_path: PathBuf::from(dev),
            inner: file,
        }
    }

    pub fn path(&self) -> &Path {
        &self.dev_path
    }

    pub fn backing_file(&self) -> &BackingFile {
        &self.inner
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
        let mountpoint = base_dir.join("mnt");
        fs::create_dir_all(&mountpoint).unwrap_or_else(|e| {
            panic!("failed to create {}: {e}", mountpoint.display())
        });
        run(
            "mount",
            &[
                "-t",
                "btrfs",
                dev.path().to_str().unwrap(),
                mountpoint.to_str().unwrap(),
            ],
        );
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

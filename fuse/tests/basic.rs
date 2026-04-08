//! Integration tests for the `btrfs-fuse` operation layer.
//!
//! These tests drive `BtrfsFuse`'s inherent `io::Result`-returning methods
//! directly, without going through `fuser::Filesystem` or an actual FUSE
//! mount. That keeps them unprivileged: the only external requirement is
//! a working `mkfs.btrfs` binary (part of `btrfs-progs`) on `$PATH`, which
//! we invoke with `--rootdir` to build a fresh image containing a known
//! directory tree for each test run.
//!
//! The fixture is built once per test process via `OnceLock` and shared
//! across every test; each test opens its own `BtrfsFuse` instance against
//! the same on-disk image (safe because all reads are read-only).

use btrfs_fuse::BtrfsFuse;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// Build a source directory under `base` with a known set of files, then
/// format `base/test.img` as btrfs using `mkfs.btrfs --rootdir`. Returns
/// the path to the formatted image file.
fn build_fixture(base: &Path) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();

    // Small regular file — will become an inline extent.
    fs::write(src.join("hello.txt"), b"hello, world\n").unwrap();

    // Empty file — exercises the "no extents" path.
    fs::write(src.join("empty.txt"), b"").unwrap();

    // Medium file of uniform bytes — exercises a regular extent.
    fs::write(src.join("large.bin"), vec![0x42u8; 100_000]).unwrap();

    // Subdirectory with a nested file — exercises parent-of resolution.
    let sub = src.join("subdir");
    fs::create_dir(&sub).unwrap();
    fs::write(sub.join("nested.txt"), b"nested content\n").unwrap();

    // Symbolic link pointing at a regular file.
    std::os::unix::fs::symlink("hello.txt", src.join("link")).unwrap();

    // Try to set a user xattr on hello.txt. This only succeeds if the
    // underlying /tmp filesystem honours `user.*` xattrs (which it usually
    // does). If it fails we leave the file alone and skip the xattr test.
    let _ = std::process::Command::new("setfattr")
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

/// Return the path to the shared fixture image, building it on first use.
/// The `TempDir` is leaked into a static `OnceLock` so it survives for the
/// lifetime of the test process.
fn fixture_path() -> &'static Path {
    static INIT: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let (_td, path) = INIT.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_fixture(td.path());
        (td, img)
    });
    path
}

/// Open a fresh `BtrfsFuse` instance against the shared fixture image.
fn open_fixture() -> BtrfsFuse {
    let file = File::open(fixture_path()).unwrap();
    BtrfsFuse::open(file).unwrap()
}

/// FUSE inode number for the filesystem root (matches `BTRFS_ROOT_DIR`
/// after translation).
const ROOT_INO: u64 = 1;

// ── lookup_entry ────────────────────────────────────────────────────

#[test]
fn lookup_finds_file_in_root() {
    let fs = open_fixture();
    let result = fs.lookup_entry(ROOT_INO, b"hello.txt").unwrap();
    let (_ino, item) = result.expect("hello.txt should exist");
    assert_eq!(item.size, 13); // "hello, world\n"
}

#[test]
fn lookup_returns_none_for_missing_name() {
    let fs = open_fixture();
    let result = fs.lookup_entry(ROOT_INO, b"does-not-exist").unwrap();
    assert!(result.is_none());
}

#[test]
fn lookup_finds_subdir() {
    let fs = open_fixture();
    let (_ino, item) = fs.lookup_entry(ROOT_INO, b"subdir").unwrap().unwrap();
    let mode = item.mode & libc::S_IFMT;
    assert_eq!(mode, libc::S_IFDIR);
}

#[test]
fn lookup_finds_symlink() {
    let fs = open_fixture();
    let (_ino, item) = fs.lookup_entry(ROOT_INO, b"link").unwrap().unwrap();
    let mode = item.mode & libc::S_IFMT;
    assert_eq!(mode, libc::S_IFLNK);
}

// ── get_attr ────────────────────────────────────────────────────────

#[test]
fn getattr_of_root_is_directory() {
    let fs = open_fixture();
    let item = fs.get_attr(ROOT_INO).unwrap().expect("root must exist");
    let mode = item.mode & libc::S_IFMT;
    assert_eq!(mode, libc::S_IFDIR);
}

#[test]
fn getattr_returns_none_for_missing_ino() {
    let fs = open_fixture();
    let result = fs.get_attr(1_000_000).unwrap();
    assert!(result.is_none());
}

// ── read_dir ────────────────────────────────────────────────────────

#[test]
fn readdir_root_lists_all_entries() {
    let fs = open_fixture();
    let entries = fs.read_dir(ROOT_INO, 0).unwrap();
    let names: Vec<&[u8]> = entries.iter().map(|e| e.name.as_slice()).collect();

    assert!(names.iter().any(|&n| n == b"."));
    assert!(names.iter().any(|&n| n == b".."));
    assert!(names.iter().any(|&n| n == b"hello.txt"));
    assert!(names.iter().any(|&n| n == b"empty.txt"));
    assert!(names.iter().any(|&n| n == b"large.bin"));
    assert!(names.iter().any(|&n| n == b"subdir"));
    assert!(names.iter().any(|&n| n == b"link"));
}

#[test]
fn readdir_pagination_skips_dot() {
    let fs = open_fixture();
    // Starting at offset 1 should skip ".".
    let entries = fs.read_dir(ROOT_INO, 1).unwrap();
    assert!(!entries.iter().any(|e| e.name == b"."));
    // But still include ".." and the real entries.
    assert!(entries.iter().any(|e| e.name == b".."));
    assert!(entries.iter().any(|e| e.name == b"hello.txt"));
}

#[test]
fn readdir_subdir_parent_is_root() {
    let fs = open_fixture();
    let (sub_ino, _) = fs.lookup_entry(ROOT_INO, b"subdir").unwrap().unwrap();
    let entries = fs.read_dir(sub_ino, 0).unwrap();

    let dotdot = entries.iter().find(|e| e.name == b"..").expect("need ..");
    assert_eq!(dotdot.ino, ROOT_INO);
    assert!(entries.iter().any(|e| e.name == b"nested.txt"));
}

// ── read_data ───────────────────────────────────────────────────────

#[test]
fn read_small_file_returns_full_contents() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"hello.txt").unwrap().unwrap();
    let data = fs.read_data(ino, 0, 1024).unwrap();
    assert_eq!(data, b"hello, world\n");
}

#[test]
fn read_empty_file_returns_empty() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"empty.txt").unwrap().unwrap();
    let data = fs.read_data(ino, 0, 1024).unwrap();
    assert!(data.is_empty());
}

#[test]
fn read_large_file_returns_full_contents() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"large.bin").unwrap().unwrap();
    let data = fs.read_data(ino, 0, 200_000).unwrap();
    assert_eq!(data.len(), 100_000);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]
fn read_large_file_with_offset_and_partial_size() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"large.bin").unwrap().unwrap();
    let data = fs.read_data(ino, 50_000, 10_000).unwrap();
    assert_eq!(data.len(), 10_000);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]
fn read_past_eof_returns_empty() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"hello.txt").unwrap().unwrap();
    let data = fs.read_data(ino, 1000, 100).unwrap();
    assert!(data.is_empty());
}

#[test]
fn read_nested_file_in_subdir() {
    let fs = open_fixture();
    let (sub_ino, _) = fs.lookup_entry(ROOT_INO, b"subdir").unwrap().unwrap();
    let (file_ino, _) =
        fs.lookup_entry(sub_ino, b"nested.txt").unwrap().unwrap();
    let data = fs.read_data(file_ino, 0, 1024).unwrap();
    assert_eq!(data, b"nested content\n");
}

// ── read_symlink ────────────────────────────────────────────────────

#[test]
fn read_symlink_returns_target_path() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"link").unwrap().unwrap();
    let target = fs.read_symlink(ino).unwrap();
    assert_eq!(target.as_deref(), Some(b"hello.txt".as_slice()));
}

// ── xattrs ──────────────────────────────────────────────────────────

#[test]
fn xattr_list_and_get_if_supported() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"hello.txt").unwrap().unwrap();
    let names = fs.list_xattrs(ino).unwrap();

    // setfattr may have failed at fixture-build time (missing tool, or
    // `/tmp` rejects user xattrs). If so the list is empty and we skip.
    if names.is_empty() {
        eprintln!(
            "xattrs not set on fixture (missing setfattr or unsupported /tmp); skipping"
        );
        return;
    }

    assert!(
        names.iter().any(|n| n == b"user.greeting"),
        "expected user.greeting in {names:?}"
    );
    let value = fs.get_xattr(ino, b"user.greeting").unwrap();
    assert_eq!(value.as_deref(), Some(b"hi".as_slice()));
}

#[test]
fn get_xattr_returns_none_for_missing_name() {
    let fs = open_fixture();
    let (ino, _) = fs.lookup_entry(ROOT_INO, b"hello.txt").unwrap().unwrap();
    let value = fs.get_xattr(ino, b"user.does-not-exist").unwrap();
    assert!(value.is_none());
}

// ── stat_fs ─────────────────────────────────────────────────────────

#[test]
fn stat_fs_returns_sensible_values() {
    let fs = open_fixture();
    let s = fs.stat_fs();
    assert!(s.blocks > 0, "blocks should be positive");
    assert!(s.bfree > 0, "bfree should be positive");
    assert!(s.bfree <= s.blocks);
    assert_eq!(s.bavail, s.bfree);
    assert_eq!(s.bsize, 4096);
    assert_eq!(s.namelen, 255);
    assert_eq!(s.frsize, 4096);
}

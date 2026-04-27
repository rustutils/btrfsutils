//! Integration tests for the `btrfs-fs` operation layer.
//!
//! Each test opens a [`btrfs_fs::Filesystem`] over a per-process fixture
//! image built once via `mkfs.btrfs --rootdir`, and drives the public
//! API directly. No FUSE mount or elevated privileges required; the
//! only external dependency is a working `mkfs.btrfs` on `$PATH`.

use btrfs_fs::{FileKind, Filesystem};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// Build a source directory under `base` with a known set of files, then
/// format `base/test.img` as btrfs using `mkfs.btrfs --rootdir`.
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

    // Try to set a user xattr; tmpfs and ext4 honour user.* but some
    // filesystems don't. Failure is silently tolerated and the xattr
    // tests skip when the list comes back empty.
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

fn fixture_path() -> &'static Path {
    static INIT: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let (_td, path) = INIT.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_fixture(td.path());
        (td, img)
    });
    path
}

fn open_fixture() -> Filesystem<File> {
    let file = File::open(fixture_path()).unwrap();
    Filesystem::open(file).unwrap()
}

// ── lookup ──────────────────────────────────────────────────────────

#[test]
fn lookup_finds_file_in_root() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (_ino, item) = fs.lookup(root, b"hello.txt").unwrap().unwrap();
    assert_eq!(item.size, 13); // "hello, world\n"
}

#[test]
fn lookup_returns_none_for_missing_name() {
    let mut fs = open_fixture();
    let root = fs.root();
    let result = fs.lookup(root, b"does-not-exist").unwrap();
    assert!(result.is_none());
}

#[test]
fn lookup_finds_subdir() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (_ino, item) = fs.lookup(root, b"subdir").unwrap().unwrap();
    assert_eq!(item.mode & libc::S_IFMT, libc::S_IFDIR);
}

#[test]
fn lookup_finds_symlink() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (_ino, item) = fs.lookup(root, b"link").unwrap().unwrap();
    assert_eq!(item.mode & libc::S_IFMT, libc::S_IFLNK);
}

// ── getattr ─────────────────────────────────────────────────────────

#[test]
fn getattr_of_root_is_directory() {
    let mut fs = open_fixture();
    let root = fs.root();
    let stat = fs.getattr(root).unwrap().expect("root must exist");
    assert_eq!(stat.kind, FileKind::Directory);
}

#[test]
fn getattr_returns_none_for_missing_ino() {
    let mut fs = open_fixture();
    let root = fs.root();
    let bogus = btrfs_fs::Inode {
        subvol: root.subvol,
        ino: 1_000_000,
    };
    let result = fs.getattr(bogus).unwrap();
    assert!(result.is_none());
}

// ── readdir ─────────────────────────────────────────────────────────

#[test]
fn readdir_root_lists_all_entries() {
    let mut fs = open_fixture();
    let root = fs.root();
    let entries = fs.readdir(root, 0).unwrap();
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
    let mut fs = open_fixture();
    let root = fs.root();
    // Starting at offset 1 should skip ".".
    let entries = fs.readdir(root, 1).unwrap();
    assert!(!entries.iter().any(|e| e.name == b"."));
    assert!(entries.iter().any(|e| e.name == b".."));
    assert!(entries.iter().any(|e| e.name == b"hello.txt"));
}

#[test]
fn readdir_subdir_parent_is_root() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (sub, _) = fs.lookup(root, b"subdir").unwrap().unwrap();
    let entries = fs.readdir(sub, 0).unwrap();

    let dotdot = entries.iter().find(|e| e.name == b"..").expect("need ..");
    assert_eq!(dotdot.ino, root);
    assert!(entries.iter().any(|e| e.name == b"nested.txt"));
}

// ── read ────────────────────────────────────────────────────────────

#[test]
fn read_small_file_returns_full_contents() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"hello.txt").unwrap().unwrap();
    let data = fs.read(ino, 0, 1024).unwrap();
    assert_eq!(data, b"hello, world\n");
}

#[test]
fn read_empty_file_returns_empty() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"empty.txt").unwrap().unwrap();
    let data = fs.read(ino, 0, 1024).unwrap();
    assert!(data.is_empty());
}

#[test]
fn read_large_file_returns_full_contents() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"large.bin").unwrap().unwrap();
    let data = fs.read(ino, 0, 200_000).unwrap();
    assert_eq!(data.len(), 100_000);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]
fn read_large_file_with_offset_and_partial_size() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"large.bin").unwrap().unwrap();
    let data = fs.read(ino, 50_000, 10_000).unwrap();
    assert_eq!(data.len(), 10_000);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]
fn read_past_eof_returns_empty() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"hello.txt").unwrap().unwrap();
    let data = fs.read(ino, 1000, 100).unwrap();
    assert!(data.is_empty());
}

#[test]
fn read_nested_file_in_subdir() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (sub, _) = fs.lookup(root, b"subdir").unwrap().unwrap();
    let (file, _) = fs.lookup(sub, b"nested.txt").unwrap().unwrap();
    let data = fs.read(file, 0, 1024).unwrap();
    assert_eq!(data, b"nested content\n");
}

// ── readlink ────────────────────────────────────────────────────────

#[test]
fn readlink_returns_target_path() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"link").unwrap().unwrap();
    let target = fs.readlink(ino).unwrap();
    assert_eq!(target.as_deref(), Some(b"hello.txt".as_slice()));
}

// ── xattrs ──────────────────────────────────────────────────────────

#[test]
fn xattr_list_and_get_if_supported() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"hello.txt").unwrap().unwrap();
    let names = fs.xattr_list(ino).unwrap();

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
    let value = fs.xattr_get(ino, b"user.greeting").unwrap();
    assert_eq!(value.as_deref(), Some(b"hi".as_slice()));
}

#[test]
fn xattr_get_returns_none_for_missing_name() {
    let mut fs = open_fixture();
    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"hello.txt").unwrap().unwrap();
    let value = fs.xattr_get(ino, b"user.does-not-exist").unwrap();
    assert!(value.is_none());
}

// ── statfs ──────────────────────────────────────────────────────────

#[test]
fn statfs_returns_sensible_values() {
    let fs = open_fixture();
    let s = fs.statfs();
    assert!(s.blocks > 0);
    assert!(s.bfree > 0);
    assert!(s.bfree <= s.blocks);
    assert_eq!(s.bavail, s.bfree);
    assert_eq!(s.bsize, 4096);
    assert_eq!(s.namelen, 255);
    assert_eq!(s.frsize, 4096);
}

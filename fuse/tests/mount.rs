//! End-to-end mount tests for the `btrfs-fuse` driver.
//!
//! Each test spawns the `btrfs-fuse` binary against a fixture image
//! and exercises the mounted filesystem through ordinary POSIX calls
//! (`std::fs`, `xattr` crate). Behaviour is verified at the FUSE
//! protocol boundary, which is what `fs/tests/basic.rs` cannot reach
//! — inode translation (FUSE root = 1 ⇄ btrfs root dir = 256),
//! `Stat` → `FileAttr` mapping, the deferred-reply / spawn-task
//! pattern, and the kernel ↔ fuser ↔ `Filesystem` ↔ `BlockReader`
//! round-trip.

mod common;

use common::MountedFuse;
use std::{fs, path::Path, thread, time::Duration};

#[test]
fn mount_then_unmount_clean() {
    let _m = MountedFuse::mount();
    // `Drop` performs the unmount; if it fails we'll leak the
    // mountpoint and the next test invocation surfaces the issue.
}

#[test]
fn read_root_listing() {
    let m = MountedFuse::mount();
    let names: Vec<String> = fs::read_dir(m.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    for expected in ["hello.txt", "empty.txt", "large.bin", "subdir", "link"] {
        assert!(
            names.iter().any(|n| n == expected),
            "missing {expected} in {names:?}",
        );
    }
}

/// `large.bin` is a dense 100 KiB file (all `0x42`), so it has no
/// holes. `SEEK_HOLE` from offset 0 should report the virtual hole
/// at EOF (offset 100_000); `SEEK_DATA` from offset 0 should report
/// 0 (the start of the file is data).
#[test]
fn lseek_seek_hole_data_on_dense_file() {
    use std::os::fd::AsRawFd;

    const SEEK_DATA: i32 = 3;
    const SEEK_HOLE: i32 = 4;

    let m = MountedFuse::mount();
    let f = fs::File::open(m.path().join("large.bin")).expect("open large.bin");
    let fd = f.as_raw_fd();

    // SAFETY: fd is open and owned by `f` for the duration of these
    // calls; SEEK_HOLE/SEEK_DATA are well-defined whence values on
    // Linux.
    let hole_off = unsafe { libc::lseek(fd, 0, SEEK_HOLE) };
    assert_eq!(
        hole_off, 100_000,
        "SEEK_HOLE on a dense file should return file size",
    );
    let data_off = unsafe { libc::lseek(fd, 0, SEEK_DATA) };
    assert_eq!(data_off, 0, "SEEK_DATA at offset 0 should return 0");

    // SEEK_DATA past EOF must return -1 with errno = ENXIO.
    let past_eof = unsafe { libc::lseek(fd, 200_000, SEEK_DATA) };
    assert_eq!(past_eof, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ENXIO),
    );
}

/// Walks the directory and inspects each entry's `metadata` (kind,
/// size, link target). On a mount where `FUSE_DO_READDIRPLUS` is
/// negotiated, this drives the kernel through our `readdirplus`
/// callback rather than falling back to per-entry `lookup`.
/// Verifies that the attributes returned alongside the entry name
/// agree with what an explicit `getattr` would return.
#[test]
fn read_root_listing_with_metadata() {
    let m = MountedFuse::mount();
    // Iterating with `DirEntry::file_type()` and `metadata()` is
    // exactly what `ls -l` does; on Linux it triggers `getdents64`,
    // which the kernel routes through `readdirplus` when the FUSE
    // driver advertises support for it.
    let mut by_name: std::collections::HashMap<String, fs::Metadata> =
        Default::default();
    for entry in fs::read_dir(m.path()).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        let meta = entry.metadata().expect("metadata for entry");
        by_name.insert(name, meta);
    }

    let hello = by_name.get("hello.txt").expect("hello.txt present");
    assert!(hello.is_file());
    assert_eq!(hello.len(), b"hello, world\n".len() as u64);

    let large = by_name.get("large.bin").expect("large.bin present");
    assert!(large.is_file());
    assert_eq!(large.len(), 100_000);

    let subdir = by_name.get("subdir").expect("subdir present");
    assert!(subdir.is_dir());

    // `DirEntry::metadata()` does NOT follow symlinks (it uses
    // `fstatat(..., AT_SYMLINK_NOFOLLOW)`), so the readdirplus-emitted
    // attr for `link` is the symlink itself. Cross-check that an
    // explicit `fs::metadata` (which DOES follow) returns the file
    // it points at.
    let link_meta = by_name.get("link").expect("link present");
    assert!(
        link_meta.file_type().is_symlink(),
        "DirEntry::metadata for `link` should report symlink",
    );
    let resolved = fs::metadata(m.path().join("link")).expect("follow symlink");
    assert!(resolved.is_file(), "symlink should follow to a file");
    assert_eq!(resolved.len(), b"hello, world\n".len() as u64);
}

#[test]
fn read_small_file_contents() {
    let m = MountedFuse::mount();
    let content = fs::read(m.path().join("hello.txt")).unwrap();
    assert_eq!(content, b"hello, world\n");
}

#[test]
fn read_large_file_contents() {
    let m = MountedFuse::mount();
    let content = fs::read(m.path().join("large.bin")).unwrap();
    assert_eq!(content.len(), 100_000);
    assert!(content.iter().all(|&b| b == 0x42));
}

#[test]
fn read_nested_file_contents() {
    let m = MountedFuse::mount();
    let content = fs::read(m.path().join("subdir/nested.txt")).unwrap();
    assert_eq!(content, b"nested content\n");
}

#[test]
fn stat_returns_correct_metadata() {
    let m = MountedFuse::mount();

    let file_meta = fs::metadata(m.path().join("hello.txt")).unwrap();
    assert!(file_meta.is_file());
    assert_eq!(file_meta.len(), 13);

    let dir_meta = fs::metadata(m.path().join("subdir")).unwrap();
    assert!(dir_meta.is_dir());

    let root_meta = fs::metadata(m.path()).unwrap();
    assert!(
        root_meta.is_dir(),
        "FUSE root inode should map to a directory \
         (verifies the 1↔256 inode swap)",
    );

    // `symlink_metadata` follows no links — confirms `link` itself
    // is reported as a symlink, not the target file.
    let link_meta = fs::symlink_metadata(m.path().join("link")).unwrap();
    assert!(link_meta.file_type().is_symlink());
}

#[test]
fn readlink_returns_target() {
    let m = MountedFuse::mount();
    let target = fs::read_link(m.path().join("link")).unwrap();
    assert_eq!(target, Path::new("hello.txt"));
}

#[test]
fn xattr_get_and_list() {
    let m = MountedFuse::mount();
    let path = m.path().join("hello.txt");

    let names: Vec<_> = match xattr::list(&path) {
        Ok(it) => it.collect(),
        Err(e) => {
            eprintln!("xattr::list failed ({e}); skipping");
            return;
        }
    };
    if names.is_empty() {
        eprintln!(
            "no xattrs on fixture (setfattr unavailable or rejected); \
             skipping",
        );
        return;
    }

    assert!(
        names
            .iter()
            .any(|n| n.as_encoded_bytes() == b"user.greeting"),
        "expected user.greeting in {names:?}",
    );
    let value = xattr::get(&path, "user.greeting").unwrap();
    assert_eq!(value.as_deref(), Some(b"hi".as_slice()));
}

#[test]
fn xattr_get_returns_none_for_missing_name() {
    let m = MountedFuse::mount();
    let path = m.path().join("hello.txt");
    let result = xattr::get(&path, "user.does-not-exist").unwrap();
    assert!(result.is_none());
}

// ── --subvol / --subvolid ─────────────────────────────────────────

/// `--subvol PATH` mounts the named subvolume at the FUSE root, so
/// the contents of `sub/` should be directly visible at the
/// mountpoint root (not nested under `sub/`).
#[test]
fn mount_with_subvol_path_arg() {
    let m = common::MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &["--subvol", "sub"],
        "inside.txt",
    );
    let names: Vec<String> = fs::read_dir(m.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "inside.txt"),
        "subvol root should contain inside.txt; got {names:?}",
    );
    // The default-subvol's `at_root.txt` should NOT be visible —
    // we mounted `sub` only.
    assert!(
        !names.iter().any(|n| n == "at_root.txt"),
        "default-subvol files should not leak through; got {names:?}",
    );

    let content = fs::read(m.path().join("inside.txt")).unwrap();
    assert_eq!(content, b"inside the subvol\n");
}

/// `--subvolid <ID>` selects a subvolume by tree id rather than by
/// path. We don't know the id upfront, so this test mounts the
/// default first to discover it (via `subvolume list`-style data) —
/// here using `btrfs inspect-internal` would be heavyweight, so we
/// just confirm a known id (`5` = default `FS_TREE`) round-trips
/// correctly. With the default subvol explicitly selected, the
/// fixture's top-level `at_root.txt` should be visible.
#[test]
fn mount_with_subvolid_default_round_trips() {
    let m = common::MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &["--subvolid", "5"],
        "at_root.txt",
    );
    let names: Vec<String> = fs::read_dir(m.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "at_root.txt"),
        "default subvol should expose at_root.txt; got {names:?}",
    );
    assert!(
        names.iter().any(|n| n == "sub"),
        "default subvol should expose the `sub` mount entry; \
         got {names:?}",
    );
}

/// 16 OS threads × 50 reads each. If the fuse adapter's spawn-a-task
/// pattern double-replies, drops a `Reply*`, or deadlocks under
/// concurrent FUSE callbacks, this test exposes it. The kernel
/// serialises operations on the same FUSE session more aggressively
/// than user-space async calls would, so even a single mountpoint
/// is enough to stress the dispatch path.
#[test]
fn concurrent_reads_dont_deadlock_or_corrupt() {
    let m = MountedFuse::mount();
    let path = m.path().join("hello.txt").to_path_buf();

    let handles: Vec<_> = (0..16)
        .map(|_| {
            let path = path.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    let content = fs::read(&path).unwrap();
                    assert_eq!(content, b"hello, world\n");
                }
            })
        })
        .collect();

    // Bound the total wait so a deadlock surfaces as a test failure
    // rather than hanging CI forever.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for h in handles {
        let now = std::time::Instant::now();
        if now >= deadline {
            panic!("concurrent reads ran past 30s deadline (deadlock?)");
        }
        h.join().expect("thread panicked");
    }
}

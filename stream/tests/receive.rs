//! Integration tests for ReceiveContext::process_command.
//!
//! Each test creates a btrfs filesystem on a loopback device, constructs
//! a ReceiveContext, and issues individual StreamCommand variants to test
//! specific receive code paths in isolation.

use btrfs_stream::{ReceiveContext, StreamCommand, Timespec};
use btrfs_test_utils::Mount;
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    process::Command,
};
use uuid::Uuid;

// ── Test helpers ────────────────────────────────────────────────────

/// Thin wrapper around (`TempDir`, `Mount`) that keeps the existing
/// `TestMount::new() -> Self; mnt.path()` API for the per-test setup
/// functions below. Drop order (inner to outer) handles unmount/detach
/// before the tempdir is removed.
struct TestMount {
    _td: tempfile::TempDir,
    mount: Mount,
}

impl TestMount {
    fn new() -> Self {
        let (td, mnt) = btrfs_test_utils::single_mount();
        Self {
            _td: td,
            mount: mnt,
        }
    }

    fn path(&self) -> &Path {
        self.mount.path()
    }
}

/// Create a ReceiveContext and a writable subvolume inside it, returning
/// the context and the subvolume's absolute path.
fn setup_receive() -> (TestMount, ReceiveContext, PathBuf) {
    let mnt = TestMount::new();
    let mut ctx = ReceiveContext::new(mnt.path()).unwrap();

    let uuid = Uuid::new_v4();
    ctx.process_command(&StreamCommand::Subvol {
        path: "test_subvol".into(),
        uuid,
        ctransid: 1,
    })
    .unwrap();

    let subvol_path = mnt.path().join("test_subvol");
    (mnt, ctx, subvol_path)
}

fn dummy_timespec() -> Timespec {
    Timespec {
        sec: 1700000000,
        nsec: 0,
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn receive_mkfile_and_write() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "hello.txt".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Write {
        path: "hello.txt".into(),
        offset: 0,
        data: b"hello world".to_vec(),
    })
    .unwrap();

    let content = std::fs::read_to_string(subvol.join("hello.txt")).unwrap();
    assert_eq!(content, "hello world");
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_mkdir_and_rmdir() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkdir {
        path: "mydir".into(),
    })
    .unwrap();
    assert!(subvol.join("mydir").is_dir());

    ctx.process_command(&StreamCommand::Rmdir {
        path: "mydir".into(),
    })
    .unwrap();
    assert!(!subvol.join("mydir").exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_mknod() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    // Character device: null (1,3).
    let dev = nix::sys::stat::makedev(1, 3);
    let mode = nix::libc::S_IFCHR as u64 | 0o666;
    ctx.process_command(&StreamCommand::Mknod {
        path: "chardev".into(),
        mode,
        rdev: dev,
    })
    .unwrap();

    let meta = std::fs::symlink_metadata(subvol.join("chardev")).unwrap();
    assert!(meta.file_type().is_char_device());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_mkfifo() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfifo {
        path: "pipe".into(),
    })
    .unwrap();

    let meta = std::fs::symlink_metadata(subvol.join("pipe")).unwrap();
    assert!(meta.file_type().is_fifo());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_mksock() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mksock {
        path: "sock".into(),
    })
    .unwrap();

    let meta = std::fs::symlink_metadata(subvol.join("sock")).unwrap();
    assert!(meta.file_type().is_socket());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_symlink() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "target.txt".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Symlink {
        path: "link.sym".into(),
        target: "target.txt".into(),
    })
    .unwrap();

    let link = std::fs::read_link(subvol.join("link.sym")).unwrap();
    assert_eq!(link.to_str().unwrap(), "target.txt");
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_link() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "original.txt".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Link {
        path: "hardlink.txt".into(),
        target: "original.txt".into(),
    })
    .unwrap();

    let orig = std::fs::metadata(subvol.join("original.txt")).unwrap();
    let hard = std::fs::metadata(subvol.join("hardlink.txt")).unwrap();
    assert_eq!(orig.ino(), hard.ino());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_rename() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "old_name.txt".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Rename {
        from: "old_name.txt".into(),
        to: "new_name.txt".into(),
    })
    .unwrap();

    assert!(!subvol.join("old_name.txt").exists());
    assert!(subvol.join("new_name.txt").exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_unlink() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "doomed.txt".into(),
    })
    .unwrap();
    assert!(subvol.join("doomed.txt").exists());

    ctx.process_command(&StreamCommand::Unlink {
        path: "doomed.txt".into(),
    })
    .unwrap();
    assert!(!subvol.join("doomed.txt").exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_set_and_remove_xattr() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "xattr_file".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::SetXattr {
        path: "xattr_file".into(),
        name: "user.test".into(),
        data: b"value123".to_vec(),
    })
    .unwrap();

    // Verify xattr was set.
    let output = Command::new("getfattr")
        .args([
            "--only-values",
            "-n",
            "user.test",
            subvol.join("xattr_file").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "value123");

    ctx.process_command(&StreamCommand::RemoveXattr {
        path: "xattr_file".into(),
        name: "user.test".into(),
    })
    .unwrap();

    // Verify xattr was removed.
    let output = Command::new("getfattr")
        .args([
            "-n",
            "user.test",
            subvol.join("xattr_file").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_truncate() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "trunc.bin".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Write {
        path: "trunc.bin".into(),
        offset: 0,
        data: vec![0xAB; 8192],
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Truncate {
        path: "trunc.bin".into(),
        size: 1024,
    })
    .unwrap();

    let meta = std::fs::metadata(subvol.join("trunc.bin")).unwrap();
    assert_eq!(meta.len(), 1024);
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_chmod() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "perms.txt".into(),
    })
    .unwrap();

    ctx.process_command(&StreamCommand::Chmod {
        path: "perms.txt".into(),
        mode: 0o755,
    })
    .unwrap();

    let meta = std::fs::metadata(subvol.join("perms.txt")).unwrap();
    assert_eq!(meta.mode() & 0o777, 0o755);
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_chown() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "owned.txt".into(),
    })
    .unwrap();

    // Set to uid=0, gid=0 (we're running as root).
    ctx.process_command(&StreamCommand::Chown {
        path: "owned.txt".into(),
        uid: 0,
        gid: 0,
    })
    .unwrap();

    let meta = std::fs::metadata(subvol.join("owned.txt")).unwrap();
    assert_eq!(meta.uid(), 0);
    assert_eq!(meta.gid(), 0);
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_utimes() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "timed.txt".into(),
    })
    .unwrap();

    let ts = dummy_timespec();
    ctx.process_command(&StreamCommand::Utimes {
        path: "timed.txt".into(),
        atime: ts,
        mtime: ts,
        ctime: ts,
    })
    .unwrap();

    let meta = std::fs::metadata(subvol.join("timed.txt")).unwrap();
    assert_eq!(meta.mtime(), 1700000000);
}

#[test]
#[ignore = "requires elevated privileges"]
fn receive_fallocate_punch_hole() {
    let (_mnt, mut ctx, subvol) = setup_receive();

    ctx.process_command(&StreamCommand::Mkfile {
        path: "holey.bin".into(),
    })
    .unwrap();

    // Write 128K of data.
    ctx.process_command(&StreamCommand::Write {
        path: "holey.bin".into(),
        offset: 0,
        data: vec![0xFF; 128 * 1024],
    })
    .unwrap();

    // Punch a 4K hole at offset 4K.
    // FALLOC_FL_PUNCH_HOLE (0x02) | FALLOC_FL_KEEP_SIZE (0x01) = 0x03
    ctx.process_command(&StreamCommand::Fallocate {
        path: "holey.bin".into(),
        mode: 0x03,
        offset: 4096,
        len: 4096,
    })
    .unwrap();

    let meta = std::fs::metadata(subvol.join("holey.bin")).unwrap();
    assert_eq!(meta.len(), 128 * 1024);

    // The punched region should be zeros.
    let data = std::fs::read(subvol.join("holey.bin")).unwrap();
    assert!(data[4096..8192].iter().all(|&b| b == 0));
    // Surrounding data should be 0xFF.
    assert!(data[0..4096].iter().all(|&b| b == 0xFF));
    assert!(data[8192..12288].iter().all(|&b| b == 0xFF));
}

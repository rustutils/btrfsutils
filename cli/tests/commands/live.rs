//! Tests that create and mutate real btrfs filesystems.
//!
//! Some tests use assertion-based checks (mutating commands, round-trips),
//! others use snapshot testing for output verification.

use super::{btrfs, btrfs_ok, common, redact};
use btrfs_uapi::{
    device::device_info_all,
    filesystem::filesystem_info,
    subvolume::{self, SubvolumeFlags, subvolume_flags_get},
};
use common::{
    BackingFile, LoopbackDevice, Mount, single_mount, verify_test_data,
    write_compressible_data, write_test_data,
};
use nix::sys::stat;
use regex_lite::Regex;
use std::{
    fs,
    io::{Read as _, Seek, SeekFrom},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, symlink},
        io::AsFd,
    },
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::tempdir;

// ── filesystem (assertions) ──────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_sync() {
    let (_td, mnt) = single_mount();
    let (_, _, code) =
        btrfs(&["filesystem", "sync", mnt.path().to_str().unwrap()]);
    assert_eq!(code, 0);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_label_get_set() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["filesystem", "label", mp, "test-label"]);

    let out = btrfs_ok(&["filesystem", "label", mp]);
    assert!(
        out.contains("test-label"),
        "expected label in output:\n{out}"
    );

    // Verify via uapi
    let label = btrfs_uapi::filesystem::label_get(mnt.fd()).unwrap();
    assert_eq!(label.to_str().unwrap(), "test-label");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_resize_grow_shrink() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    mnt.loopback().backing_file().unwrap().resize(768_000_000);
    mnt.loopback().refresh_size();
    btrfs_ok(&["filesystem", "resize", "max", mp]);

    let out = btrfs_ok(&["filesystem", "usage", mp]);
    assert!(
        out.contains("Device size:"),
        "expected usage output:\n{out}"
    );

    // Verify via uapi: after resize max, device should be close to 768MB
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    let devices = device_info_all(mnt.fd(), &fs_info).unwrap();
    let total: u64 = devices.iter().map(|d| d.total_bytes).sum();
    assert!(
        total > 700_000_000,
        "expected >700MB after resize max, got {total}"
    );

    btrfs_ok(&["filesystem", "resize", "512m", mp]);

    // Verify via uapi: after resize 512m
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    let devices = device_info_all(mnt.fd(), &fs_info).unwrap();
    let total: u64 = devices.iter().map(|d| d.total_bytes).sum();
    assert!(
        total <= 512 * 1024 * 1024,
        "expected <=512MiB after resize 512m, got {total}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du_shared() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a directory structure with reflinked and exclusive files.
    let dir = format!("{mp}/testdir");
    fs::create_dir(&dir).unwrap();
    write_test_data(Path::new(&dir), "original.bin", 256 * 1024);
    // Reflink copy: shares extents with the original.
    Command::new("cp")
        .args([
            "--reflink=always",
            &format!("{dir}/original.bin"),
            &format!("{dir}/clone.bin"),
        ])
        .status()
        .expect("cp --reflink failed");
    // A non-reflinked file: all bytes are exclusive.
    write_test_data(Path::new(&dir), "unique.bin", 128 * 1024);
    btrfs_ok(&["filesystem", "sync", mp]);

    snap!(
        "btrfs filesystem du <MOUNT>/testdir",
        redact(&btrfs_ok(&["filesystem", "du", &dir]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    write_test_data(Path::new(mp), "fragmented.bin", 65536);
    btrfs_ok(&["filesystem", "sync", mp]);

    btrfs_ok(&["filesystem", "defragment", &format!("{mp}/fragmented.bin")]);

    verify_test_data(Path::new(mp), "fragmented.bin", 65536);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag_compress() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    common::write_compressible_data(Path::new(mp), "compressible.bin", 131072);
    btrfs_ok(&["filesystem", "sync", mp]);

    btrfs_ok(&[
        "filesystem",
        "defragment",
        "-czstd",
        &format!("{mp}/compressible.bin"),
    ]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_commit_stats() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["filesystem", "commit-stats", mp]);
    assert!(
        out.contains("Total commits"),
        "expected commit count:\n{out}"
    );
    assert!(
        out.contains("Max commit duration"),
        "expected max duration:\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_mkswapfile() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let swapfile = format!("{mp}/swapfile");

    btrfs_ok(&["filesystem", "mkswapfile", "-s", "16m", &swapfile]);
    assert!(Path::new(&swapfile).exists(), "swapfile not created");

    let meta = fs::metadata(&swapfile).unwrap();
    assert!(
        meta.len() >= 16 * 1024 * 1024,
        "swapfile too small: {} bytes",
        meta.len()
    );
}

// ── filesystem resize error cases ────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_resize_missing_args() {
    // resize with no arguments should fail.
    let (_, _, code) = btrfs(&["filesystem", "resize"]);
    assert_ne!(code, 0);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_resize_invalid_size() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    // Bogus size string should fail.
    let (_, _, code) = btrfs(&["filesystem", "resize", "banana", mp]);
    assert_ne!(code, 0);
}

// ── filesystem defrag compression variants ──────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag_compress_lzo() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    common::write_compressible_data(Path::new(mp), "lzo.bin", 131072);
    btrfs_ok(&["filesystem", "sync", mp]);
    btrfs_ok(&[
        "filesystem",
        "defragment",
        "-clzo",
        &format!("{mp}/lzo.bin"),
    ]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag_compress_zlib() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    common::write_compressible_data(Path::new(mp), "zlib.bin", 131072);
    btrfs_ok(&["filesystem", "sync", mp]);
    btrfs_ok(&[
        "filesystem",
        "defragment",
        "-czlib",
        &format!("{mp}/zlib.bin"),
    ]);
}

// ── filesystem defrag recursion ─────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag_recursion_with_subvol() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a file in the root volume.
    write_test_data(Path::new(mp), "root_file.bin", 65536);
    // Create a subvolume with its own file.
    let subvol = format!("{mp}/subvol");
    btrfs_ok(&["subvolume", "create", &subvol]);
    write_test_data(Path::new(&subvol), "subvol_file.bin", 65536);
    btrfs_ok(&["filesystem", "sync", mp]);

    // Recursive defrag from the root should complete without errors,
    // stopping at the subvolume boundary.
    btrfs_ok(&["filesystem", "defragment", "-r", mp]);

    // Both files should still be intact.
    verify_test_data(Path::new(mp), "root_file.bin", 65536);
    verify_test_data(Path::new(&subvol), "subvol_file.bin", 65536);
}

// ── filesystem df unit suffixes ─────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df_raw() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let out = btrfs_ok(&["filesystem", "df", "--raw", mp]);
    // Raw mode should output plain numbers (no KiB/MiB suffixes).
    assert!(out.contains("total="), "expected raw output:\n{out}");
    assert!(!out.contains("MiB"), "raw mode should not use MiB:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df_kbytes() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let raw = btrfs_ok(&["filesystem", "df", "--raw", mp]);
    let kb = btrfs_ok(&["filesystem", "df", "--kbytes", mp]);
    // --kbytes divides values by 1024 (no suffix). The numbers should
    // be smaller than --raw output.
    assert_ne!(raw, kb, "--kbytes should differ from --raw");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df_mbytes() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let raw = btrfs_ok(&["filesystem", "df", "--raw", mp]);
    let mb = btrfs_ok(&["filesystem", "df", "--mbytes", mp]);
    assert_ne!(raw, mb, "--mbytes should differ from --raw");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df_si() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let out = btrfs_ok(&["filesystem", "df", "--si", mp]);
    // SI mode uses base-1000 suffixes (kB, MB, GB).
    assert!(
        out.contains('B'),
        "expected size suffix in --si output:\n{out}"
    );
}

// ── subvolume ────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_show_delete() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/testvol");

    btrfs_ok(&["subvolume", "create", &subvol]);
    assert!(Path::new(&subvol).is_dir());

    // Verify via uapi: subvolume appears in list
    let list = subvolume::subvolume_list(mnt.fd()).unwrap();
    assert!(
        list.iter().any(|s| s.name.contains("testvol")),
        "subvolume should appear in uapi list"
    );

    let out = btrfs_ok(&["subvolume", "show", &subvol]);
    assert!(
        out.contains("testvol"),
        "expected name in show output:\n{out}"
    );

    btrfs_ok(&["subvolume", "delete", &subvol]);
    assert!(!Path::new(&subvol).exists());

    // Verify via uapi: subvolume gone from list
    let list = subvolume::subvolume_list(mnt.fd()).unwrap();
    assert!(
        !list.iter().any(|s| s.name.contains("testvol")),
        "subvolume should not appear in uapi list after delete"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["subvolume", "create", &format!("{mp}/alpha")]);
    btrfs_ok(&["subvolume", "create", &format!("{mp}/beta")]);

    let out = btrfs_ok(&["subvolume", "list", mp]);
    assert!(out.contains("alpha"), "expected alpha in list:\n{out}");
    assert!(out.contains("beta"), "expected beta in list:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_snapshot_parent_id() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a subvolume and a snapshot of it.
    btrfs_ok(&["subvolume", "create", &format!("{mp}/parent_sv")]);
    btrfs_ok(&[
        "subvolume",
        "snapshot",
        &format!("{mp}/parent_sv"),
        &format!("{mp}/snap_of_parent"),
    ]);

    // The snapshot should appear with correct parent and name.
    let out = btrfs_ok(&["subvolume", "list", mp]);
    let lines: Vec<&str> = out.lines().collect();
    // We expect two subvolumes: parent_sv and snap_of_parent.
    assert_eq!(lines.len(), 2, "expected 2 subvolumes:\n{out}");

    // Find the snapshot line (ID 257, the second subvolume created).
    // Parse "ID NNN gen NNN top level NNN path NAME"
    let snap_line = lines
        .iter()
        .find(|l| !l.contains("parent_sv"))
        .expect("expected a snapshot line");
    assert!(
        snap_line.contains("snap_of_parent"),
        "snapshot should have name 'snap_of_parent':\n{out}"
    );
    assert!(
        snap_line.contains("top level 5"),
        "snapshot top level should be 5 (FS_TREE):\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_snapshot() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let src = format!("{mp}/src");
    let snap = format!("{mp}/snap");

    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "data.bin", 4096);

    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap]);

    verify_test_data(Path::new(&snap), "data.bin", 4096);

    let out = btrfs_ok(&["property", "get", "-t", "subvol", &snap, "ro"]);
    assert!(out.contains("true"), "expected ro=true:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_set_default() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(out.contains("5"), "expected ID 5:\n{out}");

    btrfs_ok(&["subvolume", "create", &format!("{mp}/newdefault")]);
    btrfs_ok(&["subvolume", "set-default", &format!("{mp}/newdefault")]);

    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(!out.contains("ID 5"), "expected non-5 default:\n{out}");

    // Verify via uapi: default should no longer be 5
    let default_id =
        btrfs_uapi::subvolume::subvolume_default_get(mnt.fd()).unwrap();
    assert_ne!(
        default_id, 5,
        "default subvol should not be 5 after set-default"
    );

    btrfs_ok(&["subvolume", "set-default", "5", mp]);
    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(out.contains("5"), "expected ID 5 restored:\n{out}");

    // Verify via uapi: default restored to 5
    let default_id =
        btrfs_uapi::subvolume::subvolume_default_get(mnt.fd()).unwrap();
    assert_eq!(default_id, 5, "default subvol should be 5 after restore");
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_set_flags() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/flagtest");

    btrfs_ok(&["subvolume", "create", &subvol]);

    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(
        !out.contains("readonly"),
        "expected no readonly flag:\n{out}"
    );

    btrfs_ok(&["subvolume", "set-flags", "readonly", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(out.contains("readonly"), "expected readonly flag:\n{out}");

    btrfs_ok(&["subvolume", "set-flags", "-", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(
        !out.contains("readonly"),
        "expected no readonly flag:\n{out}"
    );
}

// ── subvolume create -p (parent creation) ───────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_parents() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Without -p, creating in a non-existent parent directory should fail.
    let deep = format!("{mp}/dir1/dir2/subvol");
    let (_, _, code) = btrfs(&["subvolume", "create", &deep]);
    assert_ne!(code, 0, "create without -p in missing dir should fail");

    // With -p, parent directories are created automatically.
    btrfs_ok(&["subvolume", "create", "-p", &deep]);
    assert!(Path::new(&deep).is_dir());

    // Verify the parent directories exist and the subvolume is listed.
    let out = btrfs_ok(&["subvolume", "list", mp]);
    assert!(
        out.contains("dir1/dir2/subvol"),
        "expected nested subvol in list:\n{out}"
    );
}

// ── subvolume create failures ───────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_existing_path_fails() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let subvol = format!("{mp}/existing");
    btrfs_ok(&["subvolume", "create", &subvol]);

    // Creating the same subvolume again should fail.
    let (_, _, code) = btrfs(&["subvolume", "create", &subvol]);
    assert_ne!(code, 0, "create over existing subvolume should fail");
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_over_file_fails() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let file = format!("{mp}/afile");
    fs::write(&file, b"hello").unwrap();

    // Creating a subvolume where a regular file exists should fail.
    let (_, _, code) = btrfs(&["subvolume", "create", &file]);
    assert_ne!(code, 0, "create over existing file should fail");
}

// ── subvolume create with mixed valid/invalid paths ─────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_mixed_paths() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // One invalid path (parent doesn't exist) and two valid paths.
    // The command should fail overall, but the valid subvolumes should
    // still be created.
    let invalid = format!("{mp}/no-such-dir/sub0");
    let valid1 = format!("{mp}/sub1");
    let valid2 = format!("{mp}/sub2");

    let (_, _, code) =
        btrfs(&["subvolume", "create", &invalid, &valid1, &valid2]);
    assert_ne!(code, 0, "should fail due to invalid path");

    // The valid subvolumes should exist despite the overall failure.
    assert!(Path::new(&valid1).is_dir(), "sub1 should have been created");
    assert!(Path::new(&valid2).is_dir(), "sub2 should have been created");
    assert!(
        !Path::new(&invalid).exists(),
        "invalid path should not exist"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_parents_mixed() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // With -p, all paths should succeed (parents created as needed).
    let deep1 = format!("{mp}/dir1/deep/sub1");
    let deep2 = format!("{mp}/dir2/sub2");
    let flat = format!("{mp}/sub3");

    btrfs_ok(&["subvolume", "create", "-p", &deep1, &deep2, &flat]);

    assert!(Path::new(&deep1).is_dir(), "deep1 should exist");
    assert!(Path::new(&deep2).is_dir(), "deep2 should exist");
    assert!(Path::new(&flat).is_dir(), "flat should exist");

    let out = btrfs_ok(&["subvolume", "list", mp]);
    assert!(
        out.contains("dir1/deep/sub1"),
        "expected deep1 in list:\n{out}"
    );
    assert!(out.contains("dir2/sub2"), "expected deep2 in list:\n{out}");
    assert!(out.contains("sub3"), "expected flat in list:\n{out}");
}

// ── dry-run ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn dry_run_subvolume_delete() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let subvol = format!("{mp}/dry_target");
    btrfs_ok(&["subvolume", "create", &subvol]);
    assert!(Path::new(&subvol).is_dir());

    // --dry-run should not actually delete.
    btrfs_ok(&["--dry-run", "subvolume", "delete", &subvol]);
    assert!(
        Path::new(&subvol).is_dir(),
        "subvolume should still exist after --dry-run delete"
    );

    // Without --dry-run, it should actually delete.
    btrfs_ok(&["subvolume", "delete", &subvol]);
    assert!(
        !Path::new(&subvol).exists(),
        "subvolume should be gone after real delete"
    );
}

// ── property ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_set_ro() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/proptest");

    btrfs_ok(&["subvolume", "create", &subvol]);

    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");

    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "true"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("true"), "expected ro=true:\n{out}");

    // Verify via uapi: RDONLY flag should be set
    let subvol_file = fs::File::open(&subvol).unwrap();
    let flags = subvolume_flags_get(subvol_file.as_fd()).unwrap();
    assert!(flags.contains(SubvolumeFlags::RDONLY));

    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "false"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");

    // Verify via uapi: RDONLY flag should be cleared
    let flags = subvolume_flags_get(subvol_file.as_fd()).unwrap();
    assert!(!flags.contains(SubvolumeFlags::RDONLY));
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_set_compression() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let file = format!("{mp}/comptest.txt");
    fs::write(&file, "hello").unwrap();

    // Set compression to zlib
    btrfs_ok(&["property", "set", &file, "compression", "zlib"]);
    let out = btrfs_ok(&["property", "get", &file, "compression"]);
    assert!(out.contains("zlib"), "expected compression=zlib:\n{out}");

    // Set compression to lzo
    btrfs_ok(&["property", "set", &file, "compression", "lzo"]);
    let out = btrfs_ok(&["property", "get", &file, "compression"]);
    assert!(out.contains("lzo"), "expected compression=lzo:\n{out}");

    // Clear compression
    btrfs_ok(&["property", "set", &file, "compression", ""]);
    let out = btrfs_ok(&["property", "get", &file, "compression"]);
    assert!(
        !out.contains("compression="),
        "expected no compression:\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_set_label() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&[
        "property",
        "set",
        "-t",
        "filesystem",
        mp,
        "label",
        "newlabel",
    ]);
    let out = btrfs_ok(&["property", "get", "-t", "filesystem", mp, "label"]);
    assert!(out.contains("newlabel"), "expected label=newlabel:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_set_ro_invalid_value() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/badval");

    btrfs_ok(&["subvolume", "create", &subvol]);
    let (_stdout, stderr, code) =
        btrfs(&["property", "set", "-t", "subvol", &subvol, "ro", "yes"]);
    assert_ne!(code, 0, "expected failure for invalid ro value");
    assert!(
        stderr.contains("invalid value"),
        "expected 'invalid value' in stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_wrong_property_for_type() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let file = format!("{mp}/wrongprop.txt");
    fs::write(&file, "test").unwrap();

    // "ro" is not valid on an inode
    let (_stdout, stderr, code) = btrfs(&["property", "get", &file, "ro"]);
    assert_ne!(code, 0, "expected failure for wrong property type");
    assert!(
        stderr.contains("not applicable"),
        "expected 'not applicable' in stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_ro_no_change_when_already_set() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/nochange");

    btrfs_ok(&["subvolume", "create", &subvol]);

    // Setting ro=false on an already-rw subvolume should succeed (no-op)
    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "false"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");
}

// ── receive --chroot ─────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn receive_chroot() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/chroot.bin", _td1.path().to_str().unwrap());

    // Create and send a subvolume.
    let src = format!("{mp1}/chroot_test");
    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "data.bin", 8192);
    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);

    // Receive with --chroot. The -f path is opened before chroot, so
    // it uses the real filesystem path.
    btrfs_ok(&["receive", "-C", "-f", &stream_file, mp2]);

    // Verify the received subvolume.
    let received = format!("{mp2}/chroot_test");
    assert!(Path::new(&received).is_dir(), "received subvol not found");
    verify_test_data(Path::new(&received), "data.bin", 8192);
}

// ── property force clear received_uuid ───────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn property_force_clear_received_uuid() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/force.bin", _td1.path().to_str().unwrap());

    // Create a subvolume, send it, and receive it.
    let src = format!("{mp1}/src");
    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "file.bin", 4096);
    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    // The received subvolume should be read-only with a received_uuid.
    let received = format!("{mp2}/src");
    let out = btrfs_ok(&["subvolume", "show", &received]);
    assert!(
        out.contains("Received UUID:"),
        "expected Received UUID field:\n{out}"
    );
    // The received UUID should not be "-" (nil).
    assert!(
        !out.lines()
            .any(|l| l.contains("Received UUID:") && l.contains("-\n")),
        "expected non-nil received UUID"
    );

    // Without -f, flipping ro→rw should fail.
    let (_, stderr, code) =
        btrfs(&["property", "set", "-t", "subvol", &received, "ro", "false"]);
    assert_ne!(code, 0, "expected failure without -f");
    assert!(
        stderr.contains("received_uuid"),
        "expected received_uuid error:\n{stderr}"
    );

    // With -f, it should succeed and clear the received_uuid.
    btrfs_ok(&[
        "property", "set", "-t", "subvol", "-f", &received, "ro", "false",
    ]);

    // Verify: subvolume is now writable.
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &received, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");

    // Verify: received_uuid is now nil.
    let out = btrfs_ok(&["subvolume", "show", &received]);
    let recv_line = out.lines().find(|l| l.contains("Received UUID:")).unwrap();
    assert!(
        recv_line.contains("\t-") || recv_line.contains(" -"),
        "expected nil received UUID after force, got: {recv_line}"
    );
}

// ── send / receive ───────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_dump() {
    let (_td, mnt) = common::deterministic_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/dumptest");
    let stream_file = format!("{}/dump.bin", _td.path().to_str().unwrap());

    btrfs_ok(&["subvolume", "create", &subvol]);
    write_test_data(Path::new(&subvol), "file.bin", 4096);
    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &subvol]);

    let out = btrfs_ok(&["receive", "--dump", "-f", &stream_file]);

    let re_uuid = Regex::new(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    )
    .unwrap();
    let re_offset = Regex::new(r"offset=\d+").unwrap();
    let re_len = Regex::new(r"len=\d+").unwrap();
    let re_mode = Regex::new(r"mode=\d+").unwrap();
    let re_uid = Regex::new(r"uid=\d+").unwrap();
    let re_gid = Regex::new(r"gid=\d+").unwrap();

    let mut redacted = redact(&out, &mnt);
    redacted = re_uuid.replace_all(&redacted, "<UUID>").into_owned();
    redacted = re_offset.replace_all(&redacted, "offset=<N>").into_owned();
    redacted = re_len.replace_all(&redacted, "len=<N>").into_owned();
    redacted = re_mode.replace_all(&redacted, "mode=<N>").into_owned();
    redacted = re_uid.replace_all(&redacted, "uid=<N>").into_owned();
    redacted = re_gid.replace_all(&redacted, "gid=<N>").into_owned();
    snap!("btrfs receive --dump -f <STREAM>", redacted);
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_roundtrip() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file =
        format!("{}/roundtrip.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/origin");
    btrfs_ok(&["subvolume", "create", &src]);

    write_test_data(Path::new(&src), "file1.bin", 65536);
    write_test_data(Path::new(&src), "file2.bin", 1024);
    fs::create_dir(format!("{src}/dir")).unwrap();
    write_test_data(Path::new(&format!("{src}/dir")), "file3.bin", 32768);

    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);

    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    let received = format!("{mp2}/origin");
    assert!(Path::new(&received).is_dir(), "received subvol not found");

    verify_test_data(Path::new(&received), "file1.bin", 65536);
    verify_test_data(Path::new(&received), "file2.bin", 1024);
    verify_test_data(Path::new(&format!("{received}/dir")), "file3.bin", 32768);
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_incremental() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let base_stream = format!("{}/base.bin", _td1.path().to_str().unwrap());
    let incr_stream = format!("{}/incr.bin", _td1.path().to_str().unwrap());

    // Create a subvolume with initial content.
    let src = format!("{mp1}/data");
    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "unchanged.bin", 8192);
    write_test_data(Path::new(&src), "modified.bin", 4096);
    write_test_data(Path::new(&src), "deleted.bin", 2048);
    fs::create_dir(format!("{src}/subdir")).unwrap();
    write_test_data(Path::new(&format!("{src}/subdir")), "nested.bin", 1024);

    // Take a read-only snapshot as the base.
    let base_snap = format!("{mp1}/snap_base");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &base_snap]);

    // Modify the subvolume: add, change, and delete files.
    write_test_data(Path::new(&src), "added.bin", 16384);
    // Overwrite modified.bin with different size.
    write_test_data(Path::new(&src), "modified.bin", 32768);
    fs::remove_file(format!("{src}/deleted.bin")).unwrap();
    fs::create_dir(format!("{src}/newdir")).unwrap();
    write_test_data(Path::new(&format!("{src}/newdir")), "fresh.bin", 4096);

    // Take a second read-only snapshot.
    let incr_snap = format!("{mp1}/snap_incr");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &incr_snap]);

    // Full send the base snapshot to the second mount.
    btrfs_ok(&["send", "-f", &base_stream, &base_snap]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);

    // Verify the base was received correctly.
    let recv_base = format!("{mp2}/snap_base");
    verify_test_data(Path::new(&recv_base), "unchanged.bin", 8192);
    verify_test_data(Path::new(&recv_base), "modified.bin", 4096);
    verify_test_data(Path::new(&recv_base), "deleted.bin", 2048);
    verify_test_data(
        Path::new(&format!("{recv_base}/subdir")),
        "nested.bin",
        1024,
    );

    // Incremental send: only the changes since base.
    btrfs_ok(&["send", "-p", &base_snap, "-f", &incr_stream, &incr_snap]);

    // The incremental stream should be non-empty.
    let incr_size = fs::metadata(&incr_stream).unwrap().len();
    assert!(incr_size > 0, "incremental stream is empty");

    // Receive the incremental stream.
    btrfs_ok(&["receive", "-f", &incr_stream, mp2]);

    // Verify the incremental snapshot has the correct final state.
    let recv_incr = format!("{mp2}/snap_incr");
    assert!(
        Path::new(&recv_incr).is_dir(),
        "incremental snapshot not found"
    );

    // Unchanged file should be intact.
    verify_test_data(Path::new(&recv_incr), "unchanged.bin", 8192);

    // Modified file should have the new content and size.
    verify_test_data(Path::new(&recv_incr), "modified.bin", 32768);

    // Deleted file should be gone.
    assert!(
        !Path::new(&format!("{recv_incr}/deleted.bin")).exists(),
        "deleted.bin should not exist in incremental snapshot"
    );

    // Added file should be present.
    verify_test_data(Path::new(&recv_incr), "added.bin", 16384);

    // Original nested file should still be there.
    verify_test_data(
        Path::new(&format!("{recv_incr}/subdir")),
        "nested.bin",
        1024,
    );

    // New directory and file should be present.
    verify_test_data(
        Path::new(&format!("{recv_incr}/newdir")),
        "fresh.bin",
        4096,
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_v2_compressed() {
    // Source: mount with zstd compression so writes produce compressed extents.
    let td1 = tempdir().unwrap();
    let file1 = BackingFile::new(td1.path(), "disk.img", 512_000_000);
    file1.mkfs();
    let lo1 = LoopbackDevice::new(file1);
    let mnt1 = common::Mount::with_options(lo1, td1.path(), &["compress=zstd"]);
    let mp1 = mnt1.path().to_str().unwrap();

    // Destination: plain mount (no compression).
    let (_td2, mnt2) = single_mount();
    let mp2 = mnt2.path().to_str().unwrap();

    let stream_file = format!("{}/v2.bin", td1.path().to_str().unwrap());

    // Create a subvolume and write compressible data. The compress mount
    // option ensures the kernel stores these extents compressed on disk.
    let src = format!("{mp1}/v2test");
    btrfs_ok(&["subvolume", "create", &src]);
    common::write_compressible_data(Path::new(&src), "zeros.bin", 256 * 1024);
    write_test_data(Path::new(&src), "pattern.bin", 64 * 1024);
    btrfs_ok(&["filesystem", "sync", mp1]);

    // Make read-only and send with --compressed-data (forces v2 protocol).
    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "--compressed-data", "-f", &stream_file, &src]);

    // Verify the stream contains encoded_write commands via --dump.
    let dump = btrfs_ok(&["receive", "--dump", "-f", &stream_file]);
    assert!(
        dump.contains("encoded_write"),
        "expected encoded_write in v2 stream dump:\n{dump}"
    );

    // Receive on the second mount.
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    // Verify data integrity.
    let received = format!("{mp2}/v2test");
    assert!(Path::new(&received).is_dir(), "received subvol not found");

    // zeros.bin: 256KB of all zeros.
    let zeros = fs::read(format!("{received}/zeros.bin")).unwrap();
    assert_eq!(zeros.len(), 256 * 1024);
    assert!(
        zeros.iter().all(|&b| b == 0),
        "zeros.bin contains non-zero data"
    );

    // pattern.bin: deterministic test data.
    verify_test_data(Path::new(&received), "pattern.bin", 64 * 1024);
}

// ── send with parent and multiple subvolumes ────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn send_parent_multi_subvol() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let stream = format!("{}/multi.bin", _td.path().to_str().unwrap());

    // Create a parent subvolume with data that changes between snapshots.
    let parent = format!("{mp}/parent");
    btrfs_ok(&["subvolume", "create", &parent]);
    write_test_data(Path::new(&parent), "base.bin", 8192);

    let snap1 = format!("{mp}/snap1");
    btrfs_ok(&["subvolume", "snapshot", "-r", &parent, &snap1]);

    write_test_data(Path::new(&parent), "added1.bin", 4096);
    let snap2 = format!("{mp}/snap2");
    btrfs_ok(&["subvolume", "snapshot", "-r", &parent, &snap2]);

    write_test_data(Path::new(&parent), "added2.bin", 4096);
    let snap3 = format!("{mp}/snap3");
    btrfs_ok(&["subvolume", "snapshot", "-r", &parent, &snap3]);

    // Send snap2 and snap3 incrementally using snap1 as the parent.
    btrfs_ok(&["send", "-f", &stream, "-p", &snap1, &snap2, &snap3]);

    // Receive on a second mount.
    let (_td2, mnt2) = single_mount();
    let mp2 = mnt2.path().to_str().unwrap();

    // Must receive the parent snapshot first.
    let base_stream = format!("{}/base.bin", _td.path().to_str().unwrap());
    btrfs_ok(&["send", "-f", &base_stream, &snap1]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);

    // Then receive the incremental stream with both snap2 and snap3.
    btrfs_ok(&["receive", "-f", &stream, mp2]);

    // Verify received subvolumes exist and have the right content.
    verify_test_data(Path::new(&format!("{mp2}/snap2")), "base.bin", 8192);
    verify_test_data(Path::new(&format!("{mp2}/snap2")), "added1.bin", 4096);
    verify_test_data(Path::new(&format!("{mp2}/snap3")), "base.bin", 8192);
    verify_test_data(Path::new(&format!("{mp2}/snap3")), "added2.bin", 4096);
}

// ── scrub ────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn scrub_start_status() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["scrub", "start", mp]);

    let (out, _, code) = btrfs(&["scrub", "status", mp]);
    assert_eq!(code, 0);
    assert!(
        out.contains("scrub") || out.contains("UUID"),
        "expected scrub status output:\n{out}"
    );
}

// ── device ───────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_add_remove() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let dev2_file = BackingFile::new(_td.path(), "disk2.img", 512_000_000);
    let dev2 = LoopbackDevice::new(dev2_file);
    let dev2_path = dev2.path().to_str().unwrap();

    btrfs_ok(&["device", "add", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        out.contains(dev2_path),
        "expected new device in show output:\n{out}"
    );

    // Verify via uapi: should have 2 devices
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    assert_eq!(fs_info.num_devices, 2, "expected 2 devices after add");

    btrfs_ok(&["device", "remove", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(!out.contains(dev2_path), "device should be removed:\n{out}");

    // Verify via uapi: should be back to 1 device
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    assert_eq!(fs_info.num_devices, 1, "expected 1 device after remove");
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_add_force() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a second device and format it as btrfs so it has a superblock.
    let dev2_file = BackingFile::new(_td.path(), "disk2.img", 512_000_000);
    let dev2 = LoopbackDevice::new(dev2_file);
    let dev2_path = dev2.path().to_str().unwrap();

    Command::new("mkfs.btrfs")
        .args(["-f", dev2_path])
        .output()
        .expect("mkfs.btrfs failed");

    // Without --force, adding a device with an existing btrfs superblock should fail.
    let (_stdout, stderr, code) = btrfs(&["device", "add", dev2_path, mp]);
    assert_ne!(code, 0, "expected failure without --force:\n{stderr}");
    assert!(
        stderr.contains("already contains a btrfs filesystem"),
        "expected btrfs superblock error:\n{stderr}"
    );

    // With --force, it should succeed.
    btrfs_ok(&["device", "add", "-f", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        out.contains(dev2_path),
        "expected new device in show output:\n{out}"
    );

    // Verify via uapi: should have 2 devices.
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    assert_eq!(fs_info.num_devices, 2, "expected 2 devices after force add");
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_add_nodiscard() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let dev2_file = BackingFile::new(_td.path(), "disk2.img", 512_000_000);
    let dev2 = LoopbackDevice::new(dev2_file);
    let dev2_path = dev2.path().to_str().unwrap();

    // -K should skip TRIM and still add the device successfully.
    btrfs_ok(&["device", "add", "-K", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        out.contains(dev2_path),
        "expected new device in show output:\n{out}"
    );

    // Verify via uapi.
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    assert_eq!(
        fs_info.num_devices, 2,
        "expected 2 devices after nodiscard add"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_add_enqueue() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let dev2_file = BackingFile::new(_td.path(), "disk2.img", 512_000_000);
    let dev2 = LoopbackDevice::new(dev2_file);
    let dev2_path = dev2.path().to_str().unwrap();

    // --enqueue should work even when no exclusive operation is running.
    btrfs_ok(&["device", "add", "--enqueue", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        out.contains(dev2_path),
        "expected new device in show output:\n{out}"
    );

    // Verify via uapi.
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    assert_eq!(
        fs_info.num_devices, 2,
        "expected 2 devices after enqueue add"
    );
}

// ── device scan / ready ──────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_scan() {
    let (_td, mnt) = single_mount();
    let dev = mnt.loopback().path().to_str().unwrap();

    let out = btrfs_ok(&["device", "scan", dev]);
    assert!(
        out.contains("registered"),
        "expected registered message:\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_ready() {
    let (_td, mnt) = single_mount();
    let dev = mnt.loopback().path().to_str().unwrap();

    let out = btrfs_ok(&["device", "ready", dev]);
    assert!(out.contains("ready"), "expected ready message:\n{out}");
}

// ── balance ──────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn balance_start_status() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["balance", "start", "--full-balance", mp]);

    let (stdout, stderr, _) = btrfs(&["balance", "status", mp]);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("balance") || combined.contains("No"),
        "unexpected status output:\n{combined}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn balance_cancel_not_running() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Cancel when nothing is running should fail gracefully.
    let (_, stderr, code) = btrfs(&["balance", "cancel", mp]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("Not in progress") || stderr.contains("balance"),
        "expected not-in-progress error:\n{stderr}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn balance_pause_not_running() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let (_, stderr, code) = btrfs(&["balance", "pause", mp]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("Not in progress") || stderr.contains("balance"),
        "expected not-in-progress error:\n{stderr}"
    );
}

// ── balance without filters ──────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn balance_without_filters_warns() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Balance start without --full-balance and without filters should
    // warn the user about a full balance. On a small filesystem it
    // completes immediately, but the warning should still appear.
    let (stdout, stderr, _code) = btrfs(&["balance", "start", mp]);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("Full balance")
            || combined.contains("full balance")
            || combined.contains("without filters"),
        "expected full-balance warning:\n{combined}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn balance_full_balance_flag() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // --full-balance should succeed without a warning.
    btrfs_ok(&["balance", "start", "--full-balance", mp]);
}

// ── inspect-internal ─────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let out = btrfs_ok(&["inspect-internal", "rootid", mp]);
    assert!(out.trim() == "5", "expected rootid 5, got: {out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_dump_super() {
    let (_td, mnt) = single_mount();
    let dev = mnt.loopback().path().to_str().unwrap();
    let out = btrfs_ok(&["inspect-internal", "dump-super", dev]);
    assert!(out.contains("magic"), "expected magic field:\n{out}");
    assert!(out.contains("[match]"), "expected magic match:\n{out}");
    assert!(out.contains("nodesize"), "expected nodesize:\n{out}");
}

// ── quota / qgroup ───────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn quota_enable_disable() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);

    let out = btrfs_ok(&["quota", "status", mp]);
    assert!(
        out.contains("enabled") || out.contains("Quota"),
        "expected enabled status:\n{out}"
    );

    // Verify via uapi/sysfs
    let fs_info = filesystem_info(mnt.fd()).unwrap();
    let sysfs = btrfs_uapi::sysfs::SysfsBtrfs::new(&fs_info.uuid);
    let status = sysfs.quota_status().unwrap();
    assert!(status.enabled, "quota should be enabled via sysfs");

    btrfs_ok(&["quota", "disable", mp]);

    // Verify disabled via uapi/sysfs
    let status = sysfs.quota_status().unwrap();
    assert!(!status.enabled, "quota should be disabled via sysfs");
}

#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_show() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);
    btrfs_ok(&["subvolume", "create", &format!("{mp}/sub1")]);
    write_test_data(Path::new(&format!("{mp}/sub1")), "data.bin", 65536);
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["qgroup", "show", mp]);
    assert!(out.contains("qgroupid"), "expected header:\n{out}");
    assert!(out.contains("rfer"), "expected rfer column:\n{out}");
    assert!(out.contains("excl"), "expected excl column:\n{out}");
    assert!(
        out.contains("0/"),
        "expected level-0 qgroup entries:\n{out}"
    );

    btrfs_ok(&["quota", "disable", mp]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_show_with_columns() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);

    let out = btrfs_ok(&["qgroup", "show", "-pcre", mp]);
    assert!(out.contains("max_rfer"), "expected max_rfer column:\n{out}");
    assert!(out.contains("max_excl"), "expected max_excl column:\n{out}");
    assert!(out.contains("parent"), "expected parent column:\n{out}");
    assert!(out.contains("child"), "expected child column:\n{out}");

    btrfs_ok(&["quota", "disable", mp]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_create_destroy() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);
    btrfs_ok(&["qgroup", "create", "1/100", mp]);

    let out = btrfs_ok(&["qgroup", "show", mp]);
    assert!(out.contains("1/100"), "expected created qgroup:\n{out}");

    btrfs_ok(&["qgroup", "destroy", "1/100", mp]);
    btrfs_ok(&["quota", "disable", mp]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_assign_remove() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);
    btrfs_ok(&["subvolume", "create", &format!("{mp}/sub1")]);
    btrfs_ok(&["qgroup", "create", "1/100", mp]);

    // Assign the level-0 qgroup for sub1 to the level-1 group.
    // sub1 is the first subvolume, so its qgroup is 0/256.
    btrfs_ok(&["qgroup", "assign", "--no-rescan", "0/256", "1/100", mp]);

    let out = btrfs_ok(&["qgroup", "show", "-pc", mp]);
    assert!(
        out.contains("1/100"),
        "expected parent group in show:\n{out}"
    );

    // Remove the assignment.
    btrfs_ok(&["qgroup", "remove", "--no-rescan", "0/256", "1/100", mp]);

    btrfs_ok(&["qgroup", "destroy", "1/100", mp]);
    btrfs_ok(&["quota", "disable", mp]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_limit() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);

    // Set a referenced limit on the top-level qgroup.
    btrfs_ok(&["qgroup", "limit", "1G", "0/5", mp]);

    let out = btrfs_ok(&["qgroup", "show", "-re", mp]);
    assert!(out.contains("0/5"), "expected qgroup entry:\n{out}");

    // Verify via uapi: qgroup 0/5 should have a max_rfer limit
    let qlist = btrfs_uapi::quota::qgroup_list(mnt.fd()).unwrap();
    let qg = qlist.qgroups.iter().find(|g| g.qgroupid == 5).unwrap();
    assert!(
        qg.max_rfer > 0 && qg.max_rfer != u64::MAX,
        "expected max_rfer limit to be set, got {}",
        qg.max_rfer
    );

    // Remove the limit.
    btrfs_ok(&["qgroup", "limit", "none", "0/5", mp]);

    btrfs_ok(&["quota", "disable", mp]);
}

// ── subvolume show with qgroup limits ────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show_qgroup_limit() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a subvolume before quotas are enabled.
    let subv_no_quota = format!("{mp}/no_quota");
    btrfs_ok(&["subvolume", "create", &subv_no_quota]);

    // subvolume show should work without quotas.
    btrfs_ok(&["subvolume", "show", &subv_no_quota]);

    // Enable quotas.
    btrfs_ok(&["quota", "enable", mp]);

    // Create a subvolume with quotas active (auto-creates qgroup).
    let subv_with_limit = format!("{mp}/with_limit");
    btrfs_ok(&["subvolume", "create", &subv_with_limit]);

    // Get the rootid and set an exclusive limit on it.
    let rootid_out =
        btrfs_ok(&["inspect-internal", "rootid", &subv_with_limit]);
    let rootid = rootid_out.trim();
    btrfs_ok(&["qgroup", "limit", "-e", "1G", &format!("0/{rootid}"), mp]);

    // subvolume show should succeed for both subvolumes even with
    // quotas and limits active.
    btrfs_ok(&["subvolume", "show", &subv_no_quota]);
    btrfs_ok(&["subvolume", "show", &subv_with_limit]);

    // Verify the qgroup limit is visible via qgroup show.
    let out = btrfs_ok(&["qgroup", "show", "-re", mp]);
    assert!(
        out.contains(&format!("0/{rootid}")),
        "expected qgroup entry for subvol:\n{out}"
    );

    btrfs_ok(&["quota", "disable", mp]);
}

// ── quota rescan ─────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn quota_rescan() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["quota", "enable", mp]);

    // Start a rescan and wait for completion.
    btrfs_ok(&["quota", "rescan", "-w", mp]);

    // Query rescan status — should report no rescan in progress.
    let out = btrfs_ok(&["quota", "rescan", "-s", mp]);
    assert!(
        out.contains("rescan") || out.contains("quota"),
        "expected rescan status:\n{out}"
    );

    btrfs_ok(&["quota", "disable", mp]);
}

// ── scrub limit / resume ─────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn scrub_limit() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Read the current limit (should show a table).
    let out = btrfs_ok(&["scrub", "limit", mp]);
    assert!(
        out.contains("devid") || out.contains("limit"),
        "expected limit table:\n{out}"
    );

    // Set a limit on all devices.
    btrfs_ok(&["scrub", "limit", "-a", "-l", "100m", mp]);

    // Read it back.
    let out = btrfs_ok(&["scrub", "limit", mp]);
    assert!(
        out.contains("100") || out.contains("104857600"),
        "expected limit value in output:\n{out}"
    );

    // Clear the limit (set to 0 = unlimited).
    btrfs_ok(&["scrub", "limit", "-a", "-l", "0", mp]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn scrub_resume_no_scrub() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Resume when no scrub has been started — may succeed (starts a new
    // scrub) or fail gracefully depending on kernel state. Either way it
    // should not crash.
    let (_, _, code) = btrfs(&["scrub", "resume", mp]);
    assert!(code == 0 || code == 1, "unexpected exit code: {code}");
}

// ── balance resume ───────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn balance_resume_not_running() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let (_, stderr, code) = btrfs(&["balance", "resume", mp]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("Not in progress") || stderr.contains("balance"),
        "expected not-in-progress error:\n{stderr}"
    );
}

// ── replace status (never started) ──────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn replace_status_never_started() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["replace", "status", mp]);
    assert!(
        out.contains("no device replace")
            || out.contains("Never")
            || out.contains("started"),
        "expected never-started status:\n{out}"
    );
}

// ── device scan --forget ─────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_scan_forget_stale() {
    let (_td, _mnt) = single_mount();

    // Forget all stale (unmounted) devices. On a test system this may or
    // may not find anything to forget, but the command should succeed.
    let out = btrfs_ok(&["device", "scan", "--forget"]);
    assert!(
        out.contains("unregistered"),
        "expected unregistered message:\n{out}"
    );
}

// ── subvolume create/delete with multiple paths ──────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_multiple() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&[
        "subvolume",
        "create",
        &format!("{mp}/multi1"),
        &format!("{mp}/multi2"),
        &format!("{mp}/multi3"),
    ]);

    assert!(Path::new(&format!("{mp}/multi1")).is_dir());
    assert!(Path::new(&format!("{mp}/multi2")).is_dir());
    assert!(Path::new(&format!("{mp}/multi3")).is_dir());

    let out = btrfs_ok(&["subvolume", "list", mp]);
    assert!(out.contains("multi1"), "expected multi1:\n{out}");
    assert!(out.contains("multi2"), "expected multi2:\n{out}");
    assert!(out.contains("multi3"), "expected multi3:\n{out}");
}

// ── subvolume delete flags ───────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_delete_commit_after() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/commitvol");

    btrfs_ok(&["subvolume", "create", &subvol]);
    btrfs_ok(&["subvolume", "delete", "-c", &subvol]);
    assert!(!Path::new(&subvol).exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_delete_commit_each() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["subvolume", "create", &format!("{mp}/each1")]);
    btrfs_ok(&["subvolume", "create", &format!("{mp}/each2")]);

    btrfs_ok(&[
        "subvolume",
        "delete",
        "-C",
        &format!("{mp}/each1"),
        &format!("{mp}/each2"),
    ]);
    assert!(!Path::new(&format!("{mp}/each1")).exists());
    assert!(!Path::new(&format!("{mp}/each2")).exists());

    // Verify via uapi: no user subvolumes should remain.
    let list = subvolume::subvolume_list(mnt.fd()).unwrap();
    assert!(
        list.is_empty(),
        "expected no subvolumes after delete, got {}",
        list.len()
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_delete_by_subvolid() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/idvol");

    btrfs_ok(&["subvolume", "create", &subvol]);

    // Get the subvolume ID from `subvolume show`.
    let out = btrfs_ok(&["subvolume", "show", &subvol]);
    let id = out
        .lines()
        .find(|l| l.trim().starts_with("Subvolume ID:"))
        .expect("expected Subvolume ID line")
        .split(':')
        .nth(1)
        .unwrap()
        .trim();

    btrfs_ok(&["subvolume", "delete", "-i", id, mp]);
    assert!(!Path::new(&subvol).exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_delete_recursive() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let parent = format!("{mp}/parent");
    let child = format!("{parent}/child");
    let grandchild = format!("{child}/grandchild");

    btrfs_ok(&["subvolume", "create", &parent]);
    btrfs_ok(&["subvolume", "create", &child]);
    btrfs_ok(&["subvolume", "create", &grandchild]);

    // Without --recursive, deleting parent should fail (has nested subvols).
    let (_stdout, _stderr, code) = btrfs(&["subvolume", "delete", &parent]);
    assert_ne!(code, 0, "expected failure without --recursive");

    // With --recursive, it should succeed.
    btrfs_ok(&["subvolume", "delete", "-R", &parent]);
    assert!(!Path::new(&parent).exists());
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_delete_verbose() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/verbvol");

    btrfs_ok(&["subvolume", "create", &subvol]);

    let out = btrfs_ok(&["subvolume", "delete", "-v", &subvol]);
    assert!(
        out.contains("Delete subvolume"),
        "expected verbose delete message:\n{out}"
    );
}

// ── replace start/status/cancel ─────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn replace_start_and_status() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Write some data so the replace has something to copy.
    write_test_data(Path::new(mp), "testdata", 1024 * 1024);

    // Create target device.
    let target_file =
        BackingFile::new(_td.path(), "replace-target.img", 512_000_000);
    let target_dev = LoopbackDevice::new(target_file);
    let target_path = target_dev.path().to_str().unwrap();

    // Replace devid 1 with the target device; -B waits for completion.
    btrfs_ok(&["replace", "start", "-B", "-f", "1", target_path, mp]);

    // After completion, status should show finished/completed.
    let out = btrfs_ok(&["replace", "status", mp]);
    assert!(
        out.contains("finished")
            || out.contains("completed")
            || out.contains("Started")
            || out.contains("no device replace"),
        "unexpected replace status after completion:\n{out}"
    );

    // Verify data is still accessible.
    verify_test_data(Path::new(mp), "testdata", 1024 * 1024);
}

#[test]
#[ignore = "requires elevated privileges"]
fn replace_cancel_not_running() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Cancel with no replace running should succeed and report it.
    let out = btrfs_ok(&["replace", "cancel", mp]);
    assert!(
        out.contains("no replace") || out.contains("not in progress"),
        "expected no-op cancel message:\n{out}"
    );
}

// ── send/receive: special files ─────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_special_files() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/special.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/special");
    btrfs_ok(&["subvolume", "create", &src]);

    // Create various special files.
    write_test_data(Path::new(&src), "regular.bin", 4096);
    symlink("regular.bin", format!("{src}/link.sym")).unwrap();
    fs::hard_link(format!("{src}/regular.bin"), format!("{src}/hardlink.bin"))
        .unwrap();
    // FIFO (named pipe).
    nix::unistd::mkfifo(
        &PathBuf::from(format!("{src}/pipe")),
        stat::Mode::from_bits_truncate(0o644),
    )
    .unwrap();

    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    let recv = format!("{mp2}/special");

    // Regular file.
    verify_test_data(Path::new(&recv), "regular.bin", 4096);

    // Symlink.
    let link_target = fs::read_link(format!("{recv}/link.sym")).unwrap();
    assert_eq!(link_target.to_str().unwrap(), "regular.bin");

    // Hard link (should share inode with regular.bin).
    let meta_orig = fs::metadata(format!("{recv}/regular.bin")).unwrap();
    let meta_hard = fs::metadata(format!("{recv}/hardlink.bin")).unwrap();
    assert_eq!(meta_orig.ino(), meta_hard.ino());

    // FIFO.
    let fifo_meta = fs::symlink_metadata(format!("{recv}/pipe")).unwrap();
    assert!(
        fifo_meta.file_type().is_fifo(),
        "expected FIFO, got {:?}",
        fifo_meta.file_type()
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_xattrs() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/xattr.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/xattr_test");
    btrfs_ok(&["subvolume", "create", &src]);

    let file_path = format!("{src}/testfile");
    fs::write(&file_path, b"xattr test content").unwrap();

    // Set xattrs using the setfattr command.
    let status = Command::new("setfattr")
        .args(["-n", "user.myattr", "-v", "hello", &file_path])
        .status()
        .expect("setfattr not found");
    assert!(status.success(), "setfattr failed");

    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    // Verify xattr was preserved.
    let recv_file = format!("{mp2}/xattr_test/testfile");
    let output = Command::new("getfattr")
        .args(["--only-values", "-n", "user.myattr", &recv_file])
        .output()
        .expect("getfattr not found");
    assert!(output.status.success(), "getfattr failed");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "hello",
        "xattr value mismatch"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_force_decompress() {
    // Source: mount with zstd compression.
    let td1 = tempdir().unwrap();
    let file1 = BackingFile::new(td1.path(), "disk.img", 512_000_000);
    file1.mkfs();
    let lo1 = LoopbackDevice::new(file1);
    let mnt1 = common::Mount::with_options(lo1, td1.path(), &["compress=zstd"]);
    let mp1 = mnt1.path().to_str().unwrap();

    let (_td2, mnt2) = single_mount();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/decomp.bin", td1.path().to_str().unwrap());

    let src = format!("{mp1}/decomp_test");
    btrfs_ok(&["subvolume", "create", &src]);
    common::write_compressible_data(Path::new(&src), "data.bin", 128 * 1024);
    write_test_data(Path::new(&src), "pattern.bin", 64 * 1024);
    btrfs_ok(&["filesystem", "sync", mp1]);

    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "--compressed-data", "-f", &stream_file, &src]);

    // Receive with --force-decompress to exercise the decompression fallback.
    btrfs_ok(&["receive", "--force-decompress", "-f", &stream_file, mp2]);

    let recv = format!("{mp2}/decomp_test");
    assert!(Path::new(&recv).is_dir(), "received subvol not found");

    let data = fs::read(format!("{recv}/data.bin")).unwrap();
    assert_eq!(data.len(), 128 * 1024);
    assert!(data.iter().all(|&b| b == 0), "data.bin should be all zeros");
    verify_test_data(Path::new(&recv), "pattern.bin", 64 * 1024);
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_truncate() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let base_stream =
        format!("{}/trunc_base.bin", _td1.path().to_str().unwrap());
    let incr_stream =
        format!("{}/trunc_incr.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/trunc_src");
    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "shrink.bin", 65536);

    let snap1 = format!("{mp1}/trunc_snap1");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap1]);

    // Truncate the file to a smaller size.
    let f = fs::OpenOptions::new()
        .write(true)
        .open(format!("{src}/shrink.bin"))
        .unwrap();
    f.set_len(1024).unwrap();
    drop(f);

    let snap2 = format!("{mp1}/trunc_snap2");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap2]);

    btrfs_ok(&["send", "-f", &base_stream, &snap1]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);
    btrfs_ok(&["send", "-p", &snap1, "-f", &incr_stream, &snap2]);
    btrfs_ok(&["receive", "-f", &incr_stream, mp2]);

    let recv = format!("{mp2}/trunc_snap2/shrink.bin");
    let meta = fs::metadata(&recv).unwrap();
    assert_eq!(meta.len(), 1024, "truncated file should be 1024 bytes");
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_mknod_mksock() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/mknod.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/devnodes");
    btrfs_ok(&["subvolume", "create", &src]);

    // Create a character device node (null device: 1,3) and a unix socket.
    stat::mknod(
        &PathBuf::from(format!("{src}/chardev")),
        stat::SFlag::S_IFCHR,
        stat::Mode::from_bits_truncate(0o666),
        stat::makedev(1, 3),
    )
    .unwrap();

    // Unix socket.
    let sock_path = format!("{src}/mysock");
    let sock = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
    drop(sock);

    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    let recv = format!("{mp2}/devnodes");

    // Verify character device.
    let chardev_meta = fs::symlink_metadata(format!("{recv}/chardev")).unwrap();
    assert!(
        chardev_meta.file_type().is_char_device(),
        "expected char device"
    );

    // Verify socket.
    let sock_meta = fs::symlink_metadata(format!("{recv}/mysock")).unwrap();
    assert!(sock_meta.file_type().is_socket(), "expected socket");
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_rmdir() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let base_stream =
        format!("{}/rmdir_base.bin", _td1.path().to_str().unwrap());
    let incr_stream =
        format!("{}/rmdir_incr.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/rmdir_src");
    btrfs_ok(&["subvolume", "create", &src]);
    fs::create_dir(format!("{src}/mydir")).unwrap();
    write_test_data(Path::new(&format!("{src}/mydir")), "file.bin", 1024);

    let snap1 = format!("{mp1}/rmdir_snap1");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap1]);

    // Remove the directory and its contents.
    fs::remove_file(format!("{src}/mydir/file.bin")).unwrap();
    fs::remove_dir(format!("{src}/mydir")).unwrap();

    let snap2 = format!("{mp1}/rmdir_snap2");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap2]);

    btrfs_ok(&["send", "-f", &base_stream, &snap1]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);
    btrfs_ok(&["send", "-p", &snap1, "-f", &incr_stream, &snap2]);
    btrfs_ok(&["receive", "-f", &incr_stream, mp2]);

    let recv = format!("{mp2}/rmdir_snap2");
    assert!(
        !Path::new(&format!("{recv}/mydir")).exists(),
        "mydir should have been removed"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_remove_xattr() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let base_stream =
        format!("{}/xattr_base.bin", _td1.path().to_str().unwrap());
    let incr_stream =
        format!("{}/xattr_incr.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/xattr_rm_src");
    btrfs_ok(&["subvolume", "create", &src]);
    let file_path = format!("{src}/testfile");
    fs::write(&file_path, b"content").unwrap();

    // Set an xattr, then take a snapshot.
    let status = Command::new("setfattr")
        .args(["-n", "user.remove_me", "-v", "val", &file_path])
        .status()
        .expect("setfattr not found");
    assert!(status.success());

    let snap1 = format!("{mp1}/xattr_snap1");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap1]);

    // Remove the xattr and take another snapshot.
    let status = Command::new("setfattr")
        .args(["-x", "user.remove_me", &file_path])
        .status()
        .expect("setfattr not found");
    assert!(status.success());

    let snap2 = format!("{mp1}/xattr_snap2");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap2]);

    btrfs_ok(&["send", "-f", &base_stream, &snap1]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);
    btrfs_ok(&["send", "-p", &snap1, "-f", &incr_stream, &snap2]);

    // Verify the incremental stream contains remove_xattr.
    let dump = btrfs_ok(&["receive", "--dump", "-f", &incr_stream]);
    assert!(
        dump.contains("remove_xattr"),
        "expected remove_xattr in stream:\n{dump}"
    );

    btrfs_ok(&["receive", "-f", &incr_stream, mp2]);

    // Verify the xattr is gone on the received side.
    let recv_file = format!("{mp2}/xattr_snap2/testfile");
    let output = Command::new("getfattr")
        .args(["-n", "user.remove_me", &recv_file])
        .output()
        .expect("getfattr not found");
    assert!(!output.status.success(), "xattr should have been removed");
}

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_fallocate() {
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let base_stream =
        format!("{}/falloc_base.bin", _td1.path().to_str().unwrap());
    let incr_stream =
        format!("{}/falloc_incr.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/falloc_src");
    btrfs_ok(&["subvolume", "create", &src]);

    // Write a file, then take a base snapshot.
    write_test_data(Path::new(&src), "holey.bin", 256 * 1024);

    let snap1 = format!("{mp1}/falloc_snap1");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap1]);

    // Punch a hole in the middle of the file.
    let file_path = format!("{src}/holey.bin");
    let status = Command::new("fallocate")
        .args([
            "--punch-hole",
            "--offset",
            "65536",
            "--length",
            "65536",
            &file_path,
        ])
        .status()
        .expect("fallocate not found");
    assert!(status.success(), "fallocate --punch-hole failed");

    let snap2 = format!("{mp1}/falloc_snap2");
    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap2]);

    btrfs_ok(&["send", "-f", &base_stream, &snap1]);
    btrfs_ok(&["receive", "-f", &base_stream, mp2]);
    // Use --proto 2 to get v2 stream with fallocate commands.
    btrfs_ok(&[
        "send",
        "--proto",
        "2",
        "-p",
        &snap1,
        "-f",
        &incr_stream,
        &snap2,
    ]);
    btrfs_ok(&["receive", "-f", &incr_stream, mp2]);

    // Verify the file size is preserved but the hole exists.
    let recv_file = format!("{mp2}/falloc_snap2/holey.bin");
    let meta = fs::metadata(&recv_file).unwrap();
    assert_eq!(meta.len(), 256 * 1024);

    // Read the hole region — should be zeros.
    let mut f = fs::File::open(&recv_file).unwrap();
    f.seek(SeekFrom::Start(65536)).unwrap();
    let mut hole = vec![0u8; 65536];
    f.read_exact(&mut hole).unwrap();
    assert!(hole.iter().all(|&b| b == 0), "punched hole should be zeros");
}

// ── mkfs --rootdir (end-to-end) ─────────────────────────────────────

/// Create a directory with various file types, run our mkfs --rootdir,
/// mount the resulting image, and verify all data is intact.
#[test]
#[ignore = "requires elevated privileges"]
fn mkfs_rootdir_end_to_end() {
    let td = tempdir().unwrap();
    let rootdir = td.path().join("rootdir");
    fs::create_dir_all(&rootdir).unwrap();

    // Regular files: small (inline) and large (regular extent).
    write_test_data(&rootdir, "small.bin", 100);
    write_test_data(&rootdir, "large.bin", 2 * 1024 * 1024);

    // Subdirectory with a file.
    let sub = rootdir.join("subdir");
    fs::create_dir_all(&sub).unwrap();
    write_test_data(&sub, "nested.bin", 8192);

    // Symlink.
    symlink("small.bin", rootdir.join("link.txt")).unwrap();

    // Empty file.
    fs::File::create(rootdir.join("empty")).unwrap();

    // Create image, format with --rootdir, mount.
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_rootdir(&rootdir, &[]);
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    let mp = mnt.path();

    // Verify data integrity.
    verify_test_data(mp, "small.bin", 100);
    verify_test_data(mp, "large.bin", 2 * 1024 * 1024);
    verify_test_data(&mp.join("subdir"), "nested.bin", 8192);

    // Verify symlink.
    let link_target = fs::read_link(mp.join("link.txt")).unwrap();
    assert_eq!(link_target.to_str().unwrap(), "small.bin");

    // Verify empty file exists and is empty.
    let empty_meta = fs::metadata(mp.join("empty")).unwrap();
    assert_eq!(empty_meta.len(), 0);

    // Verify subdirectory exists.
    assert!(mp.join("subdir").is_dir());
}

/// Test --rootdir with zstd compression: data should still be readable.
#[test]
#[ignore = "requires elevated privileges"]
fn mkfs_rootdir_compressed() {
    let td = tempdir().unwrap();
    let rootdir = td.path().join("rootdir");
    fs::create_dir_all(&rootdir).unwrap();

    // Compressible data (zeros compress well).
    write_compressible_data(&rootdir, "zeros.bin", 1024 * 1024);
    // Incompressible data (random pattern).
    write_test_data(&rootdir, "random.bin", 64 * 1024);

    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_rootdir(&rootdir, &["--compress", "zstd"]);
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    let mp = mnt.path();

    // Verify data reads back correctly (kernel handles decompression).
    let zeros = fs::read(mp.join("zeros.bin")).unwrap();
    assert_eq!(zeros.len(), 1024 * 1024);
    assert!(zeros.iter().all(|&b| b == 0), "decompressed zeros mismatch");

    verify_test_data(mp, "random.bin", 64 * 1024);
}

/// Test --rootdir with --shrink: image should be smaller than the full device size.
#[test]
#[ignore = "requires elevated privileges"]
fn mkfs_rootdir_shrink() {
    let td = tempdir().unwrap();
    let rootdir = td.path().join("rootdir");
    fs::create_dir_all(&rootdir).unwrap();

    write_test_data(&rootdir, "data.bin", 4096);

    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_rootdir(&rootdir, &["--shrink"]);

    // Image should be much smaller than 512 MB.
    let img_size = fs::metadata(td.path().join("disk.img")).unwrap().len();
    assert!(
        img_size < 200_000_000,
        "shrunk image should be < 200 MB, got {img_size}"
    );

    // Should still mount and have the data.
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    verify_test_data(mnt.path(), "data.bin", 4096);
}

/// Test --rootdir with LZO compression: data should still be readable.
/// LZO uses a per-sector framing format that differs from zlib/zstd.
#[test]
#[ignore = "requires elevated privileges"]
fn mkfs_rootdir_lzo_compressed() {
    let td = tempdir().unwrap();
    let rootdir = td.path().join("rootdir");
    fs::create_dir_all(&rootdir).unwrap();

    // Compressible data (zeros compress well with LZO).
    write_compressible_data(&rootdir, "zeros.bin", 1024 * 1024);
    // Small inline file (uses single-segment LZO format).
    fs::write(rootdir.join("small.txt"), "hello LZO compression test").unwrap();
    // Incompressible data (pseudo-random pattern).
    write_test_data(&rootdir, "random.bin", 64 * 1024);

    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs_rootdir(&rootdir, &["--compress", "lzo"]);
    let lo = LoopbackDevice::new(file);
    let mnt = Mount::new(lo, td.path());
    let mp = mnt.path();

    // Verify data reads back correctly (kernel handles LZO decompression).
    let zeros = fs::read(mp.join("zeros.bin")).unwrap();
    assert_eq!(zeros.len(), 1024 * 1024);
    assert!(zeros.iter().all(|&b| b == 0), "decompressed zeros mismatch");

    let small = fs::read_to_string(mp.join("small.txt")).unwrap();
    assert_eq!(small, "hello LZO compression test");

    verify_test_data(mp, "random.bin", 64 * 1024);
}

// ── rescue ──────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_clear_uuid_tree() {
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    file.mkfs();
    let lo = LoopbackDevice::new(file);

    // Mount, create subvolumes to populate the UUID tree, then unmount.
    let lo = {
        let mnt = Mount::new(lo, td.path());
        let mp = mnt.path();
        btrfs_ok(&["subvolume", "create", mp.join("sub1").to_str().unwrap()]);
        btrfs_ok(&["subvolume", "create", mp.join("sub2").to_str().unwrap()]);
        btrfs_ok(&[
            "subvolume",
            "snapshot",
            mp.join("sub1").to_str().unwrap(),
            mp.join("snap1").to_str().unwrap(),
        ]);
        mnt.into_loopback()
    };

    let dev = lo.path().to_str().unwrap();
    let out = btrfs_ok(&["rescue", "clear-uuid-tree", dev]);
    assert!(
        out.contains("Cleared uuid tree"),
        "expected success message, got: {out}"
    );

    let check_output = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check_output.status.success() {
        let stderr = String::from_utf8_lossy(&check_output.stderr);
        let stdout = String::from_utf8_lossy(&check_output.stdout);
        panic!(
            "btrfs check failed:\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        );
    }

    // Mount read-write to verify the subvolumes survived and the
    // free space tree is consistent enough for the kernel to use it
    // for allocation decisions.
    let mnt = Mount::new(lo, td.path());
    let mp = mnt.path();
    assert!(mp.join("sub1").exists(), "sub1 should still exist");
    assert!(mp.join("sub2").exists(), "sub2 should still exist");
    assert!(mp.join("snap1").exists(), "snap1 should still exist");
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_clear_space_cache_v2() {
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    // BLOCK_GROUP_TREE requires FREE_SPACE_TREE, so disable BGT to
    // get a filesystem where clearing FST is actually legal.
    file.mkfs_with_args(&["-O", "^block-group-tree"]);
    let lo = LoopbackDevice::new(file);

    // Mount, write some data so the FST has block-group entries to
    // rebuild later, then unmount.
    let lo = {
        let mnt = Mount::new(lo, td.path());
        let mp = mnt.path();
        std::fs::write(mp.join("hello.txt"), b"hello world").unwrap();
        std::fs::create_dir(mp.join("d")).unwrap();
        std::fs::write(mp.join("d/payload.bin"), vec![0xAB; 256 * 1024])
            .unwrap();
        mnt.into_loopback()
    };

    let dev = lo.path().to_str().unwrap();

    let out = btrfs_ok(&["rescue", "clear-space-cache", "v2", dev]);
    assert!(
        out.contains("cleared free space tree")
            || out.contains("no free space tree"),
        "expected success message, got: {out}"
    );

    let check_output = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check_output.status.success() {
        let stderr = String::from_utf8_lossy(&check_output.stderr);
        let stdout = String::from_utf8_lossy(&check_output.stdout);
        panic!(
            "btrfs check failed:\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        );
    }

    // Mount read-write to force the kernel to rebuild the free
    // space tree, then verify the data we wrote earlier is intact.
    let mnt = Mount::new(lo, td.path());
    let mp = mnt.path();
    assert_eq!(std::fs::read(mp.join("hello.txt")).unwrap(), b"hello world");
    assert_eq!(
        std::fs::read(mp.join("d/payload.bin")).unwrap(),
        vec![0xAB; 256 * 1024]
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_clear_ino_cache_clean_fs() {
    // The inode_cache mount option was removed from the kernel, so
    // there's no way to produce real ino-cache items on a modern
    // system. The best we can do is verify the command runs cleanly
    // on a fresh filesystem (the common case post-deprecation),
    // walks every fs tree without corrupting anything, and reports
    // the no-op message.
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 256 * 1024 * 1024);
    file.mkfs_with_args(&["-O", "^block-group-tree"]);
    let lo = LoopbackDevice::new(file);

    // Add a couple of subvolumes so the walk has more than one fs
    // tree to traverse.
    let lo = {
        let mnt = Mount::new(lo, td.path());
        let mp = mnt.path();
        let sv1 = mp.join("sv1");
        let sv2 = mp.join("sv2");
        Command::new("btrfs")
            .args(["subvolume", "create", sv1.to_str().unwrap()])
            .status()
            .expect("btrfs subvolume create failed");
        Command::new("btrfs")
            .args(["subvolume", "create", sv2.to_str().unwrap()])
            .status()
            .expect("btrfs subvolume create failed");
        std::fs::write(sv1.join("file.bin"), vec![0xAA; 4096]).unwrap();
        mnt.into_loopback()
    };

    let dev = lo.path().to_str().unwrap();
    let out = btrfs_ok(&["rescue", "clear-ino-cache", dev]);
    assert!(
        out.contains("no inode cache items found")
            || out.contains("cleared inode cache"),
        "expected ino-cache clear message, got: {out}"
    );

    let check_output = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check_output.status.success() {
        let stderr = String::from_utf8_lossy(&check_output.stderr);
        let stdout = String::from_utf8_lossy(&check_output.stdout);
        panic!(
            "btrfs check failed:\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        );
    }
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_clear_space_cache_v1() {
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512_000_000);
    // Disable both BGT and FST so the kernel uses the v1 space cache.
    file.mkfs_with_args(&["-O", "^block-group-tree", "-O", "^free-space-tree"]);
    let lo = LoopbackDevice::new(file);

    // Mount with space_cache=v1 and write enough to force the kernel
    // to materialize cache files for at least one block group.
    let lo = {
        let mnt = Mount::with_options(lo, td.path(), &["space_cache=v1"]);
        let mp = mnt.path();
        std::fs::write(mp.join("hello.txt"), b"hello world").unwrap();
        std::fs::create_dir(mp.join("d")).unwrap();
        std::fs::write(mp.join("d/payload.bin"), vec![0xCD; 512 * 1024])
            .unwrap();
        mnt.into_loopback()
    };

    let dev = lo.path().to_str().unwrap();
    let out = btrfs_ok(&["rescue", "clear-space-cache", "v1", dev]);
    assert!(
        out.contains("cleared v1 free space cache")
            || out.contains("no v1 free space cache"),
        "expected v1 clear message, got: {out}"
    );

    let check_output = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check_output.status.success() {
        let stderr = String::from_utf8_lossy(&check_output.stderr);
        let stdout = String::from_utf8_lossy(&check_output.stdout);
        panic!(
            "btrfs check failed:\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        );
    }

    // Re-mount and verify data is intact.
    let mnt = Mount::with_options(lo, td.path(), &["space_cache=v1"]);
    let mp = mnt.path();
    assert_eq!(std::fs::read(mp.join("hello.txt")).unwrap(), b"hello world");
    assert_eq!(
        std::fs::read(mp.join("d/payload.bin")).unwrap(),
        vec![0xCD; 512 * 1024]
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_fix_device_size_shrink() {
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 512 * 1024 * 1024);
    // Disable BGT to keep the filesystem layout simple. The fresh
    // filesystem only allocates chunks in the first ~250 MiB.
    file.mkfs_with_args(&["-O", "^block-group-tree"]);

    // Truncate the backing file in place. After this, dev_item.total_bytes
    // (still 512 MiB) is larger than the actual file size (256 MiB),
    // and no DEV_EXTENT crosses 256 MiB.
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(file.path())
            .unwrap();
        f.set_len(256 * 1024 * 1024).unwrap();
    }

    let dev = file.path().to_str().unwrap();
    let out = btrfs_ok(&["rescue", "fix-device-size", dev]);
    assert!(
        out.contains("devid 1: total_bytes")
            && out.contains("superblock total_bytes"),
        "expected fixup messages, got: {out}"
    );

    let check = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check.status.success() {
        panic!(
            "btrfs check failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&check.stdout),
            String::from_utf8_lossy(&check.stderr)
        );
    }

    // Running it again should be a no-op.
    let out2 = btrfs_ok(&["rescue", "fix-device-size", dev]);
    assert!(
        out2.contains("no device size related problem found"),
        "expected no-op message, got: {out2}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_fix_data_checksum_clean() {
    // Clean filesystem: scan should find no mismatches.
    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 256 * 1024 * 1024);
    file.mkfs_with_args(&["-O", "^block-group-tree"]);
    let lo = LoopbackDevice::new(file);

    let lo = {
        let mnt = Mount::new(lo, td.path());
        let mp = mnt.path();
        std::fs::write(mp.join("a.bin"), vec![0x42u8; 8 * 1024]).unwrap();
        std::fs::write(mp.join("b.bin"), vec![0x99u8; 64 * 1024]).unwrap();
        mnt.into_loopback()
    };

    let dev = lo.path().to_str().unwrap();
    let out = btrfs_ok(&["rescue", "fix-data-checksum", "--readonly", dev]);
    assert!(
        out.contains("no data checksum mismatch found"),
        "expected clean scan, got: {out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn rescue_fix_data_checksum_repair() {
    use btrfs_disk::{
        reader::filesystem_open,
        tree::{KeyType, TreeBlock},
    };
    use std::io::{Seek, SeekFrom, Write};

    let td = tempdir().unwrap();
    let file = BackingFile::new(td.path(), "disk.img", 256 * 1024 * 1024);
    file.mkfs_with_args(&["-O", "^block-group-tree"]);
    let lo = LoopbackDevice::new(file);

    let lo = {
        let mnt = Mount::new(lo, td.path());
        let mp = mnt.path();
        // A few KiB of distinctive data, large enough to land in a
        // regular (non-inline) extent.
        std::fs::write(mp.join("payload.bin"), vec![0xC3u8; 64 * 1024])
            .unwrap();
        mnt.into_loopback()
    };

    // Walk the csum tree (read-only, via disk crate) to find the
    // first EXTENT_CSUM item and resolve its sector to a physical
    // offset on the backing device.
    let dev_path: PathBuf = lo.path().to_path_buf();
    let (corrupt_logical, corrupt_physical) = {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(&dev_path)
            .unwrap();
        let mut open = filesystem_open(f).expect("filesystem_open");
        let csum_root_bytenr = open
            .tree_roots
            .get(&7u64)
            .map(|(b, _)| *b)
            .expect("csum tree root present");

        // DFS until we find a leaf with an EXTENT_CSUM item.
        let mut found: Option<u64> = None;
        let mut stack = vec![csum_root_bytenr];
        while let Some(bytenr) = stack.pop() {
            let block = open
                .reader
                .read_tree_block(bytenr)
                .expect("read tree block");
            match block {
                TreeBlock::Node { ptrs, .. } => {
                    for p in ptrs.into_iter().rev() {
                        stack.push(p.blockptr);
                    }
                }
                TreeBlock::Leaf { items, .. } => {
                    if let Some(item) = items
                        .iter()
                        .find(|i| i.key.key_type == KeyType::ExtentCsum)
                    {
                        found = Some(item.key.offset);
                        break;
                    }
                }
            }
        }
        let logical = found.expect("no EXTENT_CSUM items in csum tree");
        let physical = open
            .reader
            .chunk_cache()
            .resolve(logical)
            .expect("chunk cache should resolve csum'd sector");
        (logical, physical)
    };

    // Overwrite the first 4 KiB at that physical offset with zeros.
    // The original payload (0xC3) won't match the stored csum.
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&dev_path)
            .unwrap();
        f.seek(SeekFrom::Start(corrupt_physical)).unwrap();
        f.write_all(&[0u8; 4096]).unwrap();
        f.sync_all().unwrap();
    }

    let dev = dev_path.to_str().unwrap();

    // Readonly scan should now flag the corrupted sector.
    let scan = btrfs_ok(&["rescue", "fix-data-checksum", "--readonly", dev]);
    let want = format!("logical={corrupt_logical}");
    assert!(
        scan.contains(&want),
        "expected mismatch report for {want}, got: {scan}"
    );

    // Repair via mirror 1.
    let fix = btrfs_ok(&["rescue", "fix-data-checksum", "--mirror", "1", dev]);
    assert!(
        fix.contains("csum item(s) updated"),
        "expected repair message, got: {fix}"
    );

    // After repair, btrfs check should succeed and a re-scan should
    // be clean.
    let check = Command::new("btrfs")
        .args(["check", "--readonly", dev])
        .output()
        .expect("failed to run btrfs check");
    if !check.status.success() {
        panic!(
            "btrfs check failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&check.stdout),
            String::from_utf8_lossy(&check.stderr)
        );
    }
    let rescan = btrfs_ok(&["rescue", "fix-data-checksum", "--readonly", dev]);
    assert!(
        rescan.contains("no data checksum mismatch found"),
        "expected clean rescan after repair, got: {rescan}"
    );
}

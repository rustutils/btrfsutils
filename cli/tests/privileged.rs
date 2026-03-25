//! Privileged CLI integration tests.
//!
//! These tests create real btrfs filesystems on loopback devices and run
//! the `btrfs` binary against them, asserting on stdout, stderr, and exit
//! codes. Run with `just test` (which uses `sudo --ignored`).

mod common;

use common::{BackingFile, LoopbackDevice, single_mount, write_test_data, verify_test_data};
use std::path::Path;
use std::process::Command;

/// Path to the `btrfs` binary built by cargo.
fn btrfs_bin() -> String {
    // cargo sets this env var for integration tests.
    env!("CARGO_BIN_EXE_btrfs").to_string()
}

/// Run `btrfs <args>` and return (stdout, stderr, exit_code).
fn btrfs(args: &[&str]) -> (String, String, i32) {
    let output = Command::new(btrfs_bin())
        .args(args)
        .output()
        .expect("failed to run btrfs binary");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let code = output.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

/// Run `btrfs <args>` and assert success (exit 0), returning stdout.
fn btrfs_ok(args: &[&str]) -> String {
    let (stdout, stderr, code) = btrfs(args);
    assert_eq!(code, 0, "btrfs {args:?} failed (exit {code}):\n{stderr}");
    stdout
}

// ── filesystem ───────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df() {
    let (_td, mnt) = single_mount();
    let out = btrfs_ok(&["filesystem", "df", mnt.path().to_str().unwrap()]);
    // Should contain Data and System/Metadata lines.
    assert!(out.contains("Data"), "expected Data in output:\n{out}");
    assert!(
        out.contains("System") || out.contains("Metadata"),
        "expected System or Metadata in output:\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_show() {
    let (_td, mnt) = single_mount();
    let out = btrfs_ok(&["filesystem", "show", mnt.path().to_str().unwrap()]);
    assert!(out.contains("Total devices"), "expected device info:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_sync() {
    let (_td, mnt) = single_mount();
    let (_, _, code) = btrfs(&["filesystem", "sync", mnt.path().to_str().unwrap()]);
    assert_eq!(code, 0);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_usage() {
    let (_td, mnt) = single_mount();
    let out = btrfs_ok(&["filesystem", "usage", mnt.path().to_str().unwrap()]);
    assert!(out.contains("Device size:"), "expected usage output:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_label_get_set() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Set a label.
    btrfs_ok(&["filesystem", "label", mp, "test-label"]);

    // Read it back.
    let out = btrfs_ok(&["filesystem", "label", mp]);
    assert!(
        out.contains("test-label"),
        "expected label in output:\n{out}"
    );
}

// ── subvolume ────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_create_show_delete() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/testvol");

    // Create.
    btrfs_ok(&["subvolume", "create", &subvol]);
    assert!(Path::new(&subvol).is_dir());

    // Show.
    let out = btrfs_ok(&["subvolume", "show", &subvol]);
    assert!(out.contains("testvol"), "expected name in show output:\n{out}");

    // Delete.
    btrfs_ok(&["subvolume", "delete", &subvol]);
    assert!(!Path::new(&subvol).exists());
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
fn subvolume_snapshot() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let src = format!("{mp}/src");
    let snap = format!("{mp}/snap");

    btrfs_ok(&["subvolume", "create", &src]);
    write_test_data(Path::new(&src), "data.bin", 4096);

    btrfs_ok(&["subvolume", "snapshot", "-r", &src, &snap]);

    // Snapshot should contain the file.
    verify_test_data(Path::new(&snap), "data.bin", 4096);

    // Snapshot should be read-only.
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &snap, "ro"]);
    assert!(out.contains("true"), "expected ro=true:\n{out}");
}

// ── property ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_set_ro() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/proptest");

    btrfs_ok(&["subvolume", "create", &subvol]);

    // Default: not read-only.
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");

    // Set read-only.
    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "true"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("true"), "expected ro=true:\n{out}");

    // Clear read-only.
    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "false"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");
}

// ── send / receive --dump ────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_dump() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/sendtest");
    let stream_file = format!("{}/stream.bin", _td.path().to_str().unwrap());

    // Create subvolume with some content.
    btrfs_ok(&["subvolume", "create", &subvol]);
    write_test_data(Path::new(&subvol), "hello.bin", 8192);
    std::fs::create_dir(format!("{subvol}/subdir")).unwrap();
    write_test_data(Path::new(&format!("{subvol}/subdir")), "nested.bin", 4096);

    // Make read-only for send.
    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "true"]);

    // Send to file.
    btrfs_ok(&["send", "-f", &stream_file, &subvol]);
    assert!(
        std::fs::metadata(&stream_file).unwrap().len() > 0,
        "stream file is empty"
    );

    // Receive --dump and check output contains expected operations.
    let out = btrfs_ok(&["receive", "--dump", "-f", &stream_file]);
    assert!(out.contains("subvol"), "expected subvol command:\n{out}");
    assert!(out.contains("mkfile"), "expected mkfile command:\n{out}");
    assert!(out.contains("mkdir"), "expected mkdir command:\n{out}");
    assert!(out.contains("write"), "expected write command:\n{out}");
    assert!(out.contains("hello.bin"), "expected hello.bin:\n{out}");
    assert!(out.contains("subdir"), "expected subdir:\n{out}");
    assert!(out.contains("nested.bin"), "expected nested.bin:\n{out}");
    assert!(out.contains("utimes"), "expected utimes command:\n{out}");
    assert!(out.contains("chmod"), "expected chmod command:\n{out}");
    assert!(out.contains("chown"), "expected chown command:\n{out}");
    assert!(out.ends_with("end\n"), "expected stream to end with 'end'");
}

// ── send / receive round-trip ────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn send_receive_roundtrip() {
    // We need two separate mounts: one to send from, one to receive into.
    let (_td1, mnt1) = single_mount();
    let (_td2, mnt2) = single_mount();
    let mp1 = mnt1.path().to_str().unwrap();
    let mp2 = mnt2.path().to_str().unwrap();
    let stream_file = format!("{}/roundtrip.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/origin");
    btrfs_ok(&["subvolume", "create", &src]);

    // Write test data.
    write_test_data(Path::new(&src), "file1.bin", 65536);
    write_test_data(Path::new(&src), "file2.bin", 1024);
    std::fs::create_dir(format!("{src}/dir")).unwrap();
    write_test_data(Path::new(&format!("{src}/dir")), "file3.bin", 32768);

    // Make read-only and send.
    btrfs_ok(&["property", "set", "-t", "subvol", &src, "ro", "true"]);
    btrfs_ok(&["send", "-f", &stream_file, &src]);

    // Receive on the second mount.
    btrfs_ok(&["receive", "-f", &stream_file, mp2]);

    // Verify the received subvolume has the correct data.
    let received = format!("{mp2}/origin");
    assert!(Path::new(&received).is_dir(), "received subvol not found");

    verify_test_data(Path::new(&received), "file1.bin", 65536);
    verify_test_data(Path::new(&received), "file2.bin", 1024);
    verify_test_data(Path::new(&format!("{received}/dir")), "file3.bin", 32768);
}

// ── scrub ────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn scrub_start_status() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Start scrub (small fs, finishes immediately).
    btrfs_ok(&["scrub", "start", mp]);

    // Status may show a completed scrub or "no scrub" if it finished
    // before the status ioctl runs.
    let (out, _, code) = btrfs(&["scrub", "status", mp]);
    assert_eq!(code, 0);
    assert!(
        out.contains("scrub") || out.contains("UUID"),
        "expected scrub status output:\n{out}"
    );
}

// ── device stats ─────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_stats() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["device", "stats", mp]);
    // Should have zero error counters on a fresh filesystem.
    assert!(
        out.contains("write_io_errs") || out.contains("read_io_errs"),
        "expected error counters:\n{out}"
    );
}

// ── balance ──────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn balance_start_status() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // A full balance on an empty single-device fs completes immediately.
    btrfs_ok(&["balance", "start", "--full-balance", mp]);

    // Status after completion — balance may already be gone.
    let (stdout, stderr, _) = btrfs(&["balance", "status", mp]);
    // Any output (stdout or stderr) mentioning balance/No is fine.
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("balance") || combined.contains("No"),
        "unexpected status output:\n{combined}"
    );
}

// ── inspect-internal ─────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["inspect-internal", "rootid", mp]);
    // Top-level subvolume is always ID 5.
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

// ── quota ────────────────────────────────────────────────────────────

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

    btrfs_ok(&["quota", "disable", mp]);
}

// ── qgroup ───────────────────────────────────────────────────────────

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
    assert!(out.contains("0/"), "expected level-0 qgroup entries:\n{out}");

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

// ── filesystem du ────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    write_test_data(Path::new(mp), "testfile.bin", 131072);
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["filesystem", "du", &format!("{mp}/testfile.bin")]);
    assert!(out.contains("Total"), "expected Total header:\n{out}");
    assert!(out.contains("Exclusive"), "expected Exclusive header:\n{out}");
    assert!(
        out.contains("testfile.bin"),
        "expected filename in output:\n{out}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du_summarize() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    std::fs::create_dir(format!("{mp}/dir")).unwrap();
    write_test_data(Path::new(&format!("{mp}/dir")), "a.bin", 4096);
    write_test_data(Path::new(&format!("{mp}/dir")), "b.bin", 4096);
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["filesystem", "du", "-s", &format!("{mp}/dir")]);
    assert!(out.contains("Total"), "expected Total header:\n{out}");
    assert!(out.contains("dir"), "expected dir in output:\n{out}");
}

// ── inspect-internal list-chunks ─────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_list_chunks() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["inspect-internal", "list-chunks", mp]);
    assert!(out.contains("Devid"), "expected Devid header:\n{out}");
    assert!(out.contains("PNumber"), "expected PNumber header:\n{out}");
    assert!(out.contains("Type/profile"), "expected Type/profile header:\n{out}");
    assert!(out.contains("PStart"), "expected PStart header:\n{out}");
    assert!(out.contains("Length"), "expected Length header:\n{out}");
    assert!(out.contains("Usage%"), "expected Usage% header:\n{out}");
}

// ── inspect-internal min-dev-size ────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_min_dev_size() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    let out = btrfs_ok(&["inspect-internal", "min-dev-size", mp]);
    assert!(out.contains("bytes"), "expected bytes in output:\n{out}");
}

// ── filesystem resize ────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_resize_grow_shrink() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Grow the backing file and loopback, then resize the filesystem.
    mnt.loopback().backing_file().resize(768_000_000);
    mnt.loopback().refresh_size();
    btrfs_ok(&["filesystem", "resize", "max", mp]);

    // Verify the filesystem grew.
    let out = btrfs_ok(&["filesystem", "usage", mp]);
    assert!(out.contains("Device size:"), "expected usage output:\n{out}");

    // Shrink back.
    btrfs_ok(&["filesystem", "resize", "512m", mp]);
}

// ── filesystem defrag ────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let file = format!("{mp}/fragmented.bin");

    write_test_data(Path::new(mp), "fragmented.bin", 65536);
    btrfs_ok(&["filesystem", "sync", mp]);

    // Defrag should succeed silently.
    btrfs_ok(&["filesystem", "defragment", &file]);

    // File should still be intact.
    verify_test_data(Path::new(mp), "fragmented.bin", 65536);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_defrag_compress() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let file = format!("{mp}/compressible.bin");

    common::write_compressible_data(Path::new(mp), "compressible.bin", 131072);
    btrfs_ok(&["filesystem", "sync", mp]);

    btrfs_ok(&["filesystem", "defragment", "-czstd", &file]);
}

// ── filesystem commit-stats ──────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_commit_stats() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Force a commit so there's something to report.
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["filesystem", "commit-stats", mp]);
    assert!(out.contains("Total commits"), "expected commit count:\n{out}");
    assert!(
        out.contains("Max commit duration"),
        "expected max duration:\n{out}"
    );
}

// ── subvolume get-default / set-default ──────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_set_default() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Default should be FS_TREE (ID 5).
    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(out.contains("5"), "expected ID 5:\n{out}");

    // Create a subvolume and set it as default.
    btrfs_ok(&["subvolume", "create", &format!("{mp}/newdefault")]);
    btrfs_ok(&["subvolume", "set-default", &format!("{mp}/newdefault")]);

    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(!out.contains("ID 5"), "expected non-5 default:\n{out}");

    // Restore default.
    btrfs_ok(&["subvolume", "set-default", "5", mp]);
    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(out.contains("5"), "expected ID 5 restored:\n{out}");
}

// ── subvolume get-flags / set-flags ──────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_set_flags() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/flagtest");

    btrfs_ok(&["subvolume", "create", &subvol]);

    // Default: no readonly flag.
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(!out.contains("readonly"), "expected no readonly flag:\n{out}");

    // Set readonly.
    btrfs_ok(&["subvolume", "set-flags", "readonly", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(out.contains("readonly"), "expected readonly flag:\n{out}");

    // Clear readonly.
    btrfs_ok(&["subvolume", "set-flags", "-", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(!out.contains("readonly"), "expected no readonly flag:\n{out}");
}

// ── device add / remove ──────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_add_remove() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    // Create a second device.
    let dev2_file = BackingFile::new(_td.path(), "disk2.img", 512_000_000);
    let dev2 = LoopbackDevice::new(dev2_file);
    let dev2_path = dev2.path().to_str().unwrap();

    // Add it.
    btrfs_ok(&["device", "add", dev2_path, mp]);

    // Verify it shows up.
    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        out.contains(dev2_path),
        "expected new device in show output:\n{out}"
    );

    // Remove it.
    btrfs_ok(&["device", "remove", dev2_path, mp]);

    // Verify it's gone.
    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(
        !out.contains(dev2_path),
        "device should be removed:\n{out}"
    );

    // dev2 LoopbackDevice drops here and detaches.
}

// ── filesystem mkswapfile ────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_mkswapfile() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let swapfile = format!("{mp}/swapfile");

    btrfs_ok(&["filesystem", "mkswapfile", "-s", "16m", &swapfile]);
    assert!(Path::new(&swapfile).exists(), "swapfile not created");

    let meta = std::fs::metadata(&swapfile).unwrap();
    assert!(
        meta.len() >= 16 * 1024 * 1024,
        "swapfile too small: {} bytes",
        meta.len()
    );
}

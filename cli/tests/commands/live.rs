//! Tests that create and mutate real btrfs filesystems.
//!
//! Some tests use assertion-based checks (mutating commands, round-trips),
//! others use snapshot testing for output verification.

use super::{btrfs, btrfs_ok, common, redact};
use common::{BackingFile, LoopbackDevice, single_mount, write_test_data, verify_test_data};
use std::path::Path;

// ── filesystem (assertions) ──────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_sync() {
    let (_td, mnt) = single_mount();
    let (_, _, code) = btrfs(&["filesystem", "sync", mnt.path().to_str().unwrap()]);
    assert_eq!(code, 0);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_label_get_set() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();

    btrfs_ok(&["filesystem", "label", mp, "test-label"]);

    let out = btrfs_ok(&["filesystem", "label", mp]);
    assert!(out.contains("test-label"), "expected label in output:\n{out}");
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
    assert!(out.contains("Device size:"), "expected usage output:\n{out}");

    btrfs_ok(&["filesystem", "resize", "512m", mp]);
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

    btrfs_ok(&["filesystem", "defragment", "-czstd", &format!("{mp}/compressible.bin")]);
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_commit_stats() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    btrfs_ok(&["filesystem", "sync", mp]);

    let out = btrfs_ok(&["filesystem", "commit-stats", mp]);
    assert!(out.contains("Total commits"), "expected commit count:\n{out}");
    assert!(out.contains("Max commit duration"), "expected max duration:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_mkswapfile() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let swapfile = format!("{mp}/swapfile");

    btrfs_ok(&["filesystem", "mkswapfile", "-s", "16m", &swapfile]);
    assert!(Path::new(&swapfile).exists(), "swapfile not created");

    let meta = std::fs::metadata(&swapfile).unwrap();
    assert!(meta.len() >= 16 * 1024 * 1024, "swapfile too small: {} bytes", meta.len());
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

    let out = btrfs_ok(&["subvolume", "show", &subvol]);
    assert!(out.contains("testvol"), "expected name in show output:\n{out}");

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

    btrfs_ok(&["subvolume", "set-default", "5", mp]);
    let out = btrfs_ok(&["subvolume", "get-default", mp]);
    assert!(out.contains("5"), "expected ID 5 restored:\n{out}");
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_set_flags() {
    let (_td, mnt) = single_mount();
    let mp = mnt.path().to_str().unwrap();
    let subvol = format!("{mp}/flagtest");

    btrfs_ok(&["subvolume", "create", &subvol]);

    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(!out.contains("readonly"), "expected no readonly flag:\n{out}");

    btrfs_ok(&["subvolume", "set-flags", "readonly", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(out.contains("readonly"), "expected readonly flag:\n{out}");

    btrfs_ok(&["subvolume", "set-flags", "-", &subvol]);
    let out = btrfs_ok(&["subvolume", "get-flags", &subvol]);
    assert!(!out.contains("readonly"), "expected no readonly flag:\n{out}");
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

    btrfs_ok(&["property", "set", "-t", "subvol", &subvol, "ro", "false"]);
    let out = btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]);
    assert!(out.contains("false"), "expected ro=false:\n{out}");
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

    let re_uuid = regex_lite::Regex::new(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    ).unwrap();
    let re_offset = regex_lite::Regex::new(r"offset=\d+").unwrap();
    let re_len = regex_lite::Regex::new(r"len=\d+").unwrap();
    let re_mode = regex_lite::Regex::new(r"mode=\d+").unwrap();
    let re_uid = regex_lite::Regex::new(r"uid=\d+").unwrap();
    let re_gid = regex_lite::Regex::new(r"gid=\d+").unwrap();

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
    let stream_file = format!("{}/roundtrip.bin", _td1.path().to_str().unwrap());

    let src = format!("{mp1}/origin");
    btrfs_ok(&["subvolume", "create", &src]);

    write_test_data(Path::new(&src), "file1.bin", 65536);
    write_test_data(Path::new(&src), "file2.bin", 1024);
    std::fs::create_dir(format!("{src}/dir")).unwrap();
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
    assert!(out.contains(dev2_path), "expected new device in show output:\n{out}");

    btrfs_ok(&["device", "remove", dev2_path, mp]);

    let out = btrfs_ok(&["filesystem", "show", mp]);
    assert!(!out.contains(dev2_path), "device should be removed:\n{out}");
}

// ── device scan / ready ──────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_scan() {
    let (_td, mnt) = single_mount();
    let dev = mnt.loopback().path().to_str().unwrap();

    let out = btrfs_ok(&["device", "scan", dev]);
    assert!(out.contains("registered"), "expected registered message:\n{out}");
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

    btrfs_ok(&["quota", "disable", mp]);
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

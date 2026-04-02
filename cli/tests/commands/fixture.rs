//! Read-only snapshot tests against the pre-built filesystem image.
//!
//! The image at `commands/fixture.img.gz` has a fixed UUID, label,
//! subvolumes, and file content, so command output is fully deterministic.
//! Only mount/device paths need redaction.
//!
//! Image contents:
//!   - UUID: deadbeef-dead-beef-dead-beefdeadbeef
//!   - Label: test-fixture
//!   - /toplevel.txt ("top level file")
//!   - /btrfs-progrs/ (copy of the project source)
//!   - subvol1/ (subvolume with hello.txt, nested/deep.txt)
//!   - snap1/ (read-only snapshot of subvol1)
//!   - subvol2/ (subvolume with zeros.bin)

use super::{
    btrfs_ok,
    common::{cached_broken_image, cached_fixture_image, fixture_mount},
    redact_paths,
};

// ── basic CLI (no filesystem needed) ─────────────────────────────────

#[test]
fn version_flag() {
    let out = btrfs_ok(&["--version"]);
    assert!(out.contains("btrfs-cli"), "--version output: {out}");
}

#[test]
fn help_flag() {
    let out = btrfs_ok(&["--help"]);
    assert!(out.contains("Usage:"), "--help output: {out}");
}

#[test]
fn no_args_returns_error() {
    let (_stdout, _stderr, code) = super::btrfs(&[]);
    assert_ne!(code, 0, "btrfs with no args should fail");
}

#[test]
fn dry_run_unsupported_command_returns_error() {
    let (_stdout, stderr, code) =
        super::btrfs(&["--dry-run", "filesystem", "df", "/"]);
    assert_ne!(code, 0, "--dry-run should fail for unsupported commands");
    assert!(
        stderr.contains("--dry-run"),
        "error should mention --dry-run: {stderr}"
    );
}

#[test]
fn json_unsupported_command_returns_error() {
    let (_stdout, stderr, code) =
        super::btrfs(&["--format", "json", "balance", "status", "/"]);
    assert_ne!(
        code, 0,
        "--format json should fail for unsupported commands"
    );
    assert!(
        stderr.contains("--format"),
        "error should mention --format: {stderr}"
    );
}

#[test]
fn subcommand_help_flag() {
    // --help on a subcommand should print usage for that subcommand.
    let out = btrfs_ok(&["filesystem", "df", "--help"]);
    assert!(
        out.contains("filesystem df"),
        "subcommand --help output: {out}"
    );
}

#[test]
fn invalid_subcommand_fails() {
    let (_, _, code) = super::btrfs(&["nonexistent-command"]);
    assert_ne!(code, 0, "invalid subcommand should fail");
}

// ── filesystem ───────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs filesystem df <MOUNT>",
        redact_paths(&btrfs_ok(&["filesystem", "df", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_show() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs filesystem show <MOUNT>",
        redact_paths(&btrfs_ok(&["filesystem", "show", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_usage() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs filesystem usage <MOUNT>",
        redact_paths(&btrfs_ok(&["filesystem", "usage", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_label() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs filesystem label <MOUNT>",
        btrfs_ok(&["filesystem", "label", mp])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs filesystem du <MOUNT>/subvol1",
        redact_paths(&btrfs_ok(&["filesystem", "du", &subvol]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du_summarize() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs filesystem du -s <MOUNT>/subvol1",
        redact_paths(&btrfs_ok(&["filesystem", "du", "-s", &subvol]), &mnt)
    );
}

// ── subvolume ────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume list <MOUNT>",
        redact_paths(&btrfs_ok(&["subvolume", "list", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume show <MOUNT>/subvol1",
        redact_paths(&btrfs_ok(&["subvolume", "show", &subvol]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show_snapshot() {
    let (_td, mnt) = fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume show <MOUNT>/snap1",
        redact_paths(&btrfs_ok(&["subvolume", "show", &snap1]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_default() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume get-default <MOUNT>",
        btrfs_ok(&["subvolume", "get-default", mp])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_flags_readonly() {
    let (_td, mnt) = fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume get-flags <MOUNT>/snap1",
        btrfs_ok(&["subvolume", "get-flags", &snap1])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_flags_writable() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume get-flags <MOUNT>/subvol1",
        btrfs_ok(&["subvolume", "get-flags", &subvol])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_table() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume list -t <MOUNT>",
        redact_paths(&btrfs_ok(&["subvolume", "list", "-t", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_only_below() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume list -o <MOUNT>",
        redact_paths(&btrfs_ok(&["subvolume", "list", "-o", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_sort_path() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume list --sort=path <MOUNT>",
        redact_paths(
            &btrfs_ok(&["subvolume", "list", "--sort=path", mp]),
            &mnt,
        )
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_list_sort_rootid_desc() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs subvolume list --sort=-rootid <MOUNT>",
        redact_paths(
            &btrfs_ok(&["subvolume", "list", "--sort=-rootid", mp]),
            &mnt,
        )
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show_by_rootid() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // subvol1 is ID 256.
    snap!(
        "btrfs subvolume show -r 256 <MOUNT>",
        redact_paths(&btrfs_ok(&["subvolume", "show", "-r", "256", mp]), &mnt,)
    );
}

// ── device ───────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_stats() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs device stats <MOUNT>",
        redact_paths(&btrfs_ok(&["device", "stats", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_usage() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs device usage <MOUNT>",
        redact_paths(&btrfs_ok(&["device", "usage", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_usage_raw() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs device usage --raw <MOUNT>",
        redact_paths(&btrfs_ok(&["device", "usage", "--raw", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_usage_kbytes() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs device usage --kbytes <MOUNT>",
        redact_paths(&btrfs_ok(&["device", "usage", "--kbytes", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn device_usage_gbytes() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs device usage --gbytes <MOUNT>",
        redact_paths(&btrfs_ok(&["device", "usage", "--gbytes", mp]), &mnt)
    );
}

// ── inspect-internal ─────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal rootid <MOUNT>",
        btrfs_ok(&["inspect-internal", "rootid", mp])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid_subvol() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs inspect-internal rootid <MOUNT>/subvol1",
        btrfs_ok(&["inspect-internal", "rootid", &subvol])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_list_chunks() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal list-chunks <MOUNT>",
        redact_paths(&btrfs_ok(&["inspect-internal", "list-chunks", mp]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_min_dev_size() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal min-dev-size <MOUNT>",
        btrfs_ok(&["inspect-internal", "min-dev-size", mp])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_dump_super() {
    let (_td, mnt) = fixture_mount();
    let dev = mnt.loopback().path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-super <DEV>",
        redact_paths(&btrfs_ok(&["inspect-internal", "dump-super", dev]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_dump_super_full() {
    let (_td, mnt) = fixture_mount();
    let dev = mnt.loopback().path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-super -f <DEV>",
        redact_paths(
            &btrfs_ok(&["inspect-internal", "dump-super", "-f", dev]),
            &mnt,
        )
    );
}

// ── inspect-internal (inode/subvolid resolve) ────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_inode_resolve() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // Inode 257 (BTRFS_FIRST_FREE_OBJECTID) is the first user inode — should
    // resolve to a file in the top-level subvolume.
    let out = btrfs_ok(&["inspect-internal", "inode-resolve", "257", mp]);
    assert!(!out.is_empty(), "expected at least one path for inode 257");
    snap!(
        "btrfs inspect-internal inode-resolve 257 <MOUNT>",
        redact_paths(&out, &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_subvolid_resolve() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // subvol1 was the first subvolume created, should be ID 256.
    let out = btrfs_ok(&["inspect-internal", "subvolid-resolve", "256", mp]);
    snap!(
        "btrfs inspect-internal subvolid-resolve 256 <MOUNT>",
        redact_paths(&out, &mnt)
    );
}

// ── property ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_ro_snapshot() {
    let (_td, mnt) = fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property get -t subvol <MOUNT>/snap1 ro",
        btrfs_ok(&["property", "get", "-t", "subvol", &snap1, "ro"])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_ro_writable() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property get -t subvol <MOUNT>/subvol1 ro",
        btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_label() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs property get -t filesystem <MOUNT> label",
        btrfs_ok(&["property", "get", "-t", "filesystem", mp, "label"])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_compression_file() {
    let (_td, mnt) = fixture_mount();
    let file = format!("{}/toplevel.txt", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property get <MOUNT>/toplevel.txt compression",
        btrfs_ok(&["property", "get", &file, "compression"])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_all_inode() {
    let (_td, mnt) = fixture_mount();
    let file = format!("{}/toplevel.txt", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property get <MOUNT>/toplevel.txt (all)",
        btrfs_ok(&["property", "get", &file])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_list_subvol() {
    let (_td, mnt) = fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property list -t subvol <MOUNT>/subvol1",
        btrfs_ok(&["property", "list", "-t", "subvol", &subvol])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_list_inode() {
    let (_td, mnt) = fixture_mount();
    let file = format!("{}/toplevel.txt", mnt.path().to_str().unwrap());
    snap!(
        "btrfs property list <MOUNT>/toplevel.txt",
        btrfs_ok(&["property", "list", &file])
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_list_filesystem() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!(
        "btrfs property list -t filesystem <MOUNT>",
        btrfs_ok(&["property", "list", "-t", "filesystem", mp])
    );
}

// ── inspect-internal dump-tree (no privileges needed) ────────────────

#[test]
fn inspect_dump_tree_roots() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-tree --roots <IMG>",
        btrfs_ok(&["inspect-internal", "dump-tree", "--roots", img_str])
    );
}

#[test]
fn inspect_dump_tree_root_tree() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-tree -t root <IMG>",
        btrfs_ok(&["inspect-internal", "dump-tree", "-t", "root", img_str])
    );
}

#[test]
fn inspect_dump_tree_chunk_tree() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-tree -t chunk <IMG>",
        btrfs_ok(&["inspect-internal", "dump-tree", "-t", "chunk", img_str])
    );
}

#[test]
fn inspect_dump_tree() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-tree <IMG>",
        btrfs_ok(&["inspect-internal", "dump-tree", img_str])
    );
}

// ── inspect-internal tree-stats (no privileges needed) ──────────────

fn redact_timing(output: &str) -> String {
    let re = regex_lite::Regex::new(r"Total read time: \d+ s \d+ us").unwrap();
    re.replace_all(output, "Total read time: <REDACTED>")
        .into_owned()
}

#[test]
fn inspect_tree_stats() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal tree-stats -b <IMG>",
        redact_timing(&btrfs_ok(&[
            "inspect-internal",
            "tree-stats",
            "-b",
            img_str
        ]))
    );
}

#[test]
fn inspect_tree_stats_single_tree() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs inspect-internal tree-stats -b -t fs <IMG>",
        redact_timing(&btrfs_ok(&[
            "inspect-internal",
            "tree-stats",
            "-b",
            "-t",
            "fs",
            img_str
        ]))
    );
}

// ── device stats --offline (no privileges needed) ───────────────────

#[test]
fn device_stats_offline_clean_image() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let out = btrfs_ok(&["device", "stats", "--offline", img_str]);

    // A clean image should have all zero counters.
    for line in out.lines() {
        let val: u64 = line
            .rsplit_once(char::is_whitespace)
            .expect("expected key-value line")
            .1
            .trim()
            .parse()
            .expect("expected numeric value");
        assert_eq!(val, 0, "expected zero counter in: {line}");
    }

    // Should have 5 counter lines (one device).
    assert_eq!(out.lines().count(), 5, "expected 5 counter lines");
}

#[test]
fn device_stats_offline_check_clean_succeeds() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    // --check should succeed (exit 0) on a clean image.
    btrfs_ok(&["device", "stats", "--offline", "--check", img_str]);
}

#[test]
fn device_stats_offline_json() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let out = btrfs_ok(&[
        "device",
        "stats",
        "--offline",
        "--format",
        "json",
        img_str,
    ]);
    let parsed: serde_json::Value =
        serde_json::from_str(&out).expect("output should be valid JSON");
    assert_eq!(parsed["__header"]["version"], "1");
    let arr = parsed["device-stats"]
        .as_array()
        .expect("expected device-stats array");
    assert_eq!(arr.len(), 1, "expected one device");
    let dev = &arr[0];
    assert_eq!(dev["write_io_errs"], 0);
    assert_eq!(dev["read_io_errs"], 0);
    assert_eq!(dev["flush_io_errs"], 0);
    assert_eq!(dev["corruption_errs"], 0);
    assert_eq!(dev["generation_errs"], 0);
    assert!(dev["devid"].as_u64().is_some(), "expected devid field");
}

#[test]
fn device_stats_offline_tabular() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let out = btrfs_ok(&["device", "stats", "--offline", "-T", img_str]);
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() >= 3, "expected header + separator + data row: {out}");
    assert!(lines[0].contains("Id"), "header should contain Id: {out}");
    assert!(
        lines[0].contains("Write errors"),
        "header should contain Write errors: {out}"
    );
    assert!(
        lines[1].chars().all(|c| c == '-' || c == ' '),
        "second line should be separator: {out}"
    );
}

#[test]
fn device_stats_offline_modern() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let out = btrfs_ok(&[
        "device", "stats", "--offline", "--format", "modern", img_str,
    ]);
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() >= 2, "expected header + data row: {out}");
    // Modern uses cols with compact headers.
    assert!(lines[0].contains("ID"), "header should contain ID: {out}");
}

// ── restore (no privileges needed — reads raw image) ─────────────────

#[test]
fn restore_list_roots() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    snap!(
        "btrfs restore --list-roots <IMG>",
        btrfs_ok(&["restore", "--list-roots", img_str])
    );
}

#[test]
fn restore_dry_run() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let out_str = tmp.path().to_str().unwrap();
    let output = btrfs_ok(&["restore", "-D", "-S", img_str, out_str]);
    // Redact the temp path for determinism.
    let redacted = output.replace(out_str, "<OUTPUT>");
    snap!("btrfs restore -D -S <IMG> <OUTPUT>", redacted);
}

#[test]
fn restore_toplevel_file() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let out_str = tmp.path().to_str().unwrap();
    btrfs_ok(&["restore", "-S", img_str, out_str]);

    // Verify toplevel.txt was restored with correct content.
    let content =
        std::fs::read_to_string(tmp.path().join("toplevel.txt")).unwrap();
    assert_eq!(content.trim(), "top level file");
}

#[test]
fn restore_subvolume_files() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let out_str = tmp.path().to_str().unwrap();
    btrfs_ok(&["restore", "-S", img_str, out_str]);

    // The default root (FS tree objectid 5) contains the top-level
    // subvolume. Subvolume contents live in separate trees and are
    // NOT restored unless --root points to them.
    assert!(tmp.path().join("toplevel.txt").exists());
}

// ── check (no privileges needed — reads raw image) ───────────────────

/// Replace the fixture image path with `<IMG>` in output strings.
fn redact_img(output: &str, img: &std::path::Path) -> String {
    output.replace(img.to_str().unwrap(), "<IMG>")
}

#[test]
fn check_clean_image() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (stdout, stderr, code) = super::btrfs(&["check", img_str]);
    assert_eq!(code, 0, "btrfs check on clean image failed:\n{stderr}");
    snap!("btrfs check <IMG> (stdout)", stdout);
    snap!("btrfs check <IMG> (stderr)", redact_img(&stderr, &img));
}

#[test]
fn check_with_data_csum() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (stdout, stderr, code) =
        super::btrfs(&["check", "--check-data-csum", img_str]);
    assert_eq!(
        code, 0,
        "btrfs check --check-data-csum on clean image failed:\n{stderr}"
    );
    snap!("btrfs check --check-data-csum <IMG> (stdout)", stdout);
    snap!(
        "btrfs check --check-data-csum <IMG> (stderr)",
        redact_img(&stderr, &img)
    );
}

#[test]
fn check_with_super_mirror() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (stdout, stderr, code) =
        super::btrfs(&["check", "--super", "0", img_str]);
    assert_eq!(
        code, 0,
        "btrfs check --super 0 on clean image failed:\n{stderr}"
    );
    snap!("btrfs check --super 0 <IMG> (stdout)", stdout);
    snap!(
        "btrfs check --super 0 <IMG> (stderr)",
        redact_img(&stderr, &img)
    );
}

#[test]
fn check_invalid_super_mirror() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (_stdout, stderr, code) =
        super::btrfs(&["check", "--super", "5", img_str]);
    assert_ne!(code, 0, "expected failure for invalid super mirror index");
    assert!(
        stderr.contains("out of range"),
        "expected 'out of range' in stderr: {stderr}"
    );
}

#[test]
fn check_nonexistent_device() {
    let (_stdout, _stderr, code) =
        super::btrfs(&["check", "/nonexistent/device"]);
    assert_ne!(code, 0, "expected failure for nonexistent device");
}

#[test]
fn check_unsupported_repair() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (_stdout, stderr, code) = super::btrfs(&["check", "--repair", img_str]);
    assert_ne!(code, 0, "expected failure for --repair");
    assert!(
        stderr.contains("not yet supported"),
        "expected 'not yet supported' in stderr: {stderr}"
    );
}

#[test]
fn check_readonly_flag() {
    let img = cached_fixture_image();
    let img_str = img.to_str().unwrap();
    let (stdout, stderr, code) =
        super::btrfs(&["check", "--readonly", img_str]);
    assert_eq!(
        code, 0,
        "btrfs check --readonly on clean image failed:\n{stderr}"
    );
    // --readonly is the default, so output should match the basic check.
    assert!(
        stdout.contains("no error found"),
        "expected 'no error found' in stdout: {stdout}"
    );
}

// ── check on broken image ───────────────────────────────────────────

#[test]
fn check_broken_image() {
    let img = cached_broken_image();
    let img_str = img.to_str().unwrap();
    let (stdout, stderr, code) = super::btrfs(&["check", img_str]);
    assert_ne!(code, 0, "btrfs check on broken image should fail");
    snap!("btrfs check <BROKEN_IMG> (stdout)", stdout);
    snap!(
        "btrfs check <BROKEN_IMG> (stderr)",
        redact_img(&stderr, &img)
    );
}

// ── quota ────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn quota_status_disabled() {
    let (_td, mnt) = fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // The fixture image does not have quotas enabled, so status should
    // report disabled.
    let (stdout, _stderr, _code) = super::btrfs(&["quota", "status", mp]);
    snap!(
        "btrfs quota status <MOUNT> (disabled)",
        redact_paths(&stdout, &mnt)
    );
}

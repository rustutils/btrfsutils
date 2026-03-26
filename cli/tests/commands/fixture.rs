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

use super::{btrfs_ok, common, redact_paths};

// ── filesystem ───────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_df() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs filesystem df <MOUNT>", redact_paths(&btrfs_ok(&["filesystem", "df", mp]), &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_show() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs filesystem show <MOUNT>", redact_paths(&btrfs_ok(&["filesystem", "show", mp]), &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_usage() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs filesystem usage <MOUNT>", redact_paths(&btrfs_ok(&["filesystem", "usage", mp]), &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_label() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs filesystem label <MOUNT>", btrfs_ok(&["filesystem", "label", mp]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs filesystem du <MOUNT>/subvol1",
        redact_paths(&btrfs_ok(&["filesystem", "du", &subvol]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn filesystem_du_summarize() {
    let (_td, mnt) = common::fixture_mount();
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
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs subvolume list <MOUNT>", redact_paths(&btrfs_ok(&["subvolume", "list", mp]), &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume show <MOUNT>/subvol1",
        redact_paths(&btrfs_ok(&["subvolume", "show", &subvol]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_show_snapshot() {
    let (_td, mnt) = common::fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!(
        "btrfs subvolume show <MOUNT>/snap1",
        redact_paths(&btrfs_ok(&["subvolume", "show", &snap1]), &mnt)
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_default() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs subvolume get-default <MOUNT>", btrfs_ok(&["subvolume", "get-default", mp]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_flags_readonly() {
    let (_td, mnt) = common::fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!("btrfs subvolume get-flags <MOUNT>/snap1", btrfs_ok(&["subvolume", "get-flags", &snap1]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn subvolume_get_flags_writable() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!("btrfs subvolume get-flags <MOUNT>/subvol1", btrfs_ok(&["subvolume", "get-flags", &subvol]));
}

// ── device ───────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn device_stats() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs device stats <MOUNT>", redact_paths(&btrfs_ok(&["device", "stats", mp]), &mnt));
}

// ── inspect-internal ─────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs inspect-internal rootid <MOUNT>", btrfs_ok(&["inspect-internal", "rootid", mp]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_rootid_subvol() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!("btrfs inspect-internal rootid <MOUNT>/subvol1", btrfs_ok(&["inspect-internal", "rootid", &subvol]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_list_chunks() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs inspect-internal list-chunks <MOUNT>", redact_paths(&btrfs_ok(&["inspect-internal", "list-chunks", mp]), &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_min_dev_size() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    snap!("btrfs inspect-internal min-dev-size <MOUNT>", btrfs_ok(&["inspect-internal", "min-dev-size", mp]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_dump_super() {
    let (_td, mnt) = common::fixture_mount();
    let dev = mnt.loopback().path().to_str().unwrap();
    snap!(
        "btrfs inspect-internal dump-super <DEV>",
        redact_paths(&btrfs_ok(&["inspect-internal", "dump-super", dev]), &mnt)
    );
}

// ── inspect-internal (inode/subvolid resolve) ────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_inode_resolve() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // Inode 257 (BTRFS_FIRST_FREE_OBJECTID) is the first user inode — should
    // resolve to a file in the top-level subvolume.
    let out = btrfs_ok(&["inspect-internal", "inode-resolve", "257", mp]);
    assert!(!out.is_empty(), "expected at least one path for inode 257");
    snap!("btrfs inspect-internal inode-resolve 257 <MOUNT>", redact_paths(&out, &mnt));
}

#[test]
#[ignore = "requires elevated privileges"]
fn inspect_subvolid_resolve() {
    let (_td, mnt) = common::fixture_mount();
    let mp = mnt.path().to_str().unwrap();
    // subvol1 was the first subvolume created, should be ID 256.
    let out = btrfs_ok(&["inspect-internal", "subvolid-resolve", "256", mp]);
    snap!("btrfs inspect-internal subvolid-resolve 256 <MOUNT>", redact_paths(&out, &mnt));
}

// ── property ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_ro_snapshot() {
    let (_td, mnt) = common::fixture_mount();
    let snap1 = format!("{}/snap1", mnt.path().to_str().unwrap());
    snap!("btrfs property get -t subvol <MOUNT>/snap1 ro", btrfs_ok(&["property", "get", "-t", "subvol", &snap1, "ro"]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_get_ro_writable() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!("btrfs property get -t subvol <MOUNT>/subvol1 ro", btrfs_ok(&["property", "get", "-t", "subvol", &subvol, "ro"]));
}

#[test]
#[ignore = "requires elevated privileges"]
fn property_list_subvol() {
    let (_td, mnt) = common::fixture_mount();
    let subvol = format!("{}/subvol1", mnt.path().to_str().unwrap());
    snap!("btrfs property list -t subvol <MOUNT>/subvol1", btrfs_ok(&["property", "list", "-t", "subvol", &subvol]));
}

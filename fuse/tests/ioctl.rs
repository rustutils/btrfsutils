//! End-to-end ioctl tests against a mounted FUSE driver.
//!
//! Each test mounts the fixture image, opens the mountpoint, issues
//! a btrfs ioctl via raw `libc::ioctl`, and asserts the response
//! parses the way it should. Verifies the FUSE_IOCTL plumbing
//! end-to-end: kernel → fuser → BtrfsFuse → ioctl::dispatch →
//! Filesystem → response back through the same path.
//!
//! F6.1 covers `BTRFS_IOC_FS_INFO`, `BTRFS_IOC_GET_FEATURES`, and
//! `BTRFS_IOC_GET_SUBVOL_INFO`. F6.2 will add the variable-size
//! ones (`TREE_SEARCH_V2` etc.).

mod common;

use bytes::Buf;
use common::MountedFuse;
use std::{fs::File, os::fd::AsRawFd};

// ── ioctl number encoding (kept in sync with src/ioctl.rs) ────────

const fn ioc_ior(magic: u8, nr: u8, size: u32) -> u32 {
    (2u32 << 30) | ((magic as u32) << 8) | (nr as u32) | (size << 16)
}

const fn ioc_iowr(magic: u8, nr: u8, size: u32) -> u32 {
    (3u32 << 30) | ((magic as u32) << 8) | (nr as u32) | (size << 16)
}

const BTRFS_IOC_FS_INFO: u32 = ioc_ior(0x94, 31, 1024);
const BTRFS_IOC_GET_FEATURES: u32 = ioc_ior(0x94, 57, 24);
const BTRFS_IOC_GET_SUBVOL_INFO: u32 = ioc_ior(0x94, 60, 504);
const BTRFS_IOC_DEV_INFO: u32 = ioc_iowr(0x94, 30, 4096);
const BTRFS_IOC_INO_LOOKUP: u32 = ioc_iowr(0x94, 18, 4096);
const BTRFS_IOC_TREE_SEARCH: u32 = ioc_iowr(0x94, 17, 4096);
const BTRFS_IOC_GET_SUBVOL_ROOTREF: u32 = ioc_iowr(0x94, 61, 4096);

/// Wrapper around `libc::ioctl` for the read-only ioctls in F6.1.
/// Returns the response bytes (length matches the ioctl's encoded
/// size) on success.
unsafe fn run_read_ioctl<P: AsRef<std::path::Path>>(
    path: P,
    cmd: u32,
    out_size: usize,
) -> std::io::Result<Vec<u8>> {
    let f = File::open(path)?;
    let mut buf = vec![0u8; out_size];
    // SAFETY: `cmd` is a valid btrfs ioctl number; `buf` has the
    // exact size encoded in `cmd`; FUSE forwards the buffer through
    // unchanged for unrestricted ioctls.
    let rc = unsafe {
        libc::ioctl(
            f.as_raw_fd(),
            cmd as libc::c_ulong,
            buf.as_mut_ptr() as *mut libc::c_void,
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(buf)
}

// ── BTRFS_IOC_FS_INFO ─────────────────────────────────────────────

#[test]
fn ioctl_fs_info_returns_superblock_geometry() {
    let m = MountedFuse::mount();
    let buf = unsafe { run_read_ioctl(m.path(), BTRFS_IOC_FS_INFO, 1024) }
        .expect("FS_INFO ioctl");
    let mut cursor = &buf[..];

    let max_id = cursor.get_u64_le();
    let num_devices = cursor.get_u64_le();
    let mut fsid = [0u8; 16];
    cursor.copy_to_slice(&mut fsid);
    let nodesize = cursor.get_u32_le();
    let sectorsize = cursor.get_u32_le();
    let clone_alignment = cursor.get_u32_le();

    assert_eq!(num_devices, 1, "fixture is single-device");
    assert_eq!(max_id, 1);
    assert_eq!(sectorsize, 4096);
    assert_eq!(clone_alignment, 4096);
    // Default mkfs.btrfs nodesize for our fixture sizes is 16 KiB.
    assert_eq!(nodesize, 16_384);
    assert_ne!(fsid, [0u8; 16], "fsid should be populated");
}

// ── BTRFS_IOC_GET_FEATURES ────────────────────────────────────────

#[test]
fn ioctl_get_features_returns_three_flag_words() {
    let m = MountedFuse::mount();
    let buf = unsafe { run_read_ioctl(m.path(), BTRFS_IOC_GET_FEATURES, 24) }
        .expect("GET_FEATURES ioctl");
    let mut cursor = &buf[..];
    let compat = cursor.get_u64_le();
    let compat_ro = cursor.get_u64_le();
    let incompat = cursor.get_u64_le();

    // mkfs.btrfs by default sets several incompat flags:
    //   MIXED_BACKREF (0x1), EXTENDED_IREF (0x40), SKINNY_METADATA (0x100),
    //   NO_HOLES (0x200) — at minimum.
    assert_ne!(incompat, 0, "expected non-zero incompat flags");
    assert_eq!(compat, 0, "no compat flags set by default mkfs");
    let _ = compat_ro; // can be set (FREE_SPACE_TREE etc.) — don't pin
}

// ── BTRFS_IOC_GET_SUBVOL_INFO ─────────────────────────────────────

#[test]
fn ioctl_get_subvol_info_for_default_subvol() {
    let m = MountedFuse::mount();
    let buf =
        unsafe { run_read_ioctl(m.path(), BTRFS_IOC_GET_SUBVOL_INFO, 504) }
            .expect("GET_SUBVOL_INFO ioctl");
    let mut cursor = &buf[..];

    let treeid = cursor.get_u64_le();
    let mut name = [0u8; 256];
    cursor.copy_to_slice(&mut name);
    let parent_id = cursor.get_u64_le();
    let dirid = cursor.get_u64_le();
    let generation = cursor.get_u64_le();
    let flags = cursor.get_u64_le();

    assert_eq!(treeid, 5, "default subvol id is 5 (FS_TREE)");
    assert_eq!(parent_id, 0, "FS_TREE has no parent");
    assert_eq!(dirid, 0, "FS_TREE has no parent dirid");
    assert_eq!(name, [0u8; 256], "FS_TREE has no name");
    assert!(generation >= 1, "generation should be set");
    assert_eq!(flags & 1, 0, "default subvol is not read-only");
}

// ── BTRFS_IOC_TREE_SEARCH ─────────────────────────────────────────

/// `BTRFS_IOC_TREE_SEARCH` (v1, 4096-byte fixed): walk the root
/// tree (id 1) for `ROOT_ITEM` keys and check that we get back the
/// expected number of subvolumes from a multi-subvol fixture
/// (default FS_TREE id 5 + 3 user subvols).
#[test]
fn ioctl_tree_search_root_items_in_root_tree() {
    let m = MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &[],
        "at_root.txt",
    );

    // Build a 4096-byte input: search_key (104) + buf (3992 zeros).
    // tree_id = 1 (BTRFS_ROOT_TREE_OBJECTID),
    // type range = ROOT_ITEM_KEY (132) only,
    // objectid range = 0..u64::MAX, offset range = 0..u64::MAX,
    // transid range = 0..u64::MAX, nr_items = 16.
    let mut input = vec![0u8; 4096];
    let key = &mut input[..104];
    key[0..8].copy_from_slice(&1u64.to_le_bytes()); // tree_id
    key[8..16].copy_from_slice(&0u64.to_le_bytes()); // min_objectid
    key[16..24].copy_from_slice(&u64::MAX.to_le_bytes()); // max_objectid
    key[24..32].copy_from_slice(&0u64.to_le_bytes()); // min_offset
    key[32..40].copy_from_slice(&u64::MAX.to_le_bytes()); // max_offset
    key[40..48].copy_from_slice(&0u64.to_le_bytes()); // min_transid
    key[48..56].copy_from_slice(&u64::MAX.to_le_bytes()); // max_transid
    key[56..60].copy_from_slice(&132u32.to_le_bytes()); // min_type ROOT_ITEM
    key[60..64].copy_from_slice(&132u32.to_le_bytes()); // max_type ROOT_ITEM
    key[64..68].copy_from_slice(&16u32.to_le_bytes()); // nr_items

    let buf = unsafe { run_iowr_ioctl(m.path(), BTRFS_IOC_TREE_SEARCH, input) }
        .expect("TREE_SEARCH ioctl");

    // Read nr_items from the response key (offset 64..68).
    let nr_items = u32::from_le_bytes(buf[64..68].try_into().unwrap()) as usize;
    // Default FS_TREE (5) + 3 user subvolumes (sub plus internal
    // tombstones for system trees can also produce ROOT_ITEM
    // entries depending on mkfs). Just assert ≥ 2 — the default
    // and at least the `sub` subvol should both appear.
    assert!(
        nr_items >= 2,
        "expected ≥ 2 ROOT_ITEM entries, got {nr_items}",
    );

    // Parse the items: each is 32-byte header + payload. The kernel
    // (and our impl) treats `(objectid, type, offset)` as a single
    // compound key — items whose type falls outside the range CAN
    // be returned when their compound key is between min and max.
    // Callers filter by type themselves; the test does too.
    let mut cursor = 104; // key prefix is 104 bytes; buf starts here for v1
    let mut found_fs_tree_root_item = false;
    for _ in 0..nr_items {
        if cursor + 32 > buf.len() {
            break;
        }
        let objectid = u64::from_le_bytes(
            buf[cursor + 8..cursor + 16].try_into().unwrap(),
        );
        let item_type = u32::from_le_bytes(
            buf[cursor + 24..cursor + 28].try_into().unwrap(),
        );
        let len = u32::from_le_bytes(
            buf[cursor + 28..cursor + 32].try_into().unwrap(),
        ) as usize;
        if item_type == 132 && objectid == 5 {
            found_fs_tree_root_item = true;
        }
        cursor += 32 + len;
    }
    assert!(
        found_fs_tree_root_item,
        "expected ROOT_ITEM for FS_TREE (objectid 5) in results",
    );
}

// ── CLI-driven end-to-end ─────────────────────────────────────────

/// `btrfs subvolume show` against our fuse mount issues
/// `BTRFS_IOC_GET_SUBVOL_INFO` and prints the response. If our
/// ioctl plumbing is right end-to-end, the upstream-compatible
/// output should appear without errors. Skips gracefully when the
/// `btrfs` binary isn't built (it lives in another workspace
/// package, so cargo doesn't set `CARGO_BIN_EXE_btrfs` for the
/// fuse test target — we look for it in the same target dir as the
/// fuse binary).
#[test]
fn our_btrfs_cli_subvolume_show_against_fuse_mount() {
    let fuse_bin = std::path::Path::new(env!("CARGO_BIN_EXE_btrfs-fuse"));
    let cli_bin = fuse_bin.parent().unwrap().join("btrfs");
    if !cli_bin.exists() {
        eprintln!(
            "btrfs CLI binary not built at {}; skipping CLI E2E test",
            cli_bin.display(),
        );
        return;
    }

    let m = MountedFuse::mount();
    let output = std::process::Command::new(&cli_bin)
        .arg("subvolume")
        .arg("show")
        .arg(m.path())
        .output()
        .expect("spawn btrfs subvolume show");

    assert!(
        output.status.success(),
        "btrfs subvolume show failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Spot-check the key fields. Order/format mirrors btrfs-progs.
    assert!(stdout.contains("Subvolume ID:"), "stdout: {stdout}");
    assert!(stdout.contains("UUID:"), "stdout: {stdout}");
    assert!(stdout.contains("Generation:"), "stdout: {stdout}");
    assert!(
        stdout.contains("\t5\n")
            || stdout.contains("\t5 ")
            || stdout.contains(" 5\n"),
        "expected default subvol id 5 in output:\n{stdout}",
    );
}

/// `btrfs subvolume list` against our fuse mount issues
/// `BTRFS_IOC_TREE_SEARCH` (v1) on the root tree. The output should
/// list the user subvolumes of the multi-subvol fixture.
#[test]
fn our_btrfs_cli_subvolume_list_against_fuse_mount() {
    let fuse_bin = std::path::Path::new(env!("CARGO_BIN_EXE_btrfs-fuse"));
    let cli_bin = fuse_bin.parent().unwrap().join("btrfs");
    if !cli_bin.exists() {
        eprintln!(
            "btrfs CLI binary not built at {}; skipping",
            cli_bin.display(),
        );
        return;
    }

    let m = MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &[],
        "at_root.txt",
    );
    let output = std::process::Command::new(&cli_bin)
        .args(["subvolume", "list"])
        .arg(m.path())
        .output()
        .expect("spawn btrfs subvolume list");

    assert!(
        output.status.success(),
        "btrfs subvolume list failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Multi-subvol fixture contains a `sub` subvolume — the CLI's
    // output format mirrors btrfs-progs and should mention it
    // somewhere.
    assert!(
        stdout.contains("sub"),
        "expected `sub` in subvolume list output:\n{stdout}",
    );
}

/// `btrfs inspect-internal rootid` calls
/// `lookup_path_rootid(fd)` which issues `BTRFS_IOC_INO_LOOKUP` with
/// `objectid = BTRFS_FIRST_FREE_OBJECTID` to discover the
/// subvolume id of the file's containing tree. Exercises our
/// INO_LOOKUP plumbing end-to-end through real CLI consumer code.
#[test]
fn our_btrfs_cli_inspect_rootid_against_fuse_mount() {
    let fuse_bin = std::path::Path::new(env!("CARGO_BIN_EXE_btrfs-fuse"));
    let cli_bin = fuse_bin.parent().unwrap().join("btrfs");
    if !cli_bin.exists() {
        eprintln!(
            "btrfs CLI binary not built at {}; skipping CLI E2E test",
            cli_bin.display(),
        );
        return;
    }

    let m = MountedFuse::mount();
    let output = std::process::Command::new(&cli_bin)
        .args(["inspect-internal", "rootid"])
        .arg(m.path())
        .output()
        .expect("spawn btrfs inspect-internal rootid");

    assert!(
        output.status.success(),
        "btrfs inspect-internal rootid failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    assert_eq!(
        trimmed, "5",
        "expected default subvol id 5 from rootid; got {trimmed:?}",
    );
}

// ── BTRFS_IOC_DEV_INFO ────────────────────────────────────────────

/// `unsafe` wrapper for `_IOWR` ioctls: pre-fills `buf` with the
/// caller's input bytes and reads the response back from the same
/// buffer.
unsafe fn run_iowr_ioctl<P: AsRef<std::path::Path>>(
    path: P,
    cmd: u32,
    mut buf: Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    let f = File::open(path)?;
    let rc = unsafe {
        libc::ioctl(
            f.as_raw_fd(),
            cmd as libc::c_ulong,
            buf.as_mut_ptr() as *mut libc::c_void,
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(buf)
}

#[test]
fn ioctl_dev_info_for_devid_one_returns_uuid_and_sizes() {
    let m = MountedFuse::mount();
    // Build a 4096-byte input where devid=1 and uuid is left zero.
    let mut input = vec![0u8; 4096];
    input[..8].copy_from_slice(&1u64.to_le_bytes());
    let buf = unsafe { run_iowr_ioctl(m.path(), BTRFS_IOC_DEV_INFO, input) }
        .expect("DEV_INFO ioctl");
    let mut cursor = &buf[..];
    let devid = cursor.get_u64_le();
    let mut uuid = [0u8; 16];
    cursor.copy_to_slice(&mut uuid);
    let bytes_used = cursor.get_u64_le();
    let total_bytes = cursor.get_u64_le();

    assert_eq!(devid, 1);
    assert_ne!(uuid, [0u8; 16], "device uuid should be populated");
    assert_eq!(total_bytes, 128 * 1024 * 1024); // fixture is 128 MiB
    assert!(bytes_used > 0 && bytes_used <= total_bytes);
}

#[test]
fn ioctl_dev_info_unknown_devid_returns_enodev() {
    let m = MountedFuse::mount();
    let mut input = vec![0u8; 4096];
    input[..8].copy_from_slice(&999u64.to_le_bytes());
    let result = unsafe { run_iowr_ioctl(m.path(), BTRFS_IOC_DEV_INFO, input) };
    let err = result.expect_err("expected ENODEV");
    assert_eq!(err.raw_os_error(), Some(libc::ENODEV));
}

// ── BTRFS_IOC_INO_LOOKUP ──────────────────────────────────────────

#[test]
fn ioctl_ino_lookup_subvol_root_returns_empty_path() {
    let m = MountedFuse::mount();
    // Resolve objectid 256 (subvol root) in treeid 0 (current subvol).
    let mut input = vec![0u8; 4096];
    // treeid=0, objectid=256
    input[8..16].copy_from_slice(&256u64.to_le_bytes());
    let buf = unsafe { run_iowr_ioctl(m.path(), BTRFS_IOC_INO_LOOKUP, input) }
        .expect("INO_LOOKUP ioctl");

    let mut cursor = &buf[..];
    let resolved_treeid = cursor.get_u64_le();
    let resolved_oid = cursor.get_u64_le();
    assert_eq!(resolved_treeid, 5, "default subvol resolves to FS_TREE");
    assert_eq!(resolved_oid, 256);
    // Path field should start with NUL (empty path for the subvol root).
    assert_eq!(buf[16], 0, "subvol root should produce empty path");
}

// ── BTRFS_IOC_GET_SUBVOL_ROOTREF ──────────────────────────────────

#[test]
fn ioctl_get_subvol_rootref_lists_child_subvolumes() {
    // Multi-subvol fixture: FS_TREE (5) is parent of one subvolume
    // named `sub`. From the mount root the ioctl should report exactly
    // one rootref entry pointing at it.
    let m = MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &[],
        "at_root.txt",
    );
    let mut input = vec![0u8; 4096];
    // min_treeid = 0 (start from the beginning).
    let buf = unsafe {
        run_iowr_ioctl(m.path(), BTRFS_IOC_GET_SUBVOL_ROOTREF, input.clone())
    }
    .expect("GET_SUBVOL_ROOTREF ioctl");

    let _next_min_treeid = u64::from_le_bytes(buf[..8].try_into().unwrap());
    // num_items lives at offset 8 + 255*16 = 4088.
    let num_items = buf[4088];
    assert_eq!(
        num_items, 1,
        "expected exactly one child subvol, got {num_items}"
    );
    let treeid = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let dirid = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    assert!(
        treeid >= 256,
        "child subvol id {treeid} should be in user range",
    );
    assert_eq!(dirid, 256, "child should sit in the parent's root dir");

    // Quiet the unused warning on the input pre-fill helper.
    input[0] = 0;
}

// ── tree_search_v2 → tree_search_auto fallback ────────────────────

/// End-to-end coverage for the F6.4 fallback path: our FUSE driver
/// can't serve `BTRFS_IOC_TREE_SEARCH_V2` (the kernel rejects the
/// `FUSE_IOCTL_RETRY` round-trip needed for it), so the driver
/// signals that with `ENOPROTOOPT` and `tree_search_auto` in the
/// uapi crate transparently re-runs the search through v1.
///
/// Three assertions in one test:
///
/// 1. `tree_search_v2` directly → `Err(ENOPROTOOPT)`. Confirms the
///    FUSE driver returns the agreed-upon signal.
/// 2. `tree_search_auto` → `Ok` with results. Confirms the
///    fallback fires and runs to completion through v1.
/// 3. Items from `tree_search_auto` match items from a direct v1
///    `tree_search`. Confirms semantic equivalence.
#[test]
fn tree_search_auto_falls_back_to_v1_on_fuse_mount() {
    use btrfs_uapi::tree_search::{
        Key, SearchFilter, tree_search, tree_search_auto, tree_search_v2,
    };
    use std::os::fd::AsFd;

    // ROOT_ITEM_KEY is the natural test target — a handful per
    // multi-subvol fixture, fits comfortably in v1's 4 KiB buffer.
    let m = MountedFuse::mount_with(
        common::multi_subvol_fixture_path(),
        &[],
        "at_root.txt",
    );
    let f = File::open(m.path()).expect("open mount root for ioctl");

    let filter = SearchFilter {
        tree_id: 1, // BTRFS_ROOT_TREE_OBJECTID
        start: Key {
            objectid: 0,
            item_type: 132, // ROOT_ITEM_KEY
            offset: 0,
        },
        end: Key {
            objectid: u64::MAX,
            item_type: 132,
            offset: u64::MAX,
        },
        min_transid: 0,
        max_transid: u64::MAX,
    };

    // 1) v2 directly: must surface ENOPROTOOPT from our driver.
    let v2_err = tree_search_v2(f.as_fd(), filter.clone(), None, |_, _| Ok(()))
        .expect_err("tree_search_v2 must fail on a FUSE mount");
    assert_eq!(
        v2_err,
        nix::errno::Errno::ENOPROTOOPT,
        "FUSE driver should signal ENOPROTOOPT for v2",
    );

    // 2) auto: must succeed via fallback to v1.
    let mut auto_items: Vec<(u64, u32, u64)> = Vec::new();
    tree_search_auto(f.as_fd(), filter.clone(), None, |hdr, _data| {
        auto_items.push((hdr.objectid, hdr.item_type, hdr.offset));
        Ok(())
    })
    .expect("tree_search_auto must succeed via v1 fallback");
    assert!(
        !auto_items.is_empty(),
        "expected ≥ 1 ROOT_ITEM in the multi-subvol fixture",
    );

    // 3) Cross-check against a direct v1 walk.
    let mut v1_items: Vec<(u64, u32, u64)> = Vec::new();
    tree_search(f.as_fd(), filter, |hdr, _data| {
        v1_items.push((hdr.objectid, hdr.item_type, hdr.offset));
        Ok(())
    })
    .expect("tree_search v1 must succeed on a FUSE mount");
    assert_eq!(
        auto_items, v1_items,
        "auto fallback should yield identical items to direct v1",
    );
}

// ── unknown ioctl error ───────────────────────────────────────────

#[test]
fn unknown_ioctl_returns_enotty() {
    let m = MountedFuse::mount();
    // `ioc_ior(0x94, 200, 8)` is a valid encoding but no such btrfs
    // ioctl exists at command number 200.
    let bogus = ioc_ior(0x94, 200, 8);
    let result = unsafe { run_read_ioctl(m.path(), bogus, 8) };
    let err = result.expect_err("expected ENOTTY for unknown ioctl");
    assert_eq!(err.raw_os_error(), Some(libc::ENOTTY));
}

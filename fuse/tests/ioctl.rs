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

const BTRFS_IOC_FS_INFO: u32 = ioc_ior(0x94, 31, 1024);
const BTRFS_IOC_GET_FEATURES: u32 = ioc_ior(0x94, 57, 24);
const BTRFS_IOC_GET_SUBVOL_INFO: u32 = ioc_ior(0x94, 60, 504);

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

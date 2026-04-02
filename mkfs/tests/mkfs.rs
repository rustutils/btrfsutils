use btrfs_disk::{
    items::{DirItem, InodeFlags, InodeItem, RootItem, RootItemFlags, RootRef},
    raw::{
        btrfs_header, BTRFS_FIRST_FREE_OBJECTID, BTRFS_FS_TREE_OBJECTID,
    },
    reader::{self, Traversal},
    superblock::read_superblock,
    tree::{KeyType, TreeBlock},
};
use btrfs_mkfs::{
    args::{
        CompressAlgorithm, Feature, FeatureArg, InodeFlagsArg, Profile,
        SubvolArg, SubvolType,
    },
    mkfs::{
        self, DeviceInfo, MkfsConfig, RootdirOptions, make_btrfs,
        make_btrfs_with_rootdir,
    },
    rootdir::CompressConfig,
    write::ChecksumType,
};
use flate2::{Compression, write::GzEncoder};
use std::{
    collections::HashMap,
    fs::{File, write},
    io::{Read as _, Seek, SeekFrom, Write},
    mem::size_of,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::{NamedTempFile, TempDir, tempdir};
use uuid::Uuid;

fn test_config(total_bytes: u64) -> MkfsConfig {
    MkfsConfig {
        nodesize: 16384,
        sectorsize: 4096,
        devices: vec![DeviceInfo {
            devid: 1,
            path: PathBuf::new(),
            total_bytes,
            dev_uuid: Uuid::from_bytes([0xAB; 16]),
        }],
        label: None,
        fs_uuid: Uuid::from_bytes([0xDE; 16]),
        chunk_tree_uuid: Uuid::from_bytes([0xCD; 16]),
        incompat_flags: MkfsConfig::default_incompat_flags(),
        compat_ro_flags: MkfsConfig::default_compat_ro_flags(),
        data_profile: Profile::Single,
        metadata_profile: Profile::Dup,
        csum_type: ChecksumType::Crc32c,
        creation_time: None,
    }
}

fn test_config_two_devices(per_device_bytes: u64) -> MkfsConfig {
    MkfsConfig {
        nodesize: 16384,
        sectorsize: 4096,
        devices: vec![
            DeviceInfo {
                devid: 1,
                path: PathBuf::new(),
                total_bytes: per_device_bytes,
                dev_uuid: Uuid::from_bytes([0xAB; 16]),
            },
            DeviceInfo {
                devid: 2,
                path: PathBuf::new(),
                total_bytes: per_device_bytes,
                dev_uuid: Uuid::from_bytes([0xBC; 16]),
            },
        ],
        label: None,
        fs_uuid: Uuid::from_bytes([0xDE; 16]),
        chunk_tree_uuid: Uuid::from_bytes([0xCD; 16]),
        incompat_flags: MkfsConfig::default_incompat_flags(),
        compat_ro_flags: MkfsConfig::default_compat_ro_flags(),
        data_profile: Profile::Single,
        metadata_profile: Profile::Raid1,
        csum_type: ChecksumType::Crc32c,
        creation_time: None,
    }
}

/// Minimum valid image size: system (5 MiB) + metadata DUP (2*32 MiB) + data (64 MiB) = 133 MiB.
/// Use 256 MiB for comfortable headroom.
const MIN_SIZE: u64 = 256 * 1024 * 1024;

/// Set the device path in the config and call make_btrfs.
fn make_btrfs_on(image: &NamedTempFile, cfg: &mut MkfsConfig) {
    cfg.devices[0].path = image.path().to_path_buf();
    make_btrfs(cfg).unwrap();
}

fn make_btrfs_on_err(
    image: &NamedTempFile,
    cfg: &mut MkfsConfig,
) -> anyhow::Error {
    cfg.devices[0].path = image.path().to_path_buf();
    make_btrfs(cfg).unwrap_err()
}

fn make_btrfs_two_devices(
    img1: &NamedTempFile,
    img2: &NamedTempFile,
    cfg: &mut MkfsConfig,
) {
    cfg.devices[0].path = img1.path().to_path_buf();
    cfg.devices[1].path = img2.path().to_path_buf();
    make_btrfs(cfg).unwrap();
}

fn create_image(size: u64) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.as_file_mut().set_len(size).unwrap();
    // Write a zero byte at the end to ensure the file is fully allocated.
    file.as_file_mut().seek(SeekFrom::Start(size - 1)).unwrap();
    file.as_file_mut().write_all(&[0]).unwrap();
    file
}

#[test]
fn mkfs_creates_valid_superblock() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
}

#[test]
fn mkfs_superblock_has_correct_uuid() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.fsid, cfg.fs_uuid);
}

#[test]
fn mkfs_superblock_has_correct_sizes() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.nodesize, 16384);
    assert_eq!(sb.sectorsize, 4096);
    assert_eq!(sb.total_bytes, MIN_SIZE);
}

#[test]
fn mkfs_superblock_has_label() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.label = Some("test-label".to_string());
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.label, "test-label");
}

#[test]
fn mkfs_superblock_generation_is_one() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.generation, 1);
}

#[test]
fn mkfs_too_small_fails() {
    let too_small = 100 * 1024 * 1024; // 100 MiB, too small for metadata DUP + data
    let image = create_image(too_small);
    let mut cfg = test_config(too_small);
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().contains("too small"),
        "expected 'too small' error, got: {err}"
    );
}

#[test]
fn has_btrfs_superblock_on_empty_file() {
    let image = create_image(MIN_SIZE);
    assert!(!mkfs::has_btrfs_superblock(image.path()));
}

#[test]
fn has_btrfs_superblock_after_mkfs() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);
    assert!(mkfs::has_btrfs_superblock(image.path()));
}

#[test]
fn minimum_device_size_matches_expected() {
    let min = mkfs::minimum_device_size(16384);
    // 5 MiB (system) + 64 MiB (2 * 32M meta DUP) + 64 MiB (data) = 133 MiB
    assert_eq!(min, 133 * 1024 * 1024);
}

#[test]
fn apply_features_enable() {
    let mut cfg = test_config(MIN_SIZE);
    let features = vec![FeatureArg {
        feature: Feature::BlockGroupTree,
        enabled: true,
    }];
    cfg.apply_features(&features).unwrap();
    assert!(cfg.has_block_group_tree());
}

#[test]
fn apply_features_disable() {
    let mut cfg = test_config(MIN_SIZE);
    assert!(cfg.has_free_space_tree());
    let features = vec![FeatureArg {
        feature: Feature::FreeSpaceTree,
        enabled: false,
    }];
    cfg.apply_features(&features).unwrap();
    assert!(!cfg.has_free_space_tree());
}

#[test]
fn apply_features_disable_skinny_metadata() {
    let mut cfg = test_config(MIN_SIZE);
    assert!(cfg.skinny_metadata());
    let features = vec![FeatureArg {
        feature: Feature::SkinnyMetadata,
        enabled: false,
    }];
    cfg.apply_features(&features).unwrap();
    assert!(!cfg.skinny_metadata());
}

#[test]
fn apply_features_unsupported_rejected() {
    let mut cfg = test_config(MIN_SIZE);
    let features = vec![FeatureArg {
        feature: Feature::Zoned,
        enabled: true,
    }];
    let err = cfg.apply_features(&features).unwrap_err();
    assert!(
        err.to_string().contains("not yet supported"),
        "expected unsupported error, got: {err}"
    );
}

#[test]
fn mkfs_with_no_free_space_tree() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    let features = vec![FeatureArg {
        feature: Feature::FreeSpaceTree,
        enabled: false,
    }];
    cfg.apply_features(&features).unwrap();
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
    // compat_ro should not have free-space-tree bits
    let fst_bit =
        btrfs_disk::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64;
    assert_eq!(sb.compat_ro_flags & fst_bit, 0);
}

#[test]
fn mkfs_with_different_nodesize() {
    // 64 KiB nodesize: still needs 133 MiB minimum for chunks.
    let size = MIN_SIZE;
    let image = create_image(size);
    let mut cfg = test_config(size);
    cfg.nodesize = 65536;
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.nodesize, 65536);
    assert!(sb.magic_is_valid());
}

#[test]
fn mkfs_raid1_two_devices_valid_superblocks() {
    let per_dev = MIN_SIZE;
    let img1 = create_image(per_dev);
    let img2 = create_image(per_dev);
    let mut cfg = test_config_two_devices(per_dev);
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    // Both devices should have valid superblocks.
    let mut f1 = File::open(img1.path()).unwrap();
    let sb1 = read_superblock(&mut f1, 0).unwrap();
    assert!(sb1.magic_is_valid());

    let mut f2 = File::open(img2.path()).unwrap();
    let sb2 = read_superblock(&mut f2, 0).unwrap();
    assert!(sb2.magic_is_valid());
}

#[test]
fn mkfs_raid1_superblocks_share_uuid() {
    let per_dev = MIN_SIZE;
    let img1 = create_image(per_dev);
    let img2 = create_image(per_dev);
    let mut cfg = test_config_two_devices(per_dev);
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    let mut f1 = File::open(img1.path()).unwrap();
    let sb1 = read_superblock(&mut f1, 0).unwrap();
    let mut f2 = File::open(img2.path()).unwrap();
    let sb2 = read_superblock(&mut f2, 0).unwrap();

    // Same filesystem UUID.
    assert_eq!(sb1.fsid, sb2.fsid);
    assert_eq!(sb1.fsid, cfg.fs_uuid);
}

#[test]
fn mkfs_raid1_superblocks_different_dev_items() {
    let per_dev = MIN_SIZE;
    let img1 = create_image(per_dev);
    let img2 = create_image(per_dev);
    let mut cfg = test_config_two_devices(per_dev);
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    let mut f1 = File::open(img1.path()).unwrap();
    let sb1 = read_superblock(&mut f1, 0).unwrap();
    let mut f2 = File::open(img2.path()).unwrap();
    let sb2 = read_superblock(&mut f2, 0).unwrap();

    // Each superblock should embed its own device's dev_item.
    assert_eq!(sb1.dev_item.devid, 1);
    assert_eq!(sb2.dev_item.devid, 2);
    // Both report num_devices = 2.
    assert_eq!(sb1.num_devices, 2);
    assert_eq!(sb2.num_devices, 2);
}

#[test]
fn mkfs_raid1_total_bytes_is_sum() {
    let per_dev = MIN_SIZE;
    let img1 = create_image(per_dev);
    let img2 = create_image(per_dev);
    let mut cfg = test_config_two_devices(per_dev);
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    let mut f1 = File::open(img1.path()).unwrap();
    let sb1 = read_superblock(&mut f1, 0).unwrap();
    assert_eq!(sb1.total_bytes, 2 * per_dev);
}

#[test]
fn mkfs_writes_super_mirror_1() {
    // MIN_SIZE (256 MiB) > 64 MiB, so mirror 1 should be written.
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();

    // Mirror 0 at 64 KiB
    let sb0 = read_superblock(&mut file, 0).unwrap();
    assert!(sb0.magic_is_valid());

    // Mirror 1 at 64 MiB
    let sb1 = read_superblock(&mut file, 1).unwrap();
    assert!(sb1.magic_is_valid());
    assert_eq!(sb0.fsid, sb1.fsid);
    assert_eq!(sb0.generation, sb1.generation);
}

#[test]
fn mkfs_raid0_data_two_devices() {
    let img1 = create_image(MIN_SIZE);
    let img2 = create_image(MIN_SIZE);
    let mut cfg = test_config_two_devices(MIN_SIZE);
    cfg.data_profile = Profile::Raid0;
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    let mut f1 = File::open(img1.path()).unwrap();
    let sb = read_superblock(&mut f1, 0).unwrap();
    assert!(sb.magic_is_valid());
    assert_eq!(sb.num_devices, 2);
}

// --- Deterministic image snapshot tests ---
//
// Create filesystem images with fixed UUIDs and timestamps, compress them,
// and snapshot the compressed bytes. Any change to the on-disk format will
// show up as a snapshot diff.

fn deterministic_config(total_bytes: u64) -> MkfsConfig {
    MkfsConfig {
        nodesize: 16384,
        sectorsize: 4096,
        devices: vec![DeviceInfo {
            devid: 1,
            path: PathBuf::new(),
            total_bytes,
            dev_uuid: Uuid::from_bytes([0xAB; 16]),
        }],
        label: None,
        fs_uuid: Uuid::from_bytes([0xDE; 16]),
        chunk_tree_uuid: Uuid::from_bytes([0xCD; 16]),
        incompat_flags: MkfsConfig::default_incompat_flags(),
        compat_ro_flags: MkfsConfig::default_compat_ro_flags(),
        data_profile: Profile::Single,
        metadata_profile: Profile::Dup,
        csum_type: ChecksumType::Crc32c,
        creation_time: Some(1700000000), // fixed timestamp
    }
}

/// Create an image, format it, read back the raw bytes, gzip them.
fn make_image_compressed(cfg: &mut MkfsConfig) -> Vec<u8> {
    let image = create_image(cfg.devices[0].total_bytes);
    cfg.devices[0].path = image.path().to_path_buf();
    make_btrfs(cfg).unwrap();

    // Read back the full image.
    let mut raw = Vec::new();
    File::open(image.path())
        .unwrap()
        .read_to_end(&mut raw)
        .unwrap();

    // Gzip compress.
    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(&raw).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn snapshot_default_single_device() {
    let mut cfg = deterministic_config(MIN_SIZE);
    let compressed = make_image_compressed(&mut cfg);
    insta::assert_binary_snapshot!(".img.gz", compressed);
}

#[test]
fn snapshot_xxhash() {
    let mut cfg = deterministic_config(MIN_SIZE);
    cfg.csum_type = ChecksumType::Xxhash64;
    let compressed = make_image_compressed(&mut cfg);
    insta::assert_binary_snapshot!(".img.gz", compressed);
}

#[test]
fn snapshot_no_block_group_tree() {
    let mut cfg = deterministic_config(MIN_SIZE);
    cfg.apply_features(&[FeatureArg {
        feature: Feature::BlockGroupTree,
        enabled: false,
    }])
    .unwrap();
    let compressed = make_image_compressed(&mut cfg);
    insta::assert_binary_snapshot!(".img.gz", compressed);
}

// --- Privileged integration tests (mount) ---
//
// These require root and loopback device support. Marked #[ignore].

fn run(cmd: &str, args: &[&str]) {
    let output = Command::new(cmd).args(args).output().unwrap_or_else(|e| {
        panic!("failed to run {cmd}: {e}");
    });
    assert!(
        output.status.success(),
        "{cmd} {:?} failed:\n{}",
        args,
        String::from_utf8_lossy(&output.stderr),
    );
}

struct LoopDev {
    path: PathBuf,
}

impl LoopDev {
    fn attach(file: &Path) -> Self {
        let output = Command::new("losetup")
            .args(["--find", "--show", &file.to_string_lossy()])
            .output()
            .expect("failed to run losetup");
        assert!(output.status.success(), "losetup failed");
        Self {
            path: PathBuf::from(
                String::from_utf8(output.stdout).unwrap().trim(),
            ),
        }
    }
}

impl Drop for LoopDev {
    fn drop(&mut self) {
        let _ = Command::new("losetup")
            .args(["-d", &self.path.to_string_lossy()])
            .status();
    }
}

struct MountPoint {
    dir: TempDir,
    _loop_dev: LoopDev,
}

impl MountPoint {
    fn mount(loop_dev: LoopDev) -> Self {
        let dir = TempDir::new().unwrap();
        run(
            "mount",
            &[
                "-t",
                "btrfs",
                &loop_dev.path.to_string_lossy(),
                &dir.path().to_string_lossy(),
            ],
        );
        Self {
            dir,
            _loop_dev: loop_dev,
        }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for MountPoint {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(self.dir.path()).status();
    }
}

/// Run `btrfs check` on an image file and assert it passes.
fn btrfs_check(image: &Path) {
    let output = Command::new("btrfs")
        .args(["check", &image.to_string_lossy()])
        .output()
        .expect("failed to run btrfs check");
    assert!(
        output.status.success(),
        "btrfs check failed on {}:\n{}",
        image.display(),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Create image files for all devices, format, run btrfs check, then mount
/// and verify the filesystem is accessible.
fn make_check_mount_verify(cfg: &mut MkfsConfig) {
    // Create image files for all devices.
    let images: Vec<_> = cfg
        .devices
        .iter()
        .map(|dev| create_image(dev.total_bytes))
        .collect();
    for (i, img) in images.iter().enumerate() {
        cfg.devices[i].path = img.path().to_path_buf();
    }

    make_btrfs(cfg).unwrap();

    // Verify with btrfs check before mounting.
    btrfs_check(images[0].path());

    // Mount the first device and verify it's accessible.
    let loop_dev = LoopDev::attach(images[0].path());
    let mount = MountPoint::mount(loop_dev);

    // Verify we can list the root directory.
    let entries: Vec<_> = std::fs::read_dir(mount.path()).unwrap().collect();
    // Empty filesystem: should have no entries (or just the default subvol).
    let _ = entries;

    // Verify we can write a file.
    let test_file = mount.path().join("hello.txt");
    write(&test_file, b"btrfs works!").unwrap();
    assert_eq!(std::fs::read_to_string(&test_file).unwrap(), "btrfs works!");
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_crc32c() {
    let mut cfg = test_config(MIN_SIZE);
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_xxhash() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.csum_type = ChecksumType::Xxhash64;
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_sha256() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.csum_type = ChecksumType::Sha256;
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_blake2() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.csum_type = ChecksumType::Blake2b;
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_no_block_group_tree() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.apply_features(&[FeatureArg {
        feature: Feature::BlockGroupTree,
        enabled: false,
    }])
    .unwrap();
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_nodesize_64k() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.nodesize = 65536;
    make_check_mount_verify(&mut cfg);
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_single_device_with_label() {
    let mut cfg = test_config(MIN_SIZE);
    cfg.label = Some("integration-test".to_string());
    make_check_mount_verify(&mut cfg);
}

// --- Validation tests (adapted from btrfs-progs mkfs-tests 003, 008) ---

/// Mixed mode requires equal nodesize and sectorsize (test 003).
#[test]
fn mixed_mode_requires_equal_nodesize_sectorsize() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 4096;
    cfg.nodesize = 16384;
    cfg.apply_features(&[FeatureArg {
        feature: Feature::MixedBg,
        enabled: true,
    }])
    .unwrap();
    // With mixed-bg enabled, nodesize != sectorsize should fail.
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().to_lowercase().contains("mixed")
            || err.to_string().to_lowercase().contains("sector")
            || err.to_string().to_lowercase().contains("node"),
        "expected mixed-mode validation error, got: {err}"
    );
}

/// Unaligned nodesize/sectorsize combos are rejected (test 008).
#[test]
fn invalid_unaligned_sectorsize_rejected() {
    // 8191 is not a power of two.
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 8191;
    cfg.nodesize = 8191;
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().contains("sectorsize")
            || err.to_string().contains("nodesize")
            || err.to_string().contains("power"),
        "expected validation error, got: {err}"
    );
}

/// Aligned sectorsize with unaligned nodesize is rejected (test 008).
#[test]
fn invalid_unaligned_nodesize_rejected() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 4096;
    cfg.nodesize = 16385; // Not a power of two.
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().contains("nodesize")
            || err.to_string().contains("power"),
        "expected nodesize validation error, got: {err}"
    );
}

/// Sectorsize larger than nodesize is rejected (test 008).
#[test]
fn sectorsize_larger_than_nodesize_rejected() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 8192;
    cfg.nodesize = 4096;
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().contains("nodesize")
            || err.to_string().contains("sectorsize"),
        "expected nodesize < sectorsize error, got: {err}"
    );
}

/// Nodesize too large (> 64K) is rejected (test 008).
#[test]
fn nodesize_too_large_rejected() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.nodesize = 131072; // 128K, exceeds 64K limit.
    let err = make_btrfs_on_err(&image, &mut cfg);
    assert!(
        err.to_string().contains("nodesize") || err.to_string().contains("64"),
        "expected nodesize too large error, got: {err}"
    );
}

/// Valid aligned sectorsize and nodesize combinations work (test 008).
#[test]
fn valid_sectorsize_nodesize_combos() {
    // 4K sector, 16K node — standard.
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 4096;
    cfg.nodesize = 16384;
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
    assert_eq!(sb.sectorsize, 4096);
    assert_eq!(sb.nodesize, 16384);
}

/// Large sectorsize (64K) with equal nodesize works (test 008).
#[test]
fn large_sectorsize_64k_works() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.sectorsize = 65536;
    cfg.nodesize = 65536;
    make_btrfs_on(&image, &mut cfg);

    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
    assert_eq!(sb.sectorsize, 65536);
    assert_eq!(sb.nodesize, 65536);
}

// --- Nodesize/sectorsize combination tests (adapted from test 007) ---

/// Various valid nodesize >= sectorsize combos produce valid superblocks.
#[test]
fn nodesize_sectorsize_combinations() {
    let sizes = [4096, 8192, 16384, 32768, 65536];
    for &nodesize in &sizes {
        for &sectorsize in &sizes {
            if nodesize < sectorsize {
                continue;
            }
            let image = create_image(MIN_SIZE);
            let mut cfg = test_config(MIN_SIZE);
            cfg.nodesize = nodesize;
            cfg.sectorsize = sectorsize;
            make_btrfs_on(&image, &mut cfg);

            let mut file = File::open(image.path()).unwrap();
            let sb = read_superblock(&mut file, 0).unwrap();
            assert!(
                sb.magic_is_valid(),
                "invalid superblock for nodesize={nodesize} sectorsize={sectorsize}"
            );
            assert_eq!(sb.nodesize, nodesize);
            assert_eq!(sb.sectorsize, sectorsize);
        }
    }
}

// --- Reserved 1M range test (adapted from test 013) ---

/// No device extent should start below 1 MiB (the reserved range).
#[test]
fn first_dev_extent_above_reserved_1m() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let file = File::open(image.path()).unwrap();
    let fs = reader::filesystem_open(file).unwrap();

    // Find the device tree (objectid 4) root.
    let dev_tree_id = btrfs_disk::raw::BTRFS_DEV_TREE_OBJECTID as u64;
    let (dev_root, _) = fs
        .tree_roots
        .get(&dev_tree_id)
        .expect("device tree not found");

    // Walk the device tree and collect all DEV_EXTENT offsets.
    let mut dev_extent_offsets = Vec::new();
    let mut block_reader = fs.reader;
    reader::tree_walk(
        &mut block_reader,
        *dev_root,
        Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, .. } = block {
                for item in items {
                    if item.key.key_type == KeyType::DeviceExtent {
                        // The key offset is the physical byte offset on device.
                        dev_extent_offsets.push(item.key.offset);
                    }
                }
            }
        },
    )
    .unwrap();

    assert!(
        !dev_extent_offsets.is_empty(),
        "no device extents found in device tree"
    );

    let one_mib = 1024 * 1024;
    for offset in &dev_extent_offsets {
        assert!(
            *offset >= one_mib,
            "device extent at offset {offset} is within the reserved 0-1M range"
        );
    }
}

// --- FS_TREE UUID and otime test (adapted from test 015) ---

/// The FS_TREE ROOT_ITEM should have a non-nil UUID and non-zero otime.
#[test]
fn fs_tree_root_item_has_uuid_and_otime() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let file = File::open(image.path()).unwrap();
    let fs = reader::filesystem_open(file).unwrap();

    // Read the root tree to find FS_TREE's ROOT_ITEM.
    let root_tree_logical = fs.superblock.root;
    let mut fs_tree_root_item: Option<RootItem> = None;
    let fs_tree_id = BTRFS_FS_TREE_OBJECTID as u64;

    let mut block_reader = fs.reader;
    reader::tree_walk(
        &mut block_reader,
        root_tree_logical,
        Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, data, .. } = block {
                let header_size = size_of::<btrfs_header>();
                for item in items {
                    if item.key.objectid == fs_tree_id
                        && item.key.key_type == KeyType::RootItem
                    {
                        let start = header_size + item.offset as usize;
                        let end = start + item.size as usize;
                        if end <= data.len() {
                            fs_tree_root_item =
                                RootItem::parse(&data[start..end]);
                        }
                    }
                }
            }
        },
    )
    .unwrap();

    let root_item =
        fs_tree_root_item.expect("FS_TREE ROOT_ITEM not found in root tree");

    // UUID must be non-nil.
    assert!(!root_item.uuid.is_nil(), "FS_TREE ROOT_ITEM uuid is nil");

    // otime must be non-zero (seconds > 0).
    assert!(root_item.otime.sec > 0, "FS_TREE ROOT_ITEM otime is zero");
}

// --- Free-space-tree no bitmaps test (adapted from test 024) ---

/// An empty filesystem's free-space-tree should have no FREE_SPACE_BITMAP items.
#[test]
fn free_space_tree_no_bitmaps_on_empty_fs() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let file = File::open(image.path()).unwrap();
    let fs = reader::filesystem_open(file).unwrap();

    // Find the free-space tree (objectid 10) root.
    let fst_id = btrfs_disk::raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64;
    let fst_root = fs.tree_roots.get(&fst_id);

    // If there's no free-space tree at all (feature disabled), that's fine.
    if let Some((fst_logical, _)) = fst_root {
        let mut bitmap_count = 0u64;
        let mut block_reader = fs.reader;
        reader::tree_walk(
            &mut block_reader,
            *fst_logical,
            Traversal::Dfs,
            &mut |block| {
                if let TreeBlock::Leaf { items, .. } = block {
                    for item in items {
                        if item.key.key_type == KeyType::FreeSpaceBitmap {
                            bitmap_count += 1;
                        }
                    }
                }
            },
        )
        .unwrap();

        assert_eq!(
            bitmap_count, 0,
            "found {bitmap_count} FREE_SPACE_BITMAP items on empty filesystem"
        );
    }
}

// --- Root tree directory "default" DIR_ITEM tests ---

/// Helper: find the "default" DIR_ITEM in the root tree and return its location key.
fn find_default_dir_item(image_path: &Path) -> DirItem {
    let file = File::open(image_path).unwrap();
    let fs = reader::filesystem_open(file).unwrap();

    let root_tree_logical = fs.superblock.root;
    let root_dir_oid = btrfs_disk::raw::BTRFS_ROOT_TREE_DIR_OBJECTID as u64;
    let header_size = size_of::<btrfs_header>();

    let mut found: Option<DirItem> = None;
    let mut block_reader = fs.reader;
    reader::tree_walk(
        &mut block_reader,
        root_tree_logical,
        Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, data, .. } = block {
                for item in items {
                    if item.key.objectid == root_dir_oid
                        && item.key.key_type == KeyType::DirItem
                    {
                        let start = header_size + item.offset as usize;
                        let end = start + item.size as usize;
                        if end <= data.len() {
                            let entries = DirItem::parse_all(&data[start..end]);
                            for entry in entries {
                                if entry.name == b"default" {
                                    found = Some(entry);
                                }
                            }
                        }
                    }
                }
            }
        },
    )
    .unwrap();

    found.expect("no 'default' DIR_ITEM found in root tree")
}

/// Normal mkfs creates a "default" DIR_ITEM pointing to FS_TREE (objectid 5).
#[test]
fn root_tree_default_dir_item_points_to_fs_tree() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let dir_item = find_default_dir_item(image.path());
    assert_eq!(
        dir_item.location.objectid,
        BTRFS_FS_TREE_OBJECTID as u64,
        "default DIR_ITEM should point to FS_TREE"
    );
    assert_eq!(dir_item.location.key_type, KeyType::RootItem);
}

/// With --subvol default:subdir, the "default" DIR_ITEM should point to the
/// subvolume's objectid (256) instead of FS_TREE.
#[test]
fn rootdir_default_subvol_dir_item_points_to_subvol() {
    let rootdir = tempdir().unwrap();
    std::fs::create_dir(rootdir.path().join("mysubvol")).unwrap();
    write(rootdir.path().join("mysubvol").join("hello.txt"), "hello").unwrap();

    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.devices[0].path = image.path().to_path_buf();
    cfg.incompat_flags |=
        u64::from(btrfs_disk::raw::BTRFS_FEATURE_INCOMPAT_DEFAULT_SUBVOL);

    let subvols = [SubvolArg {
        subvol_type: SubvolType::Default,
        path: PathBuf::from("mysubvol"),
    }];

    make_btrfs_with_rootdir(
        &cfg,
        rootdir.path(),
        CompressConfig::default(),
        &[],
        &subvols,
        RootdirOptions::default(),
    )
    .unwrap();

    let dir_item = find_default_dir_item(image.path());
    let expected_subvol_id = BTRFS_FIRST_FREE_OBJECTID as u64;
    assert_eq!(
        dir_item.location.objectid, expected_subvol_id,
        "default DIR_ITEM should point to subvolume {expected_subvol_id}, not FS_TREE"
    );
    assert_eq!(dir_item.location.key_type, KeyType::RootItem);
}

// --- Subvolume tests ---

/// Helper: create a rootdir image with subvolumes and return the image path.
fn make_rootdir_image_with_subvols(
    subvols: &[SubvolArg],
    setup: impl FnOnce(&Path),
) -> NamedTempFile {
    let rootdir = tempdir().unwrap();
    setup(rootdir.path());

    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.devices[0].path = image.path().to_path_buf();

    make_btrfs_with_rootdir(
        &cfg,
        rootdir.path(),
        CompressConfig::default(),
        &[],
        subvols,
        RootdirOptions::default(),
    )
    .unwrap();
    image
}

/// Helper: walk the root tree and collect items matching a predicate.
fn walk_root_tree_items(
    image_path: &Path,
    mut predicate: impl FnMut(u64, KeyType, u64, &[u8]) -> bool,
) -> Vec<(u64, KeyType, u64, Vec<u8>)> {
    let file = File::open(image_path).unwrap();
    let fs = reader::filesystem_open(file).unwrap();
    let root_logical = fs.superblock.root;
    let header_size = size_of::<btrfs_header>();

    let mut results = Vec::new();
    let mut block_reader = fs.reader;
    reader::tree_walk(
        &mut block_reader,
        root_logical,
        Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, data, .. } = block {
                for item in items {
                    let start = header_size + item.offset as usize;
                    let end = start + item.size as usize;
                    if end <= data.len()
                        && predicate(
                            item.key.objectid,
                            item.key.key_type,
                            item.key.offset,
                            &data[start..end],
                        )
                    {
                        results.push((
                            item.key.objectid,
                            item.key.key_type,
                            item.key.offset,
                            data[start..end].to_vec(),
                        ));
                    }
                }
            }
        },
    )
    .unwrap();
    results
}

/// A read-write subvolume should have ROOT_ITEM, ROOT_REF, and ROOT_BACKREF
/// in the root tree, with the ROOT_ITEM not marked RDONLY.
#[test]
fn subvol_rw_has_root_tree_entries() {
    let subvols = [SubvolArg {
        subvol_type: SubvolType::Rw,
        path: PathBuf::from("sub1"),
    }];
    let image = make_rootdir_image_with_subvols(&subvols, |root| {
        std::fs::create_dir(root.join("sub1")).unwrap();
        write(root.join("sub1/file.txt"), "data").unwrap();
    });

    let subvol_id = BTRFS_FIRST_FREE_OBJECTID as u64;
    let fs_tree_id = BTRFS_FS_TREE_OBJECTID as u64;

    // Check ROOT_ITEM exists for subvol.
    let root_items = walk_root_tree_items(image.path(), |oid, kt, _, _| {
        oid == subvol_id && kt == KeyType::RootItem
    });
    assert_eq!(root_items.len(), 1, "expected one ROOT_ITEM for subvolume");
    let ri = RootItem::parse(&root_items[0].3).unwrap();
    assert!(
        !ri.flags.contains(RootItemFlags::RDONLY),
        "rw subvol should not be RDONLY"
    );
    assert_ne!(ri.uuid, Uuid::nil(), "subvolume should have a UUID");

    // Check ROOT_REF: parent → child.
    let root_refs = walk_root_tree_items(image.path(), |oid, kt, offset, _| {
        oid == fs_tree_id && kt == KeyType::RootRef && offset == subvol_id
    });
    assert_eq!(root_refs.len(), 1, "expected one ROOT_REF");
    let rr = RootRef::parse(&root_refs[0].3).unwrap();
    assert_eq!(rr.name, b"sub1");

    // Check ROOT_BACKREF: child → parent.
    let backrefs = walk_root_tree_items(image.path(), |oid, kt, offset, _| {
        oid == subvol_id && kt == KeyType::RootBackref && offset == fs_tree_id
    });
    assert_eq!(backrefs.len(), 1, "expected one ROOT_BACKREF");
    let br = RootRef::parse(&backrefs[0].3).unwrap();
    assert_eq!(br.name, b"sub1");
}

/// A read-only subvolume should have BTRFS_ROOT_SUBVOL_RDONLY set in its
/// ROOT_ITEM.
#[test]
fn subvol_ro_has_rdonly_flag() {
    let subvols = [SubvolArg {
        subvol_type: SubvolType::Ro,
        path: PathBuf::from("rosub"),
    }];
    let image = make_rootdir_image_with_subvols(&subvols, |root| {
        std::fs::create_dir(root.join("rosub")).unwrap();
    });

    let subvol_id = BTRFS_FIRST_FREE_OBJECTID as u64;
    let root_items = walk_root_tree_items(image.path(), |oid, kt, _, _| {
        oid == subvol_id && kt == KeyType::RootItem
    });
    assert_eq!(root_items.len(), 1);
    let ri = RootItem::parse(&root_items[0].3).unwrap();
    assert!(
        ri.flags.contains(RootItemFlags::RDONLY),
        "ro subvol should be RDONLY"
    );
}

// --- Inode flags tests ---

/// --inode-flags NODATACOW,NODATASUM:path should set the corresponding flags
/// on the inode.
#[test]
fn rootdir_inode_flags_nodatacow_nodatasum() {
    let rootdir = tempdir().unwrap();
    // Create a file large enough to not be inlined (> 4095 bytes).
    let big_data = vec![0x42u8; 8192];
    write(rootdir.path().join("nocow.bin"), &big_data).unwrap();
    write(rootdir.path().join("normal.bin"), &big_data).unwrap();

    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.devices[0].path = image.path().to_path_buf();

    let inode_flags = [InodeFlagsArg {
        nodatacow: true,
        nodatasum: true,
        path: PathBuf::from("nocow.bin"),
    }];

    make_btrfs_with_rootdir(
        &cfg,
        rootdir.path(),
        CompressConfig::default(),
        &inode_flags,
        &[],
        RootdirOptions::default(),
    )
    .unwrap();

    // Read the FS tree.
    let file = File::open(image.path()).unwrap();
    let fs = reader::filesystem_open(file).unwrap();
    let fs_tree_id = BTRFS_FS_TREE_OBJECTID as u64;
    let (fs_root, _) =
        fs.tree_roots.get(&fs_tree_id).expect("FS tree not found");
    let fs_root = *fs_root;
    let mut block_reader = fs.reader;
    let header_size = size_of::<btrfs_header>();

    // Collect all INODE_ITEMs and DIR_ITEMs from the FS tree.
    let mut inodes: HashMap<u64, InodeItem> = HashMap::new();
    let mut name_to_ino: HashMap<Vec<u8>, u64> = HashMap::new();

    reader::tree_walk(
        &mut block_reader,
        fs_root,
        Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, data, .. } = block {
                for item in items {
                    let start = header_size + item.offset as usize;
                    let end = start + item.size as usize;
                    if end > data.len() {
                        continue;
                    }
                    match item.key.key_type {
                        KeyType::InodeItem => {
                            if let Some(ii) =
                                InodeItem::parse(&data[start..end])
                            {
                                inodes.insert(item.key.objectid, ii);
                            }
                        }
                        KeyType::DirItem => {
                            for di in DirItem::parse_all(&data[start..end]) {
                                name_to_ino
                                    .insert(di.name, di.location.objectid);
                            }
                        }
                        _ => {}
                    }
                }
            }
        },
    )
    .unwrap();

    // Check nocow.bin has NODATACOW + NODATASUM flags.
    let nocow_ino = name_to_ino
        .get(&b"nocow.bin"[..])
        .expect("nocow.bin not found in FS tree");
    let nocow_inode = inodes.get(nocow_ino).expect("nocow.bin inode not found");
    assert!(
        nocow_inode.flags.contains(InodeFlags::NODATACOW),
        "nocow.bin should have NODATACOW flag"
    );
    assert!(
        nocow_inode.flags.contains(InodeFlags::NODATASUM),
        "nocow.bin should have NODATASUM flag"
    );

    // Check normal.bin does NOT have those flags.
    let normal_ino = name_to_ino
        .get(&b"normal.bin"[..])
        .expect("normal.bin not found in FS tree");
    let normal_inode =
        inodes.get(normal_ino).expect("normal.bin inode not found");
    assert!(
        !normal_inode.flags.contains(InodeFlags::NODATACOW),
        "normal.bin should not have NODATACOW flag"
    );
    assert!(
        !normal_inode.flags.contains(InodeFlags::NODATASUM),
        "normal.bin should not have NODATASUM flag"
    );
}

// --- Reflink tests ---

/// --reflink should produce a valid filesystem image with file data intact.
/// This test only runs if the temp directory supports FICLONERANGE.
#[test]
fn rootdir_reflink_produces_valid_image() {
    let rootdir = tempdir().unwrap();
    let big_data = vec![0x55u8; 8192];
    write(rootdir.path().join("data.bin"), &big_data).unwrap();

    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.devices[0].path = image.path().to_path_buf();

    let result = make_btrfs_with_rootdir(
        &cfg,
        rootdir.path(),
        CompressConfig::default(),
        &[],
        &[],
        RootdirOptions::new().reflink(true),
    );

    match result {
        Ok(()) => {
            // Reflink succeeded — verify the image has a valid superblock.
            let mut file = File::open(image.path()).unwrap();
            let sb = read_superblock(&mut file, 0).unwrap();
            assert!(sb.magic_is_valid());
            assert_eq!(sb.fsid, cfg.fs_uuid);
        }
        Err(e) => {
            let msg = format!("{e:#}");
            // FICLONERANGE fails on filesystems that don't support it —
            // skip gracefully rather than failing the test.
            if msg.contains("FICLONERANGE") {
                eprintln!(
                    "skipping reflink test: filesystem does not support \
                     FICLONERANGE ({msg})"
                );
                return;
            }
            panic!("unexpected error: {e:#}");
        }
    }
}

/// LZO compression should produce a valid filesystem image.
#[test]
fn rootdir_lzo_compression_produces_valid_image() {
    let rootdir = tempdir().unwrap();
    // Compressible data (repeated bytes compress well with LZO).
    let big_data = vec![0x42u8; 8192];
    write(rootdir.path().join("data.bin"), &big_data).unwrap();
    // Small inline file too.
    write(rootdir.path().join("small.txt"), "hello LZO").unwrap();

    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    cfg.devices[0].path = image.path().to_path_buf();

    let compress = CompressConfig {
        algorithm: CompressAlgorithm::Lzo,
        level: None,
    };

    make_btrfs_with_rootdir(
        &cfg,
        rootdir.path(),
        compress,
        &[],
        &[],
        RootdirOptions::default(),
    )
    .unwrap();

    // Verify the image has a valid superblock.
    let mut file = File::open(image.path()).unwrap();
    let sb = read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
    assert_eq!(sb.fsid, cfg.fs_uuid);
}

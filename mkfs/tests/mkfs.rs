use btrfs_mkfs::{
    args::{Feature, FeatureArg, Profile},
    mkfs::{self, DeviceInfo, MkfsConfig},
    write::ChecksumType,
};
use std::{
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
};
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
    }
}

/// Minimum valid image size: system (5 MiB) + metadata DUP (2*32 MiB) + data (64 MiB) = 133 MiB.
/// Use 256 MiB for comfortable headroom.
const MIN_SIZE: u64 = 256 * 1024 * 1024;

/// Set the device path in the config and call make_btrfs.
fn make_btrfs_on(image: &tempfile::NamedTempFile, cfg: &mut MkfsConfig) {
    cfg.devices[0].path = image.path().to_path_buf();
    mkfs::make_btrfs(cfg).unwrap();
}

fn make_btrfs_on_err(
    image: &tempfile::NamedTempFile,
    cfg: &mut MkfsConfig,
) -> anyhow::Error {
    cfg.devices[0].path = image.path().to_path_buf();
    mkfs::make_btrfs(cfg).unwrap_err()
}

fn make_btrfs_two_devices(
    img1: &tempfile::NamedTempFile,
    img2: &tempfile::NamedTempFile,
    cfg: &mut MkfsConfig,
) {
    cfg.devices[0].path = img1.path().to_path_buf();
    cfg.devices[1].path = img2.path().to_path_buf();
    mkfs::make_btrfs(cfg).unwrap();
}

fn create_image(size: u64) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
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

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
}

#[test]
fn mkfs_superblock_has_correct_uuid() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.fsid, cfg.fs_uuid);
}

#[test]
fn mkfs_superblock_has_correct_sizes() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
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

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.label, "test-label");
}

#[test]
fn mkfs_superblock_generation_is_one() {
    let image = create_image(MIN_SIZE);
    let mut cfg = test_config(MIN_SIZE);
    make_btrfs_on(&image, &mut cfg);

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
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

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
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

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
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
    let mut f1 = std::fs::File::open(img1.path()).unwrap();
    let sb1 = btrfs_disk::superblock::read_superblock(&mut f1, 0).unwrap();
    assert!(sb1.magic_is_valid());

    let mut f2 = std::fs::File::open(img2.path()).unwrap();
    let sb2 = btrfs_disk::superblock::read_superblock(&mut f2, 0).unwrap();
    assert!(sb2.magic_is_valid());
}

#[test]
fn mkfs_raid1_superblocks_share_uuid() {
    let per_dev = MIN_SIZE;
    let img1 = create_image(per_dev);
    let img2 = create_image(per_dev);
    let mut cfg = test_config_two_devices(per_dev);
    make_btrfs_two_devices(&img1, &img2, &mut cfg);

    let mut f1 = std::fs::File::open(img1.path()).unwrap();
    let sb1 = btrfs_disk::superblock::read_superblock(&mut f1, 0).unwrap();
    let mut f2 = std::fs::File::open(img2.path()).unwrap();
    let sb2 = btrfs_disk::superblock::read_superblock(&mut f2, 0).unwrap();

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

    let mut f1 = std::fs::File::open(img1.path()).unwrap();
    let sb1 = btrfs_disk::superblock::read_superblock(&mut f1, 0).unwrap();
    let mut f2 = std::fs::File::open(img2.path()).unwrap();
    let sb2 = btrfs_disk::superblock::read_superblock(&mut f2, 0).unwrap();

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

    let mut f1 = std::fs::File::open(img1.path()).unwrap();
    let sb1 = btrfs_disk::superblock::read_superblock(&mut f1, 0).unwrap();
    assert_eq!(sb1.total_bytes, 2 * per_dev);
}

use btrfs_mkfs::{
    args::{Feature, FeatureArg},
    mkfs::{self, MkfsConfig},
};
use std::io::{Seek, SeekFrom, Write};
use uuid::Uuid;

fn test_config(total_bytes: u64) -> MkfsConfig {
    MkfsConfig {
        nodesize: 16384,
        sectorsize: 4096,
        total_bytes,
        label: None,
        fs_uuid: Uuid::from_bytes([0xDE; 16]),
        dev_uuid: Uuid::from_bytes([0xAB; 16]),
        chunk_tree_uuid: Uuid::from_bytes([0xCD; 16]),
        incompat_flags: MkfsConfig::default_incompat_flags(),
        compat_ro_flags: MkfsConfig::default_compat_ro_flags(),
    }
}

/// Minimum valid image size: system group offset (1 MiB) + system group size (4 MiB).
const MIN_SIZE: u64 = 5 * 1024 * 1024;

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
    let cfg = test_config(MIN_SIZE);
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert!(sb.magic_is_valid());
}

#[test]
fn mkfs_superblock_has_correct_uuid() {
    let image = create_image(MIN_SIZE);
    let cfg = test_config(MIN_SIZE);
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.fsid, cfg.fs_uuid);
}

#[test]
fn mkfs_superblock_has_correct_sizes() {
    let image = create_image(MIN_SIZE);
    let cfg = test_config(MIN_SIZE);
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

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
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.label, "test-label");
}

#[test]
fn mkfs_superblock_generation_is_one() {
    let image = create_image(MIN_SIZE);
    let cfg = test_config(MIN_SIZE);
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.generation, 1);
}

#[test]
fn mkfs_too_small_fails() {
    let image = create_image(1024 * 1024); // 1 MiB, too small
    let cfg = test_config(1024 * 1024);
    let err = mkfs::make_btrfs(image.path(), &cfg).unwrap_err();
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
    let cfg = test_config(MIN_SIZE);
    mkfs::make_btrfs(image.path(), &cfg).unwrap();
    assert!(mkfs::has_btrfs_superblock(image.path()));
}

#[test]
fn minimum_device_size_matches_expected() {
    let min = mkfs::minimum_device_size(16384);
    assert_eq!(min, MIN_SIZE);
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
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

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
    // 64 KiB nodesize needs a larger image: the system group must fit
    // all tree blocks at 64K each.
    let size = 8 * 1024 * 1024;
    let image = create_image(size);
    let mut cfg = test_config(size);
    cfg.nodesize = 65536;
    mkfs::make_btrfs(image.path(), &cfg).unwrap();

    let mut file = std::fs::File::open(image.path()).unwrap();
    let sb = btrfs_disk::superblock::read_superblock(&mut file, 0).unwrap();
    assert_eq!(sb.nodesize, 65536);
    assert!(sb.magic_is_valid());
}

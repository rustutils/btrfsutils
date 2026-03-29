//! # Mkfs: orchestrate filesystem creation
//!
//! Builds all tree blocks and the superblock, then writes them to disk.
//! This is the Rust equivalent of `make_btrfs()` in the C reference.

use crate::{
    items,
    layout::{BlockLayout, SYSTEM_GROUP_OFFSET, SYSTEM_GROUP_SIZE, TreeId},
    superblock::SuperblockBuilder,
    tree::{Key, LeafBuilder, LeafHeader},
    write::{self, SUPER_INFO_OFFSET},
};
use anyhow::{Context, Result, bail};
use btrfs_disk::raw;
use std::{
    fs::{File, OpenOptions},
    mem,
    os::unix::fs::FileTypeExt,
    path::Path,
    time::SystemTime,
};
use uuid::Uuid;

/// Configuration for filesystem creation.
pub struct MkfsConfig {
    pub nodesize: u32,
    pub sectorsize: u32,
    pub total_bytes: u64,
    pub label: Option<String>,
    pub fs_uuid: Uuid,
    pub dev_uuid: Uuid,
    pub chunk_tree_uuid: Uuid,
    pub incompat_flags: u64,
    pub compat_ro_flags: u64,
}

impl MkfsConfig {
    /// Default feature flags matching current btrfs-progs defaults.
    pub fn default_incompat_flags() -> u64 {
        raw::BTRFS_FEATURE_INCOMPAT_MIXED_BACKREF as u64
            | raw::BTRFS_FEATURE_INCOMPAT_BIG_METADATA as u64
            | raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64
            | raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64
            | raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64
    }

    pub fn default_compat_ro_flags() -> u64 {
        raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64
            | raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID as u64
        // block-group-tree requires a separate tree block; will be added
        // in a future phase.
    }

    /// Apply user-specified feature flags on top of defaults.
    pub fn apply_features(
        &mut self,
        features: &[crate::args::FeatureArg],
    ) -> Result<()> {
        use crate::args::Feature;

        for f in features {
            if f.feature == Feature::ListAll {
                eprintln!(
                    "Default features:   extref skinny-metadata no-holes free-space-tree"
                );
                eprintln!(
                    "Available features: mixed-bg extref raid56 skinny-metadata no-holes"
                );
                eprintln!(
                    "                    free-space-tree block-group-tree"
                );
                std::process::exit(0);
            }

            let (incompat_bit, compat_ro_bit): (Option<u64>, Option<u64>) =
                match f.feature {
                    Feature::MixedBg => (
                        Some(raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS as u64),
                        None,
                    ),
                    Feature::Extref => (
                        Some(
                            raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64,
                        ),
                        None,
                    ),
                    Feature::Raid56 => (
                        Some(raw::BTRFS_FEATURE_INCOMPAT_RAID56 as u64),
                        None,
                    ),
                    Feature::SkinnyMetadata => (
                        Some(
                            raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64,
                        ),
                        None,
                    ),
                    Feature::NoHoles => (
                        Some(raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64),
                        None,
                    ),
                    Feature::FreeSpaceTree => (
                        None,
                        Some(
                            raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64
                                | raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID
                                    as u64,
                        ),
                    ),
                    Feature::BlockGroupTree => (
                        None,
                        Some(
                            raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE
                                as u64,
                        ),
                    ),
                    Feature::Zoned
                    | Feature::Quota
                    | Feature::Squota
                    | Feature::RaidStripeTree => {
                        bail!(
                            "feature '{}' is not yet supported by mkfs",
                            f.feature
                        );
                    }
                    Feature::ListAll => unreachable!(),
                };

            if f.enabled {
                if let Some(bit) = incompat_bit {
                    self.incompat_flags |= bit;
                }
                if let Some(bit) = compat_ro_bit {
                    self.compat_ro_flags |= bit;
                }
            } else {
                if let Some(bit) = incompat_bit {
                    self.incompat_flags &= !bit;
                }
                if let Some(bit) = compat_ro_bit {
                    self.compat_ro_flags &= !bit;
                }
            }
        }
        Ok(())
    }

    pub fn skinny_metadata(&self) -> bool {
        self.incompat_flags & raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64
            != 0
    }

    pub fn has_free_space_tree(&self) -> bool {
        self.compat_ro_flags
            & raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64
            != 0
    }

    pub fn has_block_group_tree(&self) -> bool {
        self.compat_ro_flags
            & raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE as u64
            != 0
    }
}

/// Create a btrfs filesystem on the given device or image file.
pub fn make_btrfs(path: &Path, cfg: &MkfsConfig) -> Result<()> {
    let min_size = SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;
    if cfg.total_bytes < min_size {
        bail!(
            "device too small: {} bytes, need at least {} bytes",
            cfg.total_bytes,
            min_size
        );
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let layout = BlockLayout::new(cfg.nodesize);
    let generation = 1u64;

    let leaf_header = |tree: TreeId| LeafHeader {
        fsid: cfg.fs_uuid,
        chunk_tree_uuid: cfg.chunk_tree_uuid,
        generation,
        owner: tree.objectid(),
        bytenr: layout.block_addr(tree),
    };

    // Build all 8 tree blocks.
    let root_tree = build_root_tree(cfg, &layout, &leaf_header)?;
    let extent_tree = build_extent_tree(cfg, &layout, &leaf_header)?;
    let chunk_tree = build_chunk_tree(cfg, &layout, &leaf_header)?;
    let dev_tree = build_dev_tree(cfg, &layout, &leaf_header)?;
    let fs_tree = build_root_dir_tree(cfg, &leaf_header(TreeId::Fs))?;
    let csum_tree = build_empty_tree(cfg.nodesize, &leaf_header(TreeId::Csum));
    let free_space_tree = build_free_space_tree(cfg, &layout, &leaf_header)?;
    let data_reloc_tree =
        build_root_dir_tree(cfg, &leaf_header(TreeId::DataReloc))?;

    // Write tree blocks to disk.
    let trees = [
        (TreeId::Root, root_tree),
        (TreeId::Extent, extent_tree),
        (TreeId::Chunk, chunk_tree),
        (TreeId::Dev, dev_tree),
        (TreeId::Fs, fs_tree),
        (TreeId::Csum, csum_tree),
        (TreeId::FreeSpace, free_space_tree),
        (TreeId::DataReloc, data_reloc_tree),
    ];

    for (tree_id, mut block) in trees {
        write::fill_csum(&mut block);
        write::pwrite_all(&file, &block, layout.block_addr(tree_id))
            .with_context(|| {
                format!("failed to write {tree_id:?} tree block")
            })?;
    }

    // Build and write the superblock.
    let superblock = build_superblock(cfg, &layout)?;
    write::pwrite_all(&file, &superblock, SUPER_INFO_OFFSET)
        .context("failed to write superblock")?;

    file.sync_all().context("fsync failed")?;
    Ok(())
}

fn build_root_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Root));
    let generation = 1u64;

    // The root tree contains ROOT_ITEM entries for every other tree,
    // sorted by objectid. We skip Root (self) and Chunk (bootstrapped
    // via the superblock's chunk_root pointer, though we still write a
    // ROOT_ITEM for it).

    // Collect entries sorted by objectid.
    struct RootEntry {
        objectid: u64,
        bytenr: u64,
        is_fs_tree: bool,
    }

    let mut entries: Vec<RootEntry> = TreeId::ROOT_ITEM_TREES
        .iter()
        .map(|&tree| RootEntry {
            objectid: tree.objectid(),
            bytenr: layout.block_addr(tree),
            is_fs_tree: tree == TreeId::Fs,
        })
        .collect();

    entries.sort_by_key(|e| e.objectid);

    for entry in &entries {
        let key = Key::new(entry.objectid, raw::BTRFS_ROOT_ITEM_KEY as u8, 0);

        let mut data = items::root_item(
            generation,
            entry.bytenr,
            raw::BTRFS_FIRST_FREE_OBJECTID as u64,
        );

        // The FS tree root item gets a UUID, timestamps, and
        // BTRFS_INODE_ROOT_ITEM_INIT flag.
        if entry.is_fs_tree {
            let uuid = Uuid::new_v4();
            let inode_size = mem::size_of::<raw::btrfs_inode_item>();
            let uuid_off = mem::offset_of!(raw::btrfs_root_item, uuid);
            btrfs_disk::util::write_uuid(&mut data, uuid_off, &uuid);

            // Set inode flags = BTRFS_INODE_ROOT_ITEM_INIT
            let flags_off = mem::offset_of!(raw::btrfs_inode_item, flags);
            btrfs_disk::util::write_le_u64(
                &mut data,
                flags_off,
                raw::BTRFS_INODE_ROOT_ITEM_INIT as u64,
            );

            // Set inode.size = 3 (C reference convention)
            btrfs_disk::util::write_le_u64(&mut data, 16, 3);
            // Set inode.nbytes = nodesize
            btrfs_disk::util::write_le_u64(&mut data, 24, cfg.nodesize as u64);

            // Set bytes_used = nodesize (in root item, not inode)
            btrfs_disk::util::write_le_u64(
                &mut data,
                inode_size + 32,
                cfg.nodesize as u64,
            );

            // Set timestamps: otime and ctime
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let ctime_off = mem::offset_of!(raw::btrfs_root_item, ctime);
            let otime_off = mem::offset_of!(raw::btrfs_root_item, otime);
            let ts_size = mem::size_of::<raw::btrfs_timespec>();
            btrfs_disk::util::write_le_u64(&mut data, otime_off, now);
            btrfs_disk::util::write_le_u32(&mut data, otime_off + 8, 0);
            btrfs_disk::util::write_le_u64(&mut data, ctime_off, now);
            btrfs_disk::util::write_le_u32(&mut data, ctime_off + 8, 0);
            // Zero stime and rtime (already zero).
            let _ = ts_size; // used conceptually for offset calculation
        }

        leaf.push(key, &data)
            .map_err(|e| anyhow::anyhow!("root tree: {e}"))?;
    }

    Ok(leaf.finish())
}

fn build_extent_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Extent));
    let generation = 1u64;
    let skinny = cfg.skinny_metadata();

    // Items must be sorted by key. The extent tree contains:
    // 1. For each tree block: METADATA_ITEM + TREE_BLOCK_REF
    // 2. BLOCK_GROUP_ITEM for the system chunk (if not using block-group-tree)
    //
    // Block addresses are ascending (sequential layout), so we just need
    // to insert the BLOCK_GROUP_ITEM at the right position.

    let bg_key = Key::new(
        SYSTEM_GROUP_OFFSET,
        raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
        SYSTEM_GROUP_SIZE,
    );
    let bg_data = items::block_group_item(
        layout.total_used(),
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
    );
    let add_block_group = !cfg.has_block_group_tree();
    let mut bg_inserted = false;

    for &tree in &TreeId::ALL {
        let addr = layout.block_addr(tree);

        // Insert block group item before the first tree block that
        // has an address above the system group offset.
        if add_block_group && !bg_inserted && addr > SYSTEM_GROUP_OFFSET {
            leaf.push(bg_key, &bg_data)
                .map_err(|e| anyhow::anyhow!("extent tree: {e}"))?;
            bg_inserted = true;
        }

        // METADATA_ITEM (skinny) or EXTENT_ITEM
        let item_type = if skinny {
            raw::BTRFS_METADATA_ITEM_KEY as u8
        } else {
            raw::BTRFS_EXTENT_ITEM_KEY as u8
        };
        let offset = if skinny { 0 } else { cfg.nodesize as u64 };
        let key = Key::new(addr, item_type, offset);
        let data = items::extent_item(1, generation, skinny);
        leaf.push(key, &data)
            .map_err(|e| anyhow::anyhow!("extent tree: {e}"))?;

        // TREE_BLOCK_REF (empty data, offset = owning tree objectid)
        let ref_key = Key::new(
            addr,
            raw::BTRFS_TREE_BLOCK_REF_KEY as u8,
            tree.objectid(),
        );
        leaf.push_empty(ref_key)
            .map_err(|e| anyhow::anyhow!("extent tree: {e}"))?;
    }

    Ok(leaf.finish())
}

fn build_chunk_tree(
    cfg: &MkfsConfig,
    _layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Chunk));

    // DEV_ITEM for device 1
    let dev_data = items::dev_item(
        1,
        cfg.total_bytes,
        SYSTEM_GROUP_SIZE,
        cfg.sectorsize,
        &cfg.dev_uuid,
        &cfg.fs_uuid,
    );
    let dev_key = Key::new(
        raw::BTRFS_DEV_ITEMS_OBJECTID as u64,
        raw::BTRFS_DEV_ITEM_KEY as u8,
        1,
    );
    leaf.push(dev_key, &dev_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    // CHUNK_ITEM for the system chunk
    let chunk_data = items::chunk_item_single(
        SYSTEM_GROUP_SIZE,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        cfg.sectorsize,
        1,
        SYSTEM_GROUP_OFFSET,
        &cfg.dev_uuid,
    );
    let chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    leaf.push(chunk_key, &chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_dev_tree(
    cfg: &MkfsConfig,
    _layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Dev));

    // DEV_STATS (PERSISTENT_ITEM) for device 1 — all zeros
    let stats_key = Key::new(
        raw::BTRFS_DEV_STATS_OBJECTID as u64,
        raw::BTRFS_PERSISTENT_ITEM_KEY as u8,
        1,
    );
    leaf.push(stats_key, &items::dev_stats_zeroed())
        .map_err(|e| anyhow::anyhow!("dev tree: {e}"))?;

    // DEV_EXTENT for the system chunk
    let extent_data = items::dev_extent(
        raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        SYSTEM_GROUP_OFFSET,
        SYSTEM_GROUP_SIZE,
        &cfg.chunk_tree_uuid,
    );
    let extent_key = Key::new(
        1, // devid
        raw::BTRFS_DEV_EXTENT_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    leaf.push(extent_key, &extent_data)
        .map_err(|e| anyhow::anyhow!("dev tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_empty_tree(nodesize: u32, header: &LeafHeader) -> Vec<u8> {
    LeafBuilder::new(nodesize, header).finish()
}

/// Build a tree with a root directory inode (objectid 256).
///
/// Used for FS_TREE and DATA_RELOC_TREE — the kernel requires both to
/// have at least an inode item for the root directory.
fn build_root_dir_tree(
    cfg: &MkfsConfig,
    header: &LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, header);
    let generation = 1u64;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // INODE_ITEM for objectid 256 (BTRFS_FIRST_FREE_OBJECTID)
    let inode_key = Key::new(
        raw::BTRFS_FIRST_FREE_OBJECTID as u64,
        raw::BTRFS_INODE_ITEM_KEY as u8,
        0,
    );
    let inode_data =
        items::inode_item_dir(generation, cfg.nodesize as u64, now);
    leaf.push(inode_key, &inode_data)
        .map_err(|e| anyhow::anyhow!("root dir tree: {e}"))?;

    // INODE_REF for objectid 256, parent 256, name ".."
    let ref_key = Key::new(
        raw::BTRFS_FIRST_FREE_OBJECTID as u64,
        raw::BTRFS_INODE_REF_KEY as u8,
        raw::BTRFS_FIRST_FREE_OBJECTID as u64,
    );
    let ref_data = items::inode_ref(0, b"..");
    leaf.push(ref_key, &ref_data)
        .map_err(|e| anyhow::anyhow!("root dir tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_free_space_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    if !cfg.has_free_space_tree() {
        return Ok(build_empty_tree(
            cfg.nodesize,
            &leaf_header(TreeId::FreeSpace),
        ));
    }

    let mut leaf =
        LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::FreeSpace));

    let free_start = SYSTEM_GROUP_OFFSET + layout.total_used();
    let free_length = SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE - free_start;

    // FREE_SPACE_INFO for the system block group
    let info_key = Key::new(
        SYSTEM_GROUP_OFFSET,
        raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
        SYSTEM_GROUP_SIZE,
    );
    leaf.push(info_key, &items::free_space_info(1, 0))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    // FREE_SPACE_EXTENT for the unallocated space in the system group
    let extent_key = Key::new(
        free_start,
        raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
        free_length,
    );
    leaf.push_empty(extent_key)
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_superblock(cfg: &MkfsConfig, layout: &BlockLayout) -> Result<Vec<u8>> {
    let generation = 1u64;

    // Build the sys_chunk_array: disk_key + chunk_item bytes.
    let chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    let chunk_data = items::chunk_item_single(
        SYSTEM_GROUP_SIZE,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        cfg.sectorsize,
        1,
        SYSTEM_GROUP_OFFSET,
        &cfg.dev_uuid,
    );
    let mut sys_chunk_array = items::disk_key(&chunk_key);
    sys_chunk_array.extend_from_slice(&chunk_data);

    // Build the dev_item for the superblock.
    let dev_item_bytes = items::dev_item(
        1,
        cfg.total_bytes,
        SYSTEM_GROUP_SIZE,
        cfg.sectorsize,
        &cfg.dev_uuid,
        &cfg.fs_uuid,
    );

    // cache_generation: 0 if free-space-tree is enabled, u64::MAX otherwise.
    let cache_generation = if cfg.has_free_space_tree() {
        0
    } else {
        u64::MAX
    };

    let mut sb = SuperblockBuilder::new();
    sb.set_bytenr(SUPER_INFO_OFFSET)
        .set_magic()
        .set_fsid(&cfg.fs_uuid)
        .set_generation(generation)
        .set_root(layout.block_addr(TreeId::Root))
        .set_chunk_root(layout.block_addr(TreeId::Chunk))
        .set_chunk_root_generation(generation)
        .set_total_bytes(cfg.total_bytes)
        .set_bytes_used(layout.total_used())
        .set_root_dir_objectid(raw::BTRFS_FIRST_FREE_OBJECTID as u64)
        .set_num_devices(1)
        .set_sectorsize(cfg.sectorsize)
        .set_nodesize(cfg.nodesize)
        .set_stripesize(cfg.sectorsize)
        .set_incompat_flags(cfg.incompat_flags)
        .set_compat_ro_flags(cfg.compat_ro_flags)
        .set_csum_type(0) // CRC32C
        .set_cache_generation(cache_generation)
        .set_dev_item(&dev_item_bytes)
        .set_sys_chunk_array(&sys_chunk_array);

    if let Some(label) = &cfg.label {
        sb.set_label(label);
    }

    let mut buf = sb.finish();
    write::fill_csum(&mut buf);
    Ok(buf.to_vec())
}

// From linux/fs.h: #define BLKGETSIZE64 _IOR(0x12, 114, size_t)
nix::ioctl_read!(blk_getsize64, 0x12, 114, u64);

// From linux/fs.h: #define BLKDISCARD _IO(0x12, 119)
nix::ioctl_write_ptr!(blk_discard, 0x12, 119, [u64; 2]);

/// Get the size of a device or file in bytes.
pub fn device_size(path: &Path) -> Result<u64> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?;

    if metadata.file_type().is_block_device() {
        let file = File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut size: u64 = 0;
        unsafe {
            blk_getsize64(
                std::os::unix::io::AsRawFd::as_raw_fd(&file),
                &mut size,
            )
        }
        .with_context(|| {
            format!("BLKGETSIZE64 failed on {}", path.display())
        })?;
        Ok(size)
    } else {
        Ok(metadata.len())
    }
}

/// Check if the device already contains a btrfs filesystem.
pub fn has_btrfs_superblock(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    match btrfs_disk::superblock::read_superblock(&mut file, 0) {
        Ok(sb) => sb.magic_is_valid(),
        Err(_) => false,
    }
}

/// Check if a device is currently mounted (appears in /proc/mounts).
pub fn is_device_mounted(path: &Path) -> Result<bool> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("cannot resolve path '{}'", path.display()))?;
    let canonical_str = canonical.to_string_lossy();
    let contents = std::fs::read_to_string("/proc/mounts")
        .context("failed to read /proc/mounts")?;
    Ok(contents
        .lines()
        .any(|line| line.split_whitespace().next() == Some(&*canonical_str)))
}

/// Issue BLKDISCARD (TRIM) on the entire device.
pub fn discard_device(path: &Path, size: u64) -> Result<()> {
    let file =
        OpenOptions::new().write(true).open(path).with_context(|| {
            format!("failed to open '{}' for discard", path.display())
        })?;
    let range: [u64; 2] = [0, size];
    unsafe {
        blk_discard(std::os::unix::io::AsRawFd::as_raw_fd(&file), &range)
    }
    .with_context(|| format!("BLKDISCARD failed on {}", path.display()))?;
    Ok(())
}

/// Minimum filesystem size: system group offset + system group size.
pub fn minimum_device_size(nodesize: u32) -> u64 {
    // Must fit the superblock (at 64K), the system group (at 1M, 4M long),
    // and all tree blocks within the system group.
    let _ = nodesize;
    SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE
}

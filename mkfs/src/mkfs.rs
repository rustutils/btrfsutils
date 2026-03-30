//! # Mkfs: orchestrate filesystem creation
//!
//! Builds all tree blocks and the superblock, then writes them to disk.
//! This is the Rust equivalent of `make_btrfs()` in the C reference.

use crate::{
    items,
    layout::{
        BlockLayout, ChunkDevice, ChunkLayout, SYSTEM_GROUP_OFFSET,
        SYSTEM_GROUP_SIZE, StripeInfo, TreeId,
    },
    superblock::SuperblockBuilder,
    tree::{Key, LeafBuilder, LeafHeader},
    write,
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

/// Information about a single device in the filesystem.
pub struct DeviceInfo {
    pub devid: u64,
    pub path: std::path::PathBuf,
    pub total_bytes: u64,
    pub dev_uuid: Uuid,
}

/// Configuration for filesystem creation.
pub struct MkfsConfig {
    pub nodesize: u32,
    pub sectorsize: u32,
    pub devices: Vec<DeviceInfo>,
    pub label: Option<String>,
    pub fs_uuid: Uuid,
    pub chunk_tree_uuid: Uuid,
    pub incompat_flags: u64,
    pub compat_ro_flags: u64,
    pub data_profile: crate::args::Profile,
    pub metadata_profile: crate::args::Profile,
    pub csum_type: crate::write::ChecksumType,
    /// Override for the current time (seconds since epoch). Used for
    /// deterministic output in tests. None means use SystemTime::now().
    pub creation_time: Option<u64>,
}

impl MkfsConfig {
    /// Total bytes across all devices.
    pub fn total_bytes(&self) -> u64 {
        self.devices.iter().map(|d| d.total_bytes).sum()
    }

    /// Number of devices.
    pub fn num_devices(&self) -> u64 {
        self.devices.len() as u64
    }

    /// The primary device (devid 1).
    pub fn primary_device(&self) -> &DeviceInfo {
        &self.devices[0]
    }

    /// Current time in seconds since epoch (uses override if set).
    fn now_secs(&self) -> u64 {
        self.creation_time.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        })
    }
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

    /// Default compat_ro feature flags (free-space-tree + block-group-tree).
    pub fn default_compat_ro_flags() -> u64 {
        raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64
            | raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID as u64
            | raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE as u64
    }

    /// Apply user-specified feature flags (`-O` arguments) on top of defaults.
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

    /// Whether the skinny-metadata incompat feature is enabled.
    pub fn skinny_metadata(&self) -> bool {
        self.incompat_flags & raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64
            != 0
    }

    /// Whether the free-space-tree compat_ro feature is enabled.
    pub fn has_free_space_tree(&self) -> bool {
        self.compat_ro_flags
            & raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64
            != 0
    }

    /// Whether the block-group-tree compat_ro feature is enabled.
    pub fn has_block_group_tree(&self) -> bool {
        self.compat_ro_flags
            & raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE as u64
            != 0
    }
}

/// Create a btrfs filesystem on one or more devices.
pub fn make_btrfs(cfg: &MkfsConfig) -> Result<()> {
    // Validate nodesize/sectorsize.
    if !cfg.sectorsize.is_power_of_two() || cfg.sectorsize < 4096 {
        bail!(
            "invalid sectorsize {}: must be a power of 2 >= 4096",
            cfg.sectorsize
        );
    }
    if !cfg.nodesize.is_power_of_two()
        || cfg.nodesize < cfg.sectorsize
        || cfg.nodesize > 65536
    {
        bail!(
            "invalid nodesize {}: must be a power of 2, \
             >= sectorsize ({}), and <= 64K",
            cfg.nodesize,
            cfg.sectorsize
        );
    }
    // Mixed block groups require nodesize == sectorsize.
    if cfg.incompat_flags & raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS as u64 != 0
        && cfg.nodesize != cfg.sectorsize
    {
        bail!(
            "mixed block groups require nodesize ({}) == sectorsize ({})",
            cfg.nodesize,
            cfg.sectorsize
        );
    }

    let chunk_devs: Vec<ChunkDevice> = cfg
        .devices
        .iter()
        .map(|d| ChunkDevice {
            devid: d.devid,
            total_bytes: d.total_bytes,
            dev_uuid: d.dev_uuid,
        })
        .collect();
    let chunks =
        ChunkLayout::new(&chunk_devs, cfg.metadata_profile, cfg.data_profile);
    if chunks.is_none() {
        bail!(
            "device too small: {} bytes, need at least {} bytes",
            cfg.total_bytes(),
            minimum_device_size(cfg.nodesize)
        );
    }
    let chunks = chunks.unwrap();

    // Open all device files.
    let files: Vec<File> = cfg
        .devices
        .iter()
        .map(|dev| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dev.path)
                .with_context(|| {
                    format!("failed to open {}", dev.path.display())
                })
        })
        .collect::<Result<_>>()?;

    let layout = BlockLayout::new(cfg.nodesize, chunks.meta_logical);
    let generation = 1u64;

    let leaf_header = |tree: TreeId| LeafHeader {
        fsid: cfg.fs_uuid,
        chunk_tree_uuid: cfg.chunk_tree_uuid,
        generation,
        owner: tree.objectid(),
        bytenr: layout.block_addr(tree),
    };

    // Build tree blocks.
    let root_tree = build_root_tree(cfg, &layout, &leaf_header)?;
    let extent_tree = build_extent_tree(cfg, &layout, &chunks, &leaf_header)?;
    let chunk_tree = build_chunk_tree(cfg, &layout, &chunks, &leaf_header)?;
    let dev_tree = build_dev_tree(cfg, &chunks, &leaf_header)?;
    let fs_tree = build_root_dir_tree(cfg, &leaf_header(TreeId::Fs))?;
    let csum_tree = build_empty_tree(cfg.nodesize, &leaf_header(TreeId::Csum));
    let free_space_tree =
        build_free_space_tree(cfg, &layout, &chunks, &leaf_header)?;
    let data_reloc_tree =
        build_root_dir_tree(cfg, &leaf_header(TreeId::DataReloc))?;

    let mut trees: Vec<(TreeId, Vec<u8>)> = vec![
        (TreeId::Root, root_tree),
        (TreeId::Extent, extent_tree),
        (TreeId::Chunk, chunk_tree),
        (TreeId::Dev, dev_tree),
        (TreeId::Fs, fs_tree),
        (TreeId::Csum, csum_tree),
        (TreeId::FreeSpace, free_space_tree),
        (TreeId::DataReloc, data_reloc_tree),
    ];

    if cfg.has_block_group_tree() {
        let bg_tree =
            build_block_group_tree(cfg, &layout, &chunks, &leaf_header)?;
        trees.push((TreeId::BlockGroup, bg_tree));
    }

    // Write tree blocks to disk, routing each stripe to the correct device.
    for (tree_id, mut block) in trees {
        write::fill_csum(&mut block, cfg.csum_type);
        let logical = layout.block_addr(tree_id);
        for (devid, phys) in chunks.logical_to_physical(logical) {
            let file_idx = (devid - 1) as usize;
            write::pwrite_all(&files[file_idx], &block, phys)
                .with_context(|| {
                    format!(
                        "failed to write {tree_id:?} tree block to device {devid}"
                    )
                })?;
        }
    }

    // Build and write per-device superblocks at all mirror locations.
    for dev in &cfg.devices {
        let superblock = build_superblock(cfg, &layout, &chunks, dev)?;
        let file_idx = (dev.devid - 1) as usize;
        for mirror in 0..btrfs_disk::superblock::SUPER_MIRROR_MAX {
            let offset = btrfs_disk::superblock::super_mirror_offset(mirror);
            if offset + write::SUPER_INFO_SIZE as u64 > dev.total_bytes {
                break;
            }
            write::pwrite_all(&files[file_idx], &superblock, offset)
                .with_context(|| {
                    format!(
                        "failed to write superblock mirror {mirror} to device {}",
                        dev.devid
                    )
                })?;
        }
    }

    for file in &files {
        file.sync_all().context("fsync failed")?;
    }
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

    if cfg.has_block_group_tree() {
        entries.push(RootEntry {
            objectid: TreeId::BlockGroup.objectid(),
            bytenr: layout.block_addr(TreeId::BlockGroup),
            is_fs_tree: false,
        });
    }

    entries.sort_by_key(|e| e.objectid);

    for entry in &entries {
        let key = Key::new(entry.objectid, raw::BTRFS_ROOT_ITEM_KEY as u8, 0);

        let mut data = items::root_item(
            generation,
            entry.bytenr,
            raw::BTRFS_FIRST_FREE_OBJECTID as u64,
            cfg.nodesize,
        );

        // The FS tree root item gets a UUID, timestamps, and
        // BTRFS_INODE_ROOT_ITEM_INIT flag.
        if entry.is_fs_tree {
            // Derive FS tree UUID deterministically from fs_uuid by
            // flipping bits. In production fs_uuid is random, so this
            // is effectively random too.
            let mut uuid_bytes = *cfg.fs_uuid.as_bytes();
            for b in &mut uuid_bytes {
                *b ^= 0xFF;
            }
            let uuid = Uuid::from_bytes(uuid_bytes);
            let uuid_off = mem::offset_of!(raw::btrfs_root_item, uuid);
            data[uuid_off..uuid_off + 16].copy_from_slice(uuid.as_bytes());

            // Set inode flags = BTRFS_INODE_ROOT_ITEM_INIT
            let flags_off = mem::offset_of!(raw::btrfs_inode_item, flags);
            data[flags_off..flags_off + 8].copy_from_slice(
                &(raw::BTRFS_INODE_ROOT_ITEM_INIT as u64).to_le_bytes(),
            );

            // Set inode.size = 3 (C reference convention)
            data[16..24].copy_from_slice(&3u64.to_le_bytes());
            // Set inode.nbytes = nodesize
            data[24..32].copy_from_slice(&(cfg.nodesize as u64).to_le_bytes());

            // Set timestamps: otime and ctime
            let now = cfg.now_secs();
            let ctime_off = mem::offset_of!(raw::btrfs_root_item, ctime);
            let otime_off = mem::offset_of!(raw::btrfs_root_item, otime);
            data[otime_off..otime_off + 8].copy_from_slice(&now.to_le_bytes());
            data[otime_off + 8..otime_off + 12]
                .copy_from_slice(&0u32.to_le_bytes());
            data[ctime_off..ctime_off + 8].copy_from_slice(&now.to_le_bytes());
            data[ctime_off + 8..ctime_off + 12]
                .copy_from_slice(&0u32.to_le_bytes());
        }

        leaf.push(key, &data)
            .map_err(|e| anyhow::anyhow!("root tree: {e}"))?;
    }

    Ok(leaf.finish())
}

fn build_extent_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    chunks: &ChunkLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Extent));
    let generation = 1u64;
    let skinny = cfg.skinny_metadata();
    let add_block_group = !cfg.has_block_group_tree();

    // Collect all items into a Vec, then sort by key before pushing.
    // Tree blocks now span two different chunks (system and metadata),
    // so addresses are not monotonically increasing — we must sort.
    let mut extent_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // For each tree block: METADATA_ITEM with inline TREE_BLOCK_REF
    let mut all_trees: Vec<TreeId> = TreeId::ALL.to_vec();
    if cfg.has_block_group_tree() {
        all_trees.push(TreeId::BlockGroup);
    }

    for &tree in &all_trees {
        let addr = layout.block_addr(tree);

        let item_type = if skinny {
            raw::BTRFS_METADATA_ITEM_KEY as u8
        } else {
            raw::BTRFS_EXTENT_ITEM_KEY as u8
        };
        let offset = if skinny { 0 } else { cfg.nodesize as u64 };
        let key = Key::new(addr, item_type, offset);
        let data = items::extent_item(1, generation, skinny, tree.objectid());
        extent_items.push((key, data));
    }

    // BLOCK_GROUP_ITEMs for system, metadata, and data chunks
    if add_block_group {
        // System block group
        extent_items.push((
            Key::new(
                SYSTEM_GROUP_OFFSET,
                raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
                SYSTEM_GROUP_SIZE,
            ),
            items::block_group_item(
                layout.system_used(),
                raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
                raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
            ),
        ));

        // Metadata block group
        extent_items.push((
            Key::new(
                chunks.meta_logical,
                raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
                chunks.meta_size,
            ),
            items::block_group_item(
                layout.metadata_used(cfg.has_block_group_tree()),
                raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
                raw::BTRFS_BLOCK_GROUP_METADATA as u64
                    | cfg.metadata_profile.block_group_flag(),
            ),
        ));

        // Data block group
        extent_items.push((
            Key::new(
                chunks.data_logical,
                raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
                chunks.data_size,
            ),
            items::block_group_item(
                0,
                raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
                raw::BTRFS_BLOCK_GROUP_DATA as u64
                    | cfg.data_profile.block_group_flag(),
            ),
        ));
    }

    // Sort by key and push in order.
    extent_items.sort_by_key(|(k, _)| *k);

    for (key, data) in &extent_items {
        leaf.push(*key, data)
            .map_err(|e| anyhow::anyhow!("extent tree: {e}"))?;
    }

    Ok(leaf.finish())
}

fn build_chunk_tree(
    cfg: &MkfsConfig,
    _layout: &BlockLayout,
    chunks: &ChunkLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Chunk));

    // DEV_ITEM for each device (sorted by devid via insertion order)
    for dev in &cfg.devices {
        let dev_data = items::dev_item(
            dev.devid,
            dev.total_bytes,
            chunks.dev_bytes_used_for(dev.devid),
            cfg.sectorsize,
            &dev.dev_uuid,
            &cfg.fs_uuid,
        );
        let dev_key = Key::new(
            raw::BTRFS_DEV_ITEMS_OBJECTID as u64,
            raw::BTRFS_DEV_ITEM_KEY as u8,
            dev.devid,
        );
        leaf.push(dev_key, &dev_data)
            .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;
    }

    // CHUNK_ITEM for the system chunk (bootstrap: uses sectorsize for io_align)
    let dev1 = cfg.primary_device();
    let sys_stripe = StripeInfo {
        devid: dev1.devid,
        offset: SYSTEM_GROUP_OFFSET,
        dev_uuid: dev1.dev_uuid,
    };
    let sys_chunk_data = items::chunk_item_bootstrap(
        SYSTEM_GROUP_SIZE,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        cfg.sectorsize,
        &sys_stripe,
    );
    let sys_chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    leaf.push(sys_chunk_key, &sys_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    // CHUNK_ITEM for metadata chunk
    let meta_chunk_data = items::chunk_item(
        chunks.meta_size,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_METADATA as u64
            | cfg.metadata_profile.block_group_flag(),
        crate::layout::STRIPE_LEN as u32,
        crate::layout::STRIPE_LEN as u32,
        cfg.sectorsize,
        &chunks.meta_stripes,
    );
    let meta_chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        chunks.meta_logical,
    );
    leaf.push(meta_chunk_key, &meta_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    // CHUNK_ITEM for data chunk
    let data_chunk_data = items::chunk_item(
        chunks.data_size,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_DATA as u64
            | cfg.data_profile.block_group_flag(),
        crate::layout::STRIPE_LEN as u32,
        crate::layout::STRIPE_LEN as u32,
        cfg.sectorsize,
        &chunks.data_stripes,
    );
    let data_chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        chunks.data_logical,
    );
    leaf.push(data_chunk_key, &data_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_dev_tree(
    cfg: &MkfsConfig,
    chunks: &ChunkLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Dev));

    // Collect all items, then sort by key before pushing. Items span
    // multiple devids and offsets, so we must sort to satisfy btrfs's
    // sorted-key requirement.
    let mut dev_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // DEV_STATS (PERSISTENT_ITEM) for each device
    for dev in &cfg.devices {
        let stats_key = Key::new(
            raw::BTRFS_DEV_STATS_OBJECTID as u64,
            raw::BTRFS_PERSISTENT_ITEM_KEY as u8,
            dev.devid,
        );
        dev_items.push((stats_key, items::dev_stats_zeroed()));
    }

    // DEV_EXTENT for the system chunk (always device 1)
    let sys_extent = items::dev_extent(
        raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        SYSTEM_GROUP_OFFSET,
        SYSTEM_GROUP_SIZE,
        &cfg.chunk_tree_uuid,
    );
    dev_items.push((
        Key::new(1, raw::BTRFS_DEV_EXTENT_KEY as u8, SYSTEM_GROUP_OFFSET),
        sys_extent,
    ));

    // DEV_EXTENT for each metadata stripe
    for stripe in &chunks.meta_stripes {
        let ext = items::dev_extent(
            raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            chunks.meta_logical,
            chunks.meta_size,
            &cfg.chunk_tree_uuid,
        );
        dev_items.push((
            Key::new(
                stripe.devid,
                raw::BTRFS_DEV_EXTENT_KEY as u8,
                stripe.offset,
            ),
            ext,
        ));
    }

    // DEV_EXTENT for each data stripe
    for stripe in &chunks.data_stripes {
        let ext = items::dev_extent(
            raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            chunks.data_logical,
            chunks.data_size,
            &cfg.chunk_tree_uuid,
        );
        dev_items.push((
            Key::new(
                stripe.devid,
                raw::BTRFS_DEV_EXTENT_KEY as u8,
                stripe.offset,
            ),
            ext,
        ));
    }

    // Sort by key and push in order.
    dev_items.sort_by_key(|(k, _)| *k);

    for (key, data) in &dev_items {
        leaf.push(*key, data)
            .map_err(|e| anyhow::anyhow!("dev tree: {e}"))?;
    }

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

    let now = cfg.now_secs();

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
    chunks: &ChunkLayout,
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

    // System block group: free space after the chunk tree block
    let sys_free_start = SYSTEM_GROUP_OFFSET + layout.system_used();
    let sys_free_length =
        SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE - sys_free_start;

    let sys_info_key = Key::new(
        SYSTEM_GROUP_OFFSET,
        raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
        SYSTEM_GROUP_SIZE,
    );
    leaf.push(sys_info_key, &items::free_space_info(1, 0))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    let sys_extent_key = Key::new(
        sys_free_start,
        raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
        sys_free_length,
    );
    leaf.push_empty(sys_extent_key)
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    // Metadata block group: free space after the 7 tree blocks
    let meta_free_start =
        chunks.meta_logical + layout.metadata_used(cfg.has_block_group_tree());
    let meta_free_length =
        chunks.meta_size - layout.metadata_used(cfg.has_block_group_tree());

    let meta_info_key = Key::new(
        chunks.meta_logical,
        raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
        chunks.meta_size,
    );
    leaf.push(meta_info_key, &items::free_space_info(1, 0))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    let meta_extent_key = Key::new(
        meta_free_start,
        raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
        meta_free_length,
    );
    leaf.push_empty(meta_extent_key)
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    // Data block group: entirely free (used=0)
    let data_info_key = Key::new(
        chunks.data_logical,
        raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
        chunks.data_size,
    );
    leaf.push(data_info_key, &items::free_space_info(1, 0))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    let data_extent_key = Key::new(
        chunks.data_logical,
        raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
        chunks.data_size,
    );
    leaf.push_empty(data_extent_key)
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_block_group_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    chunks: &ChunkLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf =
        LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::BlockGroup));

    // System block group
    leaf.push(
        Key::new(
            SYSTEM_GROUP_OFFSET,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            SYSTEM_GROUP_SIZE,
        ),
        &items::block_group_item(
            layout.system_used(),
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    // Metadata block group
    leaf.push(
        Key::new(
            chunks.meta_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.meta_size,
        ),
        &items::block_group_item(
            layout.metadata_used(cfg.has_block_group_tree()),
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_METADATA as u64
                | cfg.metadata_profile.block_group_flag(),
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    // Data block group
    leaf.push(
        Key::new(
            chunks.data_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.data_size,
        ),
        &items::block_group_item(
            0,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_DATA as u64
                | cfg.data_profile.block_group_flag(),
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    Ok(leaf.finish())
}

fn build_superblock(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    chunks: &ChunkLayout,
    dev: &DeviceInfo,
) -> Result<Vec<u8>> {
    let generation = 1u64;

    // Build the sys_chunk_array: disk_key + chunk_item bytes.
    let chunk_key = Key::new(
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    let dev1 = cfg.primary_device();
    let chunk_data = items::chunk_item_bootstrap(
        SYSTEM_GROUP_SIZE,
        raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
        raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        cfg.sectorsize,
        &StripeInfo {
            devid: dev1.devid,
            offset: SYSTEM_GROUP_OFFSET,
            dev_uuid: dev1.dev_uuid,
        },
    );
    let mut sys_chunk_array = items::disk_key(&chunk_key);
    sys_chunk_array.extend_from_slice(&chunk_data);

    // Build the dev_item for this device's superblock.
    let dev_item_bytes = items::dev_item(
        dev.devid,
        dev.total_bytes,
        chunks.dev_bytes_used_for(dev.devid),
        cfg.sectorsize,
        &dev.dev_uuid,
        &cfg.fs_uuid,
    );

    // cache_generation: 0 if free-space-tree is enabled, u64::MAX otherwise.
    let cache_generation = if cfg.has_free_space_tree() {
        0
    } else {
        u64::MAX
    };

    let mut sb = SuperblockBuilder::new();
    sb.set_bytenr(write::SUPER_INFO_OFFSET)
        .set_magic()
        .set_fsid(&cfg.fs_uuid)
        .set_generation(generation)
        .set_root(layout.block_addr(TreeId::Root))
        .set_chunk_root(layout.block_addr(TreeId::Chunk))
        .set_chunk_root_generation(generation)
        .set_total_bytes(cfg.total_bytes())
        .set_bytes_used(
            layout.system_used()
                + layout.metadata_used(cfg.has_block_group_tree()),
        )
        .set_root_dir_objectid(raw::BTRFS_FIRST_FREE_OBJECTID as u64)
        .set_num_devices(cfg.num_devices())
        .set_sectorsize(cfg.sectorsize)
        .set_nodesize(cfg.nodesize)
        .set_stripesize(cfg.sectorsize)
        .set_incompat_flags(cfg.incompat_flags)
        .set_compat_ro_flags(cfg.compat_ro_flags)
        .set_csum_type(cfg.csum_type.to_raw())
        .set_cache_generation(cache_generation)
        .set_dev_item(&dev_item_bytes)
        .set_sys_chunk_array(&sys_chunk_array);

    if let Some(label) = &cfg.label {
        sb.set_label(label);
    }

    let mut buf = sb.finish();
    write::fill_csum(&mut buf, cfg.csum_type);
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

/// Minimum filesystem size.
///
/// Must fit the system group (5 MiB), metadata DUP (2 x 32 MiB minimum),
/// and data SINGLE (64 MiB minimum): 5 + 64 + 64 = 133 MiB.
pub fn minimum_device_size(nodesize: u32) -> u64 {
    let _ = nodesize;
    // System (5M) + 2 * min_meta (32M) + min_data (64M) = 133M.
    // ChunkLayout::new enforces this via data_phys + data_size <= total.
    SYSTEM_GROUP_OFFSET
        + SYSTEM_GROUP_SIZE
        + 2 * 32 * 1024 * 1024
        + 64 * 1024 * 1024
}

//! # Mkfs: orchestrate filesystem creation
//!
//! Builds all tree blocks and the superblock, then writes them to disk.
//! This is the Rust equivalent of `make_btrfs()` in the C reference.

use crate::{
    items,
    layout::{
        BlockAllocator, BlockLayout, ChunkDevice, ChunkLayout,
        SYSTEM_GROUP_OFFSET, SYSTEM_GROUP_SIZE, StripeInfo, TreeId,
    },
    rootdir,
    tree::{Key, LeafBuilder, LeafHeader},
    treebuilder::{TreeBlocks, TreeBuilder},
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

/// Options for `make_btrfs_with_rootdir`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RootdirOptions {
    /// Clone file extents via `FICLONERANGE` instead of copying bytes.
    pub reflink: bool,
    /// Truncate the image to minimal size after populating.
    pub shrink: bool,
}

impl RootdirOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn reflink(mut self, yes: bool) -> Self {
        self.reflink = yes;
        self
    }

    #[must_use]
    pub fn shrink(mut self, yes: bool) -> Self {
        self.shrink = yes;
        self
    }
}
use uuid::Uuid;

/// Bytes used in each block group, used to parameterize tree builders
/// that differ only in how used bytes are computed.
struct UsedBytes {
    system: u64,
    metadata: u64,
    data: u64,
}

/// Parameters that vary between the normal and rootdir superblock builders.
struct SuperblockParams {
    root_addr: u64,
    chunk_root_addr: u64,
    root_level: u8,
    bytes_used: u64,
}

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
    /// deterministic output in tests. None means use `SystemTime::now()`.
    pub creation_time: Option<u64>,
    /// Enable legacy quota tree (`-O quota`).
    pub quota: bool,
    /// Enable simple quota tree (`-O squota`).
    pub squota: bool,
}

impl MkfsConfig {
    /// Total bytes across all devices.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.devices.iter().map(|d| d.total_bytes).sum()
    }

    /// Number of devices.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // device count fits in u64
    pub fn num_devices(&self) -> u64 {
        self.devices.len() as u64
    }

    /// The primary device (devid 1).
    #[must_use]
    pub fn primary_device(&self) -> &DeviceInfo {
        &self.devices[0]
    }

    /// Current time in seconds since epoch (uses `creation_time`
    /// override if set). `pub(crate)` so `post_bootstrap` uses the
    /// same timestamp as mkfs's bootstrap.
    pub(crate) fn now_secs(&self) -> u64 {
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
    #[must_use]
    pub fn default_incompat_flags() -> u64 {
        u64::from(raw::BTRFS_FEATURE_INCOMPAT_MIXED_BACKREF)
            | u64::from(raw::BTRFS_FEATURE_INCOMPAT_BIG_METADATA)
            | u64::from(raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF)
            | u64::from(raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA)
            | u64::from(raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES)
    }

    /// Default `compat_ro` feature flags (free-space-tree + block-group-tree).
    #[must_use]
    pub fn default_compat_ro_flags() -> u64 {
        u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE)
            | u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID)
            | u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE)
    }

    /// Apply user-specified feature flags (`-O` arguments) on top of defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if an unsupported feature is requested.
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
                        Some(u64::from(raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS)),
                        None,
                    ),
                    Feature::Extref => (
                        Some(
                            u64::from(raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF),
                        ),
                        None,
                    ),
                    Feature::Raid56 => (
                        Some(u64::from(raw::BTRFS_FEATURE_INCOMPAT_RAID56)),
                        None,
                    ),
                    Feature::SkinnyMetadata => (
                        Some(
                            u64::from(raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA),
                        ),
                        None,
                    ),
                    Feature::NoHoles => (
                        Some(u64::from(raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES)),
                        None,
                    ),
                    Feature::FreeSpaceTree => (
                        None,
                        Some(
                            u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE)
                                | u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID),
                        ),
                    ),
                    Feature::BlockGroupTree => (
                        None,
                        Some(
                            u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE),
                        ),
                    ),
                    Feature::Quota => {
                        self.quota = f.enabled;
                        continue;
                    }
                    Feature::Squota => {
                        if f.enabled {
                            self.squota = true;
                        }
                        (
                            Some(u64::from(
                                raw::BTRFS_FEATURE_INCOMPAT_SIMPLE_QUOTA,
                            )),
                            None,
                        )
                    }
                    Feature::Zoned | Feature::RaidStripeTree => {
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

            // The kernel requires FST to be enabled for BGT. If the
            // user disables FST, also clear BGT to keep the
            // filesystem consistent (matches what btrfs-progs does
            // when the user passes `^free-space-tree`).
            if !f.enabled && f.feature == Feature::FreeSpaceTree {
                self.compat_ro_flags &=
                    !u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE);
            }
        }
        Ok(())
    }

    /// Whether the skinny-metadata incompat feature is enabled.
    #[must_use]
    pub fn skinny_metadata(&self) -> bool {
        self.incompat_flags
            & u64::from(raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA)
            != 0
    }

    /// Whether the free-space-tree `compat_ro` feature is enabled.
    #[must_use]
    pub fn has_free_space_tree(&self) -> bool {
        self.compat_ro_flags
            & u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE)
            != 0
    }

    /// Whether the block-group-tree `compat_ro` feature is enabled.
    #[must_use]
    pub fn has_block_group_tree(&self) -> bool {
        self.compat_ro_flags
            & u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE)
            != 0
    }

    /// Whether a quota tree should be created (`-O quota` or `-O squota`).
    #[must_use]
    pub fn has_quota_tree(&self) -> bool {
        self.quota || self.squota
    }

    /// Set incompat flags implied by the chosen RAID profiles.
    ///
    /// RAID5/6 requires the RAID56 incompat flag in the superblock.
    pub fn apply_profile_flags(&mut self) {
        use crate::args::Profile;
        if matches!(self.metadata_profile, Profile::Raid5 | Profile::Raid6)
            || matches!(self.data_profile, Profile::Raid5 | Profile::Raid6)
        {
            self.incompat_flags |=
                u64::from(raw::BTRFS_FEATURE_INCOMPAT_RAID56);
        }
    }
}

/// Create a btrfs filesystem on one or more devices.
///
/// # Errors
///
/// Returns an error if validation fails, the device is too small, or I/O fails.
///
/// # Panics
///
/// Panics if chunk layout computation succeeds but returns `None` unexpectedly.
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_possible_truncation)] // key types fit in u8, devid-1 fits usize
#[allow(clippy::cast_sign_loss)] // DATA_RELOC_TREE_OBJECTID is positive
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
    if cfg.incompat_flags & u64::from(raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS)
        != 0
        && cfg.nodesize != cfg.sectorsize
    {
        bail!(
            "mixed block groups require nodesize ({}) == sectorsize ({})",
            cfg.nodesize,
            cfg.sectorsize
        );
    }
    if cfg.quota && cfg.squota {
        bail!("cannot enable both quota and squota simultaneously");
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

    // Build the bootstrap tree blocks. mkfs writes only the four
    // always-present trees here (Root, Extent, Chunk, Dev); every
    // other tree (FS, csum, data-reloc, UUID, plus the optional
    // FST / BG-tree / quota when their features are enabled) is
    // created by `post_bootstrap` over the resulting image. The BG
    // items live in the extent tree at this stage and get migrated
    // to BGT inside post-bootstrap when the feature is on.
    let root_tree = build_root_tree(cfg, &layout, &leaf_header)?;
    let extent_tree = build_extent_tree(cfg, &layout, &chunks, &leaf_header)?;
    let chunk_tree = build_chunk_tree(cfg, &layout, &chunks, &leaf_header)?;
    let dev_tree = build_dev_tree(cfg, &chunks, &leaf_header)?;

    // (tree_id, block_data, logical_address)
    let trees: Vec<(TreeId, Vec<u8>, u64)> = vec![
        (TreeId::Root, root_tree, layout.block_addr(TreeId::Root)),
        (
            TreeId::Extent,
            extent_tree,
            layout.block_addr(TreeId::Extent),
        ),
        (TreeId::Chunk, chunk_tree, layout.block_addr(TreeId::Chunk)),
        (TreeId::Dev, dev_tree, layout.block_addr(TreeId::Dev)),
    ];

    // Initial bytes_used for the superblock: only the 4 bootstrap
    // trees mkfs wrote above. Post-bootstrap's commit will rewrite
    // the superblock with the final value once it allocates the
    // remaining trees.
    let bootstrap_bytes_used = layout.system_used()
        + layout.metadata_used(false, false, false, false, false, false);

    // Write tree blocks to disk, routing each stripe to the correct device.
    for (tree_id, mut block, logical) in trees {
        btrfs_disk::util::csum_tree_block(&mut block, cfg.csum_type);
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
    let normal_sb_params = SuperblockParams {
        root_addr: layout.block_addr(TreeId::Root),
        chunk_root_addr: layout.block_addr(TreeId::Chunk),
        root_level: 0,
        bytes_used: bootstrap_bytes_used,
    };
    for dev in &cfg.devices {
        let superblock =
            build_superblock_with_params(cfg, &chunks, dev, &normal_sb_params)?;
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

    // Files closed when `files` drops, then post_bootstrap reopens via
    // the transaction crate. Drop here explicitly to avoid holding two
    // sets of write handles to the same path.
    drop(files);

    crate::post_bootstrap::run(cfg)
        .context("post-bootstrap transaction failed")?;

    Ok(())
}

/// Create a btrfs filesystem populated from a source directory.
///
/// # Errors
///
/// Returns an error if validation fails, the device is too small, or I/O fails.
///
/// # Panics
///
/// Panics if internal consistency checks fail (e.g. extent tree block count
/// changes after convergence).
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_possible_truncation)] // key types fit in u8, devid-1 fits usize
#[allow(clippy::cast_sign_loss)] // DATA_RELOC_TREE_OBJECTID is positive
#[allow(clippy::similar_names)]
pub fn make_btrfs_with_rootdir(
    cfg: &MkfsConfig,
    rootdir: &Path,
    compress: rootdir::CompressConfig,
    inode_flags: &[crate::args::InodeFlagsArg],
    subvol_args: &[crate::args::SubvolArg],
    opts: RootdirOptions,
) -> Result<()> {
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
            "invalid nodesize {}: must be a power of 2, >= sectorsize ({}), and <= 64K",
            cfg.nodesize,
            cfg.sectorsize
        );
    }

    let generation = 1u64;
    let now = cfg.now_secs();
    let skinny = cfg.skinny_metadata();
    let has_free_space = cfg.has_free_space_tree();
    let has_block_group = cfg.has_block_group_tree();
    let root_ino = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);

    // Walk rootdir to plan all items and compute data needs.
    let plan = rootdir::walk_directory(
        rootdir,
        cfg.sectorsize,
        cfg.nodesize,
        generation,
        now,
        compress,
        inode_flags,
        subvol_args,
    )?;

    // Compute chunk layout.
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
        ChunkLayout::new(&chunk_devs, cfg.metadata_profile, cfg.data_profile)
            .ok_or_else(|| {
            anyhow::anyhow!("device too small: {} bytes", cfg.total_bytes())
        })?;

    if plan.data_bytes_needed > chunks.data_logical_size() {
        bail!(
            "rootdir requires {} bytes of data but data chunk is only {} bytes; \
             use a larger device or --byte-count",
            plan.data_bytes_needed,
            chunks.data_logical_size()
        );
    }

    // Open device files.
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

    // Write file data and get extent/csum items.
    let data_output = rootdir::write_file_data(
        &plan,
        chunks.data_logical,
        cfg.sectorsize,
        generation,
        cfg.csum_type,
        compress,
        opts.reflink,
        &files,
        &chunks,
    )?;

    // Build per-subvolume item sets: merge root dir inode + walk items + data items.
    let tb = TreeBuilder {
        nodesize: cfg.nodesize,
        owner: 0,
        fsid: cfg.fs_uuid,
        chunk_tree_uuid: cfg.chunk_tree_uuid,
        generation,
    };

    let mut subvol_trees: Vec<(u64, TreeBlocks)> = Vec::new();
    for sp in &plan.subvols {
        let mut sp_items = sp.fs_items.clone();

        // Apply nbytes updates for this subvolume.
        rootdir::apply_nbytes_updates(
            &mut sp_items,
            sp.root_objectid,
            &data_output.nbytes_updates,
        );

        let mut all_items: Vec<(Key, Vec<u8>)> = Vec::new();
        all_items.push((
            Key::new(root_ino, raw::BTRFS_INODE_ITEM_KEY as u8, 0),
            items::inode_item(&items::InodeItemArgs {
                generation,
                transid: generation,
                size: sp.root_dir_size,
                nbytes: 0,
                nlink: sp.root_dir_nlink,
                uid: 0,
                gid: 0,
                mode: 0o40755,
                rdev: 0,
                flags: 0,
                atime: (now, 0),
                ctime: (now, 0),
                mtime: (now, 0),
                otime: (now, 0),
            }),
        ));
        all_items.push((
            Key::new(root_ino, raw::BTRFS_INODE_REF_KEY as u8, root_ino),
            items::inode_ref(0, b".."),
        ));
        all_items.append(&mut sp_items);
        if let Some(data_fs) = data_output.fs_items.get(&sp.root_objectid) {
            all_items.extend(data_fs.iter().cloned());
        }
        all_items.sort_by_key(|(k, _)| *k);

        let tree = tb.clone_with_owner(sp.root_objectid).build(&all_items);
        subvol_trees.push((sp.root_objectid, tree));
    }

    let csum_tree = tb
        .clone_with_owner(u64::from(raw::BTRFS_CSUM_TREE_OBJECTID))
        .build(&data_output.csum_items);

    // Data-reloc tree items (same as normal mkfs).
    let data_reloc_items: Vec<(Key, Vec<u8>)> = vec![
        (
            Key::new(root_ino, raw::BTRFS_INODE_ITEM_KEY as u8, 0),
            items::inode_item_dir(generation, u64::from(cfg.nodesize), now),
        ),
        (
            Key::new(root_ino, raw::BTRFS_INODE_REF_KEY as u8, root_ino),
            items::inode_ref(0, b".."),
        ),
    ];
    let data_reloc_tree = tb
        .clone_with_owner(raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64)
        .build(&data_reloc_items);

    // Collect block counts for convergence loop: all subvol trees.
    let tree_block_counts: Vec<(u64, usize)> = subvol_trees
        .iter()
        .map(|(oid, tree)| (*oid, tree.blocks.len()))
        .collect();

    // Find stable extent tree block count via convergence loop.
    let extent_tree_block_count = converge_extent_tree_block_count(
        &chunks,
        &tb,
        skinny,
        generation,
        cfg.nodesize,
        has_free_space,
        has_block_group,
        &tree_block_counts,
        csum_tree.blocks.len(),
        data_reloc_tree.blocks.len(),
        &data_output.extent_items,
    );

    // Allocate real addresses in the fixed order.
    let mut alloc = BlockAllocator::new(
        cfg.nodesize,
        chunks.meta_logical,
        chunks.meta_logical_size(),
    );
    let chunk_tree_addr = alloc.alloc_system()?;
    let root_tree_addr = alloc.alloc_metadata()?;

    let mut extent_addrs = Vec::with_capacity(extent_tree_block_count);
    for _ in 0..extent_tree_block_count {
        extent_addrs.push(alloc.alloc_metadata()?);
    }

    let dev_tree_addr = alloc.alloc_metadata()?;

    // Allocate addresses for all subvolume trees (main FS tree first).
    let mut subvol_addrs: Vec<(u64, Vec<u64>)> = Vec::new();
    for (oid, tree) in &subvol_trees {
        let mut addrs = Vec::with_capacity(tree.blocks.len());
        for _ in 0..tree.blocks.len() {
            addrs.push(alloc.alloc_metadata()?);
        }
        subvol_addrs.push((*oid, addrs));
    }

    let mut csum_addrs = Vec::with_capacity(csum_tree.blocks.len());
    for _ in 0..csum_tree.blocks.len() {
        csum_addrs.push(alloc.alloc_metadata()?);
    }

    let free_space_addr = if has_free_space {
        Some(alloc.alloc_metadata()?)
    } else {
        None
    };

    let mut data_reloc_addrs = Vec::with_capacity(data_reloc_tree.blocks.len());
    for _ in 0..data_reloc_tree.blocks.len() {
        data_reloc_addrs.push(alloc.alloc_metadata()?);
    }

    let block_group_addr = if has_block_group {
        Some(alloc.alloc_metadata()?)
    } else {
        None
    };

    // Build the REAL extent tree with actual addresses.
    let mut extent_items: Vec<(Key, Vec<u8>)> = Vec::new();

    extent_items.push(metadata_extent_item(
        chunk_tree_addr,
        skinny,
        generation,
        u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
        cfg.nodesize,
    ));
    extent_items.push(metadata_extent_item(
        root_tree_addr,
        skinny,
        generation,
        u64::from(raw::BTRFS_ROOT_TREE_OBJECTID),
        cfg.nodesize,
    ));
    for &a in &extent_addrs {
        extent_items.push(metadata_extent_item(
            a,
            skinny,
            generation,
            u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
            cfg.nodesize,
        ));
    }
    extent_items.push(metadata_extent_item(
        dev_tree_addr,
        skinny,
        generation,
        u64::from(raw::BTRFS_DEV_TREE_OBJECTID),
        cfg.nodesize,
    ));
    for (oid, addrs) in &subvol_addrs {
        for &a in addrs {
            extent_items.push(metadata_extent_item(
                a,
                skinny,
                generation,
                *oid,
                cfg.nodesize,
            ));
        }
    }
    for &a in &csum_addrs {
        extent_items.push(metadata_extent_item(
            a,
            skinny,
            generation,
            u64::from(raw::BTRFS_CSUM_TREE_OBJECTID),
            cfg.nodesize,
        ));
    }
    if let Some(a) = free_space_addr {
        extent_items.push(metadata_extent_item(
            a,
            skinny,
            generation,
            u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID),
            cfg.nodesize,
        ));
    }
    for &a in &data_reloc_addrs {
        extent_items.push(metadata_extent_item(
            a,
            skinny,
            generation,
            raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
            cfg.nodesize,
        ));
    }
    if let Some(a) = block_group_addr {
        extent_items.push(metadata_extent_item(
            a,
            skinny,
            generation,
            u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID),
            cfg.nodesize,
        ));
    }
    extent_items.extend(data_output.extent_items.iter().cloned());
    if !has_block_group {
        add_block_group_items(
            &mut extent_items,
            cfg,
            &alloc,
            &chunks,
            data_output.data_used,
        );
    }
    extent_items.sort_by_key(|(k, _)| *k);

    let mut extent_tree = tb
        .clone_with_owner(u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID))
        .build(&extent_items);
    assert_eq!(
        extent_tree.blocks.len(),
        extent_addrs.len(),
        "extent tree block count changed after convergence"
    );

    // Assign addresses to all multi-block trees.
    let mut ei = 0;
    TreeBuilder::assign_addresses(&mut extent_tree, || {
        let a = extent_addrs[ei];
        ei += 1;
        a
    });
    let extent_root_addr = u64::from_le_bytes(
        extent_tree.blocks.last().unwrap().buf[48..56]
            .try_into()
            .unwrap(),
    );

    // Assign addresses to subvolume trees.
    for ((_oid, tree), (_oid2, addrs)) in
        subvol_trees.iter_mut().zip(subvol_addrs.iter())
    {
        let mut idx = 0;
        TreeBuilder::assign_addresses(tree, || {
            let a = addrs[idx];
            idx += 1;
            a
        });
    }

    let mut csum_tree = csum_tree;
    let mut ci = 0;
    TreeBuilder::assign_addresses(&mut csum_tree, || {
        let a = csum_addrs[ci];
        ci += 1;
        a
    });
    let csum_root_addr = u64::from_le_bytes(
        csum_tree.blocks.last().unwrap().buf[48..56]
            .try_into()
            .unwrap(),
    );

    let mut data_reloc_tree = data_reloc_tree;
    let mut di = 0;
    TreeBuilder::assign_addresses(&mut data_reloc_tree, || {
        let a = data_reloc_addrs[di];
        di += 1;
        a
    });
    let data_reloc_addr = u64::from_le_bytes(
        data_reloc_tree.blocks.last().unwrap().buf[48..56]
            .try_into()
            .unwrap(),
    );

    // Build single-leaf trees.
    let leaf_hdr = |owner: u64, bytenr: u64| LeafHeader {
        fsid: cfg.fs_uuid,
        chunk_tree_uuid: cfg.chunk_tree_uuid,
        generation,
        owner,
        bytenr,
    };

    // If shrinking, compute the final device size now and create a config
    // with updated total_bytes so the chunk tree DEV_ITEM and superblock agree.
    let effective_cfg;
    let shrink = opts.shrink;
    let cfg = if shrink && cfg.devices.len() == 1 {
        let dev = &cfg.devices[0];
        let mut phys_end = SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;
        for s in &chunks.meta_stripes {
            if s.devid == dev.devid {
                phys_end = phys_end.max(s.offset + chunks.meta_size);
            }
        }
        for s in &chunks.data_stripes {
            if s.devid == dev.devid {
                phys_end = phys_end.max(s.offset + chunks.data_size);
            }
        }
        let shrunk = rootdir::align_up(phys_end, u64::from(cfg.sectorsize));
        effective_cfg = MkfsConfig {
            devices: vec![DeviceInfo {
                devid: dev.devid,
                path: dev.path.clone(),
                total_bytes: shrunk,
                dev_uuid: dev.dev_uuid,
            }],
            nodesize: cfg.nodesize,
            sectorsize: cfg.sectorsize,
            label: cfg.label.clone(),
            fs_uuid: cfg.fs_uuid,
            chunk_tree_uuid: cfg.chunk_tree_uuid,
            incompat_flags: cfg.incompat_flags,
            compat_ro_flags: cfg.compat_ro_flags,
            data_profile: cfg.data_profile,
            metadata_profile: cfg.metadata_profile,
            csum_type: cfg.csum_type,
            creation_time: cfg.creation_time,
            quota: cfg.quota,
            squota: cfg.squota,
        };
        &effective_cfg
    } else {
        cfg
    };

    let chunk_tree_buf = build_chunk_tree(
        cfg,
        &BlockLayout::new(cfg.nodesize, chunks.meta_logical),
        &chunks,
        &|_| {
            leaf_hdr(u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID), chunk_tree_addr)
        },
    )?;
    let dev_tree_buf = build_dev_tree(cfg, &chunks, &|_| {
        leaf_hdr(u64::from(raw::BTRFS_DEV_TREE_OBJECTID), dev_tree_addr)
    })?;

    let rootdir_used = UsedBytes {
        system: alloc.system_used(),
        metadata: alloc.metadata_used(),
        data: data_output.data_used,
    };
    let free_space_buf = free_space_addr
        .map(|addr| {
            build_free_space_tree_with_used(
                cfg,
                &chunks,
                &LeafHeader {
                    fsid: cfg.fs_uuid,
                    chunk_tree_uuid: cfg.chunk_tree_uuid,
                    generation,
                    owner: u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID),
                    bytenr: addr,
                },
                &rootdir_used,
            )
        })
        .transpose()?;
    let block_group_buf = block_group_addr
        .map(|addr| {
            build_block_group_tree_with_used(
                cfg,
                &chunks,
                &LeafHeader {
                    fsid: cfg.fs_uuid,
                    chunk_tree_uuid: cfg.chunk_tree_uuid,
                    generation,
                    owner: u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID),
                    bytenr: addr,
                },
                &rootdir_used,
            )
        })
        .transpose()?;

    // Build root tree: standard trees + all subvolume trees.
    let mut root_trees: Vec<(u64, u64, u8)> = vec![
        (
            u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
            extent_root_addr,
            extent_tree.root_level,
        ),
        (u64::from(raw::BTRFS_DEV_TREE_OBJECTID), dev_tree_addr, 0),
        (
            u64::from(raw::BTRFS_CSUM_TREE_OBJECTID),
            csum_root_addr,
            csum_tree.root_level,
        ),
    ];
    for (oid, tree) in &subvol_trees {
        let root_addr = u64::from_le_bytes(
            tree.blocks.last().unwrap().buf[48..56].try_into().unwrap(),
        );
        root_trees.push((*oid, root_addr, tree.root_level));
    }

    let root_tree_buf = build_root_tree_rootdir(&RootTreeRootdirArgs {
        cfg,
        generation,
        now,
        addr: root_tree_addr,
        trees: &root_trees,
        subvol_meta: &plan.subvol_meta,
        free_space_addr,
        data_reloc_addr,
        data_reloc_level: data_reloc_tree.root_level,
        block_group_addr,
    })?;

    write_rootdir_trees(
        &files,
        &chunks,
        cfg.csum_type,
        chunk_tree_addr,
        root_tree_addr,
        dev_tree_addr,
        &chunk_tree_buf,
        &root_tree_buf,
        &dev_tree_buf,
        &mut extent_tree,
        &mut subvol_trees,
        &mut csum_tree,
        &mut data_reloc_tree,
        free_space_buf,
        free_space_addr,
        block_group_buf,
        block_group_addr,
    )?;

    // Superblock.
    let rootdir_sb_params = SuperblockParams {
        root_addr: root_tree_addr,
        chunk_root_addr: chunk_tree_addr,
        root_level: 0,
        bytes_used: rootdir_used.system
            + rootdir_used.metadata
            + rootdir_used.data,
    };
    for dev in &cfg.devices {
        let sb = build_superblock_with_params(
            cfg,
            &chunks,
            dev,
            &rootdir_sb_params,
        )?;
        let fidx = (dev.devid - 1) as usize;
        for mirror in 0..btrfs_disk::superblock::SUPER_MIRROR_MAX {
            let offset = btrfs_disk::superblock::super_mirror_offset(mirror);
            if offset + write::SUPER_INFO_SIZE as u64 > dev.total_bytes {
                break;
            }
            write::pwrite_all(&files[fidx], &sb, offset)?;
        }
    }

    // Shrink: truncate image to the device total_bytes (already computed
    // above if --shrink was requested).
    if shrink && cfg.devices.len() == 1 {
        let shrunk_size = cfg.devices[0].total_bytes;
        files[0]
            .set_len(shrunk_size)
            .context("failed to truncate image for --shrink")?;
    }

    for file in &files {
        file.sync_all().context("fsync failed")?;
    }

    // Files closed when `files` drops, then post_bootstrap reopens via
    // the transaction crate. Drop here to avoid holding two sets of
    // write handles to the same path.
    drop(files);

    crate::post_bootstrap::run(cfg)
        .context("post-bootstrap transaction failed")?;

    Ok(())
}

/// Convergence loop: iteratively compute the extent tree block count until
/// the number of blocks needed to hold all extent items (including the
/// extent tree's own self-referential entries) stabilizes.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // convergence loop is a single logical operation
#[allow(clippy::cast_possible_truncation)] // key type fits in u8
#[allow(clippy::cast_sign_loss)] // bindgen objectid constants are i32, but values are positive
fn converge_extent_tree_block_count(
    chunks: &ChunkLayout,
    tb: &TreeBuilder,
    skinny: bool,
    generation: u64,
    nodesize: u32,
    has_free_space: bool,
    has_block_group: bool,
    tree_block_counts: &[(u64, usize)],
    csum_block_count: usize,
    data_reloc_block_count: usize,
    data_extent_items: &[(Key, Vec<u8>)],
) -> usize {
    let ns = u64::from(nodesize);
    let mut extent_tree_block_count = 1usize;
    loop {
        let mut addr = chunks.meta_logical;
        let mut trial_items: Vec<(Key, Vec<u8>)> = Vec::new();

        // Chunk tree (system)
        trial_items.push(metadata_extent_item(
            SYSTEM_GROUP_OFFSET,
            skinny,
            generation,
            u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
            nodesize,
        ));

        // Root tree
        trial_items.push(metadata_extent_item(
            addr,
            skinny,
            generation,
            u64::from(raw::BTRFS_ROOT_TREE_OBJECTID),
            nodesize,
        ));
        addr += ns;

        // Extent tree blocks (placeholders)
        for _ in 0..extent_tree_block_count {
            trial_items.push(metadata_extent_item(
                addr,
                skinny,
                generation,
                u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
                nodesize,
            ));
            addr += ns;
        }

        // Dev tree
        trial_items.push(metadata_extent_item(
            addr,
            skinny,
            generation,
            u64::from(raw::BTRFS_DEV_TREE_OBJECTID),
            nodesize,
        ));
        addr += ns;

        // All subvolume / FS tree blocks
        for &(oid, count) in tree_block_counts {
            for _ in 0..count {
                trial_items.push(metadata_extent_item(
                    addr, skinny, generation, oid, nodesize,
                ));
                addr += ns;
            }
        }

        // Csum tree blocks
        for _ in 0..csum_block_count {
            trial_items.push(metadata_extent_item(
                addr,
                skinny,
                generation,
                u64::from(raw::BTRFS_CSUM_TREE_OBJECTID),
                nodesize,
            ));
            addr += ns;
        }

        if has_free_space {
            trial_items.push(metadata_extent_item(
                addr,
                skinny,
                generation,
                u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID),
                nodesize,
            ));
            addr += ns;
        }

        for _ in 0..data_reloc_block_count {
            trial_items.push(metadata_extent_item(
                addr,
                skinny,
                generation,
                raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
                nodesize,
            ));
            addr += ns;
        }

        if has_block_group {
            trial_items.push(metadata_extent_item(
                addr,
                skinny,
                generation,
                u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID),
                nodesize,
            ));
        }

        trial_items.extend(data_extent_items.iter().cloned());

        if !has_block_group {
            for &(logical, size) in &[
                (SYSTEM_GROUP_OFFSET, SYSTEM_GROUP_SIZE),
                (chunks.meta_logical, chunks.meta_logical_size()),
                (chunks.data_logical, chunks.data_logical_size()),
            ] {
                trial_items.push((
                    Key::new(
                        logical,
                        raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
                        size,
                    ),
                    items::block_group_item(
                        0,
                        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
                        0,
                    ),
                ));
            }
        }

        trial_items.sort_by_key(|(k, _)| *k);
        let trial = tb
            .clone_with_owner(u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID))
            .build(&trial_items);

        if trial.blocks.len() == extent_tree_block_count {
            break;
        }
        extent_tree_block_count = trial.blocks.len();
    }
    extent_tree_block_count
}

/// Write all tree blocks for the rootdir path to their physical locations.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::cast_possible_truncation)] // devid-1 fits usize
fn write_rootdir_trees(
    files: &[File],
    chunks: &ChunkLayout,
    csum_type: crate::write::ChecksumType,
    chunk_tree_addr: u64,
    root_tree_addr: u64,
    dev_tree_addr: u64,
    chunk_tree_buf: &[u8],
    root_tree_buf: &[u8],
    dev_tree_buf: &[u8],
    extent_tree: &mut TreeBlocks,
    subvol_trees: &mut [(u64, TreeBlocks)],
    csum_tree: &mut TreeBlocks,
    data_reloc_tree: &mut TreeBlocks,
    free_space_buf: Option<Vec<u8>>,
    free_space_addr: Option<u64>,
    block_group_buf: Option<Vec<u8>>,
    block_group_addr: Option<u64>,
) -> Result<()> {
    let write_block = |buf: &mut Vec<u8>, logical: u64| -> Result<()> {
        btrfs_disk::util::csum_tree_block(buf, csum_type);
        for (devid, phys) in chunks.logical_to_physical(logical) {
            write::pwrite_all(&files[(devid - 1) as usize], buf, phys)?;
        }
        Ok(())
    };
    let write_tree_blocks = |tree: &mut TreeBlocks| -> Result<()> {
        for block in &mut tree.blocks {
            let addr =
                u64::from_le_bytes(block.buf[48..56].try_into().unwrap());
            btrfs_disk::util::csum_tree_block(&mut block.buf, csum_type);
            for (devid, phys) in chunks.logical_to_physical(addr) {
                write::pwrite_all(
                    &files[(devid - 1) as usize],
                    &block.buf,
                    phys,
                )?;
            }
        }
        Ok(())
    };

    write_block(&mut chunk_tree_buf.to_vec(), chunk_tree_addr)?;
    write_block(&mut root_tree_buf.to_vec(), root_tree_addr)?;
    write_block(&mut dev_tree_buf.to_vec(), dev_tree_addr)?;
    write_tree_blocks(extent_tree)?;
    for (_, tree) in subvol_trees.iter_mut() {
        write_tree_blocks(tree)?;
    }
    write_tree_blocks(csum_tree)?;
    write_tree_blocks(data_reloc_tree)?;
    if let Some(mut buf) = free_space_buf {
        write_block(&mut buf, free_space_addr.unwrap())?;
    }
    if let Some(mut buf) = block_group_buf {
        write_block(&mut buf, block_group_addr.unwrap())?;
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn metadata_extent_item(
    addr: u64,
    skinny: bool,
    generation: u64,
    owner: u64,
    nodesize: u32,
) -> (Key, Vec<u8>) {
    let (item_type, offset) = if skinny {
        (raw::BTRFS_METADATA_ITEM_KEY as u8, 0u64)
    } else {
        (raw::BTRFS_EXTENT_ITEM_KEY as u8, u64::from(nodesize))
    };
    (
        Key::new(addr, item_type, offset),
        items::extent_item(1, generation, skinny, owner),
    )
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn add_block_group_items(
    v: &mut Vec<(Key, Vec<u8>)>,
    cfg: &MkfsConfig,
    alloc: &BlockAllocator,
    chunks: &ChunkLayout,
    data_used: u64,
) {
    v.push((
        Key::new(
            SYSTEM_GROUP_OFFSET,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            SYSTEM_GROUP_SIZE,
        ),
        items::block_group_item(
            alloc.system_used(),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
        ),
    ));
    v.push((
        Key::new(
            chunks.meta_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.meta_logical_size(),
        ),
        items::block_group_item(
            alloc.metadata_used(),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_METADATA)
                | cfg.metadata_profile.block_group_flag(),
        ),
    ));
    v.push((
        Key::new(
            chunks.data_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.data_logical_size(),
        ),
        items::block_group_item(
            data_used,
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_DATA)
                | cfg.data_profile.block_group_flag(),
        ),
    ));
}

struct RootTreeRootdirArgs<'a> {
    cfg: &'a MkfsConfig,
    generation: u64,
    now: u64,
    addr: u64,
    trees: &'a [(u64, u64, u8)],
    subvol_meta: &'a [rootdir::SubvolMeta],
    free_space_addr: Option<u64>,
    data_reloc_addr: u64,
    data_reloc_level: u8,
    block_group_addr: Option<u64>,
}

/// Patch a `ROOT_ITEM` with FS-tree-like fields (`INODE_ROOT_ITEM_INIT`,
/// size=3, nbytes=nodesize, timestamps). Used for FS tree and subvolume trees.
fn patch_root_item_fs(
    data: &mut [u8],
    uuid: &uuid::Uuid,
    flags: u64,
    nodesize: u32,
    now: u64,
) {
    let uo = mem::offset_of!(raw::btrfs_root_item, uuid);
    data[uo..uo + 16].copy_from_slice(uuid.as_bytes());
    let fo = mem::offset_of!(raw::btrfs_inode_item, flags);
    data[fo..fo + 8].copy_from_slice(
        &(u64::from(raw::BTRFS_INODE_ROOT_ITEM_INIT)).to_le_bytes(),
    );
    // inode.size = 3
    data[16..24].copy_from_slice(&3u64.to_le_bytes());
    // inode.nbytes = nodesize
    data[24..32].copy_from_slice(&(u64::from(nodesize)).to_le_bytes());
    // root_item.flags (RDONLY etc.)
    let rf = mem::offset_of!(raw::btrfs_root_item, flags);
    data[rf..rf + 8].copy_from_slice(&flags.to_le_bytes());
    // ctime, otime
    for off in [
        mem::offset_of!(raw::btrfs_root_item, ctime),
        mem::offset_of!(raw::btrfs_root_item, otime),
    ] {
        data[off..off + 8].copy_from_slice(&now.to_le_bytes());
        data[off + 8..off + 12].copy_from_slice(&0u32.to_le_bytes());
    }
}

/// Add root tree directory items: `INODE_ITEM` + `INODE_REF` for objectid 6,
/// and a `DIR_ITEM` for "default" pointing to the given root objectid.
#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn add_root_tree_dir_items(
    items_out: &mut Vec<(Key, Vec<u8>)>,
    generation: u64,
    now: u64,
    nodesize: u32,
    default_root_id: u64,
) {
    let root_dir_oid = u64::from(raw::BTRFS_ROOT_TREE_DIR_OBJECTID);

    // INODE_ITEM for the root tree directory (objectid 6).
    items_out.push((
        Key::new(root_dir_oid, raw::BTRFS_INODE_ITEM_KEY as u8, 0),
        items::inode_item_dir(generation, u64::from(nodesize), now),
    ));

    // INODE_REF: ".." self-reference for objectid 6.
    items_out.push((
        Key::new(root_dir_oid, raw::BTRFS_INODE_REF_KEY as u8, root_dir_oid),
        items::inode_ref(0, b".."),
    ));

    // DIR_ITEM: "default" entry pointing to the default subvolume.
    let name = b"default";
    let name_hash = rootdir::btrfs_name_hash(name);
    let location =
        Key::new(default_root_id, raw::BTRFS_ROOT_ITEM_KEY as u8, u64::MAX);
    items_out.push((
        Key::new(root_dir_oid, raw::BTRFS_DIR_ITEM_KEY as u8, name_hash),
        items::dir_item(&location, generation, name, raw::BTRFS_FT_DIR as u8),
    ));
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
#[allow(clippy::cast_sign_loss)] // DATA_RELOC_TREE_OBJECTID is positive
#[allow(clippy::items_after_statements)]
#[allow(clippy::too_many_lines)]
fn build_root_tree_rootdir(args: &RootTreeRootdirArgs<'_>) -> Result<Vec<u8>> {
    let cfg = args.cfg;
    let header = LeafHeader {
        fsid: cfg.fs_uuid,
        chunk_tree_uuid: cfg.chunk_tree_uuid,
        generation: args.generation,
        owner: u64::from(raw::BTRFS_ROOT_TREE_OBJECTID),
        bytenr: args.addr,
    };
    let mut leaf = LeafBuilder::new(cfg.nodesize, &header);

    // Collect all items that go into the root tree, sorted by key.
    let mut root_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // Standard tree ROOT_ITEM entries.
    struct E {
        oid: u64,
        addr: u64,
        level: u8,
        is_fs_like: bool,
    }
    let mut entries: Vec<E> = args
        .trees
        .iter()
        .map(|&(o, a, l)| E {
            oid: o,
            addr: a,
            level: l,
            is_fs_like: o == u64::from(raw::BTRFS_FS_TREE_OBJECTID)
                || o >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID),
        })
        .collect();
    if let Some(a) = args.free_space_addr {
        entries.push(E {
            oid: u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID),
            addr: a,
            level: 0,
            is_fs_like: false,
        });
    }
    entries.push(E {
        oid: raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
        addr: args.data_reloc_addr,
        level: args.data_reloc_level,
        is_fs_like: false,
    });
    if let Some(a) = args.block_group_addr {
        entries.push(E {
            oid: u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID),
            addr: a,
            level: 0,
            is_fs_like: false,
        });
    }

    // Build ROOT_ITEM for each tree.
    for e in &entries {
        let key = Key::new(e.oid, raw::BTRFS_ROOT_ITEM_KEY as u8, 0);
        let mut data = items::root_item(
            args.generation,
            e.addr,
            u64::from(raw::BTRFS_FIRST_FREE_OBJECTID),
            cfg.nodesize,
        );
        data[mem::offset_of!(raw::btrfs_root_item, level)] = e.level;

        if e.is_fs_like {
            // Determine UUID and flags for this tree.
            let (uuid, flags) =
                if e.oid == u64::from(raw::BTRFS_FS_TREE_OBJECTID) {
                    // Main FS tree: UUID derived from fs_uuid by bit-flipping.
                    let mut ub = *cfg.fs_uuid.as_bytes();
                    for b in &mut ub {
                        *b ^= 0xFF;
                    }
                    (uuid::Uuid::from_bytes(ub), 0u64)
                } else {
                    // Subvolume: look up metadata.
                    let meta =
                        args.subvol_meta.iter().find(|m| m.subvol_id == e.oid);
                    let flags = if meta.is_some_and(|m| m.readonly) {
                        u64::from(raw::BTRFS_ROOT_SUBVOL_RDONLY)
                    } else {
                        0
                    };
                    (uuid::Uuid::new_v4(), flags)
                };
            patch_root_item_fs(&mut data, &uuid, flags, cfg.nodesize, args.now);
        }
        root_items.push((key, data));
    }

    // ROOT_REF and ROOT_BACKREF entries for subvolumes.
    for meta in args.subvol_meta {
        // ROOT_BACKREF: child → parent
        root_items.push((
            Key::new(
                meta.subvol_id,
                raw::BTRFS_ROOT_BACKREF_KEY as u8,
                meta.parent_root_id,
            ),
            items::root_ref(meta.parent_dirid, meta.dir_index, &meta.name),
        ));
        // ROOT_REF: parent → child
        root_items.push((
            Key::new(
                meta.parent_root_id,
                raw::BTRFS_ROOT_REF_KEY as u8,
                meta.subvol_id,
            ),
            items::root_ref(meta.parent_dirid, meta.dir_index, &meta.name),
        ));
    }

    // Root tree directory items: INODE_ITEM + INODE_REF for objectid 6,
    // DIR_ITEM for "default" pointing to the default subvolume (or FS tree).
    let default_root_id = args
        .subvol_meta
        .iter()
        .find(|m| m.is_default)
        .map_or(u64::from(raw::BTRFS_FS_TREE_OBJECTID), |m| m.subvol_id);
    add_root_tree_dir_items(
        &mut root_items,
        args.generation,
        args.now,
        cfg.nodesize,
        default_root_id,
    );

    // Sort all items by key and push into the leaf.
    root_items.sort_by_key(|(k, _)| *k);
    for (key, data) in &root_items {
        leaf.push(*key, data)
            .map_err(|e| anyhow::anyhow!("root tree: {e}"))?;
    }
    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
#[allow(clippy::cast_sign_loss)] // DATA_RELOC_TREE_OBJECTID is positive
#[allow(clippy::too_many_lines)]
fn build_root_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Root));
    let generation = 1u64;
    let now = cfg.now_secs();

    // Collect all root tree items, then sort by key and push.
    let mut root_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // mkfs writes only the always-present non-bootstrap trees here
    // (Extent, Dev). Every other ROOT_ITEM is inserted by
    // post-bootstrap.
    for &tree in &TreeId::ROOT_ITEM_TREES {
        let key = Key::new(tree.objectid(), raw::BTRFS_ROOT_ITEM_KEY as u8, 0);
        let data = items::root_item(
            generation,
            layout.block_addr(tree),
            u64::from(raw::BTRFS_FIRST_FREE_OBJECTID),
            cfg.nodesize,
        );
        root_items.push((key, data));
    }

    // Root tree directory: INODE_ITEM + INODE_REF for objectid 6,
    // DIR_ITEM "default" pointing to the FS tree.
    add_root_tree_dir_items(
        &mut root_items,
        generation,
        now,
        cfg.nodesize,
        u64::from(raw::BTRFS_FS_TREE_OBJECTID),
    );

    root_items.sort_by_key(|(k, _)| *k);
    for (key, data) in &root_items {
        leaf.push(*key, data)
            .map_err(|e| anyhow::anyhow!("root tree: {e}"))?;
    }

    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
#[allow(clippy::too_many_lines)]
fn build_extent_tree(
    cfg: &MkfsConfig,
    layout: &BlockLayout,
    chunks: &ChunkLayout,
    leaf_header: &dyn Fn(TreeId) -> LeafHeader,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, &leaf_header(TreeId::Extent));
    let generation = 1u64;
    let skinny = cfg.skinny_metadata();

    // Collect all items into a Vec, then sort by key before pushing.
    // Tree blocks span two different chunks (system and metadata),
    // so addresses are not monotonically increasing — we must sort.
    let mut extent_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // mkfs's bootstrap writes METADATA_ITEM entries for the four
    // always-present trees only. post_bootstrap's commit inserts the
    // METADATA_ITEMs for any trees it creates (FS, csum, data-reloc,
    // UUID, plus optional FST / BGT / quota) via the standard
    // delayed-ref pipeline.
    let all_trees: Vec<(TreeId, u64)> = TreeId::ALL
        .iter()
        .map(|&t| (t, layout.block_addr(t)))
        .collect();

    for &(tree, addr) in &all_trees {
        let item_type = if skinny {
            raw::BTRFS_METADATA_ITEM_KEY as u8
        } else {
            raw::BTRFS_EXTENT_ITEM_KEY as u8
        };
        let offset = if skinny { 0 } else { u64::from(cfg.nodesize) };
        let key = Key::new(addr, item_type, offset);
        let data = items::extent_item(1, generation, skinny, tree.objectid());
        extent_items.push((key, data));
    }

    // BLOCK_GROUP_ITEMs for system, metadata, and data chunks. Live
    // in the extent tree at this stage; post-bootstrap migrates them
    // to the BG tree when that feature is enabled. The metadata
    // block group's `bytes_used` reflects only the four bootstrap
    // tree blocks; post-bootstrap rewrites it as it allocates more.
    extent_items.push((
        Key::new(
            SYSTEM_GROUP_OFFSET,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            SYSTEM_GROUP_SIZE,
        ),
        items::block_group_item(
            layout.system_used(),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
        ),
    ));
    extent_items.push((
        Key::new(
            chunks.meta_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.meta_logical_size(),
        ),
        items::block_group_item(
            layout.metadata_used(false, false, false, false, false, false),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_METADATA)
                | cfg.metadata_profile.block_group_flag(),
        ),
    ));
    extent_items.push((
        Key::new(
            chunks.data_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.data_logical_size(),
        ),
        items::block_group_item(
            0,
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_DATA)
                | cfg.data_profile.block_group_flag(),
        ),
    ));

    // Sort by key and push in order.
    extent_items.sort_by_key(|(k, _)| *k);

    for (key, data) in &extent_items {
        leaf.push(*key, data)
            .map_err(|e| anyhow::anyhow!("extent tree: {e}"))?;
    }

    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
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
            u64::from(raw::BTRFS_DEV_ITEMS_OBJECTID),
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
        u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
        u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
        cfg.sectorsize,
        &sys_stripe,
    );
    let sys_chunk_key = Key::new(
        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    leaf.push(sys_chunk_key, &sys_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    // CHUNK_ITEM for metadata chunk
    let meta_chunk_data = items::chunk_item(
        chunks.meta_logical_size(),
        u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
        u64::from(raw::BTRFS_BLOCK_GROUP_METADATA)
            | cfg.metadata_profile.block_group_flag(),
        crate::layout::STRIPE_LEN as u32,
        crate::layout::STRIPE_LEN as u32,
        cfg.sectorsize,
        cfg.metadata_profile.sub_stripes(),
        &chunks.meta_stripes,
    );
    let meta_chunk_key = Key::new(
        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        chunks.meta_logical,
    );
    leaf.push(meta_chunk_key, &meta_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    // CHUNK_ITEM for data chunk
    let data_chunk_data = items::chunk_item(
        chunks.data_logical_size(),
        u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
        u64::from(raw::BTRFS_BLOCK_GROUP_DATA)
            | cfg.data_profile.block_group_flag(),
        crate::layout::STRIPE_LEN as u32,
        crate::layout::STRIPE_LEN as u32,
        cfg.sectorsize,
        cfg.data_profile.sub_stripes(),
        &chunks.data_stripes,
    );
    let data_chunk_key = Key::new(
        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        chunks.data_logical,
    );
    leaf.push(data_chunk_key, &data_chunk_data)
        .map_err(|e| anyhow::anyhow!("chunk tree: {e}"))?;

    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
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
            u64::from(raw::BTRFS_DEV_STATS_OBJECTID),
            raw::BTRFS_PERSISTENT_ITEM_KEY as u8,
            dev.devid,
        );
        dev_items.push((stats_key, items::dev_stats_zeroed()));
    }

    // DEV_EXTENT for the system chunk (always device 1)
    let sys_extent = items::dev_extent(
        u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
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
            u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
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
            u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
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

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn build_free_space_tree_with_used(
    cfg: &MkfsConfig,
    chunks: &ChunkLayout,
    header: &LeafHeader,
    used: &UsedBytes,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, header);

    // System block group
    let sfs = SYSTEM_GROUP_OFFSET + used.system;
    let sfl = SYSTEM_GROUP_SIZE - used.system;
    leaf.push(
        Key::new(
            SYSTEM_GROUP_OFFSET,
            raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
            SYSTEM_GROUP_SIZE,
        ),
        &items::free_space_info(1, 0),
    )
    .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;
    leaf.push_empty(Key::new(sfs, raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8, sfl))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    // Metadata block group
    let meta_logical_size = chunks.meta_logical_size();
    let mfs = chunks.meta_logical + used.metadata;
    let mfl = meta_logical_size - used.metadata;
    leaf.push(
        Key::new(
            chunks.meta_logical,
            raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
            meta_logical_size,
        ),
        &items::free_space_info(1, 0),
    )
    .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;
    leaf.push_empty(Key::new(mfs, raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8, mfl))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;

    // Data block group
    let data_logical_size = chunks.data_logical_size();
    let dfs = chunks.data_logical + used.data;
    let dfl = data_logical_size - used.data;
    let dc = u32::from(dfl > 0);
    leaf.push(
        Key::new(
            chunks.data_logical,
            raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
            data_logical_size,
        ),
        &items::free_space_info(dc, 0),
    )
    .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;
    if dfl > 0 {
        leaf.push_empty(Key::new(
            dfs,
            raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
            dfl,
        ))
        .map_err(|e| anyhow::anyhow!("free space tree: {e}"))?;
    }

    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn build_block_group_tree_with_used(
    cfg: &MkfsConfig,
    chunks: &ChunkLayout,
    header: &LeafHeader,
    used: &UsedBytes,
) -> Result<Vec<u8>> {
    let mut leaf = LeafBuilder::new(cfg.nodesize, header);

    // System block group
    leaf.push(
        Key::new(
            SYSTEM_GROUP_OFFSET,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            SYSTEM_GROUP_SIZE,
        ),
        &items::block_group_item(
            used.system,
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    // Metadata block group
    leaf.push(
        Key::new(
            chunks.meta_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.meta_logical_size(),
        ),
        &items::block_group_item(
            used.metadata,
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_METADATA)
                | cfg.metadata_profile.block_group_flag(),
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    // Data block group
    leaf.push(
        Key::new(
            chunks.data_logical,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            chunks.data_logical_size(),
        ),
        &items::block_group_item(
            used.data,
            u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(raw::BTRFS_BLOCK_GROUP_DATA)
                | cfg.data_profile.block_group_flag(),
        ),
    )
    .map_err(|e| anyhow::anyhow!("block group tree: {e}"))?;

    Ok(leaf.finish())
}

#[allow(clippy::cast_possible_truncation)] // key type fits in u8, sizes fit in u32
#[allow(clippy::too_many_lines)]
#[allow(clippy::unnecessary_wraps)]
fn build_superblock_with_params(
    cfg: &MkfsConfig,
    chunks: &ChunkLayout,
    dev: &DeviceInfo,
    params: &SuperblockParams,
) -> Result<Vec<u8>> {
    let generation = 1u64;

    // Build the sys_chunk_array: disk_key + chunk_item bytes.
    let chunk_key = Key::new(
        u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        raw::BTRFS_CHUNK_ITEM_KEY as u8,
        SYSTEM_GROUP_OFFSET,
    );
    let dev1 = cfg.primary_device();
    let chunk_data = items::chunk_item_bootstrap(
        SYSTEM_GROUP_SIZE,
        u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
        u64::from(raw::BTRFS_BLOCK_GROUP_SYSTEM),
        cfg.sectorsize,
        &StripeInfo {
            devid: dev1.devid,
            offset: SYSTEM_GROUP_OFFSET,
            dev_uuid: dev1.dev_uuid,
        },
    );
    let mut sys_chunk_array = items::disk_key(&chunk_key);
    sys_chunk_array.extend_from_slice(&chunk_data);

    // cache_generation: 0 if free-space-tree is enabled, u64::MAX otherwise.
    let cache_generation = if cfg.has_free_space_tree() {
        0
    } else {
        u64::MAX
    };

    let mut sys_chunk_buf = [0u8; 2048];
    sys_chunk_buf[..sys_chunk_array.len()].copy_from_slice(&sys_chunk_array);

    let sb = btrfs_disk::superblock::Superblock {
        csum: [0; 32],
        fsid: cfg.fs_uuid,
        bytenr: write::SUPER_INFO_OFFSET,
        flags: 0,
        magic: raw::BTRFS_MAGIC,
        generation,
        root: params.root_addr,
        chunk_root: params.chunk_root_addr,
        log_root: 0,
        log_root_transid: 0,
        total_bytes: cfg.total_bytes(),
        bytes_used: params.bytes_used,
        root_dir_objectid: u64::from(raw::BTRFS_FIRST_FREE_OBJECTID),
        num_devices: cfg.num_devices(),
        sectorsize: cfg.sectorsize,
        nodesize: cfg.nodesize,
        leafsize: cfg.nodesize,
        stripesize: cfg.sectorsize,
        sys_chunk_array_size: sys_chunk_array.len() as u32,
        chunk_root_generation: generation,
        compat_flags: 0,
        compat_ro_flags: cfg.compat_ro_flags,
        incompat_flags: cfg.incompat_flags,
        csum_type: cfg.csum_type,
        root_level: params.root_level,
        chunk_root_level: 0,
        log_root_level: 0,
        dev_item: btrfs_disk::items::DeviceItem {
            devid: dev.devid,
            total_bytes: dev.total_bytes,
            bytes_used: chunks.dev_bytes_used_for(dev.devid),
            io_align: cfg.sectorsize,
            io_width: cfg.sectorsize,
            sector_size: cfg.sectorsize,
            dev_type: 0,
            generation: 0,
            start_offset: 0,
            dev_group: 0,
            seek_speed: 0,
            bandwidth: 0,
            uuid: dev.dev_uuid,
            fsid: cfg.fs_uuid,
        },
        label: cfg.label.clone().unwrap_or_default(),
        cache_generation,
        uuid_tree_generation: 0,
        metadata_uuid: Uuid::nil(),
        nr_global_roots: 0,
        backup_roots: std::array::from_fn(|_| {
            btrfs_disk::superblock::BackupRoot::default()
        }),
        sys_chunk_array: sys_chunk_buf,
    };

    let mut buf = sb.to_bytes();
    btrfs_disk::superblock::csum_superblock(&mut buf)
        .context("failed to checksum superblock")?;
    Ok(buf.to_vec())
}

// From linux/fs.h: #define BLKGETSIZE64 _IOR(0x12, 114, size_t)
nix::ioctl_read!(blk_getsize64, 0x12, 114, u64);

// From linux/fs.h: #define BLKDISCARD _IO(0x12, 119)
nix::ioctl_write_ptr!(blk_discard, 0x12, 119, [u64; 2]);

/// Get the size of a device or file in bytes.
///
/// # Errors
///
/// Returns an error if the path cannot be stat'd or the ioctl fails.
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
                std::ptr::from_mut(&mut size),
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
#[must_use]
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
#[must_use]
pub fn is_device_mounted(path: &Path) -> bool {
    btrfs_uapi::filesystem::is_mounted(path).unwrap_or(false)
}

/// Issue BLKDISCARD (TRIM) on the entire device.
///
/// # Errors
///
/// Returns an error if the device cannot be opened or the ioctl fails.
pub fn discard_device(path: &Path, size: u64) -> Result<()> {
    let file =
        OpenOptions::new().write(true).open(path).with_context(|| {
            format!("failed to open '{}' for discard", path.display())
        })?;
    let range: [u64; 2] = [0, size];
    unsafe {
        blk_discard(
            std::os::unix::io::AsRawFd::as_raw_fd(&file),
            std::ptr::from_ref(&range),
        )
    }
    .with_context(|| format!("BLKDISCARD failed on {}", path.display()))?;
    Ok(())
}

/// Minimum device size for the default single-device layout (DUP
/// metadata + SINGLE data). Must fit the system group (5 MiB),
/// metadata DUP (2 x 8 MiB), and data SINGLE (8 MiB): 29 MiB.
#[must_use]
pub fn minimum_device_size(nodesize: u32) -> u64 {
    let _ = nodesize;
    // System (5M) + 2 * min_meta (8M) + min_data (8M) = 29M.
    // ChunkLayout::new enforces this via data_phys + data_size <= total.
    SYSTEM_GROUP_OFFSET
        + SYSTEM_GROUP_SIZE
        + 2 * 8 * 1024 * 1024
        + 8 * 1024 * 1024
}

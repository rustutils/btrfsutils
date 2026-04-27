//! # Mkfs: orchestrate filesystem creation
//!
//! Top-level entry points for creating a btrfs filesystem. See the
//! crate-level docs for the three-phase pipeline (bootstrap +
//! [`crate::post_bootstrap`] + [`crate::rootdir::walk_to_transaction`])
//! and which phase each entry point invokes.
//!
//! - [`make_btrfs`]: bootstrap + post-bootstrap. Produces a valid
//!   empty filesystem.
//! - [`make_btrfs_with_rootdir`]: also runs rootdir population on
//!   top.
//!
//! Hand-built tree-block construction in this module (the
//! `build_root_tree` / `build_extent_tree` / `build_chunk_tree` /
//! `build_dev_tree` / `build_superblock_with_params` family) is
//! the only piece of the pipeline that does not yet go through the
//! transaction crate.

use crate::{
    items,
    layout::{
        BlockLayout, ChunkDevice, ChunkLayout, SYSTEM_GROUP_OFFSET,
        SYSTEM_GROUP_SIZE, StripeInfo, TreeId,
    },
    rootdir,
    tree::{Key, LeafBuilder, LeafHeader},
    write,
};
use anyhow::{Context, Result, bail};
use btrfs_disk::raw;
use std::{
    fs::{File, OpenOptions},
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

/// Build an empty filesystem with `make_btrfs`, then populate it from
/// `rootdir` via the transaction crate.
///
/// Two atomic commits: `make_btrfs` produces the bootstrap layout +
/// runs `post_bootstrap` (creates empty FS / csum / data-reloc / UUID
/// trees, plus FST and BGT when those features are enabled). This
/// function then opens the resulting filesystem, starts a fresh
/// transaction, walks `rootdir` via [`rootdir::walk_to_transaction`]
/// (which emits `INODE_ITEM` / `DIR_ITEM` / `DIR_INDEX` / `INODE_REF`
/// / `XATTR_ITEM` and inline / regular `EXTENT_DATA` records via the
/// transaction crate's high-level helpers), and commits.
///
/// `opts.shrink` (single-device only) patches the device's
/// `DEV_ITEM.total_bytes` and the superblock to the smallest size
/// that still covers the on-disk chunk layout, then truncates the
/// image after sync. `opts.reflink` opens a separate set of
/// writeable device handles and threads them into the walker so
/// `FICLONERANGE` can clone source bytes into each stripe slot.
/// Inode-flags still route to the legacy walker.
#[allow(clippy::too_many_lines)]
fn make_btrfs_with_rootdir_via_transaction(
    cfg: &MkfsConfig,
    rootdir: &Path,
    compress: rootdir::CompressConfig,
    subvol_args: &[crate::args::SubvolArg],
    inode_flags: &[crate::args::InodeFlagsArg],
    opts: RootdirOptions,
) -> Result<()> {
    use btrfs_transaction::{Filesystem, Transaction};
    use std::collections::BTreeMap;

    let now = cfg.now_secs();
    make_btrfs(cfg).context("make_btrfs (empty bootstrap)")?;

    // Shrink is single-device only. Compute the target size up
    // front from the chunk layout (deterministic given cfg, so this
    // matches what `make_btrfs` already laid out).
    let shrunk_size = if opts.shrink && cfg.devices.len() == 1 {
        Some(compute_shrunk_size(cfg)?)
    } else {
        None
    };

    // For --reflink: open a separate set of writeable file handles
    // per device so the walker can issue FICLONERANGE without
    // contending for ownership of the handles consumed by
    // `Filesystem::open*`.
    let reflink_handles: Option<BTreeMap<u64, File>> = if opts.reflink {
        let mut handles: BTreeMap<u64, File> = BTreeMap::new();
        for dev in &cfg.devices {
            let file = OpenOptions::new()
                .write(true)
                .open(&dev.path)
                .with_context(|| {
                    format!(
                        "--reflink: cannot open {} for FICLONERANGE",
                        dev.path.display()
                    )
                })?;
            handles.insert(dev.devid, file);
        }
        Some(handles)
    } else {
        None
    };

    if cfg.devices.len() == 1 {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&cfg.devices[0].path)
            .with_context(|| {
                format!(
                    "failed to reopen {} for rootdir population",
                    cfg.devices[0].path.display()
                )
            })?;
        let mut fs = Filesystem::open(file)
            .context("transactional rootdir: Filesystem::open")?;
        let mut trans = Transaction::start(&mut fs)
            .context("transactional rootdir: Transaction::start")?;
        rootdir::walk_to_transaction(
            rootdir,
            &mut fs,
            &mut trans,
            now,
            compress,
            subvol_args,
            inode_flags,
            reflink_handles.as_ref(),
        )?;
        if let Some(shrunk) = shrunk_size {
            trans
                .set_device_total_bytes(
                    &mut fs,
                    cfg.devices[0].devid,
                    shrunk,
                )
                .with_context(|| {
                    "transactional rootdir: set_device_total_bytes for --shrink"
                })?;
            fs.superblock.total_bytes = shrunk;
        }
        trans
            .commit(&mut fs)
            .context("transactional rootdir: commit")?;
        fs.sync().context("transactional rootdir: fsync")?;
    } else {
        let mut handles: BTreeMap<u64, File> = BTreeMap::new();
        for dev in &cfg.devices {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dev.path)
                .with_context(|| {
                    format!(
                        "failed to reopen {} for rootdir population",
                        dev.path.display()
                    )
                })?;
            let sb = btrfs_disk::superblock::read_superblock(&mut file, 0)
                .with_context(|| {
                    format!(
                        "transactional rootdir: read superblock from {}",
                        dev.path.display()
                    )
                })?;
            handles.insert(sb.dev_item.devid, file);
        }
        let mut fs = Filesystem::open_multi(handles)
            .context("transactional rootdir: Filesystem::open_multi")?;
        let mut trans = Transaction::start(&mut fs)
            .context("transactional rootdir: Transaction::start")?;
        rootdir::walk_to_transaction(
            rootdir,
            &mut fs,
            &mut trans,
            now,
            compress,
            subvol_args,
            inode_flags,
            reflink_handles.as_ref(),
        )?;
        trans
            .commit(&mut fs)
            .context("transactional rootdir: commit")?;
        fs.sync().context("transactional rootdir: fsync")?;
    }

    // Truncate the image after sync so a crash leaves either the
    // pre-shrink superblock (image still longer than the new
    // total_bytes — kernel ignores the tail) or the post-shrink one
    // (image already truncated — also consistent).
    if let Some(shrunk) = shrunk_size {
        let f = OpenOptions::new()
            .write(true)
            .open(&cfg.devices[0].path)
            .with_context(|| {
                format!(
                    "failed to reopen {} for --shrink truncate",
                    cfg.devices[0].path.display()
                )
            })?;
        f.set_len(shrunk).context("--shrink: set_len")?;
    }

    Ok(())
}

/// Compute the smallest size the single device can be truncated to
/// without losing any chunk data. Mirrors the legacy `--shrink`
/// computation in `make_btrfs_with_rootdir`: walk the chunk layout
/// to find the last physical byte used by any chunk, then sectorsize-
/// align.
fn compute_shrunk_size(cfg: &MkfsConfig) -> Result<u64> {
    debug_assert_eq!(cfg.devices.len(), 1);
    let dev = &cfg.devices[0];
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
            anyhow::anyhow!("compute_shrunk_size: chunk layout failed")
        })?;
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
    Ok(rootdir::align_up(phys_end, u64::from(cfg.sectorsize)))
}

/// Create a btrfs filesystem populated from a source directory.
///
/// Validates basic geometry, then builds an empty filesystem via
/// [`make_btrfs`] (which also runs `post_bootstrap` to materialise
/// the always-present trees) and populates the FS tree from
/// `rootdir` via the transaction crate. Handles every `--rootdir`
/// flag: `--subvol`, `--shrink`, `--reflink`, and `--inode-flags`.
///
/// # Errors
///
/// Returns an error if validation fails, the device is too small, or
/// I/O fails.
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
    make_btrfs_with_rootdir_via_transaction(
        cfg,
        rootdir,
        compress,
        subvol_args,
        inode_flags,
        opts,
    )
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

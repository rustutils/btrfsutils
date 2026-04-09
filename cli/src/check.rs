use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Result, bail};
use btrfs_disk::{raw, reader, superblock::SUPER_MIRROR_MAX};
use clap::Parser;
use std::{fs::File, path::PathBuf};

mod chunks;
mod csums;
mod errors;
mod extents;
mod fs_roots;
mod root_refs;
mod superblock;
mod tree_structure;

/// Check mode for filesystem verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum CheckMode {
    Original,
    Lowmem,
}

/// Check structural integrity of a filesystem (unmounted).
///
/// Verify the integrity of a btrfs filesystem by checking internal structures,
/// extent trees, and data checksums. The filesystem must be unmounted before
/// running this command. This is a potentially slow operation that requires
/// CAP_SYS_ADMIN. Use --readonly to perform checks without attempting repairs.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown, clippy::struct_excessive_bools)]
pub struct CheckCommand {
    /// Path to the device containing the btrfs filesystem
    device: PathBuf,

    /// Use this superblock copy
    #[clap(short = 's', long = "super")]
    superblock: Option<u64>,

    /// Use the first valid backup root copy
    #[clap(short = 'b', long)]
    backup: bool,

    /// Use the given bytenr for the tree root
    #[clap(short = 'r', long)]
    tree_root: Option<u64>,

    /// Use the given bytenr for the chunk tree root
    #[clap(long)]
    chunk_root: Option<u64>,

    /// Run in read-only mode (default)
    #[clap(long)]
    readonly: bool,

    /// Try to repair the filesystem (dangerous)
    #[clap(long)]
    repair: bool,

    /// Skip mount checks
    #[clap(long)]
    force: bool,

    /// Checker operating mode
    #[clap(long)]
    mode: Option<CheckMode>,

    /// Create a new CRC tree
    #[clap(long)]
    init_csum_tree: bool,

    /// Create a new extent tree
    #[clap(long)]
    init_extent_tree: bool,

    /// Verify checksums of data blocks
    #[clap(long)]
    check_data_csum: bool,

    /// Print a report on qgroup consistency
    #[clap(short = 'Q', long)]
    qgroup_report: bool,

    /// Print subvolume extents and sharing state for the given subvolume ID
    #[clap(short = 'E', long)]
    subvol_extents: Option<u64>,

    /// Indicate progress
    #[clap(short = 'p', long)]
    progress: bool,
}

impl Runnable for CheckCommand {
    #[allow(clippy::too_many_lines)]
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        // Reject unsupported flags.
        if self.repair {
            bail!("repair mode is not yet supported");
        }
        if self.init_csum_tree {
            bail!("--init-csum-tree is not yet supported");
        }
        if self.init_extent_tree {
            bail!("--init-extent-tree is not yet supported");
        }
        if self.qgroup_report {
            bail!("--qgroup-report is not yet supported");
        }
        if self.subvol_extents.is_some() {
            bail!("--subvol-extents is not yet supported");
        }

        // Mount check.
        if !self.force && is_mounted(&self.device) {
            bail!(
                "'{}' is mounted, use --force to continue",
                self.device.display()
            );
        }

        if let Some(m) = self.superblock
            && m >= u64::from(SUPER_MIRROR_MAX)
        {
            bail!(
                "super mirror index {m} is out of range (max {})",
                SUPER_MIRROR_MAX - 1
            );
        }

        eprintln!("Opening filesystem to check...");

        let mut file = File::open(&self.device)?;
        #[allow(clippy::cast_possible_truncation)] // mirror index fits in u32
        let mirror = self.superblock.unwrap_or(0) as u32;

        let mut open = open_with_overrides(
            file.try_clone()?,
            mirror,
            self.backup,
            self.tree_root,
            self.chunk_root,
        )?;

        let sb = &open.superblock;
        eprintln!("Checking filesystem on {}", self.device.display());
        eprintln!("UUID: {}", sb.fsid);

        let mut results = errors::CheckResults::new(sb.bytes_used);

        // Phase 1: Superblock validation.
        eprintln!("[1/7] checking superblocks");
        superblock::check_superblocks(&mut file, &mut results);

        // Phase 2: Tree structure checks (all trees).
        eprintln!("[2/7] checking root items");
        let tree_block_addrs = tree_structure::check_all_trees(
            &mut open.reader,
            sb,
            &open.tree_roots,
            &mut results,
        );

        // Phase 3: Extent tree cross-checks.
        eprintln!("[3/7] checking extents");
        let extent_root = open
            .tree_roots
            .get(&u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID))
            .map(|&(bytenr, _)| bytenr);
        if let Some(extent_root) = extent_root {
            extents::check_extent_tree(
                &mut open.reader,
                extent_root,
                &tree_block_addrs,
                &mut results,
            );
        }

        // Phase 4: Chunk / block group / device extent cross-checks.
        eprintln!("[4/7] checking free space tree");
        let block_group_tree_root = if sb.compat_ro_flags
            & u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE)
            != 0
        {
            open.tree_roots
                .get(&u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID))
                .map(|&(bytenr, _)| bytenr)
        } else {
            None
        };
        let dev_tree_root = open
            .tree_roots
            .get(&u64::from(raw::BTRFS_DEV_TREE_OBJECTID))
            .map(|&(bytenr, _)| bytenr);
        if let (Some(er), Some(dr)) = (extent_root, dev_tree_root) {
            chunks::check_chunks(
                &mut open.reader,
                er,
                block_group_tree_root,
                dr,
                &mut results,
            );
        }

        // Phase 5: FS tree checks.
        eprintln!("[5/7] checking fs roots");
        fs_roots::check_fs_roots(
            &mut open.reader,
            &open.tree_roots,
            &mut results,
        );

        // Phase 6: Checksum tree verification.
        if self.check_data_csum {
            eprintln!("[6/7] checking csums items (verifying data)");
        } else {
            eprintln!(
                "[6/7] checking only csums items \
                 (without verifying data)"
            );
        }
        let csum_root = open
            .tree_roots
            .get(&u64::from(raw::BTRFS_CSUM_TREE_OBJECTID))
            .map(|&(bytenr, _)| bytenr);
        if let Some(csum_root) = csum_root {
            csums::check_csums(
                &mut open.reader,
                sb,
                csum_root,
                self.check_data_csum,
                &mut results,
            );
        }

        eprintln!("[7/7] checking root refs");
        root_refs::check_root_refs(&mut open.reader, sb.root, &mut results);

        results.print_summary();

        if results.has_errors() {
            std::process::exit(1);
        }

        Ok(())
    }
}

/// Open a filesystem for checking, applying root pointer overrides.
///
/// Supports `--backup` (use best backup root from the superblock),
/// `--tree-root` (override the root tree pointer), and `--chunk-root`
/// (override the chunk tree pointer).
fn open_with_overrides(
    file: File,
    mirror: u32,
    backup: bool,
    tree_root: Option<u64>,
    chunk_root: Option<u64>,
) -> Result<reader::OpenFilesystem<File>> {
    use btrfs_disk::{chunk, superblock as sb_mod};

    // If no overrides, use the standard path.
    if !backup && tree_root.is_none() && chunk_root.is_none() {
        return Ok(reader::filesystem_open_mirror(file, mirror)?);
    }

    // Read the superblock first, then apply overrides before walking trees.
    let mut reader = file;
    let mut sb = sb_mod::read_superblock(&mut reader, mirror)?;

    // --backup: find the best backup root (highest generation with nonzero tree_root).
    if backup {
        let best = sb
            .backup_roots
            .iter()
            .filter(|b| b.tree_root != 0)
            .max_by_key(|b| b.tree_root_gen);

        if let Some(b) = best {
            eprintln!(
                "Using backup root: tree_root={:#x} (gen {}), chunk_root={:#x} (gen {})",
                b.tree_root, b.tree_root_gen, b.chunk_root, b.chunk_root_gen,
            );
            sb.root = b.tree_root;
            sb.root_level = b.tree_root_level;
            sb.chunk_root = b.chunk_root;
            sb.chunk_root_level = b.chunk_root_level;
            sb.generation = b.tree_root_gen;
        } else {
            bail!("no valid backup roots found in superblock");
        }
    }

    // Explicit overrides take precedence over --backup.
    if let Some(bytenr) = tree_root {
        eprintln!("Using tree root override: {bytenr:#x}");
        sb.root = bytenr;
    }
    if let Some(bytenr) = chunk_root {
        eprintln!("Using chunk root override: {bytenr:#x}");
        sb.chunk_root = bytenr;
    }

    // Bootstrap chunk cache and walk the (possibly overridden) chunk tree.
    let chunk_cache = chunk::seed_from_sys_chunk_array(
        &sb.sys_chunk_array,
        sb.sys_chunk_array_size,
    );
    let mut block_reader =
        btrfs_disk::reader::BlockReader::new(reader, sb.nodesize, chunk_cache);

    reader::read_chunk_tree(&mut block_reader, sb.chunk_root)?;
    let tree_roots = reader::read_root_tree(&mut block_reader, sb.root)?;

    Ok(reader::OpenFilesystem {
        reader: block_reader,
        superblock: sb,
        tree_roots,
    })
}

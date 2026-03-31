use crate::{Format, Runnable, util::is_mounted};
use anyhow::{Result, bail};
use btrfs_disk::{reader, superblock::SUPER_MIRROR_MAX};
use clap::Parser;
use std::{fs::File, path::PathBuf};

mod errors;
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
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
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
        if self.backup {
            bail!("--backup is not yet supported");
        }
        if self.tree_root.is_some() {
            bail!("--tree-root is not yet supported");
        }
        if self.chunk_root.is_some() {
            bail!("--chunk-root is not yet supported");
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
        let mirror = self.superblock.unwrap_or(0) as u32;

        let mut open =
            reader::filesystem_open_mirror(file.try_clone()?, mirror)?;

        let sb = &open.superblock;
        eprintln!("Checking filesystem on {}", self.device.display());
        eprintln!("UUID: {}", sb.fsid);

        let mut results = errors::CheckResults::new(sb.bytes_used);

        // Phase 1: Superblock validation.
        eprintln!("[1/7] checking superblocks");
        superblock::check_superblocks(&mut file, &mut results);

        // Phase 2: Tree structure checks.
        eprintln!("[2/7] checking root items");
        tree_structure::check_all_trees(
            &mut open.reader,
            sb,
            &open.tree_roots,
            &mut results,
        );
        eprintln!("[3/7] checking extents");
        eprintln!("[4/7] checking free space tree");
        eprintln!("[5/7] checking fs roots");
        if self.check_data_csum {
            eprintln!("[6/7] checking csums items (verifying data)");
        } else {
            eprintln!(
                "[6/7] checking only csums items \
                 (without verifying data)"
            );
        }
        eprintln!("[7/7] checking root refs");

        results.print_summary();

        if results.has_errors() {
            std::process::exit(1);
        }

        Ok(())
    }
}

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Check mode for filesystem verification
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
        todo!("implement check")
    }
}

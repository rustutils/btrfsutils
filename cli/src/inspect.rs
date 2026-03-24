use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

pub mod dump_super;
pub mod dump_tree;
pub mod inode_resolve;
pub mod list_chunks;
pub mod logical_resolve;
pub mod map_swapfile;
pub mod min_dev_size;
pub mod rootid;
pub mod subvolid_resolve;
pub mod tree_stats;

use dump_super::DumpSuperCommand;
use dump_tree::DumpTreeCommand;
use inode_resolve::InodeResolveCommand;
use list_chunks::ListChunksCommand;
use logical_resolve::LogicalResolveCommand;
use map_swapfile::MapSwapfileCommand;
use min_dev_size::MinDevSizeCommand;
use rootid::RootidCommand;
use subvolid_resolve::SubvolidResolveCommand;
use tree_stats::TreeStatsCommand;

/// Query various internal filesystem information
#[derive(Parser, Debug)]
pub struct InspectCommand {
    #[clap(subcommand)]
    pub subcommand: InspectSubcommand,
}

#[derive(Parser, Debug)]
pub enum InspectSubcommand {
    /// Get tree ID of the containing subvolume of path
    Rootid(RootidCommand),
    /// Get file system paths for the given inode
    InodeResolve(InodeResolveCommand),
    /// Get file system paths for the given logical address
    LogicalResolve(LogicalResolveCommand),
    /// Get subvolume ID and tree ID of the given path
    SubvolidResolve(SubvolidResolveCommand),
    /// Print physical extents of a file suitable for swap
    MapSwapfile(MapSwapfileCommand),
    /// Print minimum device size to resize a device
    MinDevSize(MinDevSizeCommand),
    /// Dump tree blocks from a btrfs filesystem
    DumpTree(DumpTreeCommand),
    /// Dump the superblock from a btrfs filesystem
    DumpSuper(DumpSuperCommand),
    /// Print statistics about trees in a btrfs filesystem
    TreeStats(TreeStatsCommand),
    /// List chunks in a btrfs filesystem
    ListChunks(ListChunksCommand),
}

impl Runnable for InspectCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            InspectSubcommand::Rootid(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::InodeResolve(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::LogicalResolve(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::SubvolidResolve(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::MapSwapfile(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::MinDevSize(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::DumpTree(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::DumpSuper(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::TreeStats(cmd) => cmd.run(format, dry_run),
            InspectSubcommand::ListChunks(cmd) => cmd.run(format, dry_run),
        }
    }
}

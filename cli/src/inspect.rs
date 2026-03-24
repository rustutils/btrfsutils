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

/// Query various internal filesystem information.
///
/// Access advanced information about filesystem internals including inode
/// resolution, logical extent to physical block mapping, subvolume IDs,
/// chunk layout, and other diagnostic data. These commands are primarily
/// useful for debugging, analysis, and recovery operations. Most operations
/// require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct InspectCommand {
    #[clap(subcommand)]
    pub subcommand: InspectSubcommand,
}

#[derive(Parser, Debug)]
pub enum InspectSubcommand {
    Rootid(RootidCommand),
    InodeResolve(InodeResolveCommand),
    LogicalResolve(LogicalResolveCommand),
    SubvolidResolve(SubvolidResolveCommand),
    MapSwapfile(MapSwapfileCommand),
    MinDevSize(MinDevSizeCommand),
    DumpTree(DumpTreeCommand),
    DumpSuper(DumpSuperCommand),
    TreeStats(TreeStatsCommand),
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

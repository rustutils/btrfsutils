use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;

mod dump_super;
mod dump_tree;
mod inode_resolve;
mod list_chunks;
mod logical_resolve;
mod map_swapfile;
mod min_dev_size;
mod print_super;
mod print_tree;
mod rootid;
mod subvolid_resolve;
mod tree_stats;

pub use self::{
    dump_super::*, dump_tree::*, inode_resolve::*, list_chunks::*,
    logical_resolve::*, map_swapfile::*, min_dev_size::*, rootid::*,
    subvolid_resolve::*, tree_stats::*,
};

/// Query various internal filesystem information.
///
/// Access advanced information about filesystem internals including inode
/// resolution, logical extent to physical block mapping, subvolume IDs,
/// chunk layout, and other diagnostic data. These commands are primarily
/// useful for debugging, analysis, and recovery operations. Most operations
/// require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
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
    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            InspectSubcommand::Rootid(cmd) => cmd.run(ctx),
            InspectSubcommand::InodeResolve(cmd) => cmd.run(ctx),
            InspectSubcommand::LogicalResolve(cmd) => cmd.run(ctx),
            InspectSubcommand::SubvolidResolve(cmd) => cmd.run(ctx),
            InspectSubcommand::MapSwapfile(cmd) => cmd.run(ctx),
            InspectSubcommand::MinDevSize(cmd) => cmd.run(ctx),
            InspectSubcommand::DumpTree(cmd) => cmd.run(ctx),
            InspectSubcommand::DumpSuper(cmd) => cmd.run(ctx),
            InspectSubcommand::TreeStats(cmd) => cmd.run(ctx),
            InspectSubcommand::ListChunks(cmd) => cmd.run(ctx),
        }
    }
}

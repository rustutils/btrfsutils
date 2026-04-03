use crate::{CommandGroup, Runnable};
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

impl CommandGroup for InspectCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            InspectSubcommand::Rootid(cmd) => cmd,
            InspectSubcommand::InodeResolve(cmd) => cmd,
            InspectSubcommand::LogicalResolve(cmd) => cmd,
            InspectSubcommand::SubvolidResolve(cmd) => cmd,
            InspectSubcommand::MapSwapfile(cmd) => cmd,
            InspectSubcommand::MinDevSize(cmd) => cmd,
            InspectSubcommand::DumpTree(cmd) => cmd,
            InspectSubcommand::DumpSuper(cmd) => cmd,
            InspectSubcommand::TreeStats(cmd) => cmd,
            InspectSubcommand::ListChunks(cmd) => cmd,
        }
    }
}

use crate::{CommandGroup, Runnable};
use clap::Parser;

mod assign;
mod clear_stale;
mod create;
mod destroy;
mod limit;
mod remove;
mod show;

pub use self::{
    assign::*, clear_stale::*, create::*, destroy::*, limit::*, remove::*,
    show::*,
};

/// Manage quota groups.
///
/// Create, destroy, and configure quota groups to enforce storage limits and
/// track usage for subvolumes and hierarchies of subvolumes. Quota groups
/// provide flexible quota management that can be applied at different levels
/// in the subvolume hierarchy. Most operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct QgroupCommand {
    #[clap(subcommand)]
    pub subcommand: QgroupSubcommand,
}

impl CommandGroup for QgroupCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            QgroupSubcommand::Assign(cmd) => cmd,
            QgroupSubcommand::Remove(cmd) => cmd,
            QgroupSubcommand::Create(cmd) => cmd,
            QgroupSubcommand::Destroy(cmd) => cmd,
            QgroupSubcommand::Show(cmd) => cmd,
            QgroupSubcommand::Limit(cmd) => cmd,
            QgroupSubcommand::ClearStale(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum QgroupSubcommand {
    Assign(QgroupAssignCommand),
    Remove(QgroupRemoveCommand),
    Create(QgroupCreateCommand),
    Destroy(QgroupDestroyCommand),
    Show(QgroupShowCommand),
    Limit(QgroupLimitCommand),
    #[command(name = "clear-stale")]
    ClearStale(QgroupClearStaleCommand),
}

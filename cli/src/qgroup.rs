use crate::{Format, Runnable};
use anyhow::Result;
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
pub struct QgroupCommand {
    #[clap(subcommand)]
    pub subcommand: QgroupSubcommand,
}

impl Runnable for QgroupCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            QgroupSubcommand::Assign(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::Remove(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::Create(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::Destroy(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::Show(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::Limit(cmd) => cmd.run(format, dry_run),
            QgroupSubcommand::ClearStale(cmd) => cmd.run(format, dry_run),
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

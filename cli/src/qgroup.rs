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

use assign::QgroupAssignCommand;
use clear_stale::QgroupClearStaleCommand;
use create::QgroupCreateCommand;
use destroy::QgroupDestroyCommand;
use limit::QgroupLimitCommand;
use remove::QgroupRemoveCommand;
use show::QgroupShowCommand;

/// Manage quota groups
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
    /// Assign a qgroup as the child of another
    Assign(QgroupAssignCommand),
    /// Remove a child qgroup relation
    Remove(QgroupRemoveCommand),
    /// Create a subvolume quota group
    Create(QgroupCreateCommand),
    /// Destroy a quota group
    Destroy(QgroupDestroyCommand),
    /// List subvolume quota groups
    Show(QgroupShowCommand),
    /// Set the limits for a subvolume quota group
    Limit(QgroupLimitCommand),
    /// Clear all stale qgroups without a subvolume
    #[command(name = "clear-stale")]
    ClearStale(QgroupClearStaleCommand),
}

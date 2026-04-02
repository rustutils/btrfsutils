use crate::{Format, RunContext, Runnable};
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
#[allow(clippy::doc_markdown)]
pub struct QgroupCommand {
    #[clap(subcommand)]
    pub subcommand: QgroupSubcommand,
}

impl Runnable for QgroupCommand {
    fn supported_formats(&self) -> &[Format] {
        match &self.subcommand {
            QgroupSubcommand::Show(cmd) => cmd.supported_formats(),
            _ => &[Format::Text, Format::Modern],
        }
    }

    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            QgroupSubcommand::Assign(cmd) => cmd.run(ctx),
            QgroupSubcommand::Remove(cmd) => cmd.run(ctx),
            QgroupSubcommand::Create(cmd) => cmd.run(ctx),
            QgroupSubcommand::Destroy(cmd) => cmd.run(ctx),
            QgroupSubcommand::Show(cmd) => cmd.run(ctx),
            QgroupSubcommand::Limit(cmd) => cmd.run(ctx),
            QgroupSubcommand::ClearStale(cmd) => cmd.run(ctx),
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

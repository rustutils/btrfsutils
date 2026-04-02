use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;

mod disable;
mod enable;
mod rescan;
mod status;

pub use self::{disable::*, enable::*, rescan::*, status::*};

/// Manage filesystem quota settings.
///
/// Enable or disable quotas, configure quota rescan operations, and view
/// quota status. Quotas allow enforcing limits on filesystem usage by
/// subvolume or quota group. Quota operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct QuotaCommand {
    #[clap(subcommand)]
    pub subcommand: QuotaSubcommand,
}

impl Runnable for QuotaCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            QuotaSubcommand::Enable(cmd) => cmd.run(ctx),
            QuotaSubcommand::Disable(cmd) => cmd.run(ctx),
            QuotaSubcommand::Rescan(cmd) => cmd.run(ctx),
            QuotaSubcommand::Status(cmd) => cmd.run(ctx),
        }
    }
}

#[derive(Parser, Debug)]
pub enum QuotaSubcommand {
    Enable(QuotaEnableCommand),
    Disable(QuotaDisableCommand),
    Rescan(QuotaRescanCommand),
    Status(QuotaStatusCommand),
}

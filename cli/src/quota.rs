use crate::{CommandGroup, Runnable};
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
#[clap(arg_required_else_help = true)]
pub struct QuotaCommand {
    #[clap(subcommand)]
    pub subcommand: QuotaSubcommand,
}

impl CommandGroup for QuotaCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            QuotaSubcommand::Enable(cmd) => cmd,
            QuotaSubcommand::Disable(cmd) => cmd,
            QuotaSubcommand::Rescan(cmd) => cmd,
            QuotaSubcommand::Status(cmd) => cmd,
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

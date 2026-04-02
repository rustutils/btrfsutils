pub(crate) use crate::util::open_path;
use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;

mod cancel;
mod filters;
mod pause;
mod resume;
mod start;
mod status;

pub use self::{cancel::*, pause::*, resume::*, start::*, status::*};

/// Balance data across devices, or change block groups using filters.
///
/// Rebalance data and metadata across devices to improve performance or
/// recover space. Balance is typically a long-running operation. You can
/// pause, resume, or cancel a balance in progress. Progress and status can
/// be queried at any time. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct BalanceCommand {
    #[clap(subcommand)]
    pub subcommand: BalanceSubcommand,
}

impl Runnable for BalanceCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            BalanceSubcommand::Start(cmd) => cmd.run(ctx),
            BalanceSubcommand::Pause(cmd) => cmd.run(ctx),
            BalanceSubcommand::Cancel(cmd) => cmd.run(ctx),
            BalanceSubcommand::Resume(cmd) => cmd.run(ctx),
            BalanceSubcommand::Status(cmd) => cmd.run(ctx),
        }
    }
}

#[derive(Parser, Debug)]
pub enum BalanceSubcommand {
    Start(BalanceStartCommand),
    Pause(BalancePauseCommand),
    Cancel(BalanceCancelCommand),
    Resume(BalanceResumeCommand),
    Status(BalanceStatusCommand),
}

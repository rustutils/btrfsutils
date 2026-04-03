pub(crate) use crate::util::open_path;
use crate::{CommandGroup, Runnable};
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
#[clap(arg_required_else_help = true)]
pub struct BalanceCommand {
    #[clap(subcommand)]
    pub subcommand: BalanceSubcommand,
}

impl CommandGroup for BalanceCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            BalanceSubcommand::Start(cmd) => cmd,
            BalanceSubcommand::Pause(cmd) => cmd,
            BalanceSubcommand::Cancel(cmd) => cmd,
            BalanceSubcommand::Resume(cmd) => cmd,
            BalanceSubcommand::Status(cmd) => cmd,
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

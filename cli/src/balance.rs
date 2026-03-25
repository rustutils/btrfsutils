use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::{fs::File, path::PathBuf};

mod cancel;
mod pause;
mod resume;
mod start;
mod status;

use cancel::BalanceCancelCommand;
use pause::BalancePauseCommand;
use resume::BalanceResumeCommand;
use start::BalanceStartCommand;
use status::BalanceStatusCommand;

/// Balance data across devices, or change block groups using filters.
///
/// Rebalance data and metadata across devices to improve performance or
/// recover space. Balance is typically a long-running operation. You can
/// pause, resume, or cancel a balance in progress. Progress and status can
/// be queried at any time. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct BalanceCommand {
    #[clap(subcommand)]
    pub subcommand: BalanceSubcommand,
}

impl Runnable for BalanceCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            BalanceSubcommand::Start(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Pause(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Cancel(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Resume(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Status(cmd) => cmd.run(format, dry_run),
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

/// Open a path as a read-only file descriptor, suitable for passing to ioctls.
fn open_path(path: &PathBuf) -> Result<File> {
    use anyhow::Context;
    File::open(path).with_context(|| format!("failed to open '{}'", path.display()))
}

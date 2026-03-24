use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod cancel;
mod start;
mod status;

use cancel::ReplaceCancelCommand;
use start::ReplaceStartCommand;
use status::ReplaceStatusCommand;

/// Replace a device in the filesystem
#[derive(Parser, Debug)]
pub struct ReplaceCommand {
    #[clap(subcommand)]
    pub subcommand: ReplaceSubcommand,
}

impl Runnable for ReplaceCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            ReplaceSubcommand::Start(cmd) => cmd.run(format, dry_run),
            ReplaceSubcommand::Status(cmd) => cmd.run(format, dry_run),
            ReplaceSubcommand::Cancel(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum ReplaceSubcommand {
    /// Replace device of a btrfs filesystem
    Start(ReplaceStartCommand),
    /// Print status of a running device replace operation
    Status(ReplaceStatusCommand),
    /// Cancel a running device replace operation
    Cancel(ReplaceCancelCommand),
}

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod cancel;
mod start;
mod status;

use cancel::ReplaceCancelCommand;
use start::ReplaceStartCommand;
use status::ReplaceStatusCommand;

/// Replace a device in the filesystem.
///
/// Replace a device with another device or a spare. During replacement,
/// data is read from the old device and written to the new one. The replace
/// operation can be monitored and cancelled. Requires CAP_SYS_ADMIN.
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
    Start(ReplaceStartCommand),
    Status(ReplaceStatusCommand),
    Cancel(ReplaceCancelCommand),
}

use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;

mod cancel;
mod start;
mod status;

pub use self::{cancel::*, start::*, status::*};

/// Replace a device in the filesystem.
///
/// Replace a device with another device or a spare. During replacement,
/// data is read from the old device and written to the new one. The replace
/// operation can be monitored and cancelled. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct ReplaceCommand {
    #[clap(subcommand)]
    pub subcommand: ReplaceSubcommand,
}

impl Runnable for ReplaceCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            ReplaceSubcommand::Start(cmd) => cmd.run(ctx),
            ReplaceSubcommand::Status(cmd) => cmd.run(ctx),
            ReplaceSubcommand::Cancel(cmd) => cmd.run(ctx),
        }
    }
}

#[derive(Parser, Debug)]
pub enum ReplaceSubcommand {
    Start(ReplaceStartCommand),
    Status(ReplaceStatusCommand),
    Cancel(ReplaceCancelCommand),
}

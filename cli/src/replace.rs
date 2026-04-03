use crate::{CommandGroup, Runnable};
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
#[clap(arg_required_else_help = true)]
pub struct ReplaceCommand {
    #[clap(subcommand)]
    pub subcommand: ReplaceSubcommand,
}

impl CommandGroup for ReplaceCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            ReplaceSubcommand::Start(cmd) => cmd,
            ReplaceSubcommand::Status(cmd) => cmd,
            ReplaceSubcommand::Cancel(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum ReplaceSubcommand {
    Start(ReplaceStartCommand),
    Status(ReplaceStatusCommand),
    Cancel(ReplaceCancelCommand),
}

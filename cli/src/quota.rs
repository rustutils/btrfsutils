use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod disable;
mod enable;
mod rescan;
mod status;

use disable::QuotaDisableCommand;
use enable::QuotaEnableCommand;
use rescan::QuotaRescanCommand;
use status::QuotaStatusCommand;

/// Manage filesystem quota settings
#[derive(Parser, Debug)]
pub struct QuotaCommand {
    #[clap(subcommand)]
    pub subcommand: QuotaSubcommand,
}

impl Runnable for QuotaCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            QuotaSubcommand::Enable(cmd) => cmd.run(format, dry_run),
            QuotaSubcommand::Disable(cmd) => cmd.run(format, dry_run),
            QuotaSubcommand::Rescan(cmd) => cmd.run(format, dry_run),
            QuotaSubcommand::Status(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum QuotaSubcommand {
    /// Enable subvolume quota support for a filesystem
    Enable(QuotaEnableCommand),
    /// Disable subvolume quota support for a filesystem
    Disable(QuotaDisableCommand),
    /// Trash all qgroup numbers and scan the metadata again
    Rescan(QuotaRescanCommand),
    /// Show status information about quota on the filesystem
    Status(QuotaStatusCommand),
}

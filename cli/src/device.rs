use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod add;
mod ready;
mod remove;
mod scan;
mod stats;
mod usage;

use add::DeviceAddCommand;
use ready::DeviceReadyCommand;
use remove::DeviceRemoveCommand;
use scan::DeviceScanCommand;
use stats::DeviceStatsCommand;
use usage::DeviceUsageCommand;

/// Manage devices in a btrfs filesystem.
///
/// Perform operations on block devices that are part of a btrfs filesystem,
/// including adding and removing devices, viewing device statistics, and
/// checking device readiness. Most operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct DeviceCommand {
    #[clap(subcommand)]
    pub subcommand: DeviceSubcommand,
}

impl Runnable for DeviceCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            DeviceSubcommand::Add(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Remove(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Delete(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Stats(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Scan(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Ready(cmd) => cmd.run(format, dry_run),
            DeviceSubcommand::Usage(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum DeviceSubcommand {
    Add(DeviceAddCommand),
    Remove(DeviceRemoveCommand),
    #[clap(alias = "del")]
    Delete(DeviceRemoveCommand),
    Stats(DeviceStatsCommand),
    Scan(DeviceScanCommand),
    Ready(DeviceReadyCommand),
    Usage(DeviceUsageCommand),
}

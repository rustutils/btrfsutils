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

/// Manage devices in a btrfs filesystem
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
    /// Add one or more devices to a mounted filesystem
    Add(DeviceAddCommand),
    /// Remove one or more devices from a mounted filesystem
    Remove(DeviceRemoveCommand),
    /// Remove one or more devices from a mounted filesystem (alias for remove)
    #[clap(alias = "del")]
    Delete(DeviceRemoveCommand),
    /// Show per-device I/O error statistics
    Stats(DeviceStatsCommand),
    /// Register or unregister devices with the kernel device scanner
    Scan(DeviceScanCommand),
    /// Check whether all devices of a filesystem are present and ready to mount
    Ready(DeviceReadyCommand),
    /// Show detailed information about internal allocations in devices
    Usage(DeviceUsageCommand),
}

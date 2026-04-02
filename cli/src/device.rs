use crate::{Format, RunContext, Runnable};
use anyhow::Result;
use clap::Parser;

mod add;
mod ready;
mod remove;
mod scan;
mod stats;
mod usage;

pub use self::{add::*, ready::*, remove::*, scan::*, stats::*, usage::*};

/// Manage devices in a btrfs filesystem.
///
/// Perform operations on block devices that are part of a btrfs filesystem,
/// including adding and removing devices, viewing device statistics, and
/// checking device readiness. Most operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct DeviceCommand {
    #[clap(subcommand)]
    pub subcommand: DeviceSubcommand,
}

impl Runnable for DeviceCommand {
    fn supported_formats(&self) -> &[Format] {
        match &self.subcommand {
            DeviceSubcommand::Stats(cmd) => cmd.supported_formats(),
            _ => &[Format::Text, Format::Modern],
        }
    }

    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            DeviceSubcommand::Add(cmd) => cmd.run(ctx),
            DeviceSubcommand::Remove(cmd) | DeviceSubcommand::Delete(cmd) => {
                cmd.run(ctx)
            }
            DeviceSubcommand::Stats(cmd) => cmd.run(ctx),
            DeviceSubcommand::Scan(cmd) => cmd.run(ctx),
            DeviceSubcommand::Ready(cmd) => cmd.run(ctx),
            DeviceSubcommand::Usage(cmd) => cmd.run(ctx),
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

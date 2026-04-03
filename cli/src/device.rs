use crate::{CommandGroup, Runnable};
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
#[clap(arg_required_else_help = true)]
pub struct DeviceCommand {
    #[clap(subcommand)]
    pub subcommand: DeviceSubcommand,
}

impl CommandGroup for DeviceCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            DeviceSubcommand::Add(cmd) => cmd,
            DeviceSubcommand::Remove(cmd) | DeviceSubcommand::Delete(cmd) => {
                cmd
            }
            DeviceSubcommand::Stats(cmd) => cmd,
            DeviceSubcommand::Scan(cmd) => cmd,
            DeviceSubcommand::Ready(cmd) => cmd,
            DeviceSubcommand::Usage(cmd) => cmd,
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

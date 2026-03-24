use anyhow::Result;
use clap::Parser;

use crate::{Format, Runnable};

/// Create /dev/btrfs-control
#[derive(Parser, Debug)]
pub struct RescueCreateControlDeviceCommand {}

impl Runnable for RescueCreateControlDeviceCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue create-control-device")
    }
}

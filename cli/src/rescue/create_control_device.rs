use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

/// Create /dev/btrfs-control
#[derive(Parser, Debug)]
pub struct RescueCreateControlDeviceCommand {}

impl Runnable for RescueCreateControlDeviceCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue create-control-device")
    }
}

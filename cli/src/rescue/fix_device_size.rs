use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Re-align device and super block sizes
#[derive(Parser, Debug)]
pub struct RescueFixDeviceSizeCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueFixDeviceSizeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        todo!("implement rescue fix-device-size")
    }
}

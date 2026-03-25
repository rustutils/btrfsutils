use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Clear the tree log (usable if it's corrupted and prevents mount)
#[derive(Parser, Debug)]
pub struct RescueZeroLogCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueZeroLogCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue zero-log")
    }
}

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Print physical extents of a file suitable for swap
#[derive(Parser, Debug)]
pub struct MapSwapfileCommand {
    /// Path to a file on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for MapSwapfileCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement map-swapfile")
    }
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Remove leftover items pertaining to the deprecated inode cache feature
#[derive(Parser, Debug)]
pub struct RescueClearInoCacheCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueClearInoCacheCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue clear-ino-cache")
    }
}

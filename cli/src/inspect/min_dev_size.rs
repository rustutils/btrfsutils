use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Print minimum device size to resize a device
#[derive(Parser, Debug)]
pub struct MinDevSizeCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for MinDevSizeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement min-dev-size")
    }
}

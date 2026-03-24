use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Print statistics about trees in a btrfs filesystem
#[derive(Parser, Debug)]
pub struct TreeStatsCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for TreeStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement tree-stats")
    }
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// List chunks in a btrfs filesystem
#[derive(Parser, Debug)]
pub struct ListChunksCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for ListChunksCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement list-chunks")
    }
}

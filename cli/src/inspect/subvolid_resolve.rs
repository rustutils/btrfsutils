use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Get subvolume ID and tree ID of the given path
#[derive(Parser, Debug)]
pub struct SubvolidResolveCommand {
    /// Subvolume ID
    subvolid: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for SubvolidResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement subvolid-resolve")
    }
}

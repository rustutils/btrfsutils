use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Dump the superblock from a btrfs filesystem
#[derive(Parser, Debug)]
pub struct DumpSuperCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,

    /// Only dump this superblock copy (0, 1, or 2)
    #[clap(short = 'i', long)]
    index: Option<u32>,

    /// Print the superblock in JSON format
    #[clap(short = 'j', long)]
    json: bool,
}

impl Runnable for DumpSuperCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement dump-super")
    }
}

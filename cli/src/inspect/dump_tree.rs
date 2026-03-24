use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Dump tree blocks from a btrfs filesystem
#[derive(Parser, Debug)]
pub struct DumpTreeCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,

    /// Only dump blocks from this tree
    #[clap(short = 't', long)]
    tree: Option<u64>,

    /// Only dump blocks at this block number
    #[clap(short = 'b', long)]
    block: Option<u64>,
}

impl Runnable for DumpTreeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement dump-tree")
    }
}

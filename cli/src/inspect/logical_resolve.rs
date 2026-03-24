use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Get file system paths for the given logical address
#[derive(Parser, Debug)]
pub struct LogicalResolveCommand {
    /// Logical address
    logical: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,

    /// Skip the path resolving and print the inodes instead
    #[clap(short = 'P')]
    ignore_offset: bool,

    /// Ignore offsets when matching references
    #[clap(short = 'o')]
    skip_paths: bool,

    /// Set inode container's size
    #[clap(short = 's')]
    bufsize: Option<u64>,
}

impl Runnable for LogicalResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement logical-resolve")
    }
}

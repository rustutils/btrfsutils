use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Get file system paths for the given inode
#[derive(Parser, Debug)]
pub struct InodeResolveCommand {
    /// Inode number
    inode: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for InodeResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement inode-resolve")
    }
}

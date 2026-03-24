use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

use crate::{Format, Runnable};

/// Get tree ID of the containing subvolume of path
#[derive(Parser, Debug)]
pub struct RootidCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for RootidCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let rootid = btrfs_uapi::inode::lookup_path_rootid(fd)
            .context("failed to look up root ID (is this a btrfs filesystem?)")?;

        println!("{}", rootid);
        Ok(())
    }
}

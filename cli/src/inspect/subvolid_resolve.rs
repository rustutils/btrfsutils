use crate::{Format, Runnable};
use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Get subvolume ID and tree ID of the given path
#[derive(Parser, Debug)]
pub struct SubvolidResolveCommand {
    /// Subvolume ID to resolve
    subvolid: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for SubvolidResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let resolved_path = btrfs_uapi::inode::subvolid_resolve(fd, self.subvolid)
            .context("failed to resolve subvolume ID (is this a btrfs filesystem?)")?;

        println!("{}", resolved_path);
        Ok(())
    }
}

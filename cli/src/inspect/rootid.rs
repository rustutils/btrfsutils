use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Get tree ID of the containing subvolume of path
#[derive(Parser, Debug)]
pub struct RootidCommand {
    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for RootidCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let rootid = btrfs_uapi::inode::lookup_path_rootid(fd).context(
            "failed to look up root ID (is this a btrfs filesystem?)",
        )?;

        println!("{rootid}");
        Ok(())
    }
}

use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Get file system paths for the given inode
#[derive(Parser, Debug)]
pub struct InodeResolveCommand {
    /// Inode number
    inode: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for InodeResolveCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let paths = btrfs_uapi::inode::ino_paths(fd, self.inode).context(
            "failed to look up inode paths (is this a btrfs filesystem?)",
        )?;

        if paths.is_empty() {
            eprintln!("no paths found for inode {}", self.inode);
        } else {
            for path in paths {
                println!("{path}");
            }
        }

        Ok(())
    }
}

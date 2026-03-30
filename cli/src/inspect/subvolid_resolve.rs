use crate::{Format, Runnable, util::open_path};
use anyhow::{Result, anyhow};
use btrfs_uapi::inode::subvolid_resolve;
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Resolve the path of a subvolume given its ID
#[derive(Parser, Debug)]
pub struct SubvolidResolveCommand {
    /// Subvolume ID to resolve
    subvolid: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for SubvolidResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let resolved_path =
            subvolid_resolve(fd, self.subvolid).map_err(|e| match e {
                Errno::EPERM | Errno::EACCES => {
                    anyhow!(
                        "failed to resolve subvolume ID {}: permission denied \
                         (requires CAP_SYS_ADMIN)",
                        self.subvolid,
                    )
                }
                _ => anyhow!(
                    "failed to resolve subvolume ID {}: {}",
                    self.subvolid,
                    e,
                ),
            })?;

        println!("{resolved_path}");
        Ok(())
    }
}

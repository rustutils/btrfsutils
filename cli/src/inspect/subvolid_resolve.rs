use crate::{Format, Runnable};
use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

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
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;
        let fd = file.as_fd();

        let resolved_path = btrfs_uapi::inode::subvolid_resolve(
            fd,
            self.subvolid,
        )
        .map_err(|e| match e {
            nix::errno::Errno::EPERM | nix::errno::Errno::EACCES => {
                anyhow::anyhow!(
                    "failed to resolve subvolume ID {}: permission denied \
                         (requires CAP_SYS_ADMIN)",
                    self.subvolid,
                )
            }
            _ => anyhow::anyhow!(
                "failed to resolve subvolume ID {}: {}",
                self.subvolid,
                e,
            ),
        })?;

        println!("{}", resolved_path);
        Ok(())
    }
}

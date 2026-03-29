use crate::{
    Format, Runnable,
    util::{open_path, parse_qgroupid},
};
use anyhow::{Context, Result};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Destroy a quota group
#[derive(Parser, Debug)]
pub struct QgroupDestroyCommand {
    /// Quota group ID to destroy (e.g. "0/5")
    pub qgroupid: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupDestroyCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let qgroupid = parse_qgroupid(&self.qgroupid)?;

        let file = open_path(&self.path)?;

        match btrfs_uapi::quota::qgroup_destroy(file.as_fd(), qgroupid) {
            Ok(()) => {
                println!("qgroup {} destroyed", self.qgroupid);
                Ok(())
            }
            Err(Errno::ENOTCONN) => {
                anyhow::bail!("quota not enabled on '{}'", self.path.display())
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "failed to destroy qgroup '{}' on '{}'",
                    self.qgroupid,
                    self.path.display()
                )
            }),
        }
    }
}

use crate::{
    RunContext, Runnable,
    util::{open_path, parse_qgroupid},
};
use anyhow::{Context, Result};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Create a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupCreateCommand {
    /// Qgroup id in the form LEVEL/ID
    pub qgroupid: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupCreateCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let qgroupid = parse_qgroupid(&self.qgroupid)?;

        let file = open_path(&self.path)?;

        match btrfs_uapi::quota::qgroup_create(file.as_fd(), qgroupid) {
            Ok(()) => {
                println!("qgroup {} created", self.qgroupid);
                Ok(())
            }
            Err(Errno::ENOTCONN) => {
                anyhow::bail!("quota not enabled on '{}'", self.path.display())
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "failed to create qgroup '{}' on '{}'",
                    self.qgroupid,
                    self.path.display()
                )
            }),
        }
    }
}

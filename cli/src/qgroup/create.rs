use crate::{Format, Runnable, util::parse_qgroupid};
use anyhow::{Context, Result};
use clap::Parser;
use nix::errno::Errno;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Create a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupCreateCommand {
    /// Qgroup id in the form <level>/<id>
    pub qgroupid: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupCreateCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let qgroupid = parse_qgroupid(&self.qgroupid)?;

        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

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

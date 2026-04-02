use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Clear all stale qgroups (level 0/subvolid) without a subvolume
#[derive(Parser, Debug)]
pub struct QgroupClearStaleCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupClearStaleCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        let n = btrfs_uapi::quota::qgroup_clear_stale(file.as_fd())
            .with_context(|| {
                format!(
                    "failed to clear stale qgroups on '{}'",
                    self.path.display()
                )
            })?;

        if n == 0 {
            println!("no stale qgroups found");
        } else {
            println!(
                "deleted {} stale qgroup{}",
                n,
                if n == 1 { "" } else { "s" }
            );
        }

        Ok(())
    }
}

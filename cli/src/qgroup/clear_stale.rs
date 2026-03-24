use crate::{Format, Runnable};
use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Clear all stale qgroups (level 0/subvolid) without a subvolume
#[derive(Parser, Debug)]
pub struct QgroupClearStaleCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupClearStaleCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let n = btrfs_uapi::qgroup::qgroup_clear_stale(file.as_fd()).with_context(|| {
            format!("failed to clear stale qgroups on '{}'", self.path.display())
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

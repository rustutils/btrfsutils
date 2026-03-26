use crate::{Format, Runnable};
use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Disable subvolume quota support for a filesystem
#[derive(Parser, Debug)]
pub struct QuotaDisableCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QuotaDisableCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        btrfs_uapi::quota::quota_disable(file.as_fd()).with_context(|| {
            format!("failed to disable quota on '{}'", self.path.display())
        })?;

        println!("quota disabled on '{}'", self.path.display());

        Ok(())
    }
}

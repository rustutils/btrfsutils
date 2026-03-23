use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::sync::sync;
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Force a sync on a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemSyncCommand {
    pub path: PathBuf,
}

impl Runnable for FilesystemSyncCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        sync(file.as_fd()).with_context(|| format!("failed to sync '{}'", self.path.display()))?;
        Ok(())
    }
}

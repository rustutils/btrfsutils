use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::filesystem::sync;
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Force a sync on a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemSyncCommand {
    pub path: PathBuf,
}

impl Runnable for FilesystemSyncCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
        sync(file.as_fd()).with_context(|| {
            format!("failed to sync '{}'", self.path.display())
        })?;
        Ok(())
    }
}

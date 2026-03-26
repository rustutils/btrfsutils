use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::scrub::scrub_cancel;
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Cancel a running scrub
#[derive(Parser, Debug)]
pub struct ScrubCancelCommand {
    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubCancelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        scrub_cancel(file.as_fd()).with_context(|| {
            format!("failed to cancel scrub on '{}'", self.path.display())
        })?;

        println!("scrub cancelled on '{}'", self.path.display());
        Ok(())
    }
}

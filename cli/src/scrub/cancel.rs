use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::scrub::scrub_cancel;
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Cancel a running scrub
#[derive(Parser, Debug)]
pub struct ScrubCancelCommand {
    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubCancelCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        scrub_cancel(file.as_fd()).with_context(|| {
            format!("failed to cancel scrub on '{}'", self.path.display())
        })?;

        println!("scrub cancelled on '{}'", self.path.display());
        Ok(())
    }
}

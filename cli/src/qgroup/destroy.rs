use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

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
        todo!("implement qgroup destroy")
    }
}

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Clear all stale qgroups (level 0/subvolid) without a subvolume
#[derive(Parser, Debug)]
pub struct QgroupClearStaleCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupClearStaleCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup clear-stale")
    }
}

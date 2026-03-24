use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Wait until given subvolume(s) are completely removed from the filesystem
///
/// Wait until given subvolume(s) are completely removed from the filesystem
/// after deletion. If no subvolume id is given, wait until all current
/// deletion requests are completed, but do not wait for subvolumes deleted
/// meanwhile. The status of subvolume ids is checked periodically.
#[derive(Parser, Debug)]
pub struct SubvolumeSyncCommand {
    /// Path to the btrfs filesystem mount point
    path: PathBuf,

    /// One or more subvolume IDs to wait for (waits for all pending if omitted)
    subvolids: Vec<u64>,

    /// Sleep N seconds between checks (default: 1)
    #[clap(short = 's', long, value_name = "N")]
    sleep: Option<u64>,
}

impl Runnable for SubvolumeSyncCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement subvolume sync")
    }
}

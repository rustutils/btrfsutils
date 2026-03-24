use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Set the limits for a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupLimitCommand {
    /// Size limit, or "none" to remove the limit
    pub size: String,

    // TODO: positional argument disambiguation between qgroupid and path
    /// Optional qgroup ID (e.g. "0/5")
    pub qgroupid: Option<String>,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Limit amount of data after compression
    #[clap(short = 'c')]
    pub compress: bool,

    /// Limit space exclusively assigned to this qgroup
    #[clap(short = 'e')]
    pub exclusive: bool,
}

impl Runnable for QgroupLimitCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup limit")
    }
}

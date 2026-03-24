use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Trash all qgroup numbers and scan the metadata again
#[derive(Parser, Debug)]
pub struct QuotaRescanCommand {
    /// Show status of a running rescan operation
    #[clap(short = 's', long)]
    pub status: bool,

    /// Start rescan and wait for it to finish
    #[clap(short = 'w', long)]
    pub wait: bool,

    /// Wait for rescan to finish without starting it
    #[clap(short = 'W', long)]
    pub wait_norescan: bool,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QuotaRescanCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement quota rescan")
    }
}

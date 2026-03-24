use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Show status information about quota on the filesystem
#[derive(Parser, Debug)]
pub struct QuotaStatusCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Only check if quotas are enabled, without printing full status
    #[clap(long)]
    pub is_enabled: bool,
}

impl Runnable for QuotaStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement btrfs quota status")
    }
}

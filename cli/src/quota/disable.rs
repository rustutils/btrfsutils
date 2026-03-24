use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Disable subvolume quota support for a filesystem
#[derive(Parser, Debug)]
pub struct QuotaDisableCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QuotaDisableCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement btrfs quota disable")
    }
}

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Enable subvolume quota support for a filesystem
#[derive(Parser, Debug)]
pub struct QuotaEnableCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Simple qgroups: account ownership by extent lifetime
    #[clap(short = 's', long)]
    pub simple: bool,
}

impl Runnable for QuotaEnableCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement quota enable")
    }
}

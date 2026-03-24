use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Print status of a running device replace operation.
///
/// Without -1 the status is printed continuously until the replace operation
/// finishes. With -1 the status is printed once and the command exits.
#[derive(Parser, Debug)]
pub struct ReplaceStatusCommand {
    /// Print once instead of continuously until the replace finishes
    #[clap(short = '1')]
    pub once: bool,

    /// Path to a mounted btrfs filesystem
    pub mount_point: PathBuf,
}

impl Runnable for ReplaceStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement replace status")
    }
}

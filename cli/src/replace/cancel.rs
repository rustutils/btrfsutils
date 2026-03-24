use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Cancel a running device replace operation.
///
/// If a replace operation is in progress on the filesystem mounted at
/// <mount_point>, it is cancelled. The target device is left in an
/// undefined state and should not be used further without reformatting.
#[derive(Parser, Debug)]
pub struct ReplaceCancelCommand {
    /// Path to the mounted btrfs filesystem
    pub mount_point: PathBuf,
}

impl Runnable for ReplaceCancelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement replace cancel")
    }
}

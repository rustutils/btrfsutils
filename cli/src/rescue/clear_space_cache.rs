use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Free space cache version to clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SpaceCacheVersion {
    V1,
    V2,
}

/// Completely remove the v1 or v2 free space cache
#[derive(Parser, Debug)]
pub struct RescueClearSpaceCacheCommand {
    /// Free space cache version to remove
    version: SpaceCacheVersion,

    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueClearSpaceCacheCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue clear-space-cache")
    }
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Delete uuid tree so that kernel can rebuild it at mount time
#[derive(Parser, Debug)]
pub struct RescueClearUuidTreeCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueClearUuidTreeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue clear-uuid-tree")
    }
}

use crate::{RunContext, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Delete uuid tree so that kernel can rebuild it at mount time
#[derive(Parser, Debug)]
pub struct RescueClearUuidTreeCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueClearUuidTreeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        todo!("implement rescue clear-uuid-tree")
    }
}

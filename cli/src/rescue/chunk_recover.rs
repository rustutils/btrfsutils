use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Recover the chunk tree by scanning the devices one by one
#[derive(Parser, Debug)]
pub struct RescueChunkRecoverCommand {
    /// Assume an answer of 'yes' to all questions
    #[clap(short = 'y', long)]
    pub yes: bool,

    /// Device to recover
    pub device: PathBuf,
}

impl Runnable for RescueChunkRecoverCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue chunk-recover")
    }
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Recover bad superblocks from good copies
#[derive(Parser, Debug)]
pub struct RescueSuperRecoverCommand {
    /// Path to the device
    device: PathBuf,

    /// Assume an answer of 'yes' to all questions
    #[clap(short = 'y', long)]
    yes: bool,
}

impl Runnable for RescueSuperRecoverCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue super-recover")
    }
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Fix data checksum mismatches
#[derive(Parser, Debug)]
pub struct RescueFixDataChecksumCommand {
    /// Device to operate on
    device: PathBuf,

    /// Readonly mode, only report errors without repair
    #[clap(short, long)]
    readonly: bool,

    /// Interactive mode, ignore the error by default
    #[clap(short, long)]
    interactive: bool,

    /// Update csum item using specified mirror
    #[clap(short, long)]
    mirror: Option<u32>,
}

impl Runnable for RescueFixDataChecksumCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement rescue fix-data-checksum")
    }
}

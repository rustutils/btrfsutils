use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// List the recently modified files in a subvolume
///
/// Prints all files that have been modified since the given generation number.
/// The generation can be found with `btrfs subvolume show`.
#[derive(Parser, Debug)]
pub struct SubvolumeFindNewCommand {
    /// Path to the subvolume to search
    path: PathBuf,

    /// Only show files modified at or after this generation number
    last_gen: u64,
}

impl Runnable for SubvolumeFindNewCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement subvolume find-new")
    }
}

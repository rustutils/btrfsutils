use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Create a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupCreateCommand {
    /// Qgroup id in the form <level>/<id>
    pub qgroupid: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QgroupCreateCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup create")
    }
}

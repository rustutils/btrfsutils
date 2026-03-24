use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Remove the relation between child qgroup SRC from DST
#[derive(Parser, Debug)]
pub struct QgroupRemoveCommand {
    /// Source qgroup id
    pub src: String,

    /// Destination qgroup id
    pub dst: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Schedule quota rescan if needed
    #[clap(long)]
    pub rescan: bool,

    /// Don't schedule quota rescan
    #[clap(long)]
    pub no_rescan: bool,
}

impl Runnable for QgroupRemoveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup remove")
    }
}

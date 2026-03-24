use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Assign a qgroup as the child of another qgroup
#[derive(Parser, Debug)]
pub struct QgroupAssignCommand {
    /// Source qgroup id
    pub src: String,
    /// Destination qgroup id
    pub dst: String,
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
    /// Schedule a quota rescan if needed
    #[clap(long)]
    pub rescan: bool,
    /// Do not schedule a quota rescan
    #[clap(long)]
    pub no_rescan: bool,
}

impl Runnable for QgroupAssignCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup assign")
    }
}

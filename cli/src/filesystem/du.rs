use super::UnitMode;
use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Summarize disk usage of each file
#[derive(Parser, Debug)]
pub struct FilesystemDuCommand {
    /// Display only a total for each argument
    #[clap(long, short)]
    pub summarize: bool,

    #[clap(flatten)]
    pub units: UnitMode,

    /// One or more paths to summarize
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for FilesystemDuCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

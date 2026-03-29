use super::UnitMode;
use crate::{
    Format, Runnable,
    util::{fmt_size, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::space::space_info;
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show space usage information for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemDfCommand {
    #[clap(flatten)]
    pub units: UnitMode,

    pub path: PathBuf,
}

impl Runnable for FilesystemDfCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let mode = self.units.resolve();
        let file = open_path(&self.path)?;
        let entries = space_info(file.as_fd()).with_context(|| {
            format!("failed to get space info for '{}'", self.path.display())
        })?;

        for entry in &entries {
            println!(
                "{}: total={}, used={}",
                entry.flags,
                fmt_size(entry.total_bytes, &mode),
                fmt_size(entry.used_bytes, &mode),
            );
        }

        Ok(())
    }
}

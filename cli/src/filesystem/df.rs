use crate::{Format, Runnable, util::human_bytes};
use anyhow::{Context, Result};
use btrfs_uapi::space::space_info;
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show space usage information for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemDfCommand {
    pub path: PathBuf,
}

impl Runnable for FilesystemDfCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let entries = space_info(file.as_fd())
            .with_context(|| format!("failed to get space info for '{}'", self.path.display()))?;

        for entry in &entries {
            println!(
                "{}: total={}, used={}",
                entry.flags,
                human_bytes(entry.total_bytes),
                human_bytes(entry.used_bytes),
            );
        }

        Ok(())
    }
}

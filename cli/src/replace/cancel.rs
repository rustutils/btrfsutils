use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::replace::replace_cancel;
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Cancel a running device replace operation.
///
/// If a replace operation is in progress on the filesystem mounted at
/// mount_point, it is cancelled. The target device is left in an undefined
/// state and should not be used further without reformatting.
#[derive(Parser, Debug)]
pub struct ReplaceCancelCommand {
    /// Path to the mounted btrfs filesystem
    pub mount_point: PathBuf,
}

impl Runnable for ReplaceCancelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.mount_point)
            .with_context(|| format!("failed to open '{}'", self.mount_point.display()))?;

        let was_running = replace_cancel(file.as_fd()).with_context(|| {
            format!(
                "failed to cancel replace on '{}'",
                self.mount_point.display()
            )
        })?;

        if was_running {
            println!("replace cancelled on '{}'", self.mount_point.display());
        } else {
            println!("no replace operation was in progress");
        }

        Ok(())
    }
}

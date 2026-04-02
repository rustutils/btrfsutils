use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::replace::replace_cancel;
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Cancel a running device replace operation.
///
/// If a replace operation is in progress on the filesystem mounted at
/// mount_point, it is cancelled. The target device is left in an undefined
/// state and should not be used further without reformatting.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct ReplaceCancelCommand {
    /// Path to the mounted btrfs filesystem
    pub mount_point: PathBuf,
}

impl Runnable for ReplaceCancelCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.mount_point)?;

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

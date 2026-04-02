use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Enable subvolume quota support for a filesystem
#[derive(Parser, Debug)]
pub struct QuotaEnableCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Simple qgroups: account ownership by extent lifetime rather than backref walks
    #[clap(short = 's', long)]
    pub simple: bool,
}

impl Runnable for QuotaEnableCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        btrfs_uapi::quota::quota_enable(file.as_fd(), self.simple)
            .with_context(|| {
                format!("failed to enable quota on '{}'", self.path.display())
            })?;

        if self.simple {
            println!(
                "quota enabled (simple mode) on '{}'",
                self.path.display()
            );
        } else {
            println!("quota enabled on '{}'", self.path.display());
        }

        Ok(())
    }
}

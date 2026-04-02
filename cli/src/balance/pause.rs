use super::open_path;
use crate::{RunContext, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::balance::{BalanceCtl, balance_ctl};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Pause a running balance operation
#[derive(Parser, Debug)]
pub struct BalancePauseCommand {
    pub path: PathBuf,
}

impl Runnable for BalancePauseCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        match balance_ctl(file.as_fd(), BalanceCtl::Pause) {
            Ok(()) => Ok(()),
            Err(Errno::ENOTCONN) => {
                anyhow::bail!(
                    "balance pause on '{}' failed: Not running",
                    self.path.display()
                )
            }
            Err(e) => Err(e).with_context(|| {
                format!("balance pause on '{}' failed", self.path.display())
            }),
        }
    }
}

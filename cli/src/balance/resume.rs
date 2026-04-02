use super::open_path;
use crate::{RunContext, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::balance::{BalanceFlags, balance};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Resume a paused balance operation
#[derive(Parser, Debug)]
pub struct BalanceResumeCommand {
    pub path: PathBuf,
}

impl Runnable for BalanceResumeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        // Resume is just a balance start with the RESUME flag and no type
        // filters; the kernel picks up where it left off.
        match balance(file.as_fd(), BalanceFlags::RESUME, None, None, None) {
            Ok(progress) => {
                println!(
                    "Done, had to relocate {} out of {} chunks",
                    progress.completed, progress.considered
                );
                Ok(())
            }
            Err(Errno::ECANCELED) => {
                eprintln!("Balance was paused or cancelled by user.");
                Ok(())
            }
            Err(Errno::ENOTCONN) => {
                anyhow::bail!(
                    "balance resume on '{}' failed: Not in progress",
                    self.path.display()
                )
            }
            Err(Errno::EINPROGRESS) => {
                anyhow::bail!(
                    "balance resume on '{}' failed: Already running",
                    self.path.display()
                )
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "error during balance resume on '{}'\n\
                     There may be more info in syslog - try dmesg | tail",
                    self.path.display()
                )
            }),
        }
    }
}

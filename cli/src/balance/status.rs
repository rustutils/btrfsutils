use super::open_path;
use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::balance::{BalanceState, balance_progress};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show status of running or paused balance operation.
#[derive(Parser, Debug)]
pub struct BalanceStatusCommand {
    pub path: PathBuf,
}

impl Runnable for BalanceStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        match balance_progress(file.as_fd()) {
            Ok((state, progress)) => {
                if state.contains(BalanceState::RUNNING) {
                    print!("Balance on '{}' is running", self.path.display());
                    if state.contains(BalanceState::CANCEL_REQ) {
                        println!(", cancel requested");
                    } else if state.contains(BalanceState::PAUSE_REQ) {
                        println!(", pause requested");
                    } else {
                        println!();
                    }
                } else {
                    println!("Balance on '{}' is paused", self.path.display());
                }

                let pct_left = if progress.expected > 0 {
                    100.0 * (1.0 - progress.completed as f64 / progress.expected as f64)
                } else {
                    0.0
                };

                println!(
                    "{} out of about {} chunks balanced ({} considered), {:3.0}% left",
                    progress.completed, progress.expected, progress.considered, pct_left
                );

                Ok(())
            }
            Err(e) if e == Errno::ENOTCONN => {
                println!("No balance found on '{}'", self.path.display());
                Ok(())
            }
            Err(e) => Err(e)
                .with_context(|| format!("balance status on '{}' failed", self.path.display())),
        }
    }
}

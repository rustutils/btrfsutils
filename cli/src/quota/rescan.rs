use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Trash all qgroup numbers and scan the metadata again
#[derive(Parser, Debug)]
pub struct QuotaRescanCommand {
    /// Show status of a running rescan operation
    #[clap(short = 's', long, conflicts_with = "wait")]
    pub status: bool,

    /// Start rescan and wait for it to finish
    #[clap(short = 'w', long)]
    pub wait: bool,

    /// Wait for rescan to finish without starting it
    #[clap(short = 'W', long)]
    pub wait_norescan: bool,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for QuotaRescanCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        if self.status {
            let st = btrfs_uapi::quota::quota_rescan_status(fd).with_context(
                || {
                    format!(
                        "failed to get quota rescan status on '{}'",
                        self.path.display()
                    )
                },
            )?;

            if st.running {
                println!(
                    "rescan operation running (current key {})",
                    st.progress
                );
            } else {
                println!("no rescan operation in progress");
            }

            return Ok(());
        }

        if self.wait_norescan {
            // Just wait — do not start a new rescan.
            btrfs_uapi::quota::quota_rescan_wait(fd).with_context(|| {
                format!(
                    "failed to wait for quota rescan on '{}'",
                    self.path.display()
                )
            })?;
            return Ok(());
        }

        // Start the rescan.  If one is already running and the caller asked to
        // wait, treat EINPROGRESS as a non-error and proceed to the wait step.
        match btrfs_uapi::quota::quota_rescan(fd) {
            Ok(()) => {
                println!("quota rescan started");
            }
            Err(Errno::EINPROGRESS) if self.wait => {
                // Already running; we'll still wait below.
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "failed to start quota rescan on '{}'",
                        self.path.display()
                    )
                });
            }
        }

        if self.wait {
            btrfs_uapi::quota::quota_rescan_wait(fd).with_context(|| {
                format!(
                    "failed to wait for quota rescan on '{}'",
                    self.path.display()
                )
            })?;
        }

        Ok(())
    }
}

use crate::{
    Format, Runnable,
    util::{format_time_short, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::replace::{ReplaceState, replace_status};
use clap::Parser;
use std::{
    io::Write, os::unix::io::AsFd, path::PathBuf, thread, time::Duration,
};

/// Print status of a running device replace operation.
///
/// Without -1 the status is printed continuously until the replace operation
/// finishes or is cancelled. With -1 the status is printed once and the
/// command exits.
#[derive(Parser, Debug)]
pub struct ReplaceStatusCommand {
    /// Print once instead of continuously until the replace finishes
    #[clap(short = '1', long)]
    pub once: bool,

    /// Path to a mounted btrfs filesystem
    pub mount_point: PathBuf,
}

impl Runnable for ReplaceStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.mount_point)?;
        let fd = file.as_fd();

        loop {
            let status = replace_status(fd).with_context(|| {
                format!(
                    "failed to get replace status on '{}'",
                    self.mount_point.display()
                )
            })?;

            let line = match status.state {
                ReplaceState::NeverStarted => "Never started".to_string(),
                ReplaceState::Started => {
                    let pct = status.progress_1000 as f64 / 10.0;
                    format!(
                        "{pct:.1}% done, {} write errs, {} uncorr. read errs",
                        status.num_write_errors,
                        status.num_uncorrectable_read_errors,
                    )
                }
                ReplaceState::Finished => {
                    let started = status
                        .time_started
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    format!(
                        "Started on {started}, finished on {stopped}, \
                         {} write errs, {} uncorr. read errs",
                        status.num_write_errors,
                        status.num_uncorrectable_read_errors,
                    )
                }
                ReplaceState::Canceled => {
                    let started = status
                        .time_started
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    let pct = status.progress_1000 as f64 / 10.0;
                    format!(
                        "Started on {started}, canceled on {stopped} at {pct:.1}%, \
                         {} write errs, {} uncorr. read errs",
                        status.num_write_errors,
                        status.num_uncorrectable_read_errors,
                    )
                }
                ReplaceState::Suspended => {
                    let started = status
                        .time_started
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time_short(&t))
                        .unwrap_or_default();
                    let pct = status.progress_1000 as f64 / 10.0;
                    format!(
                        "Started on {started}, suspended on {stopped} at {pct:.1}%, \
                         {} write errs, {} uncorr. read errs",
                        status.num_write_errors,
                        status.num_uncorrectable_read_errors,
                    )
                }
            };

            print!("\r{line}");
            std::io::stdout().flush()?;

            // Terminal states or one-shot mode: print newline and exit.
            if self.once || status.state != ReplaceState::Started {
                println!();
                break;
            }

            thread::sleep(Duration::from_secs(1));
        }

        Ok(())
    }
}

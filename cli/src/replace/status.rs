use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::replace::{ReplaceState, replace_status};
use clap::Parser;
use std::{
    fs::File, io::Write, os::unix::io::AsFd, path::PathBuf, thread,
    time::Duration,
};

/// Print status of a running device replace operation.
///
/// Without -1 the status is printed continuously until the replace operation
/// finishes or is cancelled. With -1 the status is printed once and the
/// command exits.
#[derive(Parser, Debug)]
pub struct ReplaceStatusCommand {
    /// Print once instead of continuously until the replace finishes
    #[clap(short = '1')]
    pub once: bool,

    /// Path to a mounted btrfs filesystem
    pub mount_point: PathBuf,
}

fn format_time(t: &std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Use libc::localtime_r for locale-aware formatting, matching the
    // pattern used elsewhere in the codebase (subvolume show).
    let secs_i64 = secs as nix::libc::time_t;
    let mut tm: nix::libc::tm = unsafe { std::mem::zeroed() };
    unsafe { nix::libc::localtime_r(&secs_i64, &mut tm) };

    // Format as "%e.%b %T" to match btrfs-progs output.
    let mut buf = [0u8; 64];
    let fmt = b"%e.%b %T\0";
    let len = unsafe {
        nix::libc::strftime(
            buf.as_mut_ptr() as *mut nix::libc::c_char,
            buf.len(),
            fmt.as_ptr() as *const nix::libc::c_char,
            &tm,
        )
    };
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

impl Runnable for ReplaceStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.mount_point).with_context(|| {
            format!("failed to open '{}'", self.mount_point.display())
        })?;
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
                        .map(|t| format_time(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time(&t))
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
                        .map(|t| format_time(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time(&t))
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
                        .map(|t| format_time(&t))
                        .unwrap_or_default();
                    let stopped = status
                        .time_stopped
                        .map(|t| format_time(&t))
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

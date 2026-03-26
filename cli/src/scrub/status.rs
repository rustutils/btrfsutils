use crate::{Format, Runnable, util::human_bytes};
use anyhow::{Context, Result};
use btrfs_uapi::{device::device_info_all, filesystem::filesystem_info, scrub::scrub_progress};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show the status of a running or finished scrub
#[derive(Parser, Debug)]
pub struct ScrubStatusCommand {
    /// Show stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let fs = filesystem_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;
        let devices = device_info_all(fd, &fs)
            .with_context(|| format!("failed to get device info for '{}'", self.path.display()))?;

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut any_running = false;
        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();

        for dev in &devices {
            match scrub_progress(fd, dev.devid)
                .with_context(|| format!("failed to get scrub progress for device {}", dev.devid))?
            {
                None => {
                    if self.device {
                        println!("device {} ({}): no scrub in progress", dev.devid, dev.path);
                    }
                }
                Some(progress) => {
                    any_running = true;
                    super::accumulate(&mut fs_totals, &progress);
                    if self.device {
                        super::print_progress_summary(&progress, dev.devid, &dev.path);
                    }
                }
            }
        }

        if !any_running {
            println!("\tno scrub in progress");
        } else if !self.device {
            // Show filesystem-level summary when not in per-device mode.
            println!(
                "Bytes scrubbed:   {}",
                human_bytes(fs_totals.bytes_scrubbed())
            );
            super::print_error_summary(&fs_totals);
        }

        Ok(())
    }
}

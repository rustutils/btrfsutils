use crate::{
    RunContext, Runnable,
    filesystem::UnitMode,
    util::{fmt_size, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all, filesystem::filesystem_info, scrub::scrub_progress,
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show the status of a running or finished scrub
#[derive(Parser, Debug)]
pub struct ScrubStatusCommand {
    /// Show stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Print full raw data instead of summary
    #[clap(short = 'R', long = "raw-data")]
    pub raw_data: bool,

    #[clap(flatten)]
    pub units: UnitMode,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubStatusCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let fs = filesystem_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;
        let devices = device_info_all(fd, &fs).with_context(|| {
            format!("failed to get device info for '{}'", self.path.display())
        })?;

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut any_running = false;
        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();

        for dev in &devices {
            match scrub_progress(fd, dev.devid).with_context(|| {
                format!("failed to get scrub progress for device {}", dev.devid)
            })? {
                None => {
                    if self.device {
                        println!(
                            "device {} ({}): no scrub in progress",
                            dev.devid, dev.path
                        );
                    }
                }
                Some(progress) => {
                    any_running = true;
                    super::accumulate(&mut fs_totals, &progress);
                    if self.device {
                        super::print_device_progress(
                            &progress,
                            dev.devid,
                            &dev.path,
                            self.raw_data,
                            &mode,
                        );
                    }
                }
            }
        }

        if !any_running {
            println!("\tno scrub in progress");
        } else if !self.device {
            if self.raw_data {
                super::print_raw_progress(&fs_totals, 0, "filesystem totals");
            } else {
                println!(
                    "Bytes scrubbed:   {}",
                    fmt_size(fs_totals.bytes_scrubbed(), &mode)
                );
                super::print_error_summary(&fs_totals);
            }
        }

        Ok(())
    }
}

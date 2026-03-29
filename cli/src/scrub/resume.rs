use crate::{
    Format, Runnable,
    util::{SizeFormat, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all, filesystem::filesystem_info, scrub::scrub_start,
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Resume a previously cancelled or interrupted scrub
///
/// Scrubs all devices sequentially. This command blocks until the scrub
/// completes; use Ctrl-C to cancel.
#[derive(Parser, Debug)]
pub struct ScrubResumeCommand {
    /// Do not background (default behavior, accepted for compatibility)
    #[clap(short = 'B')]
    pub no_background: bool,

    /// Stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Read-only mode: check for errors but do not attempt repairs
    #[clap(long, short)]
    pub readonly: bool,

    /// Print full raw data instead of summary
    #[clap(short = 'R')]
    pub raw: bool,

    /// Set ioprio class (see ionice(1) manpage)
    #[clap(short = 'c', value_name = "CLASS")]
    pub ioprio_class: Option<i32>,

    /// Set ioprio classdata (see ionice(1) manpage)
    #[clap(short = 'n', value_name = "CDATA")]
    pub ioprio_classdata: Option<i32>,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubResumeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // Resume uses the same ioctl as start; the kernel tracks where it left
        // off via the scrub state on disk.
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

        if self.ioprio_class.is_some() || self.ioprio_classdata.is_some() {
            super::set_ioprio(
                self.ioprio_class.unwrap_or(3), // default: idle
                self.ioprio_classdata.unwrap_or(0),
            );
        }

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mode = SizeFormat::HumanIec;
        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();

        for dev in &devices {
            println!("resuming scrub on device {} ({})", dev.devid, dev.path);

            match scrub_start(fd, dev.devid, self.readonly) {
                Ok(progress) => {
                    super::accumulate(&mut fs_totals, &progress);
                    if self.device {
                        super::print_device_progress(
                            &progress, dev.devid, &dev.path, self.raw, &mode,
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "error resuming scrub on device {}: {e}",
                        dev.devid
                    );
                }
            }
        }

        if !self.device {
            if self.raw {
                super::print_raw_progress(&fs_totals, 0, "filesystem totals");
            } else {
                super::print_error_summary(&fs_totals);
            }
        } else if devices.len() > 1 {
            println!("\nFilesystem totals:");
            if self.raw {
                super::print_raw_progress(&fs_totals, 0, "filesystem totals");
            } else {
                super::print_error_summary(&fs_totals);
            }
        }

        Ok(())
    }
}

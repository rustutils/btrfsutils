use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::{device::device_info_all, filesystem::filesystem_info, scrub::scrub_start};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Resume a previously cancelled or interrupted scrub
///
/// Scrubs all devices sequentially. This command blocks until the scrub
/// completes; use Ctrl-C to cancel.
#[derive(Parser, Debug)]
pub struct ScrubResumeCommand {
    /// Read-only mode: check for errors but do not attempt repairs
    #[clap(long, short)]
    pub readonly: bool,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubResumeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // Resume uses the same ioctl as start; the kernel tracks where it left
        // off via the scrub state on disk.
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

        for dev in &devices {
            println!("resuming scrub on device {} ({})", dev.devid, dev.path);

            match scrub_start(fd, dev.devid, self.readonly) {
                Ok(progress) => {
                    super::print_progress_summary(&progress, dev.devid, &dev.path);
                }
                Err(e) => {
                    eprintln!("error resuming scrub on device {}: {e}", dev.devid);
                }
            }
        }

        Ok(())
    }
}

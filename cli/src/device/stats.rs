use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::{DevStats, all_dev_info, dev_stats},
    filesystem::fs_info,
};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show device I/O error statistics for all devices of a filesystem
///
/// Reads per-device counters for write, read, flush, corruption, and
/// generation errors. The path can be a mount point or a device belonging
/// to the filesystem.
///
/// The operation requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct DeviceStatsCommand {
    /// Return a non-zero exit code if any error counter is greater than zero
    #[clap(long, short)]
    pub check: bool,

    /// Print current values and then atomically reset all counters to zero
    #[clap(long, short = 'z')]
    pub reset: bool,

    /// Path to a mounted btrfs filesystem or one of its devices
    pub path: PathBuf,
}

impl Runnable for DeviceStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let fs = fs_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;

        let devices = all_dev_info(fd, &fs)
            .with_context(|| format!("failed to get device info for '{}'", self.path.display()))?;

        if devices.is_empty() {
            anyhow::bail!("no devices found for '{}'", self.path.display());
        }

        let mut any_nonzero = false;

        for dev in &devices {
            let stats = dev_stats(fd, dev.devid, self.reset).with_context(|| {
                format!(
                    "failed to get stats for device {} ({})",
                    dev.devid, dev.path
                )
            })?;

            print_stats(&dev.path, &stats);

            if !stats.is_clean() {
                any_nonzero = true;
            }
        }

        if self.check && any_nonzero {
            anyhow::bail!("one or more devices have non-zero error counters");
        }

        Ok(())
    }
}

/// Print the five counters for one device in the same layout as the C tool:
/// `[/dev/path].counter_name   <value>`
fn print_stats(path: &str, stats: &DevStats) {
    let p = path;
    println!("[{p}].{:<24} {}", "write_io_errs", stats.write_errs);
    println!("[{p}].{:<24} {}", "read_io_errs", stats.read_errs);
    println!("[{p}].{:<24} {}", "flush_io_errs", stats.flush_errs);
    println!("[{p}].{:<24} {}", "corruption_errs", stats.corruption_errs);
    println!("[{p}].{:<24} {}", "generation_errs", stats.generation_errs);
}

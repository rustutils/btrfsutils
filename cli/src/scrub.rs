use crate::{Format, Runnable, util::human_bytes};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::all_dev_info,
    filesystem::fs_info,
    scrub::{ScrubProgress, scrub_cancel, scrub_progress, scrub_start},
};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Verify checksums of data and metadata
#[derive(Parser, Debug)]
pub struct ScrubCommand {
    #[clap(subcommand)]
    pub subcommand: ScrubSubcommand,
}

impl Runnable for ScrubCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            ScrubSubcommand::Start(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Cancel(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Resume(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Status(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Limit(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum ScrubSubcommand {
    Start(ScrubStartCommand),
    Cancel(ScrubCancelCommand),
    Resume(ScrubResumeCommand),
    Status(ScrubStatusCommand),
    Limit(ScrubLimitCommand),
}

/// Start a new scrub on the filesystem or a device.
///
/// Scrubs all devices sequentially. This command blocks until the scrub
/// completes; use Ctrl-C to cancel.
#[derive(Parser, Debug)]
pub struct ScrubStartCommand {
    /// Read-only mode: check for errors but do not attempt repairs
    #[clap(long, short)]
    pub readonly: bool,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

/// Cancel a running scrub
#[derive(Parser, Debug)]
pub struct ScrubCancelCommand {
    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

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

/// Show the status of a running or finished scrub
#[derive(Parser, Debug)]
pub struct ScrubStatusCommand {
    /// Show stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

/// Show or set the per-device scrub throughput limit
#[derive(Parser, Debug)]
pub struct ScrubLimitCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for ScrubStartCommand {
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

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut fs_totals = ScrubProgress::default();

        for dev in &devices {
            println!("scrubbing device {} ({})", dev.devid, dev.path);

            match scrub_start(fd, dev.devid, self.readonly) {
                Ok(progress) => {
                    accumulate(&mut fs_totals, &progress);
                    print_progress_summary(&progress, dev.devid, &dev.path);
                }
                Err(e) => {
                    eprintln!("error scrubbing device {}: {e}", dev.devid);
                }
            }
        }

        if devices.len() > 1 {
            println!("\nFilesystem totals:");
            print_error_summary(&fs_totals);
        }

        Ok(())
    }
}

impl Runnable for ScrubCancelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        scrub_cancel(file.as_fd())
            .with_context(|| format!("failed to cancel scrub on '{}'", self.path.display()))?;

        println!("scrub cancelled on '{}'", self.path.display());
        Ok(())
    }
}

impl Runnable for ScrubResumeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // Resume uses the same ioctl as start; the kernel tracks where it left
        // off via the scrub state on disk.
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

        println!("UUID: {}", fs.uuid.as_hyphenated());

        for dev in &devices {
            println!("resuming scrub on device {} ({})", dev.devid, dev.path);

            match scrub_start(fd, dev.devid, self.readonly) {
                Ok(progress) => {
                    print_progress_summary(&progress, dev.devid, &dev.path);
                }
                Err(e) => {
                    eprintln!("error resuming scrub on device {}: {e}", dev.devid);
                }
            }
        }

        Ok(())
    }
}

impl Runnable for ScrubStatusCommand {
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

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut any_running = false;
        let mut fs_totals = ScrubProgress::default();

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
                    accumulate(&mut fs_totals, &progress);
                    if self.device {
                        print_progress_summary(&progress, dev.devid, &dev.path);
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
            print_error_summary(&fs_totals);
        }

        Ok(())
    }
}

impl Runnable for ScrubLimitCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // TODO: read/write per-device scrub limits via sysfs devinfo directory
        anyhow::bail!("scrub limit is not yet implemented")
    }
}

/// Add progress counters from `src` into `dst`.
fn accumulate(dst: &mut ScrubProgress, src: &ScrubProgress) {
    dst.data_extents_scrubbed += src.data_extents_scrubbed;
    dst.tree_extents_scrubbed += src.tree_extents_scrubbed;
    dst.data_bytes_scrubbed += src.data_bytes_scrubbed;
    dst.tree_bytes_scrubbed += src.tree_bytes_scrubbed;
    dst.read_errors += src.read_errors;
    dst.csum_errors += src.csum_errors;
    dst.verify_errors += src.verify_errors;
    dst.super_errors += src.super_errors;
    dst.uncorrectable_errors += src.uncorrectable_errors;
    dst.corrected_errors += src.corrected_errors;
    dst.unverified_errors += src.unverified_errors;
    dst.no_csum += src.no_csum;
    dst.csum_discards += src.csum_discards;
    dst.malloc_errors += src.malloc_errors;
}

/// Print a single-device progress summary.
fn print_progress_summary(p: &ScrubProgress, devid: u64, path: &str) {
    println!(
        "  devid {devid} ({path}): scrubbed {}",
        human_bytes(p.bytes_scrubbed())
    );
    print_error_summary(p);
}

/// Print the error summary line.
fn print_error_summary(p: &ScrubProgress) {
    if p.malloc_errors > 0 {
        eprintln!("WARNING: memory allocation errors during scrub — results may be inaccurate");
    }
    print!("  Error summary:  ");
    if p.is_clean() {
        println!(" no errors found");
    } else {
        if p.read_errors > 0 {
            print!(" read={}", p.read_errors);
        }
        if p.super_errors > 0 {
            print!(" super={}", p.super_errors);
        }
        if p.verify_errors > 0 {
            print!(" verify={}", p.verify_errors);
        }
        if p.csum_errors > 0 {
            print!(" csum={}", p.csum_errors);
        }
        println!();
        println!("    Corrected:      {}", p.corrected_errors);
        println!("    Uncorrectable:  {}", p.uncorrectable_errors);
        println!("    Unverified:     {}", p.unverified_errors);
    }
}

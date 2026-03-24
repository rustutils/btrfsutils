use crate::{
    Format, Runnable,
    util::{human_bytes, parse_size_with_suffix},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all,
    filesystem::fs_info,
    scrub::{ScrubProgress, scrub_cancel, scrub_progress, scrub_start},
    sysfs::SysfsBtrfs,
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
///
/// Without options, shows the current limit for each device. Use -l with
/// either -a or -d to set a limit. Pass 0 to -l to remove a limit.
#[derive(Parser, Debug)]
pub struct ScrubLimitCommand {
    /// Apply the limit to all devices
    #[clap(long, short)]
    pub all: bool,

    /// Select a single device by devid
    #[clap(long, short, value_name = "DEVID")]
    pub devid: Option<u64>,

    /// Set the throughput limit (e.g. 100m, 1g); 0 removes the limit
    #[clap(long, short, value_name = "SIZE", value_parser = parse_size_with_suffix)]
    pub limit: Option<u64>,

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
        let devices = device_info_all(fd, &fs)
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
        let devices = device_info_all(fd, &fs)
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
        let devices = device_info_all(fd, &fs)
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
        if self.all && self.devid.is_some() {
            anyhow::bail!("--all and --devid cannot be used at the same time");
        }
        if self.devid.is_some() && self.limit.is_none() {
            anyhow::bail!("--devid and --limit must be set together");
        }
        if self.all && self.limit.is_none() {
            anyhow::bail!("--all and --limit must be set together");
        }
        if !self.all && self.devid.is_none() && self.limit.is_some() {
            anyhow::bail!("--limit must be used with either --all or --devid");
        }

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let fs = fs_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;
        let devices = device_info_all(fd, &fs)
            .with_context(|| format!("failed to get device info for '{}'", self.path.display()))?;

        let sysfs = SysfsBtrfs::new(&fs.uuid);

        println!("UUID: {}", fs.uuid.as_hyphenated());

        if let Some(target_devid) = self.devid {
            // Set limit for one specific device.
            let dev = devices
                .iter()
                .find(|d| d.devid == target_devid)
                .with_context(|| format!("device with devid {target_devid} not found"))?;
            let new_limit = self.limit.unwrap();
            let old_limit = sysfs
                .scrub_speed_max_get(dev.devid)
                .with_context(|| format!("failed to read scrub limit for devid {}", dev.devid))?;
            println!(
                "Set scrub limit of devid {} from {} to {}",
                dev.devid,
                format_limit(old_limit),
                format_limit(new_limit),
            );
            sysfs
                .scrub_speed_max_set(dev.devid, new_limit)
                .with_context(|| format!("failed to set scrub limit for devid {}", dev.devid))?;
            return Ok(());
        }

        if self.all {
            // Set limit for all devices.
            let new_limit = self.limit.unwrap();
            for dev in &devices {
                let old_limit = sysfs.scrub_speed_max_get(dev.devid).with_context(|| {
                    format!("failed to read scrub limit for devid {}", dev.devid)
                })?;
                println!(
                    "Set scrub limit of devid {} from {} to {}",
                    dev.devid,
                    format_limit(old_limit),
                    format_limit(new_limit),
                );
                sysfs
                    .scrub_speed_max_set(dev.devid, new_limit)
                    .with_context(|| {
                        format!("failed to set scrub limit for devid {}", dev.devid)
                    })?;
            }
            return Ok(());
        }

        // Read-only mode: print a table of current limits.
        let id_w = "Id"
            .len()
            .max(devices.iter().map(|d| digits(d.devid)).max().unwrap_or(0));
        let limit_vals: Vec<String> = devices
            .iter()
            .map(|d| {
                sysfs
                    .scrub_speed_max_get(d.devid)
                    .map(format_limit)
                    .unwrap_or_else(|_| "-".to_owned())
            })
            .collect();
        let limit_w = "Limit"
            .len()
            .max(limit_vals.iter().map(|s| s.len()).max().unwrap_or(0));

        println!("{:>id_w$}  {:>limit_w$}  Path", "Id", "Limit");
        println!("{:->id_w$}  {:->limit_w$}  ----", "", "");
        for (dev, limit_str) in devices.iter().zip(limit_vals.iter()) {
            println!(
                "{:>id_w$}  {:>limit_w$}  {}",
                dev.devid, limit_str, dev.path
            );
        }

        Ok(())
    }
}

/// Format a bytes-per-second limit for display; `0` means unlimited.
fn format_limit(limit: u64) -> String {
    if limit == 0 {
        "unlimited".to_owned()
    } else {
        format!("{}/s", human_bytes(limit))
    }
}

/// Number of decimal digits in `n` (minimum 1).
fn digits(n: u64) -> usize {
    if n == 0 { 1 } else { n.ilog10() as usize + 1 }
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

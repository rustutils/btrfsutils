use crate::{
    Format, Runnable,
    util::{SizeFormat, open_path, parse_size_with_suffix},
};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    device::device_info_all,
    filesystem::filesystem_info,
    scrub::{scrub_progress, scrub_start},
    sysfs::SysfsBtrfs,
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Start a new scrub on the filesystem or a device.
///
/// Scrubs all devices sequentially. This command blocks until the scrub
/// completes; use Ctrl-C to cancel.
#[derive(Parser, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct ScrubStartCommand {
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

    /// Force starting new scrub even if a scrub is already running
    #[clap(long, short)]
    pub force: bool,

    /// Set the throughput limit for each device (0 for unlimited), restored
    /// afterwards
    #[clap(long, value_name = "SIZE", value_parser = parse_size_with_suffix)]
    pub limit: Option<u64>,

    /// Set ioprio class (see ionice(1) manpage)
    #[clap(short = 'c', value_name = "CLASS")]
    pub ioprio_class: Option<i32>,

    /// Set ioprio classdata (see ionice(1) manpage)
    #[clap(short = 'n', value_name = "CDATA")]
    pub ioprio_classdata: Option<i32>,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubStartCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
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

        if !self.force {
            for dev in &devices {
                if scrub_progress(fd, dev.devid)
                    .with_context(|| {
                        format!(
                            "failed to check scrub status for device {}",
                            dev.devid
                        )
                    })?
                    .is_some()
                {
                    bail!(
                        "Scrub is already running.\n\
                         To cancel use 'btrfs scrub cancel {path}'.\n\
                         To see the status use 'btrfs scrub status {path}'",
                        path = self.path.display()
                    );
                }
            }
        }

        let sysfs = SysfsBtrfs::new(&fs.uuid);
        let old_limits = self.apply_limits(&sysfs, &devices)?;

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
            println!("scrubbing device {} ({})", dev.devid, dev.path);

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
                    eprintln!("error scrubbing device {}: {e}", dev.devid);
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

        self.restore_limits(&sysfs, &old_limits);

        Ok(())
    }
}

impl ScrubStartCommand {
    fn apply_limits(
        &self,
        sysfs: &SysfsBtrfs,
        devices: &[btrfs_uapi::device::DeviceInfo],
    ) -> Result<Vec<(u64, u64)>> {
        let mut old_limits = Vec::new();
        if let Some(limit) = self.limit {
            for dev in devices {
                let old = sysfs.scrub_speed_max_get(dev.devid).with_context(
                    || {
                        format!(
                            "failed to read scrub limit for devid {}",
                            dev.devid
                        )
                    },
                )?;
                old_limits.push((dev.devid, old));
                sysfs.scrub_speed_max_set(dev.devid, limit).with_context(
                    || {
                        format!(
                            "failed to set scrub limit for devid {}",
                            dev.devid
                        )
                    },
                )?;
            }
        }
        Ok(old_limits)
    }

    #[allow(clippy::unused_self)] // method kept on the command struct for consistency
    fn restore_limits(&self, sysfs: &SysfsBtrfs, old_limits: &[(u64, u64)]) {
        for &(devid, old_limit) in old_limits {
            if let Err(e) = sysfs.scrub_speed_max_set(devid, old_limit) {
                eprintln!(
                    "WARNING: failed to restore scrub limit for devid {devid}: {e}"
                );
            }
        }
    }
}

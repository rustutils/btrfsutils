use crate::{Format, Runnable, util::parse_size_with_suffix};
use anyhow::{Context, Result};
use btrfs_uapi::{device::device_info_all, filesystem::fs_info, sysfs::SysfsBtrfs};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

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
                super::format_limit(old_limit),
                super::format_limit(new_limit),
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
                    super::format_limit(old_limit),
                    super::format_limit(new_limit),
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
        let id_w = "Id".len().max(
            devices
                .iter()
                .map(|d| super::digits(d.devid))
                .max()
                .unwrap_or(0),
        );
        let limit_vals: Vec<String> = devices
            .iter()
            .map(|d| {
                sysfs
                    .scrub_speed_max_get(d.devid)
                    .map(super::format_limit)
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

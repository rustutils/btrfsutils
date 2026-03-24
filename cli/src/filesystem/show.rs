use crate::{Format, Runnable, util::human_bytes};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all, filesystem::fs_info, label::label_get, space::space_info,
};
use clap::Parser;
use std::{collections::HashSet, fs::File, os::unix::io::AsFd};

/// Show information about one or more mounted or unmounted filesystems
#[derive(Parser, Debug)]
pub struct FilesystemShowCommand {
    /// Search all devices, including unmounted ones
    #[clap(long, short = 'd')]
    pub all_devices: bool,

    /// Search only mounted filesystems
    #[clap(long, short)]
    pub mounted: bool,

    /// Path, UUID, device or label to show (shows all if omitted)
    pub filter: Option<String>,
}

impl Runnable for FilesystemShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.all_devices {
            anyhow::bail!("--all-devices is not yet implemented");
        }

        let mounts = parse_btrfs_mounts().context("failed to read /proc/self/mounts")?;

        if mounts.is_empty() {
            println!("No btrfs filesystem found.");
            return Ok(());
        }

        let mut seen_uuids = HashSet::new();
        let mut first = true;
        for mount in &mounts {
            let file = match File::open(mount) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let fd = file.as_fd();

            let info = match fs_info(fd) {
                Ok(i) => i,
                Err(_) => continue,
            };

            if let Some(filter) = &self.filter {
                let uuid_str = info.uuid.as_hyphenated().to_string();
                let label = label_get(fd).unwrap_or_default();
                let label_str = label.to_string_lossy();
                if mount != filter && uuid_str != *filter && label_str != filter.as_str() {
                    continue;
                }
            }

            if !seen_uuids.insert(info.uuid) {
                continue;
            }

            let label = label_get(fd)
                .map(|l| l.to_string_lossy().into_owned())
                .unwrap_or_default();

            let devices = device_info_all(fd, &info)
                .with_context(|| format!("failed to get device info for '{mount}'"))?;

            let used_bytes = space_info(fd)
                .map(|entries| entries.iter().map(|e| e.used_bytes).sum::<u64>())
                .unwrap_or(0);

            if !first {
                println!();
            }
            first = false;

            if label.is_empty() {
                print!("Label: none ");
            } else {
                print!("Label: '{label}' ");
            }
            println!(" uuid: {}", info.uuid.as_hyphenated());
            println!(
                "\tTotal devices {} FS bytes used {}",
                info.num_devices,
                human_bytes(used_bytes)
            );

            for dev in &devices {
                println!(
                    "\tdevid {:4} size {} used {} path {}",
                    dev.devid,
                    human_bytes(dev.total_bytes),
                    human_bytes(dev.bytes_used),
                    dev.path,
                );
            }
        }

        Ok(())
    }
}

fn parse_btrfs_mounts() -> Result<Vec<String>> {
    let contents =
        std::fs::read_to_string("/proc/self/mounts").context("failed to read /proc/self/mounts")?;
    let mounts = contents
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(6, ' ');
            let _device = fields.next()?;
            let mountpoint = fields.next()?;
            let fstype = fields.next()?;
            if fstype == "btrfs" {
                Some(mountpoint.to_owned())
            } else {
                None
            }
        })
        .collect();
    Ok(mounts)
}

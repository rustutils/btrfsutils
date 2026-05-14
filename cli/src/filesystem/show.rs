use super::UnitMode;
use crate::{
    Format, RunContext, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all,
    filesystem::{filesystem_info, label_get},
    space::space_info,
};
use clap::Parser;
use cols::Cols;
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

    #[clap(flatten)]
    pub units: UnitMode,

    /// Path, UUID, device or label to show (shows all if omitted)
    pub filter: Option<String>,
}

struct FsEntry {
    label: String,
    uuid: String,
    num_devices: u64,
    used_bytes: u64,
    devices: Vec<DevEntry>,
}

struct DevEntry {
    devid: u64,
    total_bytes: u64,
    bytes_used: u64,
    path: String,
}

#[derive(Cols)]
struct DevRow {
    #[column(header = "DEVID", right)]
    devid: u64,
    #[column(header = "SIZE", right)]
    size: String,
    #[column(header = "USED", right)]
    used: String,
    #[column(header = "PATH")]
    path: String,
}

impl Runnable for FilesystemShowCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        if self.all_devices {
            anyhow::bail!("--all-devices is not yet implemented");
        }

        let mode = self.units.resolve();
        let entries = self.collect_entries()?;

        if entries.is_empty() {
            println!("No btrfs filesystem found.");
            return Ok(());
        }

        match ctx.format {
            Format::Modern => print_modern(&entries, &mode),
            Format::Text | Format::Json => print_text(&entries, &mode),
        }

        Ok(())
    }
}

impl FilesystemShowCommand {
    fn collect_entries(&self) -> Result<Vec<FsEntry>> {
        let mounts =
            parse_btrfs_mounts().context("failed to read /proc/self/mounts")?;

        let mut entries = Vec::new();
        let mut seen_uuids = HashSet::new();

        for mount in &mounts {
            let Ok(file) = File::open(mount) else {
                continue;
            };
            let fd = file.as_fd();

            let Ok(info) = filesystem_info(fd) else {
                continue;
            };

            if let Some(filter) = &self.filter {
                let uuid_str = info.uuid.as_hyphenated().to_string();
                let label = label_get(fd).unwrap_or_default();
                let label_str = label.to_string_lossy();
                if mount != filter
                    && uuid_str != *filter
                    && label_str != filter.as_str()
                {
                    continue;
                }
            }

            if !seen_uuids.insert(info.uuid) {
                continue;
            }

            let label = label_get(fd)
                .map(|l| l.to_string_lossy().into_owned())
                .unwrap_or_default();

            let devices = device_info_all(fd, &info).with_context(|| {
                format!("failed to get device info for '{mount}'")
            })?;

            let used_bytes = space_info(fd).map_or(0, |entries| {
                entries.iter().map(|e| e.used_bytes).sum::<u64>()
            });

            entries.push(FsEntry {
                label,
                uuid: info.uuid.as_hyphenated().to_string(),
                num_devices: info.num_devices,
                used_bytes,
                devices: devices
                    .iter()
                    .map(|d| DevEntry {
                        devid: d.devid,
                        total_bytes: d.total_bytes,
                        bytes_used: d.bytes_used,
                        path: d.path.clone(),
                    })
                    .collect(),
            });
        }

        Ok(entries)
    }
}

fn print_text(entries: &[FsEntry], mode: &SizeFormat) {
    for (i, entry) in entries.iter().enumerate() {
        if i > 0 {
            println!();
        }

        if entry.label.is_empty() {
            print!("Label: none ");
        } else {
            print!("Label: '{}' ", entry.label);
        }
        println!(" uuid: {}", entry.uuid);
        println!(
            "\tTotal devices {} FS bytes used {}",
            entry.num_devices,
            fmt_size(entry.used_bytes, mode)
        );

        for dev in &entry.devices {
            println!(
                "\tdevid {:4} size {} used {} path {}",
                dev.devid,
                fmt_size(dev.total_bytes, mode),
                fmt_size(dev.bytes_used, mode),
                dev.path,
            );
        }
    }
}

fn print_modern(entries: &[FsEntry], mode: &SizeFormat) {
    for (i, entry) in entries.iter().enumerate() {
        if i > 0 {
            println!();
        }

        if entry.label.is_empty() {
            println!("Label: none");
        } else {
            println!("Label: {}", entry.label);
        }
        println!("UUID:  {}", entry.uuid);
        println!(
            "Total: {} {}, {} used",
            entry.num_devices,
            if entry.num_devices == 1 {
                "device"
            } else {
                "devices"
            },
            fmt_size(entry.used_bytes, mode)
        );
        println!();

        let rows: Vec<DevRow> = entry
            .devices
            .iter()
            .map(|d| DevRow {
                devid: d.devid,
                size: fmt_size(d.total_bytes, mode),
                used: fmt_size(d.bytes_used, mode),
                path: d.path.clone(),
            })
            .collect();
        let mut out = std::io::stdout().lock();
        let _ = DevRow::print_table(&rows, &mut out);
    }
}

fn parse_btrfs_mounts() -> Result<Vec<String>> {
    let contents = std::fs::read_to_string("/proc/self/mounts")
        .context("failed to read /proc/self/mounts")?;
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

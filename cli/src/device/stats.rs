use crate::{
    Format, RunContext, Runnable,
    util::{open_path, print_json},
};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    device::{DeviceStats, device_info_all, device_stats},
    filesystem::filesystem_info,
};
use clap::Parser;
use cols::Cols;
use serde::Serialize;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show device I/O error statistics for all devices of a filesystem
///
/// Reads per-device counters for write, read, flush, corruption, and
/// generation errors. The path can be a mount point or a device belonging
/// to the filesystem.
///
/// With --offline, reads stats directly from the on-disk device tree
/// without requiring a mounted filesystem.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct DeviceStatsCommand {
    /// Return a non-zero exit code if any error counter is greater than zero
    #[clap(long, short)]
    pub check: bool,

    /// Print current values and then atomically reset all counters to zero
    #[clap(long, short = 'z', conflicts_with = "offline")]
    pub reset: bool,

    /// Show stats in a tabular format with columns instead of per-device lines
    #[clap(short = 'T')]
    pub tabular: bool,

    /// Read stats from the on-disk device tree (no mount required)
    #[clap(long)]
    pub offline: bool,

    /// Path to a mounted btrfs filesystem or a device/image file
    pub path: PathBuf,
}

#[derive(Serialize)]
struct StatsJson {
    device: String,
    devid: u64,
    write_io_errs: u64,
    read_io_errs: u64,
    flush_io_errs: u64,
    corruption_errs: u64,
    generation_errs: u64,
}

impl StatsJson {
    fn from_uapi(path: &str, stats: &DeviceStats) -> Self {
        Self {
            device: path.to_string(),
            devid: stats.devid,
            write_io_errs: stats.write_errs,
            read_io_errs: stats.read_errs,
            flush_io_errs: stats.flush_errs,
            corruption_errs: stats.corruption_errs,
            generation_errs: stats.generation_errs,
        }
    }

    fn from_disk(
        path: &str,
        devid: u64,
        stats: &btrfs_disk::items::DeviceStats,
    ) -> Self {
        Self {
            device: path.to_string(),
            devid,
            write_io_errs: stats.values.first().map_or(0, |v| v.1),
            read_io_errs: stats.values.get(1).map_or(0, |v| v.1),
            flush_io_errs: stats.values.get(2).map_or(0, |v| v.1),
            corruption_errs: stats.values.get(3).map_or(0, |v| v.1),
            generation_errs: stats.values.get(4).map_or(0, |v| v.1),
        }
    }

    fn is_clean(&self) -> bool {
        self.write_io_errs == 0
            && self.read_io_errs == 0
            && self.flush_io_errs == 0
            && self.corruption_errs == 0
            && self.generation_errs == 0
    }
}

impl Runnable for DeviceStatsCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        if self.offline {
            return self.run_offline(ctx.format);
        }

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

        if devices.is_empty() {
            bail!("no devices found for '{}'", self.path.display());
        }

        let mut all_stats: Vec<StatsJson> = Vec::new();
        let mut any_nonzero = false;

        for dev in &devices {
            let stats =
                device_stats(fd, dev.devid, self.reset).with_context(|| {
                    format!(
                        "failed to get stats for device {} ({})",
                        dev.devid, dev.path
                    )
                })?;

            let entry = StatsJson::from_uapi(&dev.path, &stats);
            if !entry.is_clean() {
                any_nonzero = true;
            }
            all_stats.push(entry);
        }

        match ctx.format {
            Format::Text if self.tabular => print_stats_table(&all_stats),
            Format::Text => {
                for s in &all_stats {
                    print_stats_text(s);
                }
            }
            Format::Json => {
                print_json("device-stats", &all_stats)?;
            }
        }

        if self.check && any_nonzero {
            bail!("one or more devices have non-zero error counters");
        }

        Ok(())
    }
}

impl DeviceStatsCommand {
    fn run_offline(&self, format: Format) -> Result<()> {
        use btrfs_disk::{
            items::DeviceStats as DiskDeviceStats,
            reader::{self, Traversal},
            tree::{KeyType, TreeBlock},
        };

        let file = open_path(&self.path)?;
        let fs = reader::filesystem_open(file).with_context(|| {
            format!(
                "failed to open btrfs filesystem on '{}'",
                self.path.display()
            )
        })?;

        let dev_tree_id = u64::from(btrfs_disk::raw::BTRFS_DEV_TREE_OBJECTID);
        let (dev_root, _) = fs
            .tree_roots
            .get(&dev_tree_id)
            .context("device tree not found")?;
        let dev_root = *dev_root;

        let header_size = std::mem::size_of::<btrfs_disk::raw::btrfs_header>();
        let path_str = self.path.display().to_string();

        let mut all_stats: Vec<StatsJson> = Vec::new();
        let mut block_reader = fs.reader;

        reader::tree_walk(
            &mut block_reader,
            dev_root,
            Traversal::Dfs,
            &mut |block| {
                if let TreeBlock::Leaf { items, data, .. } = block {
                    for item in items {
                        if item.key.key_type == KeyType::PersistentItem {
                            let start = header_size + item.offset as usize;
                            let end = start + item.size as usize;
                            if end <= data.len() {
                                let ds =
                                    DiskDeviceStats::parse(&data[start..end]);
                                let devid = item.key.offset;
                                all_stats.push(StatsJson::from_disk(
                                    &path_str, devid, &ds,
                                ));
                            }
                        }
                    }
                }
            },
        )
        .with_context(|| {
            format!("failed to walk device tree on '{}'", self.path.display())
        })?;

        if all_stats.is_empty() {
            all_stats.push(StatsJson::from_disk(
                &path_str,
                1,
                &DiskDeviceStats::parse(&[]),
            ));
        }

        let any_nonzero = all_stats.iter().any(|s| !s.is_clean());

        match format {
            Format::Text if self.tabular => print_stats_table(&all_stats),
            Format::Text => {
                for s in &all_stats {
                    let label = format!("{}.devid.{}", s.device, s.devid);
                    print_stats_text_labeled(&label, s);
                }
            }
            Format::Json => {
                print_json("device-stats", &all_stats)?;
            }
        }

        if self.check && any_nonzero {
            bail!("one or more devices have non-zero error counters");
        }

        Ok(())
    }
}

#[derive(Cols)]
struct StatsRow {
    #[column(header = "ID", right)]
    devid: u64,
    #[column(header = "WRITE_ERR", right)]
    write_errs: u64,
    #[column(header = "READ_ERR", right)]
    read_errs: u64,
    #[column(header = "FLUSH_ERR", right)]
    flush_errs: u64,
    #[column(header = "CORRUPT_ERR", right)]
    corruption_errs: u64,
    #[column(header = "GEN_ERR", right)]
    generation_errs: u64,
    #[column(header = "PATH", wrap)]
    path: String,
}

impl StatsRow {
    fn from_json(s: &StatsJson) -> Self {
        Self {
            devid: s.devid,
            path: s.device.clone(),
            write_errs: s.write_io_errs,
            read_errs: s.read_io_errs,
            flush_errs: s.flush_io_errs,
            corruption_errs: s.corruption_errs,
            generation_errs: s.generation_errs,
        }
    }
}

fn print_stats_table(stats: &[StatsJson]) {
    let rows: Vec<StatsRow> = stats.iter().map(StatsRow::from_json).collect();
    let mut out = std::io::stdout().lock();
    let _ = StatsRow::print_table(&rows, &mut out);
}

fn print_stats_text(s: &StatsJson) {
    let p = &s.device;
    println!("[{p}].{:<24} {}", "write_io_errs", s.write_io_errs);
    println!("[{p}].{:<24} {}", "read_io_errs", s.read_io_errs);
    println!("[{p}].{:<24} {}", "flush_io_errs", s.flush_io_errs);
    println!("[{p}].{:<24} {}", "corruption_errs", s.corruption_errs);
    println!("[{p}].{:<24} {}", "generation_errs", s.generation_errs);
}

fn print_stats_text_labeled(label: &str, s: &StatsJson) {
    println!("[{label}].{:<24} {}", "write_io_errs", s.write_io_errs);
    println!("[{label}].{:<24} {}", "read_io_errs", s.read_io_errs);
    println!("[{label}].{:<24} {}", "flush_io_errs", s.flush_io_errs);
    println!("[{label}].{:<24} {}", "corruption_errs", s.corruption_errs);
    println!("[{label}].{:<24} {}", "generation_errs", s.generation_errs);
}

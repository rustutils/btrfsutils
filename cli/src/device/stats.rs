use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    device::{DeviceStats, device_info_all, device_stats},
    filesystem::filesystem_info,
};
use clap::Parser;
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

    /// Read stats from the on-disk device tree (no mount required)
    #[clap(long)]
    pub offline: bool,

    /// Path to a mounted btrfs filesystem or a device/image file
    pub path: PathBuf,
}

impl Runnable for DeviceStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.offline {
            return self.run_offline();
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

        let mut any_nonzero = false;

        for dev in &devices {
            let stats =
                device_stats(fd, dev.devid, self.reset).with_context(|| {
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
            bail!("one or more devices have non-zero error counters");
        }

        Ok(())
    }
}

impl DeviceStatsCommand {
    fn run_offline(&self) -> Result<()> {
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

        let mut any_nonzero = false;
        let mut found_any = false;
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
                                print_offline_stats(&path_str, devid, &ds);
                                for &(_, v) in &ds.values {
                                    if v > 0 {
                                        any_nonzero = true;
                                    }
                                }
                                found_any = true;
                            }
                        }
                    }
                }
            },
        )
        .with_context(|| {
            format!("failed to walk device tree on '{}'", self.path.display())
        })?;

        if !found_any {
            // No stats items: all devices are clean (print zeros).
            print_offline_stats(&path_str, 1, &DiskDeviceStats::parse(&[]));
        }

        if self.check && any_nonzero {
            bail!("one or more devices have non-zero error counters");
        }

        Ok(())
    }
}

/// Print the five counters for one device in the same layout as the C tool:
/// `[/dev/path].counter_name   <value>`
fn print_stats(path: &str, stats: &DeviceStats) {
    let p = path;
    println!("[{p}].{:<24} {}", "write_io_errs", stats.write_errs);
    println!("[{p}].{:<24} {}", "read_io_errs", stats.read_errs);
    println!("[{p}].{:<24} {}", "flush_io_errs", stats.flush_errs);
    println!("[{p}].{:<24} {}", "corruption_errs", stats.corruption_errs);
    println!("[{p}].{:<24} {}", "generation_errs", stats.generation_errs);
}

/// Print stats from offline (on-disk) format. The disk parser returns
/// named pairs; map them to the standard counter names.
#[allow(clippy::cast_possible_truncation)]
fn print_offline_stats(
    path: &str,
    devid: u64,
    stats: &btrfs_disk::items::DeviceStats,
) {
    let label = format!("{path}.devid.{devid}");
    let names = [
        "write_io_errs",
        "read_io_errs",
        "flush_io_errs",
        "corruption_errs",
        "generation_errs",
    ];
    for (i, name) in names.iter().enumerate() {
        let val = stats.values.get(i).map_or(0, |&(_, v)| v);
        println!("[{label}].{name:<24} {val}");
    }
}

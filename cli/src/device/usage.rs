use crate::{
    Format, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    chunk::device_chunk_allocations, device::device_info_all,
    filesystem::filesystem_info, space::BlockGroupFlags,
};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show detailed information about internal allocations in devices
///
/// For each device, prints the total device size, the "slack" (difference
/// between the physical block device size and the size btrfs uses), per-profile
/// chunk allocations (Data, Metadata, System), and unallocated space. Requires
/// CAP_SYS_ADMIN for the chunk tree walk.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown, clippy::struct_excessive_bools)]
pub struct DeviceUsageCommand {
    /// Path(s) to a mounted btrfs filesystem
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,

    /// Show raw numbers in bytes
    #[clap(short = 'b', long, overrides_with_all = ["human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub raw: bool,

    /// Show human-friendly numbers using base 1024 (default)
    #[clap(long, overrides_with_all = ["raw", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub human_readable: bool,

    /// Show human-friendly numbers using base 1000
    #[clap(short = 'H', overrides_with_all = ["raw", "human_readable", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub human_base1000: bool,

    /// Use 1024 as a base (KiB, MiB, GiB, TiB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "human_base1000", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub iec: bool,

    /// Use 1000 as a base (kB, MB, GB, TB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub si: bool,

    /// Show sizes in KiB, or kB with --si
    #[clap(short = 'k', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "mbytes", "gbytes", "tbytes"])]
    pub kbytes: bool,

    /// Show sizes in MiB, or MB with --si
    #[clap(short = 'm', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "gbytes", "tbytes"])]
    pub mbytes: bool,

    /// Show sizes in GiB, or GB with --si
    #[clap(short = 'g', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes"])]
    pub gbytes: bool,

    /// Show sizes in TiB, or TB with --si
    #[clap(short = 't', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes"])]
    pub tbytes: bool,
}

/// Try to get the physical block device size.  Returns 0 on failure (e.g.
/// device path is empty, inaccessible, or not a block device).
fn physical_device_size(path: &str) -> u64 {
    if path.is_empty() {
        return 0;
    }
    let Ok(file) = File::open(path) else {
        return 0;
    };
    btrfs_uapi::blkdev::device_size(file.as_fd()).unwrap_or(0)
}

impl DeviceUsageCommand {
    fn size_format(&self) -> SizeFormat {
        let si = self.si;
        if self.raw {
            SizeFormat::Raw
        } else if self.kbytes {
            SizeFormat::Fixed(if si { 1000 } else { 1024 })
        } else if self.mbytes {
            SizeFormat::Fixed(if si { 1_000_000 } else { 1024 * 1024 })
        } else if self.gbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000
            } else {
                1024 * 1024 * 1024
            })
        } else if self.tbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000_000
            } else {
                1024u64.pow(4)
            })
        } else if si || self.human_base1000 {
            SizeFormat::HumanSi
        } else {
            SizeFormat::HumanIec
        }
    }
}

impl Runnable for DeviceUsageCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let mode = self.size_format();
        for (i, path) in self.paths.iter().enumerate() {
            if i > 0 {
                println!();
            }
            print_device_usage(path, &mode)?;
        }
        Ok(())
    }
}

fn print_device_usage(path: &std::path::Path, mode: &SizeFormat) -> Result<()> {
    let file = File::open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))?;
    let fd = file.as_fd();

    let fs = filesystem_info(fd).with_context(|| {
        format!("failed to get filesystem info for '{}'", path.display())
    })?;
    let devices = device_info_all(fd, &fs).with_context(|| {
        format!("failed to get device info for '{}'", path.display())
    })?;
    let allocs = device_chunk_allocations(fd).with_context(|| {
        format!("failed to get chunk allocations for '{}'", path.display())
    })?;

    for (di, dev) in devices.iter().enumerate() {
        if di > 0 {
            println!();
        }

        let phys_size = physical_device_size(&dev.path);
        let slack = if phys_size > 0 {
            phys_size.saturating_sub(dev.total_bytes)
        } else {
            0
        };

        println!("{}, ID: {}", dev.path, dev.devid);

        print_line("Device size", &fmt_size(dev.total_bytes, mode));
        print_line("Device slack", &fmt_size(slack, mode));

        let mut allocated: u64 = 0;
        let mut dev_allocs: Vec<_> =
            allocs.iter().filter(|a| a.devid == dev.devid).collect();
        dev_allocs.sort_by_key(|a| {
            let type_order = if a.flags.contains(BlockGroupFlags::DATA) {
                0
            } else if a.flags.contains(BlockGroupFlags::METADATA) {
                1
            } else {
                2
            };
            (type_order, a.flags.bits())
        });

        for alloc in &dev_allocs {
            allocated += alloc.bytes;
            let label = format!(
                "{},{}",
                alloc.flags.type_name(),
                alloc.flags.profile_name()
            );
            print_line(&label, &fmt_size(alloc.bytes, mode));
        }

        let unallocated = dev.total_bytes.saturating_sub(allocated);
        print_line("Unallocated", &fmt_size(unallocated, mode));
    }

    Ok(())
}

fn print_line(label: &str, value: &str) {
    let padding = 20usize.saturating_sub(label.len());
    println!("   {label}:{:>pad$}{value:>10}", "", pad = padding);
}

use super::UnitMode;
use crate::{Format, Runnable, util::human_bytes};
use anyhow::{Context, Result};
use btrfs_uapi::{
    chunk::device_chunk_allocations,
    device::device_info_all,
    filesystem::filesystem_info,
    space::{BlockGroupFlags, SpaceInfo, space_info},
};
use clap::Parser;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    os::unix::io::AsFd,
    path::PathBuf,
};

/// Show detailed information about internal filesystem usage
#[derive(Parser, Debug)]
pub struct FilesystemUsageCommand {
    #[clap(flatten)]
    pub units: UnitMode,

    /// Use base 1000 for human-readable sizes
    #[clap(short = 'H', long)]
    pub human_si: bool,

    /// Show data in tabular format
    #[clap(short = 'T', long)]
    pub tabular: bool,

    /// One or more mount points to show usage for
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

/// Number of raw-device copies per chunk for a given profile.
/// Returns 0 for RAID5/6 which require chunk tree data to compute accurately.
fn profile_ncopies(flags: BlockGroupFlags) -> u64 {
    if flags.contains(BlockGroupFlags::RAID1C4) {
        4
    } else if flags.contains(BlockGroupFlags::RAID1C3) {
        3
    } else if flags.contains(BlockGroupFlags::RAID1)
        || flags.contains(BlockGroupFlags::DUP)
        || flags.contains(BlockGroupFlags::RAID10)
    {
        2
    } else if flags.contains(BlockGroupFlags::RAID5)
        || flags.contains(BlockGroupFlags::RAID6)
    {
        0
    } else {
        1
    }
}

fn has_multiple_profiles(spaces: &[SpaceInfo]) -> bool {
    let profile_mask = BlockGroupFlags::RAID0
        | BlockGroupFlags::RAID1
        | BlockGroupFlags::DUP
        | BlockGroupFlags::RAID10
        | BlockGroupFlags::RAID5
        | BlockGroupFlags::RAID6
        | BlockGroupFlags::RAID1C3
        | BlockGroupFlags::RAID1C4
        | BlockGroupFlags::SINGLE;

    let profiles_for = |type_flag: BlockGroupFlags| {
        spaces
            .iter()
            .filter(|s| {
                s.flags.contains(type_flag)
                    && !s.flags.contains(BlockGroupFlags::GLOBAL_RSV)
            })
            .map(|s| s.flags & profile_mask)
            .collect::<HashSet<_>>()
    };

    profiles_for(BlockGroupFlags::DATA).len() > 1
        || profiles_for(BlockGroupFlags::METADATA).len() > 1
        || profiles_for(BlockGroupFlags::SYSTEM).len() > 1
}

impl Runnable for FilesystemUsageCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        for (i, path) in self.paths.iter().enumerate() {
            if i > 0 {
                println!();
            }
            print_usage(path, self.tabular)?;
        }
        Ok(())
    }
}

fn print_usage(path: &std::path::Path, _tabular: bool) -> Result<()> {
    let file = File::open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))?;
    let fd = file.as_fd();

    let fs = filesystem_info(fd).with_context(|| {
        format!("failed to get filesystem info for '{}'", path.display())
    })?;
    let devices = device_info_all(fd, &fs).with_context(|| {
        format!("failed to get device info for '{}'", path.display())
    })?;
    let spaces = space_info(fd).with_context(|| {
        format!("failed to get space info for '{}'", path.display())
    })?;

    // Per-device chunk allocations from the chunk tree.  This requires
    // CAP_SYS_ADMIN; if it fails we degrade gracefully and note it below.
    let chunk_allocs = device_chunk_allocations(fd).ok();

    // Map devid -> path for display.
    let devid_to_path: HashMap<u64, &str> =
        devices.iter().map(|d| (d.devid, d.path.as_str())).collect();

    let mut r_data_chunks: u64 = 0;
    let mut r_data_used: u64 = 0;
    let mut l_data_chunks: u64 = 0;
    let mut r_meta_chunks: u64 = 0;
    let mut r_meta_used: u64 = 0;
    let mut l_meta_chunks: u64 = 0;
    let mut r_sys_chunks: u64 = 0;
    let mut r_sys_used: u64 = 0;
    let mut l_global_reserve: u64 = 0;
    let mut l_global_reserve_used: u64 = 0;
    let mut max_ncopies: u64 = 1;

    for s in &spaces {
        if s.flags.contains(BlockGroupFlags::GLOBAL_RSV) {
            l_global_reserve = s.total_bytes;
            l_global_reserve_used = s.used_bytes;
            continue;
        }
        let ncopies = profile_ncopies(s.flags);
        if ncopies > max_ncopies {
            max_ncopies = ncopies;
        }
        if s.flags.contains(BlockGroupFlags::DATA) {
            r_data_chunks += s.total_bytes * ncopies;
            r_data_used += s.used_bytes * ncopies;
            l_data_chunks += s.total_bytes;
        }
        if s.flags.contains(BlockGroupFlags::METADATA) {
            r_meta_chunks += s.total_bytes * ncopies;
            r_meta_used += s.used_bytes * ncopies;
            l_meta_chunks += s.total_bytes;
        }
        if s.flags.contains(BlockGroupFlags::SYSTEM) {
            r_sys_chunks += s.total_bytes * ncopies;
            r_sys_used += s.used_bytes * ncopies;
        }
    }

    let r_total_size: u64 = devices.iter().map(|d| d.total_bytes).sum();
    let r_total_chunks = r_data_chunks + r_meta_chunks + r_sys_chunks;
    let r_total_used = r_data_used + r_meta_used + r_sys_used;
    let r_total_unused = r_total_size.saturating_sub(r_total_chunks);

    let r_total_missing: u64 = devices
        .iter()
        .filter(|d| std::fs::metadata(&d.path).is_err())
        .map(|d| d.total_bytes)
        .sum();

    let data_ratio = if l_data_chunks > 0 {
        r_data_chunks as f64 / l_data_chunks as f64
    } else {
        1.0
    };
    let meta_ratio = if l_meta_chunks > 0 {
        r_meta_chunks as f64 / l_meta_chunks as f64
    } else {
        1.0
    };
    let max_data_ratio = max_ncopies as f64;

    const MIN_UNALLOCATED_THRESH: u64 = 16 * 1024 * 1024;
    let free_base = if data_ratio > 0.0 {
        ((r_data_chunks.saturating_sub(r_data_used)) as f64 / data_ratio) as u64
    } else {
        0
    };
    let (free_estimated, free_min) = if r_total_unused >= MIN_UNALLOCATED_THRESH
    {
        (
            free_base + (r_total_unused as f64 / data_ratio) as u64,
            free_base + (r_total_unused as f64 / max_data_ratio) as u64,
        )
    } else {
        (free_base, free_base)
    };

    let free_statfs = nix::sys::statfs::statfs(path)
        .map(|st| st.blocks_available() as u64 * st.block_size() as u64)
        .unwrap_or(0);

    let multiple = has_multiple_profiles(&spaces);

    println!("Overall:");
    println!("    Device size:\t\t{:>10}", human_bytes(r_total_size));
    println!(
        "    Device allocated:\t\t{:>10}",
        human_bytes(r_total_chunks)
    );
    println!(
        "    Device unallocated:\t\t{:>10}",
        human_bytes(r_total_unused)
    );
    println!(
        "    Device missing:\t\t{:>10}",
        human_bytes(r_total_missing)
    );
    println!("    Device slack:\t\t{:>10}", human_bytes(0));
    println!("    Used:\t\t\t{:>10}", human_bytes(r_total_used));
    println!(
        "    Free (estimated):\t\t{:>10}\t(min: {})",
        human_bytes(free_estimated),
        human_bytes(free_min)
    );
    println!("    Free (statfs, df):\t\t{:>10}", human_bytes(free_statfs));
    println!("    Data ratio:\t\t\t{:>10.2}", data_ratio);
    println!("    Metadata ratio:\t\t{:>10.2}", meta_ratio);
    println!(
        "    Global reserve:\t\t{:>10}\t(used: {})",
        human_bytes(l_global_reserve),
        human_bytes(l_global_reserve_used)
    );
    println!(
        "    Multiple profiles:\t\t{:>10}",
        if multiple { "yes" } else { "no" }
    );

    if chunk_allocs.is_none() {
        eprintln!(
            "NOTE: per-device usage breakdown unavailable \
             (chunk tree requires CAP_SYS_ADMIN)"
        );
    }

    for s in &spaces {
        if s.flags.contains(BlockGroupFlags::GLOBAL_RSV) {
            continue;
        }
        let pct = if s.total_bytes > 0 {
            100.0 * s.used_bytes as f64 / s.total_bytes as f64
        } else {
            0.0
        };
        println!(
            "\n{},{}: Size:{}, Used:{} ({:.2}%)",
            s.flags.type_name(),
            s.flags.profile_name(),
            human_bytes(s.total_bytes),
            human_bytes(s.used_bytes),
            pct
        );

        // Per-device lines: one row per device that holds stripes for this
        // exact profile.  Sorted by devid for stable output.
        if let Some(allocs) = &chunk_allocs {
            let mut profile_allocs: Vec<_> =
                allocs.iter().filter(|a| a.flags == s.flags).collect();
            profile_allocs.sort_by_key(|a| a.devid);

            for alloc in profile_allocs {
                let path = devid_to_path
                    .get(&alloc.devid)
                    .copied()
                    .unwrap_or("<unknown>");
                println!("   {}\t\t{:>10}", path, human_bytes(alloc.bytes));
            }
        }
    }

    println!("\nUnallocated:");
    for dev in &devices {
        let unallocated = dev.total_bytes.saturating_sub(dev.bytes_used);
        println!("   {}\t{:>10}", dev.path, human_bytes(unallocated));
    }

    Ok(())
}

use super::UnitMode;
use crate::{
    Format, RunContext, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    chunk::device_chunk_allocations,
    device::device_info_all,
    filesystem::filesystem_info,
    space::{BlockGroupFlags, SpaceInfo, space_info},
};
use clap::Parser;
use cols::Cols;
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
    } else {
        u64::from(
            !(flags.contains(BlockGroupFlags::RAID5)
                || flags.contains(BlockGroupFlags::RAID6)),
        )
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
    fn run(&self, ctx: &RunContext) -> Result<()> {
        let mut mode = self.units.resolve();
        if self.human_si {
            mode = SizeFormat::HumanSi;
        }
        for (i, path) in self.paths.iter().enumerate() {
            if i > 0 {
                println!();
            }
            match ctx.format {
                Format::Modern => print_usage_modern(path, &mode)?,
                Format::Text | Format::Json => {
                    print_usage(path, self.tabular, &mode)?;
                }
            }
        }
        Ok(())
    }
}

const MIN_UNALLOCATED_THRESH: u64 = 16 * 1024 * 1024;

/// Computed overall stats for a filesystem.
struct OverallStats {
    r_total_size: u64,
    r_total_chunks: u64,
    r_total_unused: u64,
    r_total_missing: u64,
    r_total_used: u64,
    data_ratio: f64,
    meta_ratio: f64,
    free_estimated: u64,
    free_min: u64,
    free_statfs: u64,
    l_global_reserve: u64,
    l_global_reserve_used: u64,
    multiple: bool,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn compute_overall(
    path: &std::path::Path,
    devices: &[btrfs_uapi::device::DeviceInfo],
    spaces: &[SpaceInfo],
) -> OverallStats {
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

    for s in spaces {
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
        .map_or(0, |st| st.blocks_available() * st.block_size() as u64);

    let multiple = has_multiple_profiles(spaces);

    OverallStats {
        r_total_size,
        r_total_chunks,
        r_total_unused,
        r_total_missing,
        r_total_used,
        data_ratio,
        meta_ratio,
        free_estimated,
        free_min,
        free_statfs,
        l_global_reserve,
        l_global_reserve_used,
        multiple,
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn print_usage(
    path: &std::path::Path,
    _tabular: bool,
    mode: &SizeFormat,
) -> Result<()> {
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

    let stats = compute_overall(path, &devices, &spaces);

    println!("Overall:");
    println!(
        "    Device size:\t\t{:>10}",
        fmt_size(stats.r_total_size, mode)
    );
    println!(
        "    Device allocated:\t\t{:>10}",
        fmt_size(stats.r_total_chunks, mode)
    );
    println!(
        "    Device unallocated:\t\t{:>10}",
        fmt_size(stats.r_total_unused, mode)
    );
    println!(
        "    Device missing:\t\t{:>10}",
        fmt_size(stats.r_total_missing, mode)
    );
    println!("    Device slack:\t\t{:>10}", fmt_size(0, mode));
    println!("    Used:\t\t\t{:>10}", fmt_size(stats.r_total_used, mode));
    println!(
        "    Free (estimated):\t\t{:>10}\t(min: {})",
        fmt_size(stats.free_estimated, mode),
        fmt_size(stats.free_min, mode)
    );
    println!(
        "    Free (statfs, df):\t\t{:>10}",
        fmt_size(stats.free_statfs, mode)
    );
    println!("    Data ratio:\t\t\t{:>10.2}", stats.data_ratio);
    println!("    Metadata ratio:\t\t{:>10.2}", stats.meta_ratio);
    println!(
        "    Global reserve:\t\t{:>10}\t(used: {})",
        fmt_size(stats.l_global_reserve, mode),
        fmt_size(stats.l_global_reserve_used, mode)
    );
    println!(
        "    Multiple profiles:\t\t{:>10}",
        if stats.multiple { "yes" } else { "no" }
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
        #[allow(clippy::cast_precision_loss)]
        let pct = if s.total_bytes > 0 {
            100.0 * s.used_bytes as f64 / s.total_bytes as f64
        } else {
            0.0
        };
        println!(
            "\n{},{}: Size:{}, Used:{} ({:.2}%)",
            s.flags.type_name(),
            s.flags.profile_name(),
            fmt_size(s.total_bytes, mode),
            fmt_size(s.used_bytes, mode),
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
                println!("   {}\t\t{:>10}", path, fmt_size(alloc.bytes, mode));
            }
        }
    }

    println!("\nUnallocated:");
    for dev in &devices {
        let unallocated = dev.total_bytes.saturating_sub(dev.bytes_used);
        println!("   {}\t{:>10}", dev.path, fmt_size(unallocated, mode));
    }

    Ok(())
}

// -- Modern output -----------------------------------------------------------

#[derive(Cols)]
struct OverallRow {
    #[column(header = "PROPERTY")]
    label: String,
    #[column(header = "VALUE", right)]
    value: String,
}

#[derive(Cols)]
struct ProfileRow {
    #[column(header = "TYPE")]
    bg_type: String,
    #[column(header = "PROFILE")]
    profile: String,
    #[column(header = "TOTAL", right)]
    total: String,
    #[column(header = "USED", right)]
    used: String,
    #[column(header = "USED%", right)]
    pct: String,
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn print_usage_modern(path: &std::path::Path, mode: &SizeFormat) -> Result<()> {
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
    let chunk_allocs = device_chunk_allocations(fd).ok();

    let stats = compute_overall(path, &devices, &spaces);

    // Section 1: Overall key-value table
    println!("Overall:");
    let mut overall_rows = vec![
        OverallRow {
            label: "Device size".to_string(),
            value: fmt_size(stats.r_total_size, mode),
        },
        OverallRow {
            label: "Device allocated".to_string(),
            value: fmt_size(stats.r_total_chunks, mode),
        },
        OverallRow {
            label: "Device unallocated".to_string(),
            value: fmt_size(stats.r_total_unused, mode),
        },
        OverallRow {
            label: "Device missing".to_string(),
            value: fmt_size(stats.r_total_missing, mode),
        },
        OverallRow {
            label: "Device slack".to_string(),
            value: fmt_size(0, mode),
        },
        OverallRow {
            label: "Used".to_string(),
            value: fmt_size(stats.r_total_used, mode),
        },
        OverallRow {
            label: "Free (estimated)".to_string(),
            value: format!(
                "{}  (min: {})",
                fmt_size(stats.free_estimated, mode),
                fmt_size(stats.free_min, mode)
            ),
        },
        OverallRow {
            label: "Free (statfs, df)".to_string(),
            value: fmt_size(stats.free_statfs, mode),
        },
        OverallRow {
            label: "Data ratio".to_string(),
            value: format!("{:.2}", stats.data_ratio),
        },
        OverallRow {
            label: "Metadata ratio".to_string(),
            value: format!("{:.2}", stats.meta_ratio),
        },
    ];

    overall_rows.push(OverallRow {
        label: "Global reserve".to_string(),
        value: format!(
            "{}  (used: {})",
            fmt_size(stats.l_global_reserve, mode),
            fmt_size(stats.l_global_reserve_used, mode)
        ),
    });
    overall_rows.push(OverallRow {
        label: "Multiple profiles".to_string(),
        value: if stats.multiple { "yes" } else { "no" }.to_string(),
    });

    let mut out = std::io::stdout().lock();
    let _ = OverallRow::print_table(&overall_rows, &mut out);

    if chunk_allocs.is_none() {
        eprintln!(
            "NOTE: per-device usage breakdown unavailable \
             (chunk tree requires CAP_SYS_ADMIN)"
        );
    }

    // Section 2: Profile summary table
    let profile_rows: Vec<ProfileRow> = spaces
        .iter()
        .filter(|s| !s.flags.contains(BlockGroupFlags::GLOBAL_RSV))
        .map(|s| {
            let pct = if s.total_bytes > 0 {
                100.0 * s.used_bytes as f64 / s.total_bytes as f64
            } else {
                0.0
            };
            ProfileRow {
                bg_type: s.flags.type_name().to_string(),
                profile: s.flags.profile_name().to_string(),
                total: fmt_size(s.total_bytes, mode),
                used: fmt_size(s.used_bytes, mode),
                pct: format!("{pct:.2}%"),
            }
        })
        .collect();

    if !profile_rows.is_empty() {
        println!();
        let _ = ProfileRow::print_table(&profile_rows, &mut out);
    }

    // Section 3: Per-device allocation table (dynamic columns)
    if let Some(allocs) = &chunk_allocs {
        // Collect unique profiles in display order.
        let mut profile_flags: Vec<BlockGroupFlags> = Vec::new();
        for s in &spaces {
            if s.flags.contains(BlockGroupFlags::GLOBAL_RSV) {
                continue;
            }
            if !profile_flags.contains(&s.flags) {
                profile_flags.push(s.flags);
            }
        }

        let mut table = cols::Table::new();
        table.add_column(cols::Column::new("PATH"));
        for flags in &profile_flags {
            table.add_column(
                cols::Column::new(&format!(
                    "{},{}",
                    flags.type_name(),
                    flags.profile_name()
                ))
                .right(true),
            );
        }
        table.add_column(cols::Column::new("UNALLOC").right(true));

        for dev in &devices {
            let line = table.new_line(None);
            let row = table.line_mut(line);
            row.data_set(0, &dev.path);

            let mut allocated: u64 = 0;
            for (ci, flags) in profile_flags.iter().enumerate() {
                let bytes: u64 = allocs
                    .iter()
                    .filter(|a| a.devid == dev.devid && a.flags == *flags)
                    .map(|a| a.bytes)
                    .sum();
                allocated += bytes;
                if bytes > 0 {
                    row.data_set(ci + 1, &fmt_size(bytes, mode));
                } else {
                    row.data_set(ci + 1, "-");
                }
            }

            let unallocated = dev.total_bytes.saturating_sub(allocated);
            row.data_set(profile_flags.len() + 1, &fmt_size(unallocated, mode));
        }

        println!();
        let _ = cols::print_table(&table, &mut out);
    }

    Ok(())
}

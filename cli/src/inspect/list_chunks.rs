use crate::{
    RunContext, Runnable,
    filesystem::UnitMode,
    util::{fmt_size, open_path},
};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{chunk::chunk_list, filesystem::filesystem_info};
use clap::Parser;
use std::{cmp::Ordering, os::unix::io::AsFd, path::PathBuf};

/// List all chunks in the filesystem, one row per stripe
///
/// Enumerates every chunk across all devices by walking the chunk tree.
/// For striped profiles (RAID0, RAID10, RAID5, RAID6) each logical chunk
/// maps to multiple stripes on different devices, so it appears on multiple
/// rows. For DUP each logical chunk maps to two physical stripes on the same
/// device, so it also appears twice. For single and non-striped profiles
/// there is a 1:1 correspondence between logical chunks and rows.
///
/// Requires CAP_SYS_ADMIN.
///
/// Columns:
///
/// Devid: btrfs device ID the stripe lives on.
///
/// PNumber: physical chunk index on this device, ordered by physical start
/// offset (1-based).
///
/// Type/profile: block-group type (data, metadata, system) and replication
/// profile (single, dup, raid0, raid1, ...).
///
/// PStart: physical byte offset of this stripe on the device.
///
/// Length: logical length of the chunk (shared by all its stripes).
///
/// PEnd: physical byte offset of the end of this stripe (PStart + Length).
///
/// LNumber: logical chunk index for this device, ordered by logical start
/// offset (1-based); DUP stripes share the same value.
///
/// LStart: logical byte offset of the chunk in the filesystem address space.
///
/// Usage%: percentage of the chunk's logical space currently occupied
/// (used / length * 100), sourced from the extent tree.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct ListChunksCommand {
    #[clap(flatten)]
    pub units: UnitMode,

    /// Sort output by the given columns (comma-separated).
    /// Prepend - for descending order.
    /// Keys: devid, pstart, lstart, usage, length, type, profile.
    /// Default: devid,pstart.
    #[clap(long, value_name = "KEYS")]
    pub sort: Option<String>,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

/// One row in the output table.
struct Row {
    devid: u64,
    pnumber: u64,
    flags_str: String,
    physical_start: u64,
    length: u64,
    physical_end: u64,
    lnumber: u64,
    logical_start: u64,
    usage_pct: f64,
}

impl Runnable for ListChunksCommand {
    #[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        let fmt = |bytes| fmt_size(bytes, &mode);
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let fs = filesystem_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut entries = chunk_list(fd).with_context(|| {
            format!("failed to read chunk tree for '{}'", self.path.display())
        })?;

        if entries.is_empty() {
            println!("no chunks found");
            return Ok(());
        }

        // Sort by (devid, physical_start) to assign pnumber sequentially
        // per device in physical order.
        entries.sort_by_key(|e| (e.devid, e.physical_start));

        // Assign pnumber (1-based, per devid) and lnumber (1-based,
        // per devid, in the order we encounter logical chunks).
        let mut rows: Vec<Row> = Vec::with_capacity(entries.len());
        let mut pcount: Vec<(u64, u64)> = Vec::new(); // (devid, count)

        // Build lnumber by iterating in the original (logical) order first.
        // Re-sort a copy by (devid, logical_start) to assign lnumbers.
        let mut logical_order = entries.clone();
        logical_order.sort_by_key(|e| (e.devid, e.logical_start));
        // Map (devid, logical_start) -> lnumber (1-based).
        let mut lnumber_map: std::collections::HashMap<(u64, u64), u64> =
            std::collections::HashMap::new();
        {
            let mut lcnt: Vec<(u64, u64)> = Vec::new();
            for e in &logical_order {
                let key = (e.devid, e.logical_start);
                lnumber_map
                    .entry(key)
                    .or_insert_with(|| get_or_insert_count(&mut lcnt, e.devid));
            }
        }

        for e in &entries {
            let pnumber = get_or_insert_count(&mut pcount, e.devid);
            let lnumber =
                *lnumber_map.get(&(e.devid, e.logical_start)).unwrap_or(&1);
            let usage_pct = if e.length > 0 {
                e.used as f64 / e.length as f64 * 100.0
            } else {
                0.0
            };
            rows.push(Row {
                devid: e.devid,
                pnumber,
                flags_str: format_flags(e.flags),
                physical_start: e.physical_start,
                length: e.length,
                physical_end: e.physical_start + e.length,
                lnumber,
                logical_start: e.logical_start,
                usage_pct,
            });
        }

        // Apply user-specified sort if given.
        if let Some(ref sort_str) = self.sort {
            let specs = parse_sort_specs(sort_str)?;
            rows.sort_by(|a, b| compare_rows(a, b, &specs));
        }

        // Compute column widths.
        let devid_w = col_w("Devid", rows.iter().map(|r| digits(r.devid)));
        let pnum_w = col_w("PNumber", rows.iter().map(|r| digits(r.pnumber)));
        let type_w =
            col_w("Type/profile", rows.iter().map(|r| r.flags_str.len()));
        let pstart_w =
            col_w("PStart", rows.iter().map(|r| fmt(r.physical_start).len()));
        let length_w =
            col_w("Length", rows.iter().map(|r| fmt(r.length).len()));
        let pend_w =
            col_w("PEnd", rows.iter().map(|r| fmt(r.physical_end).len()));
        let lnum_w = col_w("LNumber", rows.iter().map(|r| digits(r.lnumber)));
        let lstart_w =
            col_w("LStart", rows.iter().map(|r| fmt(r.logical_start).len()));
        let usage_w = "Usage%".len().max("100.00".len());

        // Header
        println!(
            "{:>devid_w$}  {:>pnum_w$}  {:type_w$}  {:>pstart_w$}  {:>length_w$}  {:>pend_w$}  {:>lnum_w$}  {:>lstart_w$}  {:>usage_w$}",
            "Devid",
            "PNumber",
            "Type/profile",
            "PStart",
            "Length",
            "PEnd",
            "LNumber",
            "LStart",
            "Usage%",
        );
        // Separator
        println!(
            "{:->devid_w$}  {:->pnum_w$}  {:->type_w$}  {:->pstart_w$}  {:->length_w$}  {:->pend_w$}  {:->lnum_w$}  {:->lstart_w$}  {:->usage_w$}",
            "", "", "", "", "", "", "", "", "",
        );

        // Rows
        for r in &rows {
            println!(
                "{:>devid_w$}  {:>pnum_w$}  {:type_w$}  {:>pstart_w$}  {:>length_w$}  {:>pend_w$}  {:>lnum_w$}  {:>lstart_w$}  {:>usage_w$.2}",
                r.devid,
                r.pnumber,
                r.flags_str,
                fmt(r.physical_start),
                fmt(r.length),
                fmt(r.physical_end),
                r.lnumber,
                fmt(r.logical_start),
                r.usage_pct,
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum SortKey {
    Devid,
    PStart,
    LStart,
    Usage,
    Length,
    Type,
    Profile,
}

#[derive(Debug, Clone, Copy)]
struct SortSpec {
    key: SortKey,
    descending: bool,
}

fn parse_sort_specs(input: &str) -> Result<Vec<SortSpec>> {
    let mut specs = Vec::new();
    for token in input.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let (descending, name) = if let Some(rest) = token.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = token.strip_prefix('+') {
            (false, rest)
        } else {
            (false, token)
        };
        let key = match name {
            "devid" => SortKey::Devid,
            "pstart" => SortKey::PStart,
            "lstart" => SortKey::LStart,
            "usage" => SortKey::Usage,
            "length" => SortKey::Length,
            "type" => SortKey::Type,
            "profile" => SortKey::Profile,
            _ => bail!("unknown sort key: '{name}'"),
        };
        specs.push(SortSpec { key, descending });
    }
    Ok(specs)
}

fn type_ord(flags: &str) -> u8 {
    if flags.starts_with("data/") {
        0
    } else if flags.starts_with("metadata/") {
        1
    } else if flags.starts_with("system/") {
        2
    } else {
        3
    }
}

fn profile_ord(flags: &str) -> u8 {
    let profile = flags.rsplit('/').next().unwrap_or("");
    match profile {
        "single" => 0,
        "dup" => 1,
        "raid0" => 2,
        "raid1" => 3,
        "raid1c3" => 4,
        "raid1c4" => 5,
        "raid10" => 6,
        "raid5" => 7,
        "raid6" => 8,
        _ => 9,
    }
}

fn compare_rows(a: &Row, b: &Row, specs: &[SortSpec]) -> Ordering {
    for spec in specs {
        let ord = match spec.key {
            SortKey::Devid => a.devid.cmp(&b.devid),
            SortKey::PStart => a.physical_start.cmp(&b.physical_start),
            SortKey::LStart => a.logical_start.cmp(&b.logical_start),
            SortKey::Usage => a.usage_pct.total_cmp(&b.usage_pct),
            SortKey::Length => a.length.cmp(&b.length),
            SortKey::Type => {
                type_ord(&a.flags_str).cmp(&type_ord(&b.flags_str))
            }
            SortKey::Profile => {
                profile_ord(&a.flags_str).cmp(&profile_ord(&b.flags_str))
            }
        };
        let ord = if spec.descending { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Format `BlockGroupFlags` as `"<type>/<profile>"`, e.g. `"data/single"`.
fn format_flags(flags: btrfs_uapi::space::BlockGroupFlags) -> String {
    use btrfs_uapi::space::BlockGroupFlags as F;

    let type_str = if flags.contains(F::DATA) {
        "data"
    } else if flags.contains(F::METADATA) {
        "metadata"
    } else if flags.contains(F::SYSTEM) {
        "system"
    } else {
        "unknown"
    };

    let profile_str = if flags.contains(F::RAID10) {
        "raid10"
    } else if flags.contains(F::RAID1C4) {
        "raid1c4"
    } else if flags.contains(F::RAID1C3) {
        "raid1c3"
    } else if flags.contains(F::RAID1) {
        "raid1"
    } else if flags.contains(F::DUP) {
        "dup"
    } else if flags.contains(F::RAID0) {
        "raid0"
    } else if flags.contains(F::RAID5) {
        "raid5"
    } else if flags.contains(F::RAID6) {
        "raid6"
    } else {
        "single"
    };

    format!("{type_str}/{profile_str}")
}

/// Increment the counter for `devid` in the vec, returning the new value
/// (1-based).
fn get_or_insert_count(counts: &mut Vec<(u64, u64)>, devid: u64) -> u64 {
    if let Some(entry) = counts.iter_mut().find(|(d, _)| *d == devid) {
        entry.1 += 1;
        entry.1
    } else {
        counts.push((devid, 1));
        1
    }
}

/// Compute the display width for a column: the max of the header width and
/// the widths of all data values.
fn col_w(header: &str, values: impl Iterator<Item = usize>) -> usize {
    values.fold(header.len(), std::cmp::Ord::max)
}

/// Number of decimal digits in `n` (minimum 1).
fn digits(n: u64) -> usize {
    if n == 0 { 1 } else { n.ilog10() as usize + 1 }
}

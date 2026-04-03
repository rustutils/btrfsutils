//! Implementation of `btrfs filesystem du`.
//!
//! Uses [`btrfs_uapi::fiemap::file_extents`] to query the physical extent
//! layout of each file.  For each file we report:
//!
//! * **Total** — sum of all non-skipped extent lengths
//! * **Exclusive** — bytes not marked `FIEMAP_EXTENT_SHARED`
//! * **Set shared** — at the top-level only: physical bytes shared with at
//!   least one other file in the same argument's subtree, computed by
//!   collecting all shared physical ranges, sorting, and merging overlaps.
//!   Always `-` for non-top-level lines.
//!
//! The output format and semantics match `btrfs-progs filesystem du`.

use super::UnitMode;
use crate::{
    RunContext, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_uapi::fiemap::file_extents;
use clap::Parser;
use std::{
    collections::HashSet,
    fs::{self, File},
    os::unix::{fs::MetadataExt, io::AsFd},
    path::{Path, PathBuf},
};

/// Summarize disk usage of each file, showing shared extents
///
/// For each path, prints three columns:
///
///   Total      — logical bytes used by non-inline extents
///   Exclusive  — bytes not shared with any other file
///   Set shared — (top-level only) physical bytes shared within this subtree
#[derive(Parser, Debug)]
pub struct FilesystemDuCommand {
    /// Display only a total for each argument, not per-file lines
    #[clap(long, short)]
    pub summarize: bool,

    #[clap(flatten)]
    pub units: UnitMode,

    /// One or more files or directories to summarize
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for FilesystemDuCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        println!(
            "{:>10}  {:>10}  {:>10}  Filename",
            "Total", "Exclusive", "Set shared"
        );

        for path in &self.paths {
            process_top_level(path, self.summarize, &mode).with_context(
                || format!("cannot check space of '{}'", path.display()),
            )?;
        }
        Ok(())
    }
}

fn process_top_level(
    path: &Path,
    summarize: bool,
    mode: &SizeFormat,
) -> Result<()> {
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    // Physical (start, end_exclusive) ranges of all shared extents in this subtree.
    let mut shared_ranges: Vec<(u64, u64)> = Vec::new();

    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("cannot stat '{}'", path.display()))?;

    let root_dev = meta.dev();

    let (total, file_shared) = if meta.is_file() {
        let file = File::open(path)
            .with_context(|| format!("cannot open '{}'", path.display()))?;
        let info = file_extents(file.as_fd()).map_err(|e| {
            anyhow::anyhow!("fiemap failed on '{}': {e}", path.display())
        })?;
        shared_ranges.extend_from_slice(&info.shared_extents);
        (info.total_bytes, info.shared_bytes)
    } else if meta.is_dir() {
        walk_dir(
            path,
            root_dev,
            &mut seen,
            &mut shared_ranges,
            summarize,
            mode,
        )?
    } else {
        (0, 0)
    };

    let set_shared = compute_set_shared(&mut shared_ranges);
    let exclusive = total.saturating_sub(file_shared);

    println!(
        "{:>10}  {:>10}  {:>10}  {}",
        fmt_size(total, mode),
        fmt_size(exclusive, mode),
        fmt_size(set_shared, mode),
        path.display()
    );

    Ok(())
}

/// Walk `dir` recursively, printing one line per entry (unless `summarize`),
/// and return `(total_bytes, shared_bytes)` for the whole subtree.
///
/// `root_dev` is the device number of the top-level path.  Subdirectories on
/// a different device (i.e. other mounted filesystems) are silently skipped so
/// that the walk stays within a single filesystem, matching the behaviour of
/// the C reference implementation.
fn walk_dir(
    dir: &Path,
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
    shared_ranges: &mut Vec<(u64, u64)>,
    summarize: bool,
    mode: &SizeFormat,
) -> Result<(u64, u64)> {
    let mut dir_total: u64 = 0;
    let mut dir_shared: u64 = 0;

    let entries = fs::read_dir(dir).with_context(|| {
        format!("cannot read directory '{}'", dir.display())
    })?;

    for entry in entries {
        let entry = entry.with_context(|| {
            format!("error reading entry in '{}'", dir.display())
        })?;
        let entry_path = entry.path();

        let meta = match fs::symlink_metadata(&entry_path) {
            Ok(m) => m,
            Err(e) => {
                eprintln!(
                    "warning: cannot stat '{}': {e}",
                    entry_path.display()
                );
                continue;
            }
        };

        if !meta.is_file() && !meta.is_dir() {
            continue;
        }

        // Don't cross mount boundaries.
        if meta.dev() != root_dev {
            continue;
        }

        let key = (meta.dev(), meta.ino());
        if !seen.insert(key) {
            continue; // hard-linked inode already counted
        }

        if meta.is_file() {
            let file = match File::open(&entry_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!(
                        "warning: cannot open '{}': {e}",
                        entry_path.display()
                    );
                    continue;
                }
            };

            let info = match file_extents(file.as_fd()) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "warning: fiemap failed on '{}': {e}",
                        entry_path.display()
                    );
                    continue;
                }
            };

            if !summarize {
                let excl = info.total_bytes.saturating_sub(info.shared_bytes);
                println!(
                    "{:>10}  {:>10}  {:>10}  {}",
                    fmt_size(info.total_bytes, mode),
                    fmt_size(excl, mode),
                    "-",
                    entry_path.display()
                );
            }

            shared_ranges.extend_from_slice(&info.shared_extents);
            dir_total += info.total_bytes;
            dir_shared += info.shared_bytes;
        } else {
            let (sub_total, sub_shared) = walk_dir(
                &entry_path,
                root_dev,
                seen,
                shared_ranges,
                summarize,
                mode,
            )?;

            if !summarize {
                let excl = sub_total.saturating_sub(sub_shared);
                println!(
                    "{:>10}  {:>10}  {:>10}  {}",
                    fmt_size(sub_total, mode),
                    fmt_size(excl, mode),
                    "-",
                    entry_path.display()
                );
            }

            dir_total += sub_total;
            dir_shared += sub_shared;
        }
    }

    Ok((dir_total, dir_shared))
}

/// Merge `ranges` in place and return the total bytes covered by the union.
///
/// This gives the "set shared" value: physical bytes referenced by at least
/// one `FIEMAP_EXTENT_SHARED` extent anywhere in the subtree.
fn compute_set_shared(ranges: &mut [(u64, u64)]) -> u64 {
    if ranges.is_empty() {
        return 0;
    }
    ranges.sort_unstable();

    let mut total = 0u64;
    let (mut cur_start, mut cur_end) = ranges[0];

    for &(start, end) in &ranges[1..] {
        if start <= cur_end {
            if end > cur_end {
                cur_end = end;
            }
        } else {
            total += cur_end - cur_start;
            cur_start = start;
            cur_end = end;
        }
    }
    total += cur_end - cur_start;
    total
}

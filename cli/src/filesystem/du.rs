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
    Format, RunContext, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_uapi::fiemap::file_extents;
use clap::Parser;
use cols::Cols;
use serde::Serialize;
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
    #[clap(long, short, conflicts_with = "depth")]
    pub summarize: bool,

    /// Maximum depth of entries to display.
    ///
    /// 0 is equivalent to --summarize, 1 shows only immediate children, etc.
    /// All levels are still computed for totals.
    #[clap(long, short = 'd', conflicts_with = "summarize")]
    pub depth: Option<usize>,

    /// Sort entries within each directory (modern output only). [default: path]
    ///
    /// Size keys sort largest first, path sorts alphabetically.
    #[clap(long, value_enum)]
    pub sort: Option<DuSort>,

    #[clap(flatten)]
    pub units: UnitMode,

    /// One or more files or directories to summarize
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

/// Sort order for entries within each directory.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DuSort {
    /// Alphabetical by path (ascending)
    #[default]
    Path,
    /// By total size (largest first)
    Total,
    /// By exclusive size (largest first)
    Exclusive,
    /// By shared size (largest first)
    Shared,
}

/// Collected size info for a single file or directory, used for sorting.
struct DuEntry {
    path: PathBuf,
    total: u64,
    shared: u64,
    /// Shared extent physical ranges (files only).
    shared_extents: Vec<(u64, u64)>,
    /// Children rows for modern tree output (directories only).
    tree_children: Vec<DuRow>,
}

impl DuEntry {
    fn exclusive(&self) -> u64 {
        self.total.saturating_sub(self.shared)
    }
}

fn sort_entries(entries: &mut [DuEntry], sort: DuSort) {
    entries.sort_by(|a, b| match sort {
        DuSort::Path => a.path.cmp(&b.path),
        DuSort::Total => b.total.cmp(&a.total),
        DuSort::Exclusive => b.exclusive().cmp(&a.exclusive()),
        DuSort::Shared => b.shared.cmp(&a.shared),
    });
}

impl Runnable for FilesystemDuCommand {
    fn supported_formats(&self) -> &[Format] {
        &[Format::Text, Format::Modern, Format::Json]
    }

    fn run(&self, ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        let max_depth = if self.summarize { Some(0) } else { self.depth };

        if self.sort.is_some() && ctx.format != Format::Modern {
            anyhow::bail!("--sort is only supported with --format modern");
        }
        let sort = self.sort.unwrap_or(DuSort::Path);

        match ctx.format {
            Format::Modern => {
                let mut trees: Vec<DuRow> = Vec::new();
                for path in &self.paths {
                    let row = collect_tree(path, max_depth, sort, &mode)
                        .with_context(|| {
                            format!(
                                "cannot check space of '{}'",
                                path.display()
                            )
                        })?;
                    trees.push(row);
                }
                let mut out = std::io::stdout().lock();
                let _ = DuRow::print_table(&trees, &mut out);
            }
            Format::Text => {
                println!(
                    "{:>10}  {:>10}  {:>10}  Filename",
                    "Total", "Exclusive", "Set shared"
                );
                for path in &self.paths {
                    print_top_level(path, max_depth, &mode).with_context(
                        || {
                            format!(
                                "cannot check space of '{}'",
                                path.display()
                            )
                        },
                    )?;
                }
            }
            Format::Json => {
                let mut all: Vec<DuEntryJson> = Vec::new();
                for path in &self.paths {
                    collect_json(path, max_depth, &mut all)?;
                }
                crate::util::print_json("filesystem-du", &all)?;
            }
        }

        Ok(())
    }
}

// ── Modern output (cols tree) ──────────────────────────────────────────

#[derive(Clone, Cols)]
struct DuRow {
    #[column(header = "TOTAL", right)]
    total: String,
    #[column(header = "EXCL", right)]
    exclusive: String,
    #[column(header = "SET SHARED", right)]
    set_shared: String,
    #[column(tree, wrap)]
    path: String,
    #[column(children)]
    children: Vec<Self>,
}

/// Build a `DuRow` tree for a top-level path.
fn collect_tree(
    path: &Path,
    max_depth: Option<usize>,
    sort: DuSort,
    mode: &SizeFormat,
) -> Result<DuRow> {
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let mut shared_ranges: Vec<(u64, u64)> = Vec::new();

    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("cannot stat '{}'", path.display()))?;

    let root_dev = meta.dev();

    let (total, file_shared, children) = if meta.is_file() {
        // Skip open() + FIEMAP for zero-length files (no extents).
        if meta.len() == 0 {
            (0, 0, Vec::new())
        } else {
            let file = File::open(path)
                .with_context(|| format!("cannot open '{}'", path.display()))?;
            let info = file_extents(file.as_fd()).map_err(|e| {
                anyhow::anyhow!("fiemap failed on '{}': {e}", path.display())
            })?;
            shared_ranges.extend_from_slice(&info.shared_extents);
            (info.total_bytes, info.shared_bytes, Vec::new())
        }
    } else if meta.is_dir() {
        collect_dir_tree(
            path,
            root_dev,
            &mut seen,
            &mut shared_ranges,
            max_depth,
            0,
            sort,
            mode,
        )?
    } else {
        (0, 0, Vec::new())
    };

    let set_shared = compute_set_shared(&mut shared_ranges);
    let exclusive = total.saturating_sub(file_shared);

    Ok(DuRow {
        total: fmt_size(total, mode),
        exclusive: fmt_size(exclusive, mode),
        set_shared: fmt_size(set_shared, mode),
        path: path.display().to_string(),
        children,
    })
}

/// Recursively collect a directory into `DuRow` children.
///
/// Returns `(total_bytes, shared_bytes, child_rows)`. Children are only
/// included in the output when `depth < max_depth` (or `max_depth` is
/// `None` for unlimited). The walk always descends fully for correct totals.
#[allow(clippy::too_many_arguments)]
fn collect_dir_tree(
    dir: &Path,
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
    shared_ranges: &mut Vec<(u64, u64)>,
    max_depth: Option<usize>,
    depth: usize,
    sort: DuSort,
    mode: &SizeFormat,
) -> Result<(u64, u64, Vec<DuRow>)> {
    let mut dir_total: u64 = 0;
    let mut dir_shared: u64 = 0;
    let show = max_depth.is_none_or(|m| depth < m);

    let raw_items = collect_dir_entries(dir, root_dev, seen)?;
    let mut entries = collect_entry_sizes(
        &raw_items,
        root_dev,
        seen,
        shared_ranges,
        max_depth,
        depth,
        sort,
        mode,
    )?;

    sort_entries(&mut entries, sort);

    let mut children: Vec<DuRow> = Vec::new();
    for e in &entries {
        shared_ranges.extend_from_slice(&e.shared_extents);
        dir_total += e.total;
        dir_shared += e.shared;

        if show {
            let excl = e.exclusive();
            children.push(DuRow {
                total: fmt_size(e.total, mode),
                exclusive: fmt_size(excl, mode),
                set_shared: "-".to_string(),
                path: path_name(&e.path),
                children: e.tree_children.clone(),
            });
        }
    }

    Ok((dir_total, dir_shared, children))
}

/// Stat and FIEMAP each entry, returning `DuEntry` items with raw sizes.
#[allow(clippy::too_many_arguments)]
fn collect_entry_sizes(
    items: &[(PathBuf, fs::Metadata)],
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
    shared_ranges: &mut Vec<(u64, u64)>,
    max_depth: Option<usize>,
    depth: usize,
    sort: DuSort,
    mode: &SizeFormat,
) -> Result<Vec<DuEntry>> {
    let mut entries = Vec::new();

    for (entry_path, meta) in items {
        if meta.is_file() {
            // Zero-length files have no extents, so skip the open() +
            // FIEMAP syscalls. This avoids two syscalls per empty file,
            // which adds up on large trees with many small/empty files.
            if meta.len() == 0 {
                entries.push(DuEntry {
                    path: entry_path.clone(),

                    total: 0,
                    shared: 0,
                    shared_extents: Vec::new(),
                    tree_children: Vec::new(),
                });
                continue;
            }

            let file = match File::open(entry_path) {
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

            entries.push(DuEntry {
                path: entry_path.clone(),
                total: info.total_bytes,
                shared: info.shared_bytes,
                shared_extents: info.shared_extents,
                tree_children: Vec::new(),
            });
        } else {
            let (sub_total, sub_shared, sub_children) = collect_dir_tree(
                entry_path,
                root_dev,
                seen,
                shared_ranges,
                max_depth,
                depth + 1,
                sort,
                mode,
            )?;

            entries.push(DuEntry {
                path: entry_path.clone(),
                total: sub_total,
                shared: sub_shared,
                shared_extents: Vec::new(),
                tree_children: sub_children,
            });
        }
    }

    Ok(entries)
}

// ── Legacy text output ─────────────────────────────────────────────────

fn print_top_level(
    path: &Path,
    max_depth: Option<usize>,
    mode: &SizeFormat,
) -> Result<()> {
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let mut shared_ranges: Vec<(u64, u64)> = Vec::new();

    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("cannot stat '{}'", path.display()))?;

    let root_dev = meta.dev();

    let (total, file_shared) = if meta.is_file() {
        // Skip open() + FIEMAP for zero-length files (no extents).
        if meta.len() == 0 {
            (0, 0)
        } else {
            let file = File::open(path)
                .with_context(|| format!("cannot open '{}'", path.display()))?;
            let info = file_extents(file.as_fd()).map_err(|e| {
                anyhow::anyhow!("fiemap failed on '{}': {e}", path.display())
            })?;
            shared_ranges.extend_from_slice(&info.shared_extents);
            (info.total_bytes, info.shared_bytes)
        }
    } else if meta.is_dir() {
        print_walk_dir(
            path,
            root_dev,
            &mut seen,
            &mut shared_ranges,
            max_depth,
            0,
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

#[allow(clippy::too_many_lines)]
fn print_walk_dir(
    dir: &Path,
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
    shared_ranges: &mut Vec<(u64, u64)>,
    max_depth: Option<usize>,
    depth: usize,
    mode: &SizeFormat,
) -> Result<(u64, u64)> {
    let mut dir_total: u64 = 0;
    let mut dir_shared: u64 = 0;
    let show = max_depth.is_none_or(|m| depth < m);

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

        if meta.dev() != root_dev {
            continue;
        }

        let key = (meta.dev(), meta.ino());
        if !seen.insert(key) {
            continue;
        }

        if meta.is_file() {
            // Zero-length files have no extents, so skip the open() +
            // FIEMAP syscalls. This avoids two syscalls per empty file,
            // which adds up on large trees with many small/empty files.
            if meta.len() == 0 {
                if show {
                    println!(
                        "{:>10}  {:>10}  {:>10}  {}",
                        fmt_size(0, mode),
                        fmt_size(0, mode),
                        "-",
                        entry_path.display()
                    );
                }
                continue;
            }

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

            if show {
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
            let (sub_total, sub_shared) = print_walk_dir(
                &entry_path,
                root_dev,
                seen,
                shared_ranges,
                max_depth,
                depth + 1,
                mode,
            )?;

            if show {
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

// ── Shared helpers ─────────────────────────────────────────────────────

/// Collect and filter directory entries, returning sorted (path, metadata) pairs.
fn collect_dir_entries(
    dir: &Path,
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
) -> Result<Vec<(PathBuf, fs::Metadata)>> {
    let mut items = Vec::new();
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

        if meta.dev() != root_dev {
            continue;
        }

        let key = (meta.dev(), meta.ino());
        if !seen.insert(key) {
            continue;
        }

        items.push((entry_path, meta));
    }

    items.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(items)
}

/// Extract the file/directory name from a path (last component).
fn path_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}

// ── JSON output ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct DuEntryJson {
    path: String,
    total: u64,
    exclusive: u64,
    /// `None` for non-top-level entries (rendered as JSON `null`).
    set_shared: Option<u64>,
}

/// Collect JSON entries for a top-level path.
fn collect_json(
    path: &Path,
    max_depth: Option<usize>,
    out: &mut Vec<DuEntryJson>,
) -> Result<()> {
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let mut shared_ranges: Vec<(u64, u64)> = Vec::new();

    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("cannot stat '{}'", path.display()))?;

    let root_dev = meta.dev();

    let (total, file_shared) = if meta.is_file() {
        if meta.len() == 0 {
            (0, 0)
        } else {
            let file = File::open(path)
                .with_context(|| format!("cannot open '{}'", path.display()))?;
            let info = file_extents(file.as_fd()).map_err(|e| {
                anyhow::anyhow!("fiemap failed on '{}': {e}", path.display())
            })?;
            shared_ranges.extend_from_slice(&info.shared_extents);
            (info.total_bytes, info.shared_bytes)
        }
    } else if meta.is_dir() {
        collect_json_walk(
            path,
            root_dev,
            &mut seen,
            &mut shared_ranges,
            max_depth,
            0,
            out,
        )?
    } else {
        (0, 0)
    };

    let set_shared = compute_set_shared(&mut shared_ranges);
    let exclusive = total.saturating_sub(file_shared);

    out.push(DuEntryJson {
        path: path.display().to_string(),
        total,
        exclusive,
        set_shared: Some(set_shared),
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_json_walk(
    dir: &Path,
    root_dev: u64,
    seen: &mut HashSet<(u64, u64)>,
    shared_ranges: &mut Vec<(u64, u64)>,
    max_depth: Option<usize>,
    depth: usize,
    out: &mut Vec<DuEntryJson>,
) -> Result<(u64, u64)> {
    let mut dir_total: u64 = 0;
    let mut dir_shared: u64 = 0;
    let show = max_depth.is_none_or(|m| depth < m);

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

        if meta.dev() != root_dev {
            continue;
        }

        let key = (meta.dev(), meta.ino());
        if !seen.insert(key) {
            continue;
        }

        if meta.is_file() {
            // Zero-length files have no extents, so skip the open() +
            // FIEMAP syscalls. This avoids two syscalls per empty file,
            // which adds up on large trees with many small/empty files.
            if meta.len() == 0 {
                if show {
                    out.push(DuEntryJson {
                        path: entry_path.display().to_string(),
                        total: 0,
                        exclusive: 0,
                        set_shared: None,
                    });
                }
                continue;
            }

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

            if show {
                let excl = info.total_bytes.saturating_sub(info.shared_bytes);
                out.push(DuEntryJson {
                    path: entry_path.display().to_string(),
                    total: info.total_bytes,
                    exclusive: excl,
                    set_shared: None,
                });
            }

            shared_ranges.extend_from_slice(&info.shared_extents);
            dir_total += info.total_bytes;
            dir_shared += info.shared_bytes;
        } else {
            let (sub_total, sub_shared) = collect_json_walk(
                &entry_path,
                root_dev,
                seen,
                shared_ranges,
                max_depth,
                depth + 1,
                out,
            )?;

            if show {
                let excl = sub_total.saturating_sub(sub_shared);
                out.push(DuEntryJson {
                    path: entry_path.display().to_string(),
                    total: sub_total,
                    exclusive: excl,
                    set_shared: None,
                });
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

use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Try to restore files from a damaged filesystem (unmounted).
///
/// Attempt to recover files from a damaged or inaccessible btrfs filesystem
/// by scanning the raw filesystem structures. This command works on unmounted
/// devices and can recover files even when the filesystem cannot be mounted
/// normally. Recovery options allow selective restoration of files, metadata,
/// and extended attributes. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct RestoreCommand {
    /// Block device containing the damaged filesystem
    device: PathBuf,

    /// Destination path for recovered files (not needed with --list-roots)
    path: Option<PathBuf>,

    /// Dry run (only list files that would be recovered)
    #[clap(short = 'D', long = "dry-run")]
    dry_run: bool,

    /// Ignore errors
    #[clap(short = 'i', long)]
    ignore_errors: bool,

    /// Overwrite existing files
    #[clap(short = 'o', long)]
    overwrite: bool,

    /// Restore owner, mode and times
    #[clap(short = 'm', long)]
    metadata: bool,

    /// Restore symbolic links
    #[clap(short = 'S', long)]
    symlink: bool,

    /// Get snapshots
    #[clap(short = 's', long)]
    snapshots: bool,

    /// Restore extended attributes
    #[clap(short = 'x', long)]
    xattr: bool,

    /// Restore only filenames matching regex
    #[clap(long)]
    path_regex: Option<String>,

    /// Ignore case (used with --path-regex)
    #[clap(short = 'c')]
    ignore_case: bool,

    /// Find dir
    #[clap(short = 'd')]
    find_dir: bool,

    /// List tree roots
    #[clap(short = 'l', long)]
    list_roots: bool,

    /// Filesystem location (bytenr)
    #[clap(short = 'f')]
    fs_location: Option<u64>,

    /// Root objectid
    #[clap(short = 'r', long)]
    root: Option<u64>,

    /// Tree location (bytenr)
    #[clap(short = 't')]
    tree_location: Option<u64>,

    /// Super mirror index
    #[clap(short = 'u', long = "super")]
    super_mirror: Option<u64>,
}

impl Runnable for RestoreCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement restore")
    }
}

//! `btrfs-fuse` — userspace FUSE driver for btrfs, read-only v1.
//!
//! Thin CLI front-end for the `btrfs_fuse` library: parse arguments,
//! optionally resolve a `--subvol PATH` to a subvolume id, open the
//! backing image, and hand the resulting [`btrfs_fuse::BtrfsFuse`] to
//! `fuser::mount2`.

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use anyhow::{Context, Result, anyhow};
use btrfs_fs::{Filesystem, SubvolId, SubvolInfo};
use btrfs_fuse::BtrfsFuse;
use clap::Parser;
use fuser::{Config, MountOption, SessionACL};
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};

/// Mount a btrfs image or block device read-only via FUSE.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Path to the btrfs image file or block device.
    image: PathBuf,
    /// Mount point.
    mountpoint: PathBuf,
    /// Run in the foreground (do not daemonize).
    #[arg(short = 'f', long)]
    foreground: bool,
    /// Allow other users to access the mount.
    #[arg(long)]
    allow_other: bool,
    /// Mount the named subvolume as the root of the FUSE filesystem.
    /// The path is interpreted relative to the filesystem root, e.g.
    /// `--subvol home/snapshots/2025-01-01`. Mutually exclusive with
    /// `--subvolid`.
    #[arg(long)]
    subvol: Option<String>,
    /// Mount the subvolume with this tree id. Use the value reported
    /// by `btrfs subvolume list`. Mutually exclusive with `--subvol`.
    #[arg(long, conflicts_with = "subvol")]
    subvolid: Option<u64>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let target_subvol = if let Some(id) = args.subvolid {
        Some(SubvolId(id))
    } else if let Some(path) = args.subvol.as_deref() {
        Some(resolve_subvol_path(&args.image, path)?)
    } else {
        None
    };

    let file = File::open(&args.image)
        .with_context(|| format!("opening {}", args.image.display()))?;
    let fs = match target_subvol {
        Some(id) => BtrfsFuse::open_subvol(file, id)
            .with_context(|| format!("opening subvolume {}", id.0))?,
        None => {
            BtrfsFuse::open(file).context("bootstrapping btrfs filesystem")?
        }
    };

    // `Config` is `#[non_exhaustive]`, so we can't use a struct literal even
    // with `..default()` from outside the crate; mutate fields instead.
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("btrfs-fuse".to_string()),
        MountOption::Subtype("btrfs".to_string()),
    ];
    config.acl = if args.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };

    // TODO: respect `--foreground=false` once we add a daemonize path.
    let _ = args.foreground;
    fuser::mount(fs, &args.mountpoint, &config).with_context(|| {
        format!("mounting at {}", args.mountpoint.display())
    })?;
    Ok(())
}

/// Resolve a slash-separated subvolume path (relative to the FS root)
/// to its [`SubvolId`].
///
/// Opens the filesystem in a temporary capacity, calls
/// [`Filesystem::list_subvolumes`], and matches the requested path
/// against the full path each subvolume reaches by walking its
/// parent chain. Empty path / `"/"` resolves to the default
/// `FS_TREE`.
fn resolve_subvol_path(image: &Path, path: &str) -> Result<SubvolId> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(SubvolId(5));
    }

    let file = File::open(image)
        .with_context(|| format!("opening {}", image.display()))?;
    let fs = Filesystem::open(file)
        .context("bootstrapping btrfs filesystem to resolve --subvol")?;

    // `list_subvolumes` is async; spin up a single-threaded runtime
    // just for this lookup and tear it down afterwards.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("creating temporary tokio runtime")?;
    let subvols = runtime
        .block_on(fs.list_subvolumes())
        .context("listing subvolumes")?;

    let by_id: HashMap<SubvolId, &SubvolInfo> =
        subvols.iter().map(|s| (s.id, s)).collect();
    let target = trimmed.as_bytes();
    for s in &subvols {
        if full_path_of(&by_id, s.id) == target {
            return Ok(s.id);
        }
    }
    Err(anyhow!(
        "subvolume path {path:?} not found on {}",
        image.display()
    ))
}

/// Build the full path (slash-separated, no leading slash) for the
/// subvolume `id` by walking its parent chain. The default `FS_TREE`
/// has an empty path; user subvolumes accumulate `name` components
/// from each ancestor.
fn full_path_of(
    by_id: &HashMap<SubvolId, &SubvolInfo>,
    id: SubvolId,
) -> Vec<u8> {
    let mut components: Vec<Vec<u8>> = Vec::new();
    let mut current = id;
    while let Some(info) = by_id.get(&current) {
        if !info.name.is_empty() {
            components.push(info.name.clone());
        }
        match info.parent {
            Some(parent) => current = parent,
            None => break,
        }
    }
    components.reverse();
    let mut out: Vec<u8> = Vec::new();
    for (i, c) in components.iter().enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(c);
    }
    out
}

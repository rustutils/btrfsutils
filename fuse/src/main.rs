//! `btrfs-fuse` — userspace FUSE driver for btrfs, read-only v1.
//!
//! Thin CLI front-end for the `btrfs_fuse` library: parse arguments,
//! open the backing image, and hand the resulting [`btrfs_fuse::BtrfsFuse`]
//! to `fuser::mount2`.

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use anyhow::{Context, Result};
use btrfs_fuse::BtrfsFuse;
use clap::Parser;
use fuser::{Config, MountOption, SessionACL};
use std::path::PathBuf;

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
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let file = std::fs::File::open(&args.image)
        .with_context(|| format!("opening {}", args.image.display()))?;
    let fs = BtrfsFuse::open(file).context("bootstrapping btrfs filesystem")?;

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
    fuser::mount2(fs, &args.mountpoint, &config).with_context(|| {
        format!("mounting at {}", args.mountpoint.display())
    })?;
    Ok(())
}

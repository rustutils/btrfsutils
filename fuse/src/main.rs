//! `btrfs-fuse`: read-only userspace FUSE driver for btrfs.
//!
//! Thin shim around [`btrfs_fuse::run::run_mount`] that parses the
//! arguments and hands them off. The same entry point is reachable as
//! `btrfs fuse <IMG> <MOUNTPOINT> ...` via the `fuse` feature on
//! `btrfs-cli`.

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use anyhow::Result;
use btrfs_fuse::{args::MountArgs, run::run_mount};
use clap::Parser;

fn main() -> Result<()> {
    env_logger::init();
    let args = MountArgs::parse();
    run_mount(&args)
}

//! Argument types for the `btrfs-fuse` mount entry point.
//!
//! Same split as `mkfs` and `tune`: the clap-derived [`MountArgs`]
//! struct lives in the library, so the standalone `btrfs-fuse` binary,
//! the embedded `btrfs fuse` subcommand in `btrfs-cli`, and any other
//! front-end share one definition of the flags. The mount logic lives
//! in [`crate::run::run_mount`].

use clap::Parser;
use std::path::PathBuf;

/// Mount a btrfs image or block device read-only via FUSE.
#[derive(Parser, Debug, Clone)]
#[command(version, arg_required_else_help = true, max_term_width = 100)]
pub struct MountArgs {
    /// Path to the btrfs image file or block device.
    pub image: PathBuf,
    /// Mount point.
    pub mountpoint: PathBuf,
    /// Run in the foreground (do not daemonize).
    #[arg(short = 'f', long)]
    pub foreground: bool,
    /// Allow other users to access the mount.
    #[arg(long)]
    pub allow_other: bool,
    /// Mount the named subvolume as the root of the FUSE filesystem.
    ///
    /// The path is relative to the filesystem root, for example
    /// --subvol home/snapshots/2025-01-01. Mutually exclusive with
    /// --subvolid.
    #[arg(long)]
    pub subvol: Option<String>,
    /// Mount the subvolume with this tree id.
    ///
    /// Use the value reported by btrfs subvolume list. Mutually
    /// exclusive with --subvol.
    #[arg(long, conflicts_with = "subvol")]
    pub subvolid: Option<u64>,
    /// Don't have the kernel check file mode and ownership against the
    /// calling user.
    ///
    /// By default the FUSE mount uses default_permissions, so a
    /// non-root user can't read root-owned files in the image, as with
    /// kernel btrfs. Pass this flag to skip that check, for example
    /// when inspecting an image whose stored ownership doesn't match
    /// your local UIDs.
    #[arg(long)]
    pub no_default_permissions: bool,
    /// Number of tree blocks to cache (~16 KiB each).
    ///
    /// Set to 1 to effectively disable the cache. Larger values trade RAM
    /// for fewer disk reads on tree walks.
    #[arg(long, default_value_t = 4096)]
    pub cache_tree_blocks: usize,
    /// Number of parsed inode items to cache.
    #[arg(long, default_value_t = 4096)]
    pub cache_inodes: usize,
    /// Number of per-inode extent maps to cache.
    ///
    /// Each map holds one entry per EXTENT_DATA item in the file.
    #[arg(long, default_value_t = 1024)]
    pub cache_extent_maps: usize,
}

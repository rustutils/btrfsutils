//! # Offline superblock tuning for btrfs filesystems
//!
//! This crate provides the implementation behind the `btrfs-tune` binary,
//! which modifies btrfs filesystem parameters by writing directly to the
//! on-disk superblock (and, for full UUID rewrites, to every tree block).
//! The filesystem must be unmounted.
//!
//! Supported operations:
//!
//! - **Legacy feature flags** (`-r`, `-x`, `-n`): enable `extref`,
//!   `skinny-metadata`, or `no-holes`. These features are now defaults on
//!   new filesystems but may be absent on older ones.
//! - **Seeding** (`-S 0`/`-S 1`): mark or unmark a filesystem as a seed
//!   device for sprouted filesystems.
//! - **Metadata UUID** (`-m`, `-M UUID`): change the user-visible fsid via
//!   the lightweight `metadata_uuid` mechanism (superblock-only, no tree walk).
//! - **Full fsid rewrite** (`-u`, `-U UUID`): rewrite the fsid in every
//!   tree block header and device item on disk, with crash-safety via
//!   `BTRFS_SUPER_FLAG_CHANGING_FSID`.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

/// Argument parsing for the `btrfs-tune` binary.
pub mod args;
/// Entry point logic, callable from both standalone binary and CLI subcommand.
pub mod run;
/// Core tuning operations: feature flags, seeding, UUID changes.
pub mod tune;

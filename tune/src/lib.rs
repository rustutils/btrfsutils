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
//!
//! # Stability
//!
//! This is a pre-1.0 release. The conversion operations
//! (`--convert-to-free-space-tree`, `--convert-to-block-group-tree`)
//! are experimental: they go through the new `btrfs-transaction`
//! crate, which is a clean-room reimplementation and may have
//! edge cases that testing doesn't cover. Take a backup before
//! running them on filesystems you care about. The other
//! operations (feature flags, seeding, UUID changes) are stable.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Test code uses literal byte buffers and small cast conversions that
// pedantic clippy flags but that are intentional in unit tests.
#![cfg_attr(
    test,
    allow(
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::identity_op,
        clippy::match_wildcard_for_single_variants,
        clippy::semicolon_if_nothing_returned,
        clippy::unreadable_literal,
    )
)]

/// Argument parsing for the `btrfs-tune` binary.
pub mod args;
/// Entry point logic, callable from both standalone binary and CLI subcommand.
pub mod run;
/// Core tuning operations: feature flags, seeding, UUID changes.
pub mod tune;

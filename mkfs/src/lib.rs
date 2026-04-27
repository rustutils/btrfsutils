//! # Filesystem creation for btrfs
//!
//! Creates a new btrfs filesystem on one or more block devices or image
//! files. Does not call any ioctls; uses raw `pwrite` for the bootstrap
//! and the `btrfs-transaction` crate's commit pipeline for everything
//! else.
//!
//! ## How it works
//!
//! The entry point is [`mkfs::make_btrfs`] (or
//! [`mkfs::make_btrfs_with_rootdir`] for `--rootdir`), which takes a
//! [`mkfs::MkfsConfig`] describing the desired layout (devices,
//! profiles, features, checksum algorithm).
//!
//! Creation runs in three phases:
//!
//! 1. **Bootstrap** — write the four always-present trees (Root,
//!    Extent, Chunk, Dev) plus the superblock to disk via raw
//!    `pwrite`. Uses [`tree::LeafBuilder`] / [`treebuilder::TreeBuilder`]
//!    to construct in-memory leaves, [`layout::ChunkLayout`] to
//!    decide chunk geometry, and [`layout::BlockLayout`] to assign
//!    static addresses for the bootstrap blocks. The result is the
//!    minimum on-disk state that the transaction crate's
//!    `Filesystem::open` will accept.
//!
//! 2. **Post-bootstrap** ([`post_bootstrap`]) — reopen the bootstrap
//!    image with [`btrfs_transaction::Filesystem`], start a
//!    transaction, and create the always-present empty trees the
//!    bootstrap omits: block-group tree (when enabled), FS tree
//!    (with inode 256 + ".." `INODE_REF` + `ROOT_ITEM` patches),
//!    csum tree, data-reloc tree, quota tree (when enabled),
//!    free-space tree (when enabled), UUID tree. Commit + sync.
//!
//! 3. **Rootdir population** ([`rootdir::walk_to_transaction`]) —
//!    only for `--rootdir`. Reopen the freshly-built empty
//!    filesystem, start a transaction, walk the source directory
//!    depth-first, and emit `INODE_ITEM` / `DIR_ITEM` / `DIR_INDEX`
//!    / `INODE_REF` / `XATTR_ITEM` / inline-or-regular `EXTENT_DATA`
//!    records via the transaction crate's high-level helpers. Handles
//!    `--subvol`, `--reflink` (FICLONERANGE), `--shrink`, and
//!    `--inode-flags`. Commit + sync.
//!
//! Phase 1 is the only piece that still hand-builds tree blocks;
//! migrating it would require a `Filesystem::create` primitive in
//! the transaction crate. Phases 2 and 3 are the steady-state
//! shape: every modification goes through the same `Transaction`
//! pipeline as the rest of the codebase.
//!
//! ## Supported features
//!
//! - Single and multi-device
//! - All RAID profiles: SINGLE, DUP, RAID0, RAID1, RAID1C3, RAID1C4,
//!   RAID10, RAID5, RAID6 (for both metadata and data)
//! - All four checksum algorithms: CRC32C, xxhash64, SHA-256, BLAKE-2b
//! - Quota (`-O quota`) and simple quota (`-O squota`)
//! - Free-space-tree and block-group-tree feature flags
//! - Device validation (mounted check, existing FS detection, TRIM)
//! - `--rootdir` with subvolumes, reflink, shrink, inode flags, and
//!   zlib / zstd / LZO compression

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
        clippy::uninlined_format_args,
        clippy::unreadable_literal,
    )
)]

pub mod args;
pub mod items;
pub mod layout;
pub mod mkfs;
pub mod post_bootstrap;
pub mod rootdir;
pub mod run;
pub mod tree;
pub mod treebuilder;
pub mod write;

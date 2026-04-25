//! # Filesystem creation for btrfs
//!
//! Creates a new btrfs filesystem by constructing B-tree nodes as raw byte
//! buffers and writing them directly to block devices or image files with
//! `pwrite`. Does not use ioctls or require a mounted filesystem.
//!
//! ## How it works
//!
//! The entry point is [`mkfs::make_btrfs`], which takes a [`mkfs::MkfsConfig`]
//! describing the desired filesystem layout (devices, profiles, features,
//! checksum algorithm).
//!
//! The creation process:
//!
//! 1. **Compute layout** ([`layout::ChunkLayout`]): determine the physical
//!    placement of system, metadata, and data chunks across devices. Metadata
//!    uses DUP (single device) or RAID1/RAID1C3/RAID1C4 (multi-device); data
//!    uses SINGLE or RAID0.
//!
//! 2. **Build tree blocks** ([`tree::LeafBuilder`]): construct 8-9 leaf nodes
//!    (root, extent, chunk, dev, fs, csum, free-space, data-reloc, and
//!    optionally block-group tree). Each tree is populated with the items
//!    serialized by the functions in [`items`].
//!
//! 3. **Build superblock** ([`btrfs_disk::superblock::Superblock`]): construct
//!    the superblock with root pointers, device info, feature flags, and the
//!    `sys_chunk_array` bootstrap, then serialize via `to_bytes()`.
//!
//! 4. **Write to disk** ([`write::pwrite_all`]): write each tree block to its
//!    physical location(s) — DUP/RAID1 blocks are written to multiple stripes.
//!    Superblocks are written to all mirror offsets (64K, 64M, 256G) on all
//!    devices. Checksums ([`write::ChecksumType`]) are computed per-block.
//!
//! ## Supported features
//!
//! - Single and multi-device (up to N devices)
//! - Metadata profiles: SINGLE, DUP, RAID1, RAID1C3, RAID1C4
//! - Data profiles: SINGLE, DUP, RAID0, RAID1, RAID1C3, RAID1C4
//! - Checksum algorithms: CRC32C, xxhash64, SHA256, `BLAKE2b`
//! - Free-space-tree, block-group-tree feature flags
//! - Device validation (mounted check, existing FS detection, TRIM)

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

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

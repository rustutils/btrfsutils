//! High-level filesystem API on top of `btrfs-disk`.
//!
//! [`Filesystem`] exposes the operations a userspace driver needs
//! (`lookup`, `readdir`, `read`, `getattr`, xattrs, `seek_hole_data`,
//! and so on) plus the reads behind the btrfs ioctls (`tree_search`,
//! `ino_lookup`, subvolume listing) and offline `send`. Fallible ops
//! return plain `std::io::Result` values and nothing depends on a FUSE
//! crate, so the same API drives the [`btrfs-fuse`] mount and other
//! embedders (offline tools, tests, alternate FUSE bindings).
//!
//! # Inode model
//!
//! [`Inode`] is the pair `(subvol, ino)`. `lookup` follows subvolume
//! crossings, so one handle can traverse every subvolume below the
//! one it was opened on. Callers that need a flat `u64` (e.g. FUSE)
//! translate at the boundary.
//!
//! # Status
//!
//! Read-only. Write support is planned on top of the
//! `btrfs-transaction` crate.
//!
//! [`btrfs-fuse`]: https://docs.rs/btrfs-fuse

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

mod cache;
mod filesystem;
mod read;
mod xattr;

pub mod dir;
pub mod stat;

pub use btrfs_disk::{
    items::{DeviceItem, RootRef},
    superblock::Superblock,
};
pub use cache::{CacheConfig, CacheStats, LruTreeBlockCache};
pub use dir::{Entry, FileKind};
pub use filesystem::{
    Filesystem, Inode, SearchFilter, SearchItem, SeekHoleData, StatFs,
    SubvolId, SubvolInfo,
};
pub use stat::Stat;
pub use uuid::Uuid;

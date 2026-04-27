//! High-level filesystem API on top of `btrfs-disk`.
//!
//! [`Filesystem`] exposes the operations a userspace driver needs:
//! `lookup`, `readdir`, `read`, `readlink`, `getattr`, `xattr_get`,
//! `xattr_list`, and `statfs`. Each returns plain `std::io::Result`
//! values and does not depend on any FUSE protocol crate, so the same
//! API drives the [`btrfs-fuse`] mount and any other embedder
//! (offline tools, tests, alternate FUSE bindings).
//!
//! # Inode model
//!
//! [`Inode`] is the pair `(subvol, ino)`. For now only the default
//! subvolume is exposed, but multi-subvolume support is the next phase
//! and the API is shaped for it from the start. Callers that need a
//! flat `u64` (e.g. FUSE) translate at the boundary.
//!
//! # Status
//!
//! Read-only. Write support is planned via the `btrfs-transaction`
//! crate; see the project roadmap.
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

pub use btrfs_disk::{items::DeviceItem, superblock::Superblock};
pub use cache::{CacheStats, LruTreeBlockCache};
pub use dir::{Entry, FileKind};
pub use filesystem::{
    Filesystem, Inode, SearchFilter, SearchItem, StatFs, SubvolId, SubvolInfo,
};
pub use stat::Stat;
pub use uuid::Uuid;

//! `btrfs-fuse` library: a thin `fuser::Filesystem` adapter on top of
//! the [`btrfs_fs`] crate.
//!
//! All filesystem semantics (lookup, readdir, read, xattr, etc.) live in
//! [`btrfs_fs`]. This crate adds the FUSE protocol mapping: inode-number
//! translation, [`btrfs_fs::Stat`] → `fuser::FileAttr` conversion, and
//! the `fuser::Filesystem` trait impl. Embedders that don't need FUSE
//! should depend on [`btrfs_fs`] directly.

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod fs;
pub mod inode;
pub mod ioctl;

pub use fs::BtrfsFuse;

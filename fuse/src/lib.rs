//! `btrfs-fuse` library: a userspace btrfs driver split into a plain-Rust
//! operation layer and a thin `fuser::Filesystem` adapter.
//!
//! The inherent methods on [`BtrfsFuse`] (`lookup_entry`, `get_attr`,
//! `read_dir`, `read_symlink`, `read_data`, `list_xattrs`, `get_xattr`,
//! `stat_fs`) return plain `std::io::Result` values and can be driven
//! directly from tests and other embedders without ever going through
//! the FUSE protocol. The [`fuser::Filesystem`] trait impl lives in the
//! same file as those methods and is a narrow adapter that maps the
//! `io::Result` / `Option` returns to the appropriate `Reply*` calls.

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod dir;
pub mod fs;
pub mod inode;
pub mod read;
pub mod stat;
pub mod xattr;

pub use fs::{BtrfsFuse, StatfsInfo};

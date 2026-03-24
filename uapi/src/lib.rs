//! # Btrfs Userspace API
//!
//! This crate provides a safe Rust interface to the Btrfs userspace API. Communication
//! with the kernel is done via ioctl calls, and in some cases sysfs is used
//! for status queries.
//!
//! This crate uses bindgen to generate Rust bindings for the raw structs defined
//! in the kernel headers that are used by the ioctl interface. It uses the nix
//! crate to provide raw, unsafe wrappers for these ioctls. Both of these are in
//! the `raw` module.
//!
//! Further, the crate provides safe, high-level wrappers for the ioctls in the
//! `balance`, `chunk`, `defrag`, `device`, `fiemap`, `filesystem`, `inode`,
//! `label`, `qgroup`, `quota`, `resize`, `scrub`, `space`, `subvolume`, `sync`,
//! `sysfs`, and `tree_search` modules.
//!
//! ## Portability
//!
//! This crate should work on any platform that supports the Btrfs userspace API,
//! but it has only been tested on Linux. It is possible that earlier versions
//! of the kernel may not support all the ioctls used by this crate.

pub mod balance;
pub mod chunk;
pub mod defrag;
pub mod device;
pub mod fiemap;
pub mod filesystem;
pub mod inode;
pub mod label;
pub mod qgroup;
pub mod quota;
pub mod raw;
pub mod resize;
pub mod scrub;
pub mod space;
pub mod subvolume;
pub mod sync;
pub mod sysfs;
pub mod tree_search;

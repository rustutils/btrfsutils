//! # btrfs-disk: on-disk format parsing for btrfs filesystems
//!
//! This crate reads and parses btrfs on-disk structures directly from block
//! devices, without going through the kernel. It covers superblock parsing,
//! and will eventually include tree node and leaf parsing.
//!
//! Unlike `btrfs-uapi` (which wraps Linux-only ioctls), this crate is
//! platform-independent: any system that can read raw bytes from a block
//! device or image file can use it.

pub mod raw;
pub mod superblock;

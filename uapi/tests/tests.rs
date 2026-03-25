//! Integration tests for the ioctls.
//!
//! These tests require a mounted btrfs filesystem and root privileges. They are
//! skipped automatically unless the test is run with `--include-ignored`.
//!
//! To run:
//!   just test-privileged

mod common;

mod balance;
mod chunk;
mod defrag;
mod dev_extent;
mod device;
mod fiemap;
mod filesystem;
mod inode;
mod label;
mod qgroup;
mod quota;
mod replace;
mod resize;
mod scrub;
mod space;
mod subvolume;
mod sync_test;
mod sysfs;
mod tree_search;

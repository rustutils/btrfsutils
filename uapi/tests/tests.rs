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
mod device;
mod features;
mod fiemap;
mod filesystem;
mod inode;
mod quota;
mod replace;
mod scrub;
mod space;
mod subvolume;
mod sysfs;
mod tree_search;

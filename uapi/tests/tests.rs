//! Integration tests for the ioctls.
//!
//! These tests require a mounted btrfs filesystem and root privileges. They are
//! skipped automatically unless the test is run with `--include-ignored`.
//!
//! To run:
//!   just test-privileged

// Test code uses C-string literals, byte casts, and small idioms that
// pedantic clippy flags but that are intentional here.
#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::identity_op,
    clippy::manual_c_str_literals,
    clippy::needless_borrows_for_generic_args,
    clippy::redundant_guards,
    clippy::unnecessary_cast,
    clippy::unreadable_literal
)]

mod common;

mod balance;
mod chunk;
mod dedupe;
mod defrag;
mod device;
mod features;
mod fiemap;
mod filesystem;
mod inode;
mod quota;
mod replace;
mod scrub;
mod send_receive;
mod space;
mod subvolume;
mod sysfs;
mod tree_search;

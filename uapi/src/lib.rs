//! # btrfs-uapi: typed Rust wrappers around the btrfs kernel interface
//!
//! This crate provides typed, safe access to the btrfs kernel interface.
//! Kernel communication uses two mechanisms:
//!
//! - **ioctls** for most operations (balance, scrub, subvolume management, ...)
//! - **sysfs** under `/sys/fs/btrfs/<uuid>/` for status that the kernel does
//!   not expose via ioctl (quota state, commit statistics, scrub speed limits)
//!
//! ## Safety
//!
//! All `unsafe` code is confined to the [`raw`] module, which contains
//! bindgen-generated types from the kernel UAPI headers (`btrfs.h` and
//! `btrfs_tree.h`) and `nix` ioctl macro declarations for every `BTRFS_IOC_*`
//! call.
//!
//! Every other module wraps [`raw`] into a public API that is entirely safe,
//! exposes no kernel types, and uses idiomatic Rust types throughout:
//! `BorrowedFd`, `Uuid`, `bitflags`, `SystemTime`, `CString`, and so on.
//!
//! ## Usage
//!
//! Every function that issues an ioctl takes a [`BorrowedFd`][`std::os::unix::io::BorrowedFd`]
//! open on any file within the target btrfs filesystem. Functions in [`sysfs`]
//! instead take a [`uuid::Uuid`], which can be obtained from
//! [`filesystem::filesystem_info`].
//!
//! Most ioctl-based operations require `CAP_SYS_ADMIN`.
//!
//! ## Portability
//!
//! btrfs is Linux-only; this crate does not support other operating systems.
//! Some ioctls used here were introduced in relatively recent kernel versions;
//! this crate targets modern kernels (5.x and later) and does not attempt to
//! detect or work around missing ioctls on older kernels. It is only tested
//! on `amd64`, but all architectures supported by the kernel (and Rust) should
//! work.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Test code uses literal byte buffers, raw pointer casts (for crafting
// kernel-shaped data structures by hand), and small cast conversions
// that pedantic clippy flags but that are intentional in unit tests.
#![cfg_attr(
    test,
    allow(
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::decimal_bitwise_operands,
        clippy::identity_op,
        clippy::items_after_test_module,
        clippy::match_wildcard_for_single_variants,
        clippy::ptr_cast_constness,
        clippy::semicolon_if_nothing_returned,
        clippy::uninlined_format_args,
        clippy::unreadable_literal,
    )
)]

pub mod balance;
pub mod blkdev;
pub mod chunk;
pub mod dedupe;
pub mod defrag;
pub mod device;
pub mod features;
pub mod fiemap;
pub mod filesystem;
pub mod inode;
pub mod quota;
#[allow(missing_docs)]
pub mod raw;
pub mod replace;
pub mod scrub;
pub mod send_receive;
pub mod space;
pub mod subvolume;
pub mod sysfs;
pub mod tree_search;
pub mod util;

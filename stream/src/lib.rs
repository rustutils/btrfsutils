//! # btrfs-stream: btrfs send stream parser and receive operations
//!
//! This crate handles the btrfs send stream format: a binary TLV protocol
//! used by `btrfs send` / `btrfs receive` to serialize and replay filesystem
//! changes between subvolume snapshots.
//!
//! ## Stream parsing (default, platform-independent)
//!
//! The default feature set provides a zero-copy stream parser that works on
//! any platform:
//!
//! - [`StreamReader`] reads a btrfs send stream from any `impl Read`,
//!   validates the stream header (magic, protocol version 1-3), and yields
//!   [`StreamCommand`] values with CRC32C integrity checks on every command.
//! - [`StreamCommand`] is an enum covering all v1, v2, and v3 command types
//!   (subvol, snapshot, write, clone, encoded write, fallocate, enable
//!   verity, and so on).
//! - [`Timespec`] represents timestamps carried in the stream.
//!
//! ## Receive operations (feature `receive`, Linux-only)
//!
//! Enable the `receive` feature to get [`ReceiveContext`], which applies a
//! parsed stream to a mounted btrfs filesystem. It creates subvolumes and
//! snapshots, writes files, clones extents, sets xattrs and permissions, and
//! finalizes received subvolumes with their received UUID. It handles v2
//! encoded writes with automatic decompression fallback (zlib, zstd, lzo)
//! and v3 fs-verity enablement.
//!
//! This feature depends on `btrfs-uapi` for ioctl access and requires
//! `CAP_SYS_ADMIN` on Linux.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Test code uses literal byte buffers and small cast conversions that
// pedantic clippy flags but that are intentional in unit tests.
#![cfg_attr(
    test,
    allow(
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::identity_op,
        clippy::match_wildcard_for_single_variants,
        clippy::semicolon_if_nothing_returned,
        clippy::unreadable_literal,
    )
)]

mod stream;

#[cfg(feature = "receive")]
mod receive;
#[cfg(feature = "receive")]
mod verity;

#[cfg(feature = "receive")]
pub use receive::ReceiveContext;
pub use stream::{StreamCommand, StreamError, StreamReader, Timespec};

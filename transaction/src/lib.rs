//! # Userspace transaction infrastructure for btrfs
//!
//! This crate provides the write-path infrastructure needed to modify btrfs
//! filesystems from userspace. It builds on `btrfs-disk` (which provides the
//! read path) and adds mutable tree blocks, B-tree search, copy-on-write,
//! item insertion/deletion, node splitting, transaction commit, and extent
//! allocation.
//!
//! The primary entry point is [`Filesystem::open`], which opens a device or
//! image file for modification. From there, start a transaction with
//! [`Transaction::start`], modify trees through [`search::search_slot`] and
//! the item operation functions, and commit with [`Transaction::commit`].
//!
//! This is a clean-room implementation based on the on-disk format
//! specification and UAPI headers. It is licensed MIT/Apache-2.0.
//!
//! # Stability
//!
//! This is a pre-1.0, experimental crate. It is a clean-room
//! reimplementation of btrfs's read-write tree machinery and may
//! have edge cases that testing doesn't cover. Do not use it on
//! filesystems you care about without taking a backup first.

#![warn(clippy::pedantic)]
#![allow(clippy::cast_possible_truncation)] // nodesize ≤ 64K, offsets always fit u32
#![allow(clippy::cast_possible_wrap)] // bytes_used u64↔i64 conversions are intentional
#![allow(clippy::cast_sign_loss)] // bytes_used i64↔u64 conversions are intentional
#![allow(clippy::missing_errors_doc)] // error conditions obvious from Result<T>
#![allow(clippy::missing_panics_doc)] // path.nodes[].unwrap() always valid in context
#![allow(clippy::module_name_repetitions)]
// Test code uses literal byte buffers and small cast conversions that
// pedantic clippy flags but that are intentional in unit tests.
#![cfg_attr(
    test,
    allow(
        clippy::cast_lossless,
        clippy::identity_op,
        clippy::match_wildcard_for_single_variants,
        clippy::semicolon_if_nothing_returned,
        clippy::unreadable_literal,
    )
)]

pub mod allocation;
pub mod balance;
pub mod buffer;
pub mod convert;
pub mod cow;
pub mod delayed_ref;
pub mod extent_walk;
pub mod filesystem;
pub mod free_space;
pub mod inode;
pub mod items;
pub mod path;
pub mod search;
pub mod split;
pub mod transaction;

pub use crate::{filesystem::Filesystem, transaction::Transaction};

#[doc(hidden)]
pub mod test_helpers;

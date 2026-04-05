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

#![warn(clippy::pedantic)]
#![allow(clippy::cast_possible_truncation)] // nodesize ≤ 64K, offsets always fit u32
#![allow(clippy::cast_possible_wrap)] // bytes_used u64↔i64 conversions are intentional
#![allow(clippy::cast_sign_loss)] // bytes_used i64↔u64 conversions are intentional
#![allow(clippy::missing_errors_doc)] // error conditions obvious from Result<T>
#![allow(clippy::missing_panics_doc)] // path.nodes[].unwrap() always valid in context
#![allow(clippy::module_name_repetitions)]

pub mod allocation;
pub mod balance;
pub mod buffer;
pub mod cow;
pub mod delayed_ref;
pub mod filesystem;
pub mod items;
pub mod path;
pub mod search;
pub mod split;
pub mod transaction;

pub use crate::{filesystem::Filesystem, transaction::Transaction};

#[cfg(test)]
mod test_helpers;

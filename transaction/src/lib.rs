//! # Userspace transaction infrastructure for btrfs
//!
//! This crate provides the write-path infrastructure needed to
//! modify btrfs filesystems from userspace. It builds on
//! `btrfs-disk` (the read path) and adds mutable tree blocks,
//! B-tree search, copy-on-write, item insertion / deletion / split
//! / balance, transaction commit, extent allocation, free-space
//! tree maintenance, and a layer of high-level helpers (file data,
//! inodes, dir entries, xattrs, subvolume creation, device-size
//! patches).
//!
//! ## Entry points
//!
//! - Single-device: [`Filesystem::open`] takes a device or image
//!   file open for read+write.
//! - Multi-device: [`filesystem::Filesystem::open_multi`] takes a
//!   `BTreeMap<devid, handle>`. Used by every multi-device tool
//!   (mkfs, rescue, tune).
//!
//! From there, [`Transaction::start`] opens a transaction;
//! mutations go through [`search::search_slot`] +
//! [`items::insert_item`] / [`items::del_items`] /
//! [`items::update_item`] for raw access, or through the
//! [`Transaction`] helpers (`create_inode`, `link_dir_entry`,
//! `set_xattr`, `write_file_data`, `create_empty_tree`,
//! `insert_root_ref`, `reserve_data_extent`, etc.) for higher-level
//! patterns. [`Transaction::commit`] closes out the transaction;
//! [`Transaction::abort`] discards it.
//!
//! Whole-tree conversion paths
//! ([`convert::convert_to_free_space_tree`],
//! [`convert::convert_to_block_group_tree`], plus the per-step
//! [`convert::seed_free_space_tree`] and
//! [`convert::create_block_group_tree`] helpers) live in the
//! [`convert`] module.
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

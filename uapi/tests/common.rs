//! Re-exports of the shared test harness from the `btrfs-test-utils` crate.
//!
//! `BackingFile`, `LoopbackDevice`, and `Mount` live in `util/testing`.
//! This module re-exports them under `crate::common::*` so the
//! `use crate::common::{...}` imports in test files keep working.

#![allow(unused_imports)]

pub use btrfs_test_utils::{
    BackingFile, LoopbackDevice, Mount, TEST_LABEL, TEST_UUID,
    cache_gzipped_image, deterministic_mount, mount_existing_readonly, run,
    single_mount, verify_test_data, write_compressible_data, write_test_data,
};

#![allow(dead_code)]
//! Re-exports of the shared test harness from `btrfs-test-utils`, plus cli-
//! specific glue: fixture image paths rooted under this crate, and the
//! `btrfs-mkfs` binary lookup (which needs `env!("CARGO_BIN_EXE_btrfs")`
//! and therefore can only run inside this crate).

pub use btrfs_test_utils::{
    BackingFile, LoopbackDevice, Mount, cache_gzipped_image,
    deterministic_mount, mount_existing_readonly, single_mount,
    verify_test_data, write_compressible_data, write_test_data,
};
use std::path::{Path, PathBuf};

/// Path to our `btrfs-mkfs` binary (in the same target dir as the test
/// binary). Uses `env!("CARGO_BIN_EXE_btrfs")` so it only resolves inside
/// this crate.
pub fn our_mkfs_bin() -> PathBuf {
    let btrfs = env!("CARGO_BIN_EXE_btrfs");
    let dir = Path::new(btrfs).parent().unwrap();
    let mkfs = dir.join("btrfs-mkfs");
    assert!(
        mkfs.exists(),
        "btrfs-mkfs not found at {}; run `cargo build -p btrfs-mkfs` first",
        mkfs.display()
    );
    mkfs
}

/// Directory where decompressed fixture images are cached across test runs.
/// Cleaned by `cargo clean`.
fn fixture_cache_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/test-fixtures")
}

/// Return the path to the cached decompressed fixture image, extracting it
/// on first use. The cache lives at `target/test-fixtures/test-fs.img`.
pub fn cached_fixture_image() -> PathBuf {
    let gz = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/commands/fixture.img.gz");
    cache_gzipped_image(&gz, &fixture_cache_dir(), "test-fs.img")
}

/// Return the path to the cached decompressed broken image (for check tests).
pub fn cached_broken_image() -> PathBuf {
    let gz = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/commands/broken.img.gz");
    cache_gzipped_image(&gz, &fixture_cache_dir(), "broken.img")
}

/// Mount the pre-built fixture image read-only. The decompressed image is
/// cached in `target/test-fixtures/` so only the first test pays the gunzip
/// cost. Each test attaches its own loopback device directly to the shared
/// cached file — no copy needed since we mount read-only.
pub fn fixture_mount() -> (tempfile::TempDir, Mount) {
    mount_existing_readonly(&cached_fixture_image())
}

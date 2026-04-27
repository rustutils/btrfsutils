//! Cache effectiveness tests.
//!
//! Verifies that:
//! 1. Tree-block reads hit the LRU cache on the second access.
//! 2. `Filesystem` operations populate the cache (so a second op
//!    re-uses tree blocks fetched by the first).

use btrfs_fs::{Filesystem, LruTreeBlockCache};
use btrfs_test_utils;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

fn build_fixture(base: &Path) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("hello.txt"), b"hello, world\n").unwrap();
    fs::write(src.join("large.bin"), vec![0x42u8; 100_000]).unwrap();
    let img = base.join("test.img");
    File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    btrfs_test_utils::run(
        "mkfs.btrfs",
        &[
            "-f",
            "--rootdir",
            src.to_str().unwrap(),
            img.to_str().unwrap(),
        ],
    );
    img
}

fn fixture_path() -> &'static Path {
    static INIT: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let (_td, path) = INIT.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_fixture(td.path());
        (td, img)
    });
    path
}

/// Direct test of the `LruTreeBlockCache` against a `BlockReader`:
/// reading the same logical address twice should produce one miss
/// followed by one hit.
#[test]
fn tree_block_cache_hits_on_repeat_read() {
    use btrfs_disk::reader::filesystem_open;

    let mut fs = filesystem_open(File::open(fixture_path()).unwrap()).unwrap();
    let cache = Arc::new(LruTreeBlockCache::new(1024));
    fs.reader.set_cache(Some(cache.clone()));

    // Pick a tree root we know exists — the FS tree root from the
    // root-tree map.
    let (fs_tree_root, _) = fs.tree_roots[&5];

    let _ = fs.reader.read_tree_block(fs_tree_root).unwrap();
    let stats_after_first = cache.stats();
    assert_eq!(stats_after_first.misses, 1);
    assert_eq!(stats_after_first.hits, 0);
    assert_eq!(stats_after_first.insertions, 1);

    let _ = fs.reader.read_tree_block(fs_tree_root).unwrap();
    let stats_after_second = cache.stats();
    assert_eq!(
        stats_after_second.hits, 1,
        "second read should be a cache hit, got stats {stats_after_second:?}",
    );
    assert_eq!(stats_after_second.misses, 1);
    assert_eq!(stats_after_second.insertions, 1);
}

/// Tree-block cache integrates with `Filesystem`: repeating an
/// operation grows the hit counter while keeping the miss counter
/// pinned (no new disk reads).
#[tokio::test]
async fn filesystem_repeat_op_hits_cache() {
    let fs = Filesystem::open(File::open(fixture_path()).unwrap()).unwrap();
    let root = fs.root();

    // First lookup: walks the FS tree, fills the cache.
    let (ino, _) = fs.lookup(root, b"large.bin").await.unwrap().unwrap();
    let after_first = fs.tree_block_cache_stats();
    assert!(
        after_first.misses > 0,
        "first lookup should miss: {after_first:?}",
    );
    let cold_misses = after_first.misses;

    // Subsequent identical lookups: should hit, not miss.
    for _ in 0..5 {
        let result = fs.lookup(root, b"large.bin").await.unwrap();
        assert!(result.is_some());
    }
    let after_warm = fs.tree_block_cache_stats();
    assert_eq!(
        after_warm.misses, cold_misses,
        "warm lookups should not miss; stats={after_warm:?}",
    );
    assert!(
        after_warm.hits > after_first.hits,
        "warm lookups should produce hits; stats={after_warm:?}",
    );

    // Reads of the same file should also benefit — the second `read`
    // skips the FS-tree walk entirely (extent-map cache) and gets
    // any tree-block lookups it needs from the cache.
    let _ = fs.read(ino, 0, 1024).await.unwrap();
    let after_first_read = fs.tree_block_cache_stats();
    let _ = fs.read(ino, 0, 1024).await.unwrap();
    let after_second_read = fs.tree_block_cache_stats();
    assert_eq!(
        after_second_read.misses, after_first_read.misses,
        "second read should not miss tree-block cache; \
         stats={after_second_read:?}",
    );
}

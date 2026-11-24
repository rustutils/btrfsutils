//! Cache layer for the filesystem.
//!
//! Three caches sit on the read path:
//!
//! - [`LruTreeBlockCache`]: a [`btrfs_disk::reader::TreeBlockCache`] impl
//!   keyed by logical address. Wired into the `BlockReader`, so every
//!   tree walk uses it.
//! - [`InodeCache`]: `Inode` → parsed `InodeItem`. Hot path for
//!   `getattr` and the inode lookups in `lookup`, `readlink`, and
//!   `read`.
//! - [`ExtentMapCache`]: `Inode` → sorted list of `EXTENT_DATA`
//!   records for that file. Built on first read and reused for later
//!   reads of the same file.
//!
//! All three use interior mutability behind a `Mutex`, so they're
//! `Send + Sync` and shareable through the `Arc<Inner>` filesystem
//! handle. `Mutex` rather than `RwLock` because every LRU access
//! updates MRU order, so even a lookup needs exclusive access.

use crate::Inode;
use btrfs_disk::{
    items::{FileExtentItem, InodeItem},
    reader::TreeBlockCache,
    tree::TreeBlock,
};
use lru::LruCache;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// Default tree-block cache capacity in entries (~16 KiB each, so 4096
/// entries ≈ 64 MiB).
pub(crate) const TREE_BLOCK_CACHE_DEFAULT_ENTRIES: usize = 4096;

/// Default inode cache capacity in entries.
pub(crate) const INODE_CACHE_DEFAULT_ENTRIES: usize = 4096;

/// Default extent-map cache capacity in entries.
pub(crate) const EXTENT_MAP_CACHE_DEFAULT_ENTRIES: usize = 1024;

/// Capacities for the three caches a [`crate::Filesystem`] holds.
/// Pass to [`crate::Filesystem::open_with_caches`] or
/// [`crate::Filesystem::open_subvol_with_caches`] to override the
/// defaults, for memory-constrained embedders or benchmarking the
/// cold path.
///
/// All three values must be `> 0`. The underlying LRU caches reject
/// zero capacity. Use [`CacheConfig::no_cache`] for the
/// minimum-viable single-entry caches.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Number of tree blocks to cache (~16 KiB each on default
    /// nodesize). Default: 4096 (~64 MiB).
    pub tree_blocks: usize,
    /// Number of parsed inode items to cache. Default: 4096.
    pub inodes: usize,
    /// Number of per-inode extent maps to cache. Default: 1024.
    pub extent_maps: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            tree_blocks: TREE_BLOCK_CACHE_DEFAULT_ENTRIES,
            inodes: INODE_CACHE_DEFAULT_ENTRIES,
            extent_maps: EXTENT_MAP_CACHE_DEFAULT_ENTRIES,
        }
    }
}

impl CacheConfig {
    /// Smallest viable configuration: each cache holds a single
    /// entry. Useful for benchmarking the cold path or to effectively
    /// disable caching. The LRU implementations reject `0`, so this is
    /// the floor.
    #[must_use]
    pub fn no_cache() -> Self {
        Self {
            tree_blocks: 1,
            inodes: 1,
            extent_maps: 1,
        }
    }
}

/// Live counters for an [`LruTreeBlockCache`], for tests, benchmarks,
/// and embedders that export cache hit ratios as metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub invalidations: u64,
}

/// LRU-evicting [`TreeBlockCache`] keyed by logical address.
///
/// Counters are atomics, so reading `stats()` doesn't contend with
/// cache traffic.
pub struct LruTreeBlockCache {
    inner: Mutex<LruCache<u64, Arc<TreeBlock>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: AtomicU64,
    invalidations: AtomicU64,
}

impl LruTreeBlockCache {
    /// Create a cache with `capacity` entries. `capacity` must be > 0.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity)
            .expect("LruTreeBlockCache capacity must be > 0");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            insertions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    /// Snapshot of the current counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }
}

impl TreeBlockCache for LruTreeBlockCache {
    fn get(&self, addr: u64) -> Option<Arc<TreeBlock>> {
        // `LruCache::get` updates MRU order, so it needs the lock even
        // for a lookup.
        let hit = self.inner.lock().unwrap().get(&addr).map(Arc::clone);
        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    fn put(&self, addr: u64, block: Arc<TreeBlock>) {
        self.inner.lock().unwrap().put(addr, block);
        self.insertions.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate(&self, addr: u64) {
        self.inner.lock().unwrap().pop(&addr);
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }
}

/// LRU cache mapping `Inode` → parsed `InodeItem`. Values are `Arc`s
/// so lookups don't copy the 160-byte struct.
pub(crate) struct InodeCache {
    inner: Mutex<LruCache<Inode, Arc<InodeItem>>>,
}

impl InodeCache {
    pub(crate) fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity)
            .expect("InodeCache capacity must be > 0");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    pub(crate) fn get(&self, ino: Inode) -> Option<Arc<InodeItem>> {
        self.inner.lock().unwrap().get(&ino).map(Arc::clone)
    }

    pub(crate) fn put(&self, ino: Inode, item: Arc<InodeItem>) {
        self.inner.lock().unwrap().put(ino, item);
    }

    /// Drop a single entry. Called from FUSE `forget` so memory is
    /// freed when the kernel drops its reference, ahead of LRU
    /// eviction. Future write operations will use it for invalidation.
    pub(crate) fn invalidate(&self, ino: Inode) {
        self.inner.lock().unwrap().pop(&ino);
    }
}

/// A single `EXTENT_DATA` item collected for a file. Has the shape the
/// read path uses, so cache hits feed straight into `read::read_file`
/// without re-parsing.
#[derive(Clone)]
pub(crate) struct ExtentRecord {
    pub file_pos: u64,
    pub item: FileExtentItem,
    /// Raw on-disk payload of the `EXTENT_DATA` item, used to extract
    /// inline extent bytes.
    pub raw: Vec<u8>,
}

/// Sorted (by `file_pos`) list of `EXTENT_DATA` items for one inode.
#[derive(Default)]
pub(crate) struct ExtentMap {
    pub records: Vec<ExtentRecord>,
}

/// LRU cache mapping `Inode` → its full extent map. Built on the first
/// `read` of a file and reused for later reads.
pub(crate) struct ExtentMapCache {
    inner: Mutex<LruCache<Inode, Arc<ExtentMap>>>,
}

impl ExtentMapCache {
    pub(crate) fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity)
            .expect("ExtentMapCache capacity must be > 0");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    pub(crate) fn get(&self, ino: Inode) -> Option<Arc<ExtentMap>> {
        self.inner.lock().unwrap().get(&ino).map(Arc::clone)
    }

    pub(crate) fn put(&self, ino: Inode, map: Arc<ExtentMap>) {
        self.inner.lock().unwrap().put(ino, map);
    }

    /// Drop a single entry. Called from FUSE `forget` so memory is
    /// freed when the kernel drops its reference, ahead of LRU
    /// eviction. Future write operations will use it for invalidation.
    pub(crate) fn invalidate(&self, ino: Inode) {
        self.inner.lock().unwrap().pop(&ino);
    }
}

#[cfg(test)]
mod tests {
    //! Cache behaviour is tested end-to-end by `fs/tests/basic.rs` and
    //! `fs/tests/cache.rs`. A unit test here would have to build an
    //! `InodeItem` from scratch (16+ fields, four `Timespec`s, bitflags)
    //! only to show that `lru::LruCache` evicts correctly, which tests
    //! the upstream crate rather than this wrapper.
}

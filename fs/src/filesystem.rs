//! The [`Filesystem`] type and its operation methods.
//!
//! [`Filesystem`] is `Clone` (cheap `Arc` bump) and exposes all
//! operations as `async fn`. Internally each op runs the (currently
//! sync) I/O work inside [`tokio::task::spawn_blocking`] so the
//! async runtime is never blocked. Future work (a native async I/O
//! backend, lock-free cache hits) is internal and won't change the
//! API.
//!
//! Embedders must call these methods inside a tokio runtime context.
//! `btrfs-fuse` provides one; tests use `#[tokio::test]`; other
//! embedders bring their own.

use crate::{
    Entry, FileKind, Stat,
    cache::{
        EXTENT_MAP_CACHE_DEFAULT_ENTRIES, ExtentMapCache,
        INODE_CACHE_DEFAULT_ENTRIES, InodeCache, LruTreeBlockCache,
        TREE_BLOCK_CACHE_DEFAULT_ENTRIES,
    },
    dir, read, xattr,
};
use btrfs_disk::{
    items::{DirItem, InodeItem},
    reader::{BlockReader, Traversal, filesystem_open, tree_walk},
    superblock::Superblock,
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::BTreeMap,
    io, mem,
    sync::{Arc, Mutex, MutexGuard},
};

/// `BTRFS_FS_TREE_OBJECTID` — the default subvolume's tree root.
const FS_TREE_OBJECTID: u64 = 5;

/// `BTRFS_FIRST_FREE_OBJECTID` — the root directory of any subvolume.
const ROOT_DIR_OBJECTID: u64 = 256;

/// Identifier for a subvolume tree (the tree's root objectid).
///
/// For the default subvolume this is `5` (`BTRFS_FS_TREE_OBJECTID`).
/// Custom subvolumes use `256` and up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubvolId(pub u64);

/// A filesystem-level inode reference: the subvolume it lives in plus
/// the on-disk objectid within that subvolume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Inode {
    pub subvol: SubvolId,
    pub ino: u64,
}

/// Filesystem-wide statistics, returned by [`Filesystem::statfs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatFs {
    /// Total blocks (in units of `bsize`).
    pub blocks: u64,
    /// Free blocks (in units of `bsize`).
    pub bfree: u64,
    /// Blocks available to unprivileged users (same as `bfree` for btrfs).
    pub bavail: u64,
    /// Preferred block size.
    pub bsize: u32,
    /// Maximum filename length.
    pub namelen: u32,
    /// Fragment size (same as `bsize` for btrfs).
    pub frsize: u32,
}

/// High-level read-only btrfs filesystem.
///
/// `Filesystem` is a cheap-to-clone handle (`Arc` internally) and all
/// operations are `async fn`, so multiple tokio tasks can drive the
/// same filesystem concurrently. I/O still serialises on a single
/// internal mutex (held only inside `spawn_blocking`); a future phase
/// can swap that for a reader pool without changing the public API.
pub struct Filesystem<R: io::Read + io::Seek + Send + 'static> {
    inner: Arc<Inner<R>>,
}

impl<R: io::Read + io::Seek + Send + 'static> Clone for Filesystem<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Shared state behind every [`Filesystem`] handle.
struct Inner<R: io::Read + io::Seek + Send + 'static> {
    /// Parsed primary-device superblock.
    superblock: Superblock,
    /// Map of tree id → (root block logical address, key offset).
    /// Multi-subvolume support will look up additional entries here.
    tree_roots: BTreeMap<u64, (u64, u64)>,
    /// Cached objectid of the default subvolume.
    default_subvol: SubvolId,
    /// Filesystem sectorsize, forwarded from the superblock.
    blksize: u32,
    /// I/O backend. Held only inside `spawn_blocking`, never across an
    /// `.await`. Future work can swap this for a pool of readers
    /// without changing the public `&self` API.
    reader: Mutex<BlockReader<R>>,
    /// Concrete reference to the tree-block cache. Stored alongside
    /// the `Arc<dyn TreeBlockCache>` inside the reader so callers can
    /// inspect [`crate::CacheStats`] without going through the trait.
    tree_block_cache: Arc<LruTreeBlockCache>,
    /// Inode cache. Hit on `getattr`, `lookup`, `readlink`, `read`.
    /// Populated whenever an `InodeItem` is parsed from the tree.
    inode_cache: InodeCache,
    /// Per-inode extent map cache. Built lazily on first `read` of a
    /// file; reused for subsequent reads of the same inode.
    extent_map_cache: ExtentMapCache,
}

impl<R: io::Read + io::Seek + Send + 'static> Filesystem<R> {
    /// Bootstrap the filesystem from a reader over an image or block
    /// device.
    ///
    /// This is sync because the heavy work happens during the bootstrap
    /// (chunk tree walk, root tree walk) and only runs once. Embedders
    /// that want non-blocking open can wrap the call in
    /// `tokio::task::spawn_blocking` themselves.
    pub fn open(reader: R) -> io::Result<Self> {
        let mut fs = filesystem_open(reader)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let blksize = fs.superblock.sectorsize;
        if !fs.tree_roots.contains_key(&FS_TREE_OBJECTID) {
            return Err(io::Error::other(
                "default FS tree (objectid 5) not found in root tree",
            ));
        }
        // Attach a tree-block cache to the underlying reader so every
        // tree walk past the first benefits transparently. We keep
        // both a typed `Arc<LruTreeBlockCache>` (for stats) and the
        // erased `Arc<dyn TreeBlockCache>` (for the reader) — they
        // point at the same instance.
        let tree_block_cache =
            Arc::new(LruTreeBlockCache::new(TREE_BLOCK_CACHE_DEFAULT_ENTRIES));
        fs.reader.set_cache(Some(tree_block_cache.clone()
            as Arc<dyn btrfs_disk::reader::TreeBlockCache>));
        Ok(Self {
            inner: Arc::new(Inner {
                superblock: fs.superblock,
                tree_roots: fs.tree_roots,
                default_subvol: SubvolId(FS_TREE_OBJECTID),
                blksize,
                reader: Mutex::new(fs.reader),
                tree_block_cache,
                inode_cache: InodeCache::new(INODE_CACHE_DEFAULT_ENTRIES),
                extent_map_cache: ExtentMapCache::new(
                    EXTENT_MAP_CACHE_DEFAULT_ENTRIES,
                ),
            }),
        })
    }

    /// Snapshot of the tree-block cache hit/miss counters. Useful for
    /// tests, benchmarks, and embedders surfacing cache metrics.
    #[must_use]
    pub fn tree_block_cache_stats(&self) -> crate::CacheStats {
        self.inner.tree_block_cache.stats()
    }

    /// Inode of the default subvolume's root directory (objectid 256).
    #[must_use]
    pub fn root(&self) -> Inode {
        Inode {
            subvol: self.inner.default_subvol,
            ino: ROOT_DIR_OBJECTID,
        }
    }

    /// Filesystem sectorsize.
    #[must_use]
    pub fn blksize(&self) -> u32 {
        self.inner.blksize
    }

    /// Look up a child of `parent` by name.
    pub async fn lookup(
        &self,
        parent: Inode,
        name: &[u8],
    ) -> io::Result<Option<(Inode, InodeItem)>> {
        let this = self.clone();
        let name = name.to_vec();
        spawn_blocking(move || this.lookup_blocking(parent, &name)).await
    }

    /// Read the inode item for `ino`.
    pub async fn read_inode_item(
        &self,
        ino: Inode,
    ) -> io::Result<Option<InodeItem>> {
        let this = self.clone();
        spawn_blocking(move || this.read_inode_item_blocking(ino)).await
    }

    /// Read inode metadata as a [`Stat`].
    pub async fn getattr(&self, ino: Inode) -> io::Result<Option<Stat>> {
        let this = self.clone();
        spawn_blocking(move || this.getattr_blocking(ino)).await
    }

    /// List the entries of a directory inode, starting strictly after
    /// `offset`. `.` and `..` are synthesised at offsets 0 and 1.
    pub async fn readdir(
        &self,
        dir_ino: Inode,
        offset: u64,
    ) -> io::Result<Vec<Entry>> {
        let this = self.clone();
        spawn_blocking(move || this.readdir_blocking(dir_ino, offset)).await
    }

    /// Read the target of a symbolic link.
    pub async fn readlink(&self, ino: Inode) -> io::Result<Option<Vec<u8>>> {
        let this = self.clone();
        spawn_blocking(move || this.readlink_blocking(ino)).await
    }

    /// Read `size` bytes from `ino` starting at `offset`. Sparse holes
    /// and prealloc extents return zeros; compressed extents are
    /// decompressed.
    pub async fn read(
        &self,
        ino: Inode,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let this = self.clone();
        spawn_blocking(move || this.read_blocking(ino, offset, size)).await
    }

    /// List all xattr names for an inode.
    pub async fn xattr_list(&self, ino: Inode) -> io::Result<Vec<Vec<u8>>> {
        let this = self.clone();
        spawn_blocking(move || this.xattr_list_blocking(ino)).await
    }

    /// Look up the value of a single xattr by exact name.
    pub async fn xattr_get(
        &self,
        ino: Inode,
        name: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let this = self.clone();
        let name = name.to_vec();
        spawn_blocking(move || this.xattr_get_blocking(ino, &name)).await
    }

    /// Filesystem-wide statistics pulled straight from the superblock.
    /// No I/O — sync.
    #[must_use]
    pub fn statfs(&self) -> StatFs {
        let sb = &self.inner.superblock;
        let bsize = u64::from(sb.sectorsize);
        let blocks = sb.total_bytes / bsize;
        let bfree = sb.total_bytes.saturating_sub(sb.bytes_used) / bsize;
        StatFs {
            blocks,
            bfree,
            bavail: bfree,
            bsize: sb.sectorsize,
            namelen: 255,
            frsize: sb.sectorsize,
        }
    }

    // ── Sync (blocking) implementations ─────────────────────────────
    //
    // The `_blocking` methods carry the actual logic. They run inside
    // `spawn_blocking`, so they're allowed to take the reader Mutex
    // and do sync I/O without blocking the runtime.

    fn lookup_blocking(
        &self,
        parent: Inode,
        name: &[u8],
    ) -> io::Result<Option<(Inode, InodeItem)>> {
        let tree_root = self.tree_root_for(parent.subvol)?;
        let mut reader = self.lock_reader();
        let Some(entry) =
            lookup_in_dir(&mut reader, tree_root, parent.ino, name)?
        else {
            return Ok(None);
        };
        let child_oid = entry.location.objectid;
        let child = Inode {
            subvol: parent.subvol,
            ino: child_oid,
        };
        // Reuse the cached InodeItem if present; otherwise fetch and
        // populate. Holding the reader mutex across both calls is fine
        // because the inode cache uses interior mutability.
        let item = if let Some(cached) = self.inner.inode_cache.get(child) {
            (*cached).clone()
        } else {
            let Some(item) = read_inode(&mut reader, tree_root, child_oid)?
            else {
                return Ok(None);
            };
            self.inner.inode_cache.put(child, Arc::new(item.clone()));
            item
        };
        Ok(Some((child, item)))
    }

    fn read_inode_item_blocking(
        &self,
        ino: Inode,
    ) -> io::Result<Option<InodeItem>> {
        if let Some(cached) = self.inner.inode_cache.get(ino) {
            return Ok(Some((*cached).clone()));
        }
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        let Some(item) = read_inode(&mut reader, tree_root, ino.ino)? else {
            return Ok(None);
        };
        self.inner.inode_cache.put(ino, Arc::new(item.clone()));
        Ok(Some(item))
    }

    fn getattr_blocking(&self, ino: Inode) -> io::Result<Option<Stat>> {
        Ok(self
            .read_inode_item_blocking(ino)?
            .map(|item| Stat::from_inode(ino, &item, self.inner.blksize)))
    }

    fn readdir_blocking(
        &self,
        dir_ino: Inode,
        offset: u64,
    ) -> io::Result<Vec<Entry>> {
        let tree_root = self.tree_root_for(dir_ino.subvol)?;
        let mut entries: Vec<Entry> = Vec::new();
        let mut reader = self.lock_reader();

        if offset == 0 {
            entries.push(Entry {
                ino: dir_ino,
                kind: FileKind::Directory,
                name: b".".to_vec(),
                offset: 1,
            });
        }
        if offset <= 1 {
            let parent_oid =
                find_parent_oid(&mut reader, tree_root, dir_ino.ino)?;
            entries.push(Entry {
                ino: Inode {
                    subvol: dir_ino.subvol,
                    ino: parent_oid,
                },
                kind: FileKind::Directory,
                name: b"..".to_vec(),
                offset: 2,
            });
        }

        let cursor = offset.max(2);
        let mut dir_entries: Vec<Entry> = Vec::new();
        for_each_item(&mut reader, tree_root, |key, data| {
            if key.objectid != dir_ino.ino || key.key_type != KeyType::DirIndex
            {
                return;
            }
            if key.offset < cursor {
                return;
            }
            for item in DirItem::parse_all(data) {
                // Cookie is "next offset to start from", so add 1.
                let entry = dir::Entry::from_dir_item(
                    dir_ino.subvol,
                    &item,
                    key.offset + 1,
                );
                dir_entries.push(entry);
            }
        })?;
        dir_entries.sort_by_key(|e| e.offset);
        entries.extend(dir_entries);
        Ok(entries)
    }

    fn readlink_blocking(&self, ino: Inode) -> io::Result<Option<Vec<u8>>> {
        let Some(item) = self.read_inode_item_blocking(ino)? else {
            return Ok(None);
        };
        let tree_root = self.tree_root_for(ino.subvol)?;
        let blksize = self.inner.blksize;
        let mut reader = self.lock_reader();
        let target =
            read::read_symlink(&mut reader, tree_root, ino.ino, blksize)?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(target.map(|mut t| {
            t.truncate(item.size as usize);
            t
        }))
    }

    fn read_blocking(
        &self,
        ino: Inode,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let Some(item) = self.read_inode_item_blocking(ino)? else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("inode {} not found", ino.ino),
            ));
        };
        let tree_root = self.tree_root_for(ino.subvol)?;
        let blksize = self.inner.blksize;
        // Build or fetch the extent map so repeated reads of the same
        // file don't re-walk the FS tree.
        let extent_map = self.extent_map_for(ino, tree_root)?;
        let mut reader = self.lock_reader();
        read::read_file_with_map(
            &mut reader,
            &extent_map.records,
            item.size,
            offset,
            size,
            blksize,
        )
    }

    /// Build (or fetch from cache) the [`ExtentMap`] for `ino`.
    fn extent_map_for(
        &self,
        ino: Inode,
        tree_root: u64,
    ) -> io::Result<Arc<crate::cache::ExtentMap>> {
        if let Some(cached) = self.inner.extent_map_cache.get(ino) {
            return Ok(cached);
        }
        let mut reader = self.lock_reader();
        let records = read::collect_extents(&mut reader, tree_root, ino.ino)?;
        drop(reader);
        let map = Arc::new(crate::cache::ExtentMap { records });
        self.inner.extent_map_cache.put(ino, Arc::clone(&map));
        Ok(map)
    }

    fn xattr_list_blocking(&self, ino: Inode) -> io::Result<Vec<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        xattr::list_xattrs(&mut reader, tree_root, ino.ino)
    }

    fn xattr_get_blocking(
        &self,
        ino: Inode,
        name: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        xattr::get_xattr(&mut reader, tree_root, ino.ino, name)
    }

    /// Acquire the I/O lock. Forwards a poisoned mutex without
    /// unwrapping at every call site.
    fn lock_reader(&self) -> MutexGuard<'_, BlockReader<R>> {
        self.inner.reader.lock().unwrap()
    }

    /// Map a [`SubvolId`] to its tree root logical address.
    fn tree_root_for(&self, subvol: SubvolId) -> io::Result<u64> {
        if subvol == self.inner.default_subvol {
            // Validated in `open` that this entry exists.
            Ok(self.inner.tree_roots[&subvol.0].0)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("subvolume {} not yet supported", subvol.0),
            ))
        }
    }
}

/// Run a sync closure on the tokio blocking pool, mapping a
/// `JoinError` to an `io::Error` so callers see a single error type.
async fn spawn_blocking<F, T>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| io::Error::other(format!("blocking task failed: {e}")))?
}

/// DFS the given tree, calling `visitor(item_key, item_data)` for every
/// leaf item.
fn for_each_item<R, F>(
    reader: &mut BlockReader<R>,
    tree_root: u64,
    mut visitor: F,
) -> io::Result<()>
where
    R: io::Read + io::Seek,
    F: FnMut(&btrfs_disk::tree::DiskKey, &[u8]),
{
    tree_walk(reader, tree_root, Traversal::Dfs, &mut |block| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            let header_size = mem::size_of::<btrfs_disk::raw::btrfs_header>();
            for item in items {
                let start = header_size + item.offset as usize;
                let end = start + item.size as usize;
                if end <= data.len() {
                    visitor(&item.key, &data[start..end]);
                }
            }
        }
    })
}

fn read_inode<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    tree_root: u64,
    objectid: u64,
) -> io::Result<Option<InodeItem>> {
    let mut found = None;
    for_each_item(reader, tree_root, |key, data| {
        if found.is_some() {
            return;
        }
        if key.objectid == objectid && key.key_type == KeyType::InodeItem {
            found = InodeItem::parse(data);
        }
    })?;
    Ok(found)
}

fn lookup_in_dir<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    tree_root: u64,
    parent_objectid: u64,
    name: &[u8],
) -> io::Result<Option<DirItem>> {
    let mut found = None;
    for_each_item(reader, tree_root, |key, data| {
        if found.is_some() {
            return;
        }
        if key.objectid != parent_objectid || key.key_type != KeyType::DirItem {
            return;
        }
        for item in DirItem::parse_all(data) {
            if item.name == name {
                found = Some(item);
                return;
            }
        }
    })?;
    Ok(found)
}

/// Find the parent objectid of `oid` via `INODE_REF`. The `INODE_REF`
/// key offset field contains the parent objectid directly. Returns
/// `oid` itself if no ref is found (root directory).
fn find_parent_oid<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    tree_root: u64,
    oid: u64,
) -> io::Result<u64> {
    let mut parent = oid;
    for_each_item(reader, tree_root, |key, _data| {
        if parent != oid {
            return;
        }
        if key.objectid == oid && key.key_type == KeyType::InodeRef {
            parent = key.offset;
        }
    })?;
    Ok(parent)
}

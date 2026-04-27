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
    dir, read,
    stat::to_system_time,
    xattr,
};
use btrfs_disk::{
    items::{
        DeviceItem, DirItem, InodeItem, InodeRef, RootItem, RootItemFlags,
        RootRef,
    },
    reader::{BlockReader, Traversal, filesystem_open, tree_walk},
    superblock::Superblock,
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::BTreeMap,
    io, mem,
    sync::{Arc, Mutex, MutexGuard},
    time::SystemTime,
};
use uuid::Uuid;

/// `BTRFS_FS_TREE_OBJECTID` — the default subvolume's tree root.
const FS_TREE_OBJECTID: u64 = 5;

/// `BTRFS_FIRST_FREE_OBJECTID` — the root directory of any subvolume,
/// and the lower bound for non-default subvolume IDs.
const ROOT_DIR_OBJECTID: u64 = 256;

/// `BTRFS_LAST_FREE_OBJECTID` — upper bound of the user-subvolume id
/// range. Anything above is reserved for system trees (UUID, etc.).
const LAST_FREE_OBJECTID: u64 = u64::MAX - 256;

/// Whether `id` names a subvolume tree (the default `FS_TREE` plus
/// the user-allocatable range). Used to filter system trees out of
/// [`Filesystem::list_subvolumes`] and to validate `open_subvol`.
fn is_subvolume_id(id: u64) -> bool {
    id == FS_TREE_OBJECTID
        || (ROOT_DIR_OBJECTID..=LAST_FREE_OBJECTID).contains(&id)
}

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

/// Metadata for a single subvolume, returned by
/// [`Filesystem::list_subvolumes`] and
/// [`Filesystem::get_subvol_info`].
///
/// Marked `#[non_exhaustive]` so additional fields can be added in
/// the future without breaking pattern destructuring at call sites.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubvolInfo {
    /// The subvolume's tree id.
    pub id: SubvolId,
    /// Parent subvolume, or `None` for the default `FS_TREE`
    /// (`SubvolId(5)`) — it has no `ROOT_BACKREF` because nothing
    /// contains it.
    pub parent: Option<SubvolId>,
    /// Path component of this subvolume within its parent. Empty
    /// for the default `FS_TREE`.
    pub name: Vec<u8>,
    /// Inode number of the directory in `parent` that holds this
    /// subvolume entry. Zero for top-level subvolumes (no parent).
    pub dirid: u64,
    /// Read-only flag (set on snapshots taken with `-r`, or `--ro`
    /// on `mkfs --subvol`).
    pub readonly: bool,
    /// Last modification time of the subvolume's `ROOT_ITEM`.
    pub ctime: SystemTime,
    /// Creation time.
    pub otime: SystemTime,
    /// Generation when the subvolume was created or last modified.
    pub generation: u64,
    /// Transaction id of the last `ROOT_ITEM` update.
    pub ctransid: u64,
    /// Transaction id when the subvolume was created.
    pub otransid: u64,
    /// UUID of this subvolume.
    pub uuid: Uuid,
    /// UUID of the parent subvolume (for snapshots). All zeros for
    /// non-snapshot subvolumes.
    pub parent_uuid: Uuid,
    /// UUID of the subvolume this was received from (for `btrfs
    /// receive`). All zeros for non-received subvolumes.
    pub received_uuid: Uuid,
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
        Self::open_inner(reader, SubvolId(FS_TREE_OBJECTID))
    }

    /// Bootstrap the filesystem and select a non-default subvolume
    /// as the [`Filesystem::root`].
    ///
    /// `subvol` must be the tree id of an existing subvolume — pass
    /// the value from a previously-listed [`SubvolInfo::id`], or use
    /// `SubvolId(5)` to get the default. Errors with `NotFound` if
    /// the id is unknown, `InvalidInput` if it's outside the
    /// subvolume id range.
    pub fn open_subvol(reader: R, subvol: SubvolId) -> io::Result<Self> {
        Self::open_inner(reader, subvol)
    }

    fn open_inner(reader: R, default_subvol: SubvolId) -> io::Result<Self> {
        if !is_subvolume_id(default_subvol.0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} is not a valid subvolume id (must be 5 or in \
                     [256, u64::MAX - 256])",
                    default_subvol.0,
                ),
            ));
        }
        let mut fs = filesystem_open(reader)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let blksize = fs.superblock.sectorsize;
        if !fs.tree_roots.contains_key(&FS_TREE_OBJECTID) {
            return Err(io::Error::other(
                "default FS tree (objectid 5) not found in root tree",
            ));
        }
        if !fs.tree_roots.contains_key(&default_subvol.0) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("subvolume {} not found", default_subvol.0),
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
                default_subvol,
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

    /// The subvolume `Filesystem` was opened against (the default
    /// `FS_TREE` unless [`Filesystem::open_subvol`] was used).
    #[must_use]
    pub fn default_subvol(&self) -> SubvolId {
        self.inner.default_subvol
    }

    /// Enumerate every subvolume on the filesystem.
    ///
    /// Walks the root tree, parsing `ROOT_ITEM` and `ROOT_BACKREF`
    /// entries to build a [`SubvolInfo`] per subvolume. The default
    /// `FS_TREE` (`SubvolId(5)`) is included with `parent: None` and
    /// an empty `name`.
    pub async fn list_subvolumes(&self) -> io::Result<Vec<SubvolInfo>> {
        let this = self.clone();
        spawn_blocking(move || this.list_subvolumes_blocking()).await
    }

    /// Fetch metadata for a single subvolume.
    ///
    /// Currently implemented as a filtered [`Filesystem::list_subvolumes`]
    /// call — the root-tree walk dominates either way, so a one-shot
    /// lookup wouldn't be faster than the cached/single-pass list.
    /// Returns `Ok(None)` for an unknown id.
    pub async fn get_subvol_info(
        &self,
        id: SubvolId,
    ) -> io::Result<Option<SubvolInfo>> {
        Ok(self
            .list_subvolumes()
            .await?
            .into_iter()
            .find(|s| s.id == id))
    }

    /// Read-only access to the parsed primary-device superblock.
    /// Used by ioctl handlers (`FS_INFO`, `GET_FEATURES`, etc.) and
    /// by embedders that need to inspect format-level fields without
    /// re-parsing.
    #[must_use]
    pub fn superblock(&self) -> &Superblock {
        &self.inner.superblock
    }

    /// Return the [`DeviceItem`] for `devid`, or `None` if no such
    /// device exists on this filesystem.
    ///
    /// Currently single-device-only: returns the primary device's
    /// embedded `dev_item` from the superblock when `devid == 1`,
    /// `None` otherwise. Multi-device support would walk the dev
    /// tree; landing alongside multi-device write support.
    #[must_use]
    pub fn dev_info(&self, devid: u64) -> Option<DeviceItem> {
        if self.inner.superblock.dev_item.devid == devid {
            Some(self.inner.superblock.dev_item.clone())
        } else {
            None
        }
    }

    /// Resolve `objectid` in `subvol` to its slash-separated path
    /// from the subvolume root.
    ///
    /// Walks the `INODE_REF` chain upwards from `objectid` until it
    /// reaches the subvolume root (objectid 256). For directories
    /// the kernel `INO_LOOKUP` ioctl returns the path with a
    /// trailing `/`; this helper does NOT add one — the caller can
    /// append if it needs to mimic that exactly.
    ///
    /// Returns `Ok(None)` if any step in the chain has no
    /// `INODE_REF` (orphaned inode, or wrong subvol).
    pub async fn ino_lookup(
        &self,
        subvol: SubvolId,
        objectid: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        let this = self.clone();
        spawn_blocking(move || this.ino_lookup_blocking(subvol, objectid)).await
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
        let parent_tree = self.tree_root_for(parent.subvol)?;
        let mut reader = self.lock_reader();
        let Some(entry) =
            lookup_in_dir(&mut reader, parent_tree, parent.ino, name)?
        else {
            return Ok(None);
        };

        // Subvolume crossing: a `DirItem` whose `location` points at a
        // `ROOT_ITEM` key is not a regular dirent — it's a mount of
        // another subvolume's root. The child inode lives at objectid
        // 256 of that subvolume's tree, not at `entry.location.objectid`
        // in the parent's tree.
        let child = if entry.location.key_type == KeyType::RootItem {
            Inode {
                subvol: SubvolId(entry.location.objectid),
                ino: ROOT_DIR_OBJECTID,
            }
        } else {
            Inode {
                subvol: parent.subvol,
                ino: entry.location.objectid,
            }
        };

        // Reuse the cached InodeItem if present; otherwise fetch and
        // populate. The fetch uses the *child*'s tree (which equals
        // the parent's for non-crossings).
        let item = if let Some(cached) = self.inner.inode_cache.get(child) {
            (*cached).clone()
        } else {
            let child_tree = self.tree_root_for(child.subvol)?;
            let Some(item) = read_inode(&mut reader, child_tree, child.ino)?
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
            // For most inodes `..` lives in the same subvolume tree
            // (walk INODE_REF). For a subvolume root we instead walk
            // ROOT_BACKREF in the root tree to find the parent
            // subvolume's containing directory.
            let parent = if dir_ino.ino == ROOT_DIR_OBJECTID
                && dir_ino.subvol.0 != FS_TREE_OBJECTID
            {
                find_root_backref_parent(
                    &mut reader,
                    self.inner.superblock.root,
                    dir_ino.subvol.0,
                )?
                .unwrap_or(dir_ino)
            } else {
                let parent_oid =
                    find_parent_oid(&mut reader, tree_root, dir_ino.ino)?;
                Inode {
                    subvol: dir_ino.subvol,
                    ino: parent_oid,
                }
            };
            entries.push(Entry {
                ino: parent,
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

    fn ino_lookup_blocking(
        &self,
        subvol: SubvolId,
        objectid: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        // The subvolume root has no `INODE_REF`; an empty path is
        // the right answer for it.
        if objectid == ROOT_DIR_OBJECTID {
            return Ok(Some(Vec::new()));
        }
        let tree_root = self.tree_root_for(subvol)?;
        let mut reader = self.lock_reader();
        let mut components: Vec<Vec<u8>> = Vec::new();
        let mut current = objectid;
        // Bound the walk so a corrupted INODE_REF cycle can't hang
        // the FUSE worker forever. 4096 nesting levels is more than
        // any sane btrfs depth.
        for _ in 0..4096 {
            if current == ROOT_DIR_OBJECTID {
                components.reverse();
                return Ok(Some(join_path(&components)));
            }
            let mut next_parent: Option<u64> = None;
            let mut name: Option<Vec<u8>> = None;
            for_each_item(&mut reader, tree_root, |key, data| {
                if next_parent.is_some() {
                    return;
                }
                if key.objectid == current && key.key_type == KeyType::InodeRef
                {
                    // A single INODE_REF item may pack multiple
                    // entries (one per hardlink to this inode). The
                    // kernel `INO_LOOKUP` picks the first; do the
                    // same.
                    if let Some(iref) =
                        InodeRef::parse_all(data).into_iter().next()
                    {
                        next_parent = Some(key.offset);
                        name = Some(iref.name);
                    }
                }
            })?;
            match (next_parent, name) {
                (Some(p), Some(n)) => {
                    components.push(n);
                    current = p;
                }
                _ => return Ok(None),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("INODE_REF chain for objectid {objectid} too deep"),
        ))
    }

    fn list_subvolumes_blocking(&self) -> io::Result<Vec<SubvolInfo>> {
        let root_tree = self.inner.superblock.root;
        // Collect ROOT_ITEM (id → metadata) and ROOT_BACKREF (id →
        // (parent, name)) in a single root-tree walk. Each subvolume
        // has at most one BACKREF; we keep the first if any duplicate
        // appears.
        let mut roots: BTreeMap<u64, RootItem> = BTreeMap::new();
        let mut backrefs: BTreeMap<u64, (u64, RootRef)> = BTreeMap::new();
        let mut reader = self.lock_reader();
        for_each_item(&mut reader, root_tree, |key, data| {
            match key.key_type {
                KeyType::RootItem if is_subvolume_id(key.objectid) => {
                    if let Some(item) = RootItem::parse(data) {
                        roots.entry(key.objectid).or_insert(item);
                    }
                }
                KeyType::RootBackref => {
                    if let Some(rr) = RootRef::parse(data) {
                        backrefs
                            .entry(key.objectid)
                            .or_insert((key.offset, rr));
                    }
                }
                _ => {}
            }
        })?;
        drop(reader);

        let mut out = Vec::with_capacity(roots.len());
        for (id, item) in roots {
            let (parent, name, dirid) = match backrefs.get(&id) {
                Some((parent_id, rr)) => {
                    (Some(SubvolId(*parent_id)), rr.name.clone(), rr.dirid)
                }
                None => (None, Vec::new(), 0),
            };
            out.push(SubvolInfo {
                id: SubvolId(id),
                parent,
                name,
                dirid,
                readonly: item.flags.contains(RootItemFlags::RDONLY),
                ctime: to_system_time(&item.ctime),
                otime: to_system_time(&item.otime),
                generation: item.generation,
                ctransid: item.ctransid,
                otransid: item.otransid,
                uuid: item.uuid,
                parent_uuid: item.parent_uuid,
                received_uuid: item.received_uuid,
            });
        }
        Ok(out)
    }

    /// Acquire the I/O lock. Forwards a poisoned mutex without
    /// unwrapping at every call site.
    fn lock_reader(&self) -> MutexGuard<'_, BlockReader<R>> {
        self.inner.reader.lock().unwrap()
    }

    /// Map a [`SubvolId`] to its tree root logical address.
    fn tree_root_for(&self, subvol: SubvolId) -> io::Result<u64> {
        if !is_subvolume_id(subvol.0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a valid subvolume id", subvol.0),
            ));
        }
        self.inner
            .tree_roots
            .get(&subvol.0)
            .map(|(logical, _)| *logical)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("subvolume {} not found", subvol.0),
                )
            })
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

/// Join path components with `/` separators (no leading or trailing
/// slash). Used by [`Filesystem::ino_lookup_blocking`] to assemble
/// the result of an `INODE_REF` walk.
fn join_path(components: &[Vec<u8>]) -> Vec<u8> {
    let total = components.iter().map(Vec::len).sum::<usize>()
        + components.len().saturating_sub(1);
    let mut out: Vec<u8> = Vec::with_capacity(total);
    for (i, c) in components.iter().enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(c);
    }
    out
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

/// Resolve the parent of a subvolume root via `ROOT_BACKREF` in the
/// root tree. Returns `Some(parent_inode)` where `parent_inode` is
/// the directory in the parent subvolume that contains this one,
/// or `None` for top-level subvolumes (no `ROOT_BACKREF`, e.g. the
/// default `FS_TREE`).
fn find_root_backref_parent<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    root_tree_logical: u64,
    child_subvol: u64,
) -> io::Result<Option<Inode>> {
    let mut found = None;
    for_each_item(reader, root_tree_logical, |key, data| {
        if found.is_some() {
            return;
        }
        if key.objectid == child_subvol && key.key_type == KeyType::RootBackref
        {
            if let Some(rr) = RootRef::parse(data) {
                found = Some(Inode {
                    subvol: SubvolId(key.offset),
                    ino: rr.dirid,
                });
            }
        }
    })?;
    Ok(found)
}

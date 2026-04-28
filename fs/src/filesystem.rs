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
    CacheConfig, Entry, FileKind, Stat,
    cache::{ExtentMapCache, InodeCache, LruTreeBlockCache},
    dir, read,
    stat::to_system_time,
    xattr,
};
use btrfs_disk::{
    items::{
        DeviceItem, DirItem, FileExtentBody, InodeExtref, InodeItem, InodeRef,
        RootItem, RootItemFlags, RootRef, Timespec as DiskTimespec,
    },
    reader::{BlockReader, Traversal, filesystem_open, tree_walk},
    superblock::Superblock,
    tree::{KeyType, TreeBlock},
};
use btrfs_stream::{StreamCommand, StreamWriter, Timespec as StreamTimespec};
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

/// Stream version emitted by [`Filesystem::send`]. v1 is the
/// conservative pick — every byte goes through plain `WRITE`, no
/// encoded passthrough, no clone refs. Maximum compatibility with
/// receivers in the wild.
const SEND_STREAM_VERSION: u32 = 1;

/// Maximum bytes per `WRITE` command on the v1 stream. The TLV
/// length field is `u16`, so a strict upper bound is 65 535 bytes;
/// we leave headroom for the path/offset attributes plus the
/// framed-command overhead. 48 KiB is what the kernel uses.
const SEND_WRITE_CHUNK_BYTES: usize = 48 * 1024;

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

/// Compound-key range filter for [`Filesystem::tree_search`]. Mirrors
/// the kernel's `btrfs_ioctl_search_key` semantics: items are returned
/// where `(min_objectid, min_type, min_offset) <= (key) <= (max_objectid,
/// max_type, max_offset)` treated as a single 17-byte compound key, AND
/// the leaf's generation falls in `[min_transid, max_transid]`.
#[derive(Debug, Clone, Copy)]
pub struct SearchFilter {
    /// Tree to search (e.g. `1` for root tree, `5` for default FS tree,
    /// or any subvolume id).
    pub tree_id: u64,
    pub min_objectid: u64,
    pub max_objectid: u64,
    pub min_type: u32,
    pub max_type: u32,
    pub min_offset: u64,
    pub max_offset: u64,
    pub min_transid: u64,
    pub max_transid: u64,
    /// Maximum items to return.
    pub max_items: u32,
}

/// One item returned by [`Filesystem::tree_search`]. `transid` is the
/// leaf's generation (matching the kernel ioctl's
/// `btrfs_ioctl_search_header.transid`).
#[derive(Debug, Clone)]
pub struct SearchItem {
    pub transid: u64,
    pub objectid: u64,
    pub item_type: u32,
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Whence value for [`Filesystem::seek_hole_data`]. Maps to the
/// POSIX `SEEK_HOLE` / `SEEK_DATA` whence constants used by
/// `lseek(2)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekHoleData {
    /// `SEEK_DATA`: return the offset of the start of the next data
    /// region at or after the given offset.
    Data,
    /// `SEEK_HOLE`: return the offset of the start of the next hole
    /// at or after the given offset. Always succeeds within the
    /// file because EOF is treated as a virtual hole.
    Hole,
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
    /// device, with default cache sizes.
    ///
    /// This is sync because the heavy work happens during the bootstrap
    /// (chunk tree walk, root tree walk) and only runs once. Embedders
    /// that want non-blocking open can wrap the call in
    /// `tokio::task::spawn_blocking` themselves.
    pub fn open(reader: R) -> io::Result<Self> {
        Self::open_inner(
            reader,
            SubvolId(FS_TREE_OBJECTID),
            CacheConfig::default(),
        )
    }

    /// Bootstrap the filesystem and select a non-default subvolume
    /// as the [`Filesystem::root`], with default cache sizes.
    ///
    /// `subvol` must be the tree id of an existing subvolume — pass
    /// the value from a previously-listed [`SubvolInfo::id`], or use
    /// `SubvolId(5)` to get the default. Errors with `NotFound` if
    /// the id is unknown, `InvalidInput` if it's outside the
    /// subvolume id range.
    pub fn open_subvol(reader: R, subvol: SubvolId) -> io::Result<Self> {
        Self::open_inner(reader, subvol, CacheConfig::default())
    }

    /// Like [`Filesystem::open`] but with caller-chosen cache sizes.
    /// Use [`CacheConfig::no_cache`] for benchmarking the cold path
    /// or memory-constrained embedders; otherwise tune the
    /// individual entries to match your workload.
    pub fn open_with_caches(
        reader: R,
        cache_config: CacheConfig,
    ) -> io::Result<Self> {
        Self::open_inner(reader, SubvolId(FS_TREE_OBJECTID), cache_config)
    }

    /// Like [`Filesystem::open_subvol`] but with caller-chosen cache
    /// sizes.
    pub fn open_subvol_with_caches(
        reader: R,
        subvol: SubvolId,
        cache_config: CacheConfig,
    ) -> io::Result<Self> {
        Self::open_inner(reader, subvol, cache_config)
    }

    fn open_inner(
        reader: R,
        default_subvol: SubvolId,
        cache_config: CacheConfig,
    ) -> io::Result<Self> {
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
            Arc::new(LruTreeBlockCache::new(cache_config.tree_blocks));
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
                inode_cache: InodeCache::new(cache_config.inodes),
                extent_map_cache: ExtentMapCache::new(cache_config.extent_maps),
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

    /// Run a tree search matching the kernel's `BTRFS_IOC_TREE_SEARCH_V2`
    /// semantics. Returns at most `filter.max_items` items, stopping
    /// early if `max_buf_size` (the userspace buffer cap, including
    /// per-item 32-byte headers) would be exceeded by adding the next
    /// item.
    ///
    /// Note: the kernel runs the search against any tree by id; we
    /// only resolve subvolume trees and the root tree (`tree_id == 1`).
    /// Searches against the chunk/extent/csum/etc. trees would need
    /// additional plumbing — they're not exposed today.
    pub async fn tree_search(
        &self,
        filter: SearchFilter,
        max_buf_size: usize,
    ) -> io::Result<Vec<SearchItem>> {
        let this = self.clone();
        spawn_blocking(move || this.tree_search_blocking(filter, max_buf_size))
            .await
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

    /// Resolve every path in `subvol` that names `objectid`. A regular
    /// inode has a single path; hard-linked files have one entry per
    /// link, in unspecified order. Returns an empty vector for an
    /// orphan inode (no `INODE_REF` / `INODE_EXTREF`).
    ///
    /// Each returned path is relative to the subvolume root, with no
    /// leading slash.
    pub async fn ino_paths(
        &self,
        subvol: SubvolId,
        objectid: u64,
    ) -> io::Result<Vec<Vec<u8>>> {
        let this = self.clone();
        spawn_blocking(move || this.ino_paths_blocking(subvol, objectid)).await
    }

    /// Filesystem sectorsize.
    #[must_use]
    pub fn blksize(&self) -> u32 {
        self.inner.blksize
    }

    /// Drop cached state for `ino` from both the inode and
    /// extent-map caches. Embedders that observe inode-level
    /// invalidation events (FUSE `forget`, manual cache pressure)
    /// can call this to release memory ahead of LRU eviction.
    /// Safe to call for an inode that's never been cached: it's a
    /// no-op in that case.
    pub fn forget(&self, ino: Inode) {
        self.inner.inode_cache.invalidate(ino);
        self.inner.extent_map_cache.invalidate(ino);
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

    /// Like [`Filesystem::readdir`] but pairs each [`Entry`] with its
    /// [`Stat`] so callers don't need a separate `getattr` per entry.
    /// The FUSE driver feeds this into the kernel's `READDIRPLUS`
    /// path, which collapses `ls -l`-style listings into one round
    /// trip.
    ///
    /// Entries that vanish between the directory walk and the stat
    /// (effectively impossible on a read-only mount, but defended
    /// for robustness) are dropped from the result rather than
    /// erroring.
    pub async fn readdirplus(
        &self,
        dir_ino: Inode,
        offset: u64,
    ) -> io::Result<Vec<(Entry, Stat)>> {
        let this = self.clone();
        spawn_blocking(move || this.readdirplus_blocking(dir_ino, offset)).await
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

    /// Generate a v1 send stream describing `snapshot` and write it
    /// to `output`. Tier 1 of the send roadmap: full sends only
    /// (no `parent`), no clone sources, no encoded-write
    /// passthrough.
    ///
    /// The stream begins with a `SUBVOL` command, then walks the
    /// subvolume tree path-first emitting per-inode creation
    /// commands (`Mkfile` / `Mkdir` / `Symlink` / `Mknod` / `Mkfifo`
    /// / `Mksock`), `SetXattr` for each xattr, `Write` chunks for
    /// regular file contents, and `Truncate` / `Chmod` / `Chown` /
    /// `Utimes` to finalise. Terminates with `End`.
    ///
    /// Hardlinks beyond the first reference become `Link` commands
    /// rather than re-creating the inode. Subvolume crossings
    /// (`DirItem` whose `location.key_type == ROOT_ITEM`) are
    /// skipped — caller must run `send` again per subvolume.
    ///
    /// Encodes paths as UTF-8 (lossy on invalid byte sequences).
    /// Real btrfs filenames are arbitrary bytes; full-fidelity
    /// non-UTF-8 support can come later if a real workload needs
    /// it.
    ///
    /// # Errors
    ///
    /// Returns an error if the subvolume isn't found, any tree
    /// read fails, or the underlying writer fails.
    pub async fn send<W: io::Write + Send + 'static>(
        &self,
        snapshot: SubvolId,
        output: W,
    ) -> io::Result<W> {
        let this = self.clone();
        spawn_blocking(move || this.send_blocking(snapshot, output)).await
    }

    /// Find the next hole or data region in `ino` at or after
    /// `offset`. Mirrors `lseek(fd, offset, SEEK_HOLE)` /
    /// `lseek(fd, offset, SEEK_DATA)` semantics.
    ///
    /// `SEEK_DATA` returns the offset of the next byte that is part
    /// of a data region. Returns `Err(ENXIO)` if no data exists at
    /// or after `offset` (e.g. `offset >= file_size`).
    ///
    /// `SEEK_HOLE` returns the offset of the next hole. EOF is
    /// always considered a virtual hole, so this succeeds for any
    /// `offset < file_size`. Returns `Err(ENXIO)` only when
    /// `offset >= file_size`.
    ///
    /// Holes include both implicit (gaps with no `EXTENT_DATA` item)
    /// and explicit (regular extent with `disk_bytenr == 0`)
    /// representations. Inline and prealloc extents are treated as
    /// data — matching kernel btrfs and POSIX convention.
    pub async fn seek_hole_data(
        &self,
        ino: Inode,
        offset: u64,
        whence: SeekHoleData,
    ) -> io::Result<u64> {
        let this = self.clone();
        spawn_blocking(move || {
            this.seek_hole_data_blocking(ino, offset, whence)
        })
        .await
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

    fn readdirplus_blocking(
        &self,
        dir_ino: Inode,
        offset: u64,
    ) -> io::Result<Vec<(Entry, Stat)>> {
        let entries = self.readdir_blocking(dir_ino, offset)?;
        let blksize = self.inner.blksize;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            // `read_inode_item_blocking` consults the inode cache
            // first, so repeated `readdirplus` over the same dir
            // pays at most one tree walk per inode across the
            // working set.
            if let Some(item) = self.read_inode_item_blocking(entry.ino)? {
                let stat = Stat::from_inode(entry.ino, &item, blksize);
                out.push((entry, stat));
            }
        }
        Ok(out)
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

    fn send_blocking<W: io::Write>(
        &self,
        snapshot: SubvolId,
        output: W,
    ) -> io::Result<W> {
        let info = self
            .list_subvolumes_blocking()?
            .into_iter()
            .find(|s| s.id == snapshot)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("subvolume {} not found", snapshot.0),
                )
            })?;

        let mut writer = StreamWriter::new(output, SEND_STREAM_VERSION)?;

        // The SUBVOL command names the receive-side directory. We use
        // the subvolume's recorded name; for the default `FS_TREE`
        // (no name) fall back to a synthetic identifier so receive
        // has something to mkdir with.
        let subvol_path = if info.name.is_empty() {
            format!("subvol-{}", info.id.0)
        } else {
            String::from_utf8_lossy(&info.name).into_owned()
        };
        writer.write_command(&StreamCommand::Subvol {
            path: subvol_path,
            uuid: info.uuid,
            ctransid: info.ctransid,
        })?;

        // Walk the subvolume tree starting at the root directory.
        // `seen` tracks inodes we've already created so a second
        // `INODE_REF` for the same inode emits `Link` instead of a
        // duplicate creation. Stores the first emitted path so the
        // `Link` target is reachable.
        let root = Inode {
            subvol: snapshot,
            ino: ROOT_DIR_OBJECTID,
        };
        let mut seen: BTreeMap<u64, String> = BTreeMap::new();
        seen.insert(ROOT_DIR_OBJECTID, String::new());
        self.send_dir_recursive(&mut writer, root, "", &mut seen)?;

        writer.write_command(&StreamCommand::End)?;
        writer.finish()
    }

    fn send_dir_recursive<W: io::Write>(
        &self,
        writer: &mut StreamWriter<W>,
        dir: Inode,
        dir_path: &str,
        seen: &mut BTreeMap<u64, String>,
    ) -> io::Result<()> {
        // Skip `.` and `..` (offsets 0 and 1) — those are synthetic.
        let entries = self.readdir_blocking(dir, 1)?;
        let mut subdirs: Vec<(Inode, String)> = Vec::new();
        for entry in entries {
            // Skip the synthetic `.` / `..` slots.
            if entry.name == b"." || entry.name == b".." {
                continue;
            }
            // Subvolume crossings are out of scope for tier 1: the
            // child subvolume is a separate tree and would need its
            // own SUBVOL/SNAPSHOT command. Caller can re-invoke
            // send() on each subvol they want.
            if entry.ino.subvol != dir.subvol {
                continue;
            }
            let entry_name = String::from_utf8_lossy(&entry.name).into_owned();
            let entry_path = if dir_path.is_empty() {
                entry_name
            } else {
                format!("{dir_path}/{entry_name}")
            };

            // Hardlink case: we've already emitted the inode under
            // its first path. Just attach a Link and move on.
            if let Some(first_path) = seen.get(&entry.ino.ino) {
                writer.write_command(&StreamCommand::Link {
                    path: entry_path.clone(),
                    target: first_path.clone(),
                })?;
                continue;
            }
            let item =
                self.read_inode_item_blocking(entry.ino)?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("inode {} item missing", entry.ino.ino),
                    )
                })?;
            seen.insert(entry.ino.ino, entry_path.clone());

            self.send_create_command(writer, &entry, &entry_path, &item)?;
            self.send_xattrs(writer, entry.ino, &entry_path)?;
            if entry.kind == FileKind::RegularFile {
                self.send_file_data(writer, entry.ino, &entry_path, item.size)?;
                writer.write_command(&StreamCommand::Truncate {
                    path: entry_path.clone(),
                    size: item.size,
                })?;
            }
            send_metadata(writer, &entry_path, &item)?;

            // Defer recursion until after we've finished this
            // directory's own entries — keeps the per-dir command
            // ordering cleaner.
            if entry.kind == FileKind::Directory {
                subdirs.push((entry.ino, entry_path));
            }
        }
        for (subdir_ino, subdir_path) in subdirs {
            self.send_dir_recursive(writer, subdir_ino, &subdir_path, seen)?;
        }
        Ok(())
    }

    fn send_create_command<W: io::Write>(
        &self,
        writer: &mut StreamWriter<W>,
        entry: &Entry,
        path: &str,
        item: &InodeItem,
    ) -> io::Result<()> {
        let cmd = match entry.kind {
            FileKind::RegularFile => {
                StreamCommand::Mkfile { path: path.into() }
            }
            FileKind::Directory => StreamCommand::Mkdir { path: path.into() },
            FileKind::Symlink => {
                let target =
                    self.readlink_blocking(entry.ino)?.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("symlink {} target missing", entry.ino.ino),
                        )
                    })?;
                StreamCommand::Symlink {
                    path: path.into(),
                    target: String::from_utf8_lossy(&target).into_owned(),
                }
            }
            FileKind::NamedPipe => StreamCommand::Mkfifo { path: path.into() },
            FileKind::Socket => StreamCommand::Mksock { path: path.into() },
            FileKind::BlockDevice | FileKind::CharDevice => {
                StreamCommand::Mknod {
                    path: path.into(),
                    mode: u64::from(item.mode),
                    rdev: item.rdev,
                }
            }
        };
        writer.write_command(&cmd)
    }

    fn send_xattrs<W: io::Write>(
        &self,
        writer: &mut StreamWriter<W>,
        ino: Inode,
        path: &str,
    ) -> io::Result<()> {
        for name in self.xattr_list_blocking(ino)? {
            let Some(data) = self.xattr_get_blocking(ino, &name)? else {
                continue;
            };
            writer.write_command(&StreamCommand::SetXattr {
                path: path.into(),
                name: String::from_utf8_lossy(&name).into_owned(),
                data,
            })?;
        }
        Ok(())
    }

    /// Emit `Write` commands covering `[0, size)` of `ino` in
    /// chunks that fit comfortably in v1's u16 TLV length field
    /// (we cap at [`SEND_WRITE_CHUNK_BYTES`] to leave headroom for
    /// the path/offset attributes). `read_blocking` materialises
    /// holes and prealloc as zeros and decompresses any compressed
    /// extents, so `data` is always plain bytes.
    fn send_file_data<W: io::Write>(
        &self,
        writer: &mut StreamWriter<W>,
        ino: Inode,
        path: &str,
        size: u64,
    ) -> io::Result<()> {
        let mut offset = 0u64;
        while offset < size {
            let remaining = size - offset;
            #[allow(clippy::cast_possible_truncation)]
            let chunk = remaining.min(SEND_WRITE_CHUNK_BYTES as u64) as u32;
            let data = self.read_blocking(ino, offset, chunk)?;
            if data.is_empty() {
                break;
            }
            writer.write_command(&StreamCommand::Write {
                path: path.into(),
                offset,
                data,
            })?;
            offset += u64::from(chunk);
        }
        Ok(())
    }

    fn seek_hole_data_blocking(
        &self,
        ino: Inode,
        offset: u64,
        whence: SeekHoleData,
    ) -> io::Result<u64> {
        let item = self.read_inode_item_blocking(ino)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("inode {} not found", ino.ino),
            )
        })?;
        let file_size = item.size;

        // POSIX: any whence at or past EOF is ENXIO. Linux returns
        // ENXIO for both SEEK_HOLE and SEEK_DATA in that case.
        if offset >= file_size {
            return Err(io::Error::from_raw_os_error(libc::ENXIO));
        }

        let tree_root = self.tree_root_for(ino.subvol)?;
        let extent_map = self.extent_map_for(ino, tree_root)?;
        let want_hole = matches!(whence, SeekHoleData::Hole);

        // Walk records in file_pos order, classifying each region as
        // data or hole. An implicit hole sits before any record whose
        // file_pos > cursor; a regular extent with disk_bytenr == 0
        // is an explicit hole; inline and prealloc count as data.
        let mut cursor = 0u64;
        for r in &extent_map.records {
            // Implicit hole [cursor, r.file_pos).
            if r.file_pos > cursor {
                let hole_end = r.file_pos.min(file_size);
                if hole_end > offset && want_hole {
                    return Ok(offset.max(cursor));
                }
                cursor = hole_end;
                if cursor >= file_size {
                    break;
                }
            }
            let body_len = match &r.item.body {
                FileExtentBody::Inline { .. } => r.item.ram_bytes,
                FileExtentBody::Regular { num_bytes, .. } => *num_bytes,
            };
            let r_start = r.file_pos.max(cursor);
            let r_end = (r.file_pos + body_len).min(file_size);
            if r_end <= r_start {
                continue;
            }
            let r_is_hole = matches!(
                &r.item.body,
                FileExtentBody::Regular { disk_bytenr: 0, .. },
            );
            if r_end > offset && r_is_hole == want_hole {
                return Ok(offset.max(r_start));
            }
            cursor = r_end;
            if cursor >= file_size {
                break;
            }
        }

        // Past every record, the rest of the file (if any) is a
        // trailing implicit hole. SEEK_HOLE additionally treats EOF
        // itself as a virtual hole, so it always finds *something*
        // for any offset within the file.
        if want_hole {
            if cursor < file_size && cursor > offset {
                Ok(cursor)
            } else if offset < file_size {
                // No real hole found; report the virtual EOF hole.
                Ok(file_size)
            } else {
                Err(io::Error::from_raw_os_error(libc::ENXIO))
            }
        } else {
            Err(io::Error::from_raw_os_error(libc::ENXIO))
        }
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

    fn tree_search_blocking(
        &self,
        filter: SearchFilter,
        max_buf_size: usize,
    ) -> io::Result<Vec<SearchItem>> {
        // The root tree itself is at superblock.root; subvolume trees
        // are looked up via tree_root_for. tree_id == 1 is the root
        // tree (BTRFS_ROOT_TREE_OBJECTID). Anything else has to be a
        // subvolume id we know about.
        // sizeof(btrfs_ioctl_search_header).
        const HEADER_SIZE: usize = 32;

        let tree_root = if filter.tree_id == 1 {
            self.inner.superblock.root
        } else {
            self.tree_root_for(SubvolId(filter.tree_id))?
        };
        let min = (filter.min_objectid, filter.min_type, filter.min_offset);
        let max = (filter.max_objectid, filter.max_type, filter.max_offset);

        let mut results: Vec<SearchItem> = Vec::new();
        let mut buf_used: usize = 0;
        let mut reader = self.lock_reader();
        tree_walk(&mut reader, tree_root, Traversal::Dfs, &mut |block| {
            if results.len() >= filter.max_items as usize {
                return;
            }
            let TreeBlock::Leaf {
                items,
                data,
                header,
            } = block
            else {
                return;
            };
            let leaf_transid = header.generation;
            if leaf_transid < filter.min_transid
                || leaf_transid > filter.max_transid
            {
                return;
            }
            let hdr_size = mem::size_of::<btrfs_disk::raw::btrfs_header>();
            for item in items {
                if results.len() >= filter.max_items as usize {
                    return;
                }
                let key = &item.key;
                let item_type = u32::from(key.key_type.to_raw());
                let compound = (key.objectid, item_type, key.offset);
                if compound < min || compound > max {
                    continue;
                }
                let start = hdr_size + item.offset as usize;
                let end = start + item.size as usize;
                if end > data.len() {
                    continue;
                }
                let payload = &data[start..end];
                let next_used = buf_used + HEADER_SIZE + payload.len();
                if next_used > max_buf_size {
                    // Stop entirely — no more items will fit.
                    return;
                }
                results.push(SearchItem {
                    transid: leaf_transid,
                    objectid: key.objectid,
                    item_type,
                    offset: key.offset,
                    data: payload.to_vec(),
                });
                buf_used = next_used;
            }
        })?;
        Ok(results)
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

    fn ino_paths_blocking(
        &self,
        subvol: SubvolId,
        objectid: u64,
    ) -> io::Result<Vec<Vec<u8>>> {
        // The subvolume root has no INODE_REF; the empty path names it.
        if objectid == ROOT_DIR_OBJECTID {
            return Ok(vec![Vec::new()]);
        }
        let tree_root = self.tree_root_for(subvol)?;
        // Collect all (parent_dirid, name) pairs in one tree walk.
        // INODE_REF packs every link to the same parent dir into one
        // item; INODE_EXTREF holds links whose name+parent pair didn't
        // fit (typically across many parents), with the parent stored
        // in the struct rather than the key offset.
        let mut refs: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut reader = self.lock_reader();
        for_each_item(&mut reader, tree_root, |key, data| {
            if key.objectid != objectid {
                return;
            }
            match key.key_type {
                KeyType::InodeRef => {
                    for iref in InodeRef::parse_all(data) {
                        refs.push((key.offset, iref.name));
                    }
                }
                KeyType::InodeExtref => {
                    for eref in InodeExtref::parse_all(data) {
                        refs.push((eref.parent, eref.name));
                    }
                }
                _ => {}
            }
        })?;
        drop(reader);

        // For each (parent, name) prepend the parent's path. We reuse
        // ino_lookup_blocking which re-acquires the reader lock, so it
        // matters that the lock is released above.
        let mut paths = Vec::with_capacity(refs.len());
        for (parent, name) in refs {
            let Some(parent_path) = self.ino_lookup_blocking(subvol, parent)?
            else {
                continue;
            };
            let mut p = parent_path;
            if !p.is_empty() {
                p.push(b'/');
            }
            p.extend_from_slice(&name);
            paths.push(p);
        }
        Ok(paths)
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

/// Convert a btrfs on-disk [`DiskTimespec`] into the
/// [`StreamTimespec`] shape carried by send-stream `Utimes`
/// commands. Both types are `(sec: u64, nsec: u32)` — separate
/// types for type-system hygiene rather than wire-level
/// difference.
fn to_stream_timespec(t: &DiskTimespec) -> StreamTimespec {
    StreamTimespec {
        sec: t.sec,
        nsec: t.nsec,
    }
}

/// Emit the trailing `Chown`/`Chmod`/`Utimes` triple every inode
/// gets after creation. Free function rather than a method since
/// it doesn't touch the [`Filesystem`] state — only forwards
/// fields from the inode item we've already loaded.
fn send_metadata<W: io::Write>(
    writer: &mut StreamWriter<W>,
    path: &str,
    item: &InodeItem,
) -> io::Result<()> {
    writer.write_command(&StreamCommand::Chown {
        path: path.into(),
        uid: u64::from(item.uid),
        gid: u64::from(item.gid),
    })?;
    writer.write_command(&StreamCommand::Chmod {
        path: path.into(),
        // Strip the file-type bits; receive applies these as
        // permission/setuid/setgid/sticky only.
        mode: u64::from(item.mode & 0o7777),
    })?;
    writer.write_command(&StreamCommand::Utimes {
        path: path.into(),
        atime: to_stream_timespec(&item.atime),
        mtime: to_stream_timespec(&item.mtime),
        ctime: to_stream_timespec(&item.ctime),
    })?;
    Ok(())
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

//! The [`Filesystem`] type and its operation methods.
//!
//! [`Filesystem`] is `Clone` and exposes all operations through `&self`,
//! so embedders can hold an `Arc`-like handle and call concurrently
//! from multiple threads. The current implementation serialises I/O
//! behind a single `Mutex<BlockReader<R>>`; future work (per-thread
//! readers, lock-free cache hits) is internal and won't change the API.

use crate::{Entry, FileKind, Stat, dir, read, xattr};
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
/// operations take `&self`, so multiple threads can drive the same
/// filesystem concurrently. Today, I/O still serialises on a single
/// internal mutex; that's an implementation detail that can be relaxed
/// later (per-thread readers, RAII cache hits) without an API change.
pub struct Filesystem<R: io::Read + io::Seek + Send> {
    inner: Arc<Inner<R>>,
}

impl<R: io::Read + io::Seek + Send> Clone for Filesystem<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Shared state behind every [`Filesystem`] handle.
struct Inner<R: io::Read + io::Seek + Send> {
    /// Parsed primary-device superblock.
    superblock: Superblock,
    /// Map of tree id → (root block logical address, key offset). Used
    /// for resolving subvolume tree roots; multi-subvolume support will
    /// look up additional entries here.
    tree_roots: BTreeMap<u64, (u64, u64)>,
    /// Cached objectid of the default subvolume.
    default_subvol: SubvolId,
    /// Filesystem sectorsize, forwarded from the superblock.
    blksize: u32,
    /// I/O backend. The lock serialises all on-disk reads; future work
    /// can swap this for a pool of readers without changing the public
    /// `&self` API.
    reader: Mutex<BlockReader<R>>,
}

impl<R: io::Read + io::Seek + Send> Filesystem<R> {
    /// Bootstrap the filesystem from a reader over an image or block device.
    pub fn open(reader: R) -> io::Result<Self> {
        let fs = filesystem_open(reader)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let blksize = fs.superblock.sectorsize;
        if !fs.tree_roots.contains_key(&FS_TREE_OBJECTID) {
            return Err(io::Error::other(
                "default FS tree (objectid 5) not found in root tree",
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                superblock: fs.superblock,
                tree_roots: fs.tree_roots,
                default_subvol: SubvolId(FS_TREE_OBJECTID),
                blksize,
                reader: Mutex::new(fs.reader),
            }),
        })
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
    ///
    /// Returns the child inode and its parsed [`InodeItem`], or `None` if
    /// no entry with that name exists.
    pub fn lookup(
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
        let Some(item) = read_inode(&mut reader, tree_root, child_oid)? else {
            return Ok(None);
        };
        let child = Inode {
            subvol: parent.subvol,
            ino: child_oid,
        };
        Ok(Some((child, item)))
    }

    /// Read the inode item for `ino`.
    pub fn read_inode_item(&self, ino: Inode) -> io::Result<Option<InodeItem>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        read_inode(&mut reader, tree_root, ino.ino)
    }

    /// Read inode metadata as a [`Stat`].
    pub fn getattr(&self, ino: Inode) -> io::Result<Option<Stat>> {
        Ok(self
            .read_inode_item(ino)?
            .map(|item| Stat::from_inode(ino, &item, self.inner.blksize)))
    }

    /// List the entries of a directory inode, starting strictly after
    /// `offset`. `.` and `..` are synthesised at offsets 0 and 1.
    pub fn readdir(
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

    /// Read the target of a symbolic link.
    ///
    /// Returns `None` if the inode has no inline extent data or does not
    /// exist. The result is trimmed to the inode's `size`, since
    /// `mkfs.btrfs --rootdir` stores a trailing NUL after the target in
    /// the inline extent payload.
    pub fn readlink(&self, ino: Inode) -> io::Result<Option<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let blksize = self.inner.blksize;
        let mut reader = self.lock_reader();
        let Some(item) = read_inode(&mut reader, tree_root, ino.ino)? else {
            return Ok(None);
        };
        let target =
            read::read_symlink(&mut reader, tree_root, ino.ino, blksize)?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(target.map(|mut t| {
            t.truncate(item.size as usize);
            t
        }))
    }

    /// Read `size` bytes from `ino` starting at `offset`.
    ///
    /// Sparse holes and prealloc extents return zeros; compressed extents
    /// are decompressed.
    pub fn read(
        &self,
        ino: Inode,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let blksize = self.inner.blksize;
        let mut reader = self.lock_reader();
        let Some(item) = read_inode(&mut reader, tree_root, ino.ino)? else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("inode {} not found", ino.ino),
            ));
        };
        read::read_file(
            &mut reader,
            tree_root,
            ino.ino,
            item.size,
            offset,
            size,
            blksize,
        )
    }

    /// List all xattr names for an inode.
    pub fn xattr_list(&self, ino: Inode) -> io::Result<Vec<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        xattr::list_xattrs(&mut reader, tree_root, ino.ino)
    }

    /// Look up the value of a single xattr by exact name.
    pub fn xattr_get(
        &self,
        ino: Inode,
        name: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let mut reader = self.lock_reader();
        xattr::get_xattr(&mut reader, tree_root, ino.ino, name)
    }

    /// Filesystem-wide statistics pulled straight from the superblock.
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

    /// Acquire the I/O lock. Helper that forwards a poisoned mutex to a
    /// caller without unwrapping at every call site.
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

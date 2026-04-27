//! The [`Filesystem`] type and its operation methods.

use crate::{Entry, FileKind, Stat, dir, read, xattr};
use btrfs_disk::{
    items::{DirItem, InodeItem},
    reader::{OpenFilesystem, Traversal, filesystem_open, tree_walk},
    tree::{KeyType, TreeBlock},
};
use std::{io, mem};

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
///
/// `Inode` is opaque to the FUSE protocol; embedders translate to/from
/// FUSE's flat `u64` at the boundary.
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
/// Wraps a [`btrfs_disk::reader::OpenFilesystem`] and a small amount of
/// derived state (default subvolume root, sectorsize) and exposes the
/// operations a userspace driver needs.
///
/// Operations take `&mut self` because the underlying [`btrfs_disk`]
/// reader is single-threaded; embedders that need shared access wrap
/// the [`Filesystem`] in a `Mutex`.
pub struct Filesystem<R: io::Read + io::Seek> {
    fs: OpenFilesystem<R>,
    default_subvol: SubvolId,
    fs_tree_root_logical: u64,
    blksize: u32,
}

impl<R: io::Read + io::Seek> Filesystem<R> {
    /// Bootstrap the filesystem from a reader over an image or block device.
    pub fn open(reader: R) -> io::Result<Self> {
        let fs = filesystem_open(reader)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let blksize = fs.superblock.sectorsize;
        let fs_tree_root_logical = fs
            .tree_roots
            .get(&FS_TREE_OBJECTID)
            .map(|(logical, _)| *logical)
            .ok_or_else(|| {
                io::Error::other(
                    "default FS tree (objectid 5) not found in root tree",
                )
            })?;
        Ok(Self {
            fs,
            default_subvol: SubvolId(FS_TREE_OBJECTID),
            fs_tree_root_logical,
            blksize,
        })
    }

    /// Inode of the default subvolume's root directory (objectid 256).
    #[must_use]
    pub fn root(&self) -> Inode {
        Inode {
            subvol: self.default_subvol,
            ino: ROOT_DIR_OBJECTID,
        }
    }

    /// Filesystem sectorsize.
    #[must_use]
    pub fn blksize(&self) -> u32 {
        self.blksize
    }

    /// Look up a child of `parent` by name.
    ///
    /// Returns the child inode and its parsed [`InodeItem`], or `None` if
    /// no entry with that name exists.
    pub fn lookup(
        &mut self,
        parent: Inode,
        name: &[u8],
    ) -> io::Result<Option<(Inode, InodeItem)>> {
        let tree_root = self.tree_root_for(parent.subvol)?;
        let Some(entry) =
            lookup_in_dir(&mut self.fs.reader, tree_root, parent.ino, name)?
        else {
            return Ok(None);
        };
        let child_oid = entry.location.objectid;
        let Some(item) = read_inode(&mut self.fs.reader, tree_root, child_oid)?
        else {
            return Ok(None);
        };
        let child = Inode {
            subvol: parent.subvol,
            ino: child_oid,
        };
        Ok(Some((child, item)))
    }

    /// Read the inode item for `ino`.
    pub fn read_inode_item(
        &mut self,
        ino: Inode,
    ) -> io::Result<Option<InodeItem>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        read_inode(&mut self.fs.reader, tree_root, ino.ino)
    }

    /// Read inode metadata as a [`Stat`].
    pub fn getattr(&mut self, ino: Inode) -> io::Result<Option<Stat>> {
        Ok(self
            .read_inode_item(ino)?
            .map(|item| Stat::from_inode(ino, &item, self.blksize)))
    }

    /// List the entries of a directory inode, starting strictly after
    /// `offset`. `.` and `..` are synthesised at offsets 0 and 1.
    pub fn readdir(
        &mut self,
        dir_ino: Inode,
        offset: u64,
    ) -> io::Result<Vec<Entry>> {
        let tree_root = self.tree_root_for(dir_ino.subvol)?;
        let mut entries: Vec<Entry> = Vec::new();

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
                find_parent_oid(&mut self.fs.reader, tree_root, dir_ino.ino)?;
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
        for_each_item(&mut self.fs.reader, tree_root, |key, data| {
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
    pub fn readlink(&mut self, ino: Inode) -> io::Result<Option<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let Some(item) = read_inode(&mut self.fs.reader, tree_root, ino.ino)?
        else {
            return Ok(None);
        };
        let target = read::read_symlink(
            &mut self.fs.reader,
            tree_root,
            ino.ino,
            self.blksize,
        )?;
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
        &mut self,
        ino: Inode,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        let Some(item) = read_inode(&mut self.fs.reader, tree_root, ino.ino)?
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("inode {} not found", ino.ino),
            ));
        };
        read::read_file(
            &mut self.fs.reader,
            tree_root,
            ino.ino,
            item.size,
            offset,
            size,
            self.blksize,
        )
    }

    /// List all xattr names for an inode.
    pub fn xattr_list(&mut self, ino: Inode) -> io::Result<Vec<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        xattr::list_xattrs(&mut self.fs.reader, tree_root, ino.ino)
    }

    /// Look up the value of a single xattr by exact name.
    pub fn xattr_get(
        &mut self,
        ino: Inode,
        name: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let tree_root = self.tree_root_for(ino.subvol)?;
        xattr::get_xattr(&mut self.fs.reader, tree_root, ino.ino, name)
    }

    /// Filesystem-wide statistics pulled straight from the superblock.
    #[must_use]
    pub fn statfs(&self) -> StatFs {
        let sb = &self.fs.superblock;
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

    /// Map a [`SubvolId`] to its tree root logical address. v1 only knows
    /// about the default subvolume; multi-subvolume support will replace
    /// this with a lookup against `OpenFilesystem::tree_roots`.
    fn tree_root_for(&self, subvol: SubvolId) -> io::Result<u64> {
        if subvol == self.default_subvol {
            Ok(self.fs_tree_root_logical)
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
    reader: &mut btrfs_disk::reader::BlockReader<R>,
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
    reader: &mut btrfs_disk::reader::BlockReader<R>,
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
    reader: &mut btrfs_disk::reader::BlockReader<R>,
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
    reader: &mut btrfs_disk::reader::BlockReader<R>,
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

//! `BtrfsFuse`: the main filesystem type, its inherent operation methods,
//! and the thin `fuser::Filesystem` adapter.
//!
//! The inherent methods (`lookup_entry`, `get_attr`, `read_dir`,
//! `read_symlink`, `read_data`, `list_xattrs`, `get_xattr`, `stat_fs`)
//! return plain `std::io::Result` / `Option` values and can be driven
//! directly from tests. The `Filesystem` trait impl at the bottom of this
//! file is a narrow wrapper that maps each operation's return value to the
//! appropriate fuser `reply.*` call and maps errors to an `EIO`.
//!
//! All methods currently DFS the entire FS tree per call; once the driver
//! is stable we will replace the walks with a proper key-based descent
//! helper in `btrfs-disk` and cache decoded inodes.

use crate::{dir, inode, read, stat, xattr};
use anyhow::Result;
use btrfs_disk::{
    items::{DirItem, InodeItem},
    reader::{OpenFilesystem, Traversal, filesystem_open, tree_walk},
    tree::{KeyType, TreeBlock},
};
use fuser::{
    Errno, FileHandle, FileType, Filesystem, Generation, INodeNo, LockOwner,
    OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyStatfs,
    ReplyXattr, Request,
};
use std::{
    ffi::OsStr, fs::File, io, mem, os::unix::ffi::OsStrExt, sync::Mutex,
    time::Duration,
};

const TTL: Duration = Duration::from_secs(1);

/// Default FS tree objectid (`BTRFS_FS_TREE_OBJECTID`).
///
/// For v1 we always operate on this tree. Multi-subvolume support will
/// resolve the tree id from a `subvol=` mount option (or the superblock's
/// default subvolume) and store it on `BtrfsFuse`.
const FS_TREE_OBJECTID: u64 = 5;

/// Mutable filesystem state, behind a mutex because `fuser::Filesystem`
/// methods take `&self`.
struct State {
    fs: OpenFilesystem<File>,
    fs_tree_root: u64,
}

/// Filesystem-wide statistics returned by [`BtrfsFuse::stat_fs`].
///
/// Fields map directly onto the POSIX `statvfs` / `statfs` structures that
/// FUSE exposes via [`fuser::ReplyStatfs::statfs`], but as a plain struct
/// so callers can read them without going through the FUSE protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatfsInfo {
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

pub struct BtrfsFuse {
    state: Mutex<State>,
    blksize: u32,
}

impl BtrfsFuse {
    /// Bootstrap the filesystem from an open image file or block device.
    pub fn open(file: File) -> Result<Self> {
        let fs = filesystem_open(file)?;
        let blksize = fs.superblock.sectorsize;
        let fs_tree_root = fs
            .tree_roots
            .get(&FS_TREE_OBJECTID)
            .map(|(logical, _)| *logical)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "default FS tree (objectid 5) not found in root tree"
                )
            })?;
        Ok(Self {
            state: Mutex::new(State { fs, fs_tree_root }),
            blksize,
        })
    }

    /// Filesystem sectorsize, used by the inline `FileAttr` builder.
    #[must_use]
    pub fn blksize(&self) -> u32 {
        self.blksize
    }

    // -----------------------------------------------------------------
    // Operation layer — plain `io::Result` returns, no fuser involvement.
    // -----------------------------------------------------------------

    /// Look up a child of `parent` (a FUSE inode number) by name. Returns
    /// the child's FUSE inode number and parsed `InodeItem`, or `None` if
    /// no entry with that name exists.
    pub fn lookup_entry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> io::Result<Option<(u64, InodeItem)>> {
        let mut state = self.state.lock().unwrap();
        let parent_oid = inode::fuse_to_btrfs(parent);
        let Some(entry) = state.lookup_in_dir(parent_oid, name)? else {
            return Ok(None);
        };
        let child_oid = entry.location.objectid;
        let Some(item) = state.read_inode(child_oid)? else {
            return Ok(None);
        };
        Ok(Some((inode::btrfs_to_fuse(child_oid), item)))
    }

    /// Read the inode item for a FUSE inode number. Returns `None` if no
    /// matching inode exists.
    pub fn get_attr(&self, ino: u64) -> io::Result<Option<InodeItem>> {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino);
        state.read_inode(oid)
    }

    /// List the entries of a directory inode, starting strictly after
    /// `offset`. `.` and `..` are synthesised at offsets 0 and 1. Returns
    /// the full list in one shot; the caller is free to paginate.
    pub fn read_dir(
        &self,
        ino: u64,
        offset: u64,
    ) -> io::Result<Vec<dir::Entry>> {
        let mut state = self.state.lock().unwrap();
        let dir_oid = inode::fuse_to_btrfs(ino);
        let mut entries: Vec<dir::Entry> = Vec::new();

        if offset == 0 {
            entries.push(dir::Entry {
                ino,
                kind: FileType::Directory,
                name: b".".to_vec(),
                offset: 1,
            });
        }
        if offset <= 1 {
            let parent_oid = state.find_parent_oid(dir_oid)?;
            entries.push(dir::Entry {
                ino: inode::btrfs_to_fuse(parent_oid),
                kind: FileType::Directory,
                name: b"..".to_vec(),
                offset: 2,
            });
        }

        // Collect DIR_INDEX entries past `offset` in offset-sorted order.
        let cursor = offset.max(2);
        let mut dir_entries: Vec<dir::Entry> = Vec::new();
        state.for_each_item(|key, data| {
            if key.objectid != dir_oid || key.key_type != KeyType::DirIndex {
                return;
            }
            if key.offset < cursor {
                return;
            }
            for item in DirItem::parse_all(data) {
                let mut entry = dir::Entry::from_dir_item(&item, key.offset);
                // Cookie is "next offset to start from", so add 1.
                entry.offset = key.offset + 1;
                dir_entries.push(entry);
            }
        })?;
        dir_entries.sort_by_key(|e| e.offset);
        entries.extend(dir_entries);
        Ok(entries)
    }

    /// Read the target of a symbolic link. Returns `None` if the inode has
    /// no inline extent data or does not exist. The returned byte slice is
    /// trimmed to the authoritative length from the inode's `size` field,
    /// since `mkfs.btrfs --rootdir` stores a trailing NUL after the target
    /// in the inline extent payload (kernel-mounted btrfs relies on
    /// `inode.size` to find the valid range, not the extent length).
    pub fn read_symlink(&self, ino: u64) -> io::Result<Option<Vec<u8>>> {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino);
        let Some(inode_item) = state.read_inode(oid)? else {
            return Ok(None);
        };
        let fs_tree_root = state.fs_tree_root;
        let blksize = self.blksize;
        let target = read::read_symlink(
            &mut state.fs.reader,
            fs_tree_root,
            oid,
            blksize,
        )?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(target.map(|mut t| {
            t.truncate(inode_item.size as usize);
            t
        }))
    }

    /// Read `size` bytes from `ino` starting at `offset`. Returns the bytes
    /// that actually exist in the file (up to `file_size - offset`).
    /// Sparse holes and prealloc extents are returned as zeros; compressed
    /// extents are decompressed.
    pub fn read_data(
        &self,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino);
        let Some(item) = state.read_inode(oid)? else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("inode {ino} not found"),
            ));
        };
        let file_size = item.size;
        let fs_tree_root = state.fs_tree_root;
        let blksize = self.blksize;
        read::read_file(
            &mut state.fs.reader,
            fs_tree_root,
            oid,
            file_size,
            offset,
            size,
            blksize,
        )
    }

    /// List all xattr names for an inode.
    pub fn list_xattrs(&self, ino: u64) -> io::Result<Vec<Vec<u8>>> {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino);
        let fs_tree_root = state.fs_tree_root;
        xattr::list_xattrs(&mut state.fs.reader, fs_tree_root, oid)
    }

    /// Look up the value of a single xattr by exact name. Returns `None`
    /// if the xattr does not exist.
    pub fn get_xattr(
        &self,
        ino: u64,
        name: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino);
        let fs_tree_root = state.fs_tree_root;
        xattr::get_xattr(&mut state.fs.reader, fs_tree_root, oid, name)
    }

    /// Filesystem-wide statistics pulled straight from the superblock.
    #[must_use]
    pub fn stat_fs(&self) -> StatfsInfo {
        let state = self.state.lock().unwrap();
        let sb = &state.fs.superblock;
        let bsize = u64::from(sb.sectorsize);
        let blocks = sb.total_bytes / bsize;
        let bfree = sb.total_bytes.saturating_sub(sb.bytes_used) / bsize;
        StatfsInfo {
            blocks,
            bfree,
            bavail: bfree,
            bsize: sb.sectorsize,
            namelen: 255,
            frsize: sb.sectorsize,
        }
    }
}

impl State {
    /// DFS the FS tree, calling `visitor(item_key, item_data)` for every leaf
    /// item.
    fn for_each_item<F>(&mut self, mut visitor: F) -> io::Result<()>
    where
        F: FnMut(&btrfs_disk::tree::DiskKey, &[u8]),
    {
        tree_walk(
            &mut self.fs.reader,
            self.fs_tree_root,
            Traversal::Dfs,
            &mut |block| {
                if let TreeBlock::Leaf { items, data, .. } = block {
                    let header_size =
                        mem::size_of::<btrfs_disk::raw::btrfs_header>();
                    for item in items {
                        let start = header_size + item.offset as usize;
                        let end = start + item.size as usize;
                        if end <= data.len() {
                            visitor(&item.key, &data[start..end]);
                        }
                    }
                }
            },
        )
    }

    fn read_inode(&mut self, objectid: u64) -> io::Result<Option<InodeItem>> {
        let mut found = None;
        self.for_each_item(|key, data| {
            if found.is_some() {
                return;
            }
            if key.objectid == objectid && key.key_type == KeyType::InodeItem {
                found = InodeItem::parse(data);
            }
        })?;
        Ok(found)
    }

    fn lookup_in_dir(
        &mut self,
        parent_objectid: u64,
        name: &[u8],
    ) -> io::Result<Option<DirItem>> {
        let mut found = None;
        self.for_each_item(|key, data| {
            if found.is_some() {
                return;
            }
            if key.objectid != parent_objectid
                || key.key_type != KeyType::DirItem
            {
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

    /// Find the btrfs objectid of the parent directory for `oid` via
    /// `INODE_REF`. The `INODE_REF` key offset field contains the parent
    /// objectid directly. Returns `oid` itself if no ref is found.
    fn find_parent_oid(&mut self, oid: u64) -> io::Result<u64> {
        let mut parent = oid;
        self.for_each_item(|key, _data| {
            if parent != oid {
                return;
            }
            if key.objectid == oid && key.key_type == KeyType::InodeRef {
                parent = key.offset;
            }
        })?;
        Ok(parent)
    }
}

// -----------------------------------------------------------------------
// fuser::Filesystem adapter — each method calls an inherent op, maps the
// result to the matching `reply.*` call, and EIO's any I/O errors.
// -----------------------------------------------------------------------

impl Filesystem for BtrfsFuse {
    fn lookup(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        match self.lookup_entry(parent.0, name.as_bytes()) {
            Ok(Some((ino, item))) => {
                let attr = stat::make_attr(ino, &item, self.blksize);
                reply.entry(&TTL, &attr, Generation(0));
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                log::warn!(
                    "lookup parent={} name={}: {e}",
                    parent.0,
                    name.display()
                );
                reply.error(Errno::EIO);
            }
        }
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        match self.get_attr(ino.0) {
            Ok(Some(item)) => {
                let attr = stat::make_attr(ino.0, &item, self.blksize);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                log::warn!("getattr ino={}: {e}", ino.0);
                reply.error(Errno::EIO);
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.read_dir(ino.0, offset) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("readdir ino={} offset={offset}: {e}", ino.0);
                reply.error(Errno::EIO);
                return;
            }
        };
        for entry in entries {
            let child_ino = INodeNo(inode::btrfs_to_fuse(entry.ino));
            if reply.add(
                child_ino,
                entry.offset,
                entry.kind,
                OsStr::from_bytes(&entry.name),
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.read_symlink(ino.0) {
            Ok(Some(target)) => reply.data(&target),
            Ok(None) => {
                log::warn!("readlink ino={}: no inline extent found", ino.0);
                reply.error(Errno::EIO);
            }
            Err(e) => {
                log::warn!("readlink ino={}: {e}", ino.0);
                reply.error(Errno::EIO);
            }
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.read_data(ino.0, offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                reply.error(Errno::ENOENT);
            }
            Err(e) => {
                log::warn!(
                    "read ino={} offset={offset} size={size}: {e}",
                    ino.0
                );
                reply.error(Errno::EIO);
            }
        }
    }

    fn listxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        size: u32,
        reply: ReplyXattr,
    ) {
        let names = match self.list_xattrs(ino.0) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("listxattr ino={}: {e}", ino.0);
                reply.error(Errno::EIO);
                return;
            }
        };

        let mut buf: Vec<u8> = Vec::new();
        for name in &names {
            buf.extend_from_slice(name);
            buf.push(0);
        }

        #[allow(clippy::cast_possible_truncation)]
        if size == 0 {
            reply.size(buf.len() as u32);
        } else if buf.len() <= size as usize {
            reply.data(&buf);
        } else {
            reply.error(Errno::ERANGE);
        }
    }

    fn getxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        match self.get_xattr(ino.0, name.as_bytes()) {
            Ok(Some(value)) =>
            {
                #[allow(clippy::cast_possible_truncation)]
                if size == 0 {
                    reply.size(value.len() as u32);
                } else if value.len() <= size as usize {
                    reply.data(&value);
                } else {
                    reply.error(Errno::ERANGE);
                }
            }
            Ok(None) => reply.error(Errno::ENODATA),
            Err(e) => {
                log::warn!(
                    "getxattr ino={} name={}: {e}",
                    ino.0,
                    name.display()
                );
                reply.error(Errno::EIO);
            }
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let s = self.stat_fs();
        reply.statfs(
            s.blocks, s.bfree, s.bavail, 0, 0, s.bsize, s.namelen, s.frsize,
        );
    }
}

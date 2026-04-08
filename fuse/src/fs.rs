//! `impl fuser::Filesystem for BtrfsFuse` — milestone M1.
//!
//! Implements `lookup`, `getattr`, and `readdir` against the default FS tree
//! by linearly walking it (DFS) and filtering items. This is intentionally
//! the simplest possible implementation; once M1 is solid we will replace
//! the walks with a proper key-based descent helper in `btrfs-disk` and
//! cache decoded inodes.

use crate::{dir, inode, stat};
use anyhow::Result;
use btrfs_disk::{
    items::{DirItem, InodeItem},
    reader::{OpenFilesystem, Traversal, filesystem_open, tree_walk},
    tree::{KeyType, TreeBlock},
};
use fuser::{
    Errno, FileHandle, FileType, Filesystem, Generation, INodeNo, LockOwner,
    OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
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

pub struct BtrfsFuse {
    state: Mutex<State>,
    blksize: u32,
}

impl BtrfsFuse {
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
}

impl State {
    /// DFS the FS tree, calling `visitor(item_key, item_data)` for every leaf
    /// item. M1: replace with proper key-based descent once we factor a
    /// `tree_search` helper out of `btrfs-disk`.
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

    fn read_inode(&mut self, objectid: u64) -> Option<InodeItem> {
        let mut found = None;
        let _ = self.for_each_item(|key, data| {
            if found.is_some() {
                return;
            }
            if key.objectid == objectid && key.key_type == KeyType::InodeItem {
                found = InodeItem::parse(data);
            }
        });
        found
    }

    fn lookup_in_dir(
        &mut self,
        parent_objectid: u64,
        name: &[u8],
    ) -> Option<DirItem> {
        let mut found = None;
        let _ = self.for_each_item(|key, data| {
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
        });
        found
    }
}

impl Filesystem for BtrfsFuse {
    fn lookup(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        let mut state = self.state.lock().unwrap();
        let parent_oid = inode::fuse_to_btrfs(parent.0);
        let Some(entry) = state.lookup_in_dir(parent_oid, name.as_bytes())
        else {
            reply.error(Errno::ENOENT);
            return;
        };
        let child_oid = entry.location.objectid;
        let Some(item) = state.read_inode(child_oid) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let attr = stat::make_attr(
            inode::btrfs_to_fuse(child_oid),
            &item,
            self.blksize,
        );
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        let mut state = self.state.lock().unwrap();
        let oid = inode::fuse_to_btrfs(ino.0);
        let Some(item) = state.read_inode(oid) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let attr = stat::make_attr(ino.0, &item, self.blksize);
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let mut state = self.state.lock().unwrap();
        let dir_oid = inode::fuse_to_btrfs(ino.0);

        // Synthesise `.` and `..` at offsets 0 and 1.
        // M1 sketch: `..` is set to self for the root; real parent
        // resolution requires walking INODE_REF.
        let mut entries: Vec<dir::Entry> = Vec::new();
        if offset == 0 {
            entries.push(dir::Entry {
                ino: ino.0,
                kind: FileType::Directory,
                name: b".".to_vec(),
                offset: 1,
            });
        }
        if offset <= 1 {
            entries.push(dir::Entry {
                ino: ino.0,
                kind: FileType::Directory,
                name: b"..".to_vec(),
                offset: 2,
            });
        }

        // Collect DIR_INDEX entries past `offset` in offset-sorted order.
        let cursor = offset.max(2);
        let mut dir_entries: Vec<dir::Entry> = Vec::new();
        let _ = state.for_each_item(|key, data| {
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
        });
        dir_entries.sort_by_key(|e| e.offset);
        entries.extend(dir_entries);

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

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        // M1 stub: file reads land in milestones M2-M4.
        reply.error(Errno::ENOSYS);
    }
}

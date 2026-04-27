//! `BtrfsFuse`: a thin `fuser::Filesystem` adapter on top of [`btrfs_fs`].
//!
//! All filesystem semantics live in the [`btrfs_fs`] crate. This module
//! is responsible for the FUSE protocol mapping only:
//!
//! - inode-number translation (FUSE root = 1 ⇄ btrfs root dir = 256),
//! - converting [`btrfs_fs::Stat`] → [`fuser::FileAttr`] and
//!   [`btrfs_fs::FileKind`] → [`fuser::FileType`],
//! - turning each operation's `io::Result`/`Option` return into the
//!   appropriate `reply.*` call.

use crate::inode;
use anyhow::Result;
use btrfs_fs::{FileKind, Filesystem, Inode, Stat, SubvolId};
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem as FuserFilesystem,
    Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, ReplyStatfs, ReplyXattr, Request,
};
use std::{ffi::OsStr, fs::File, io, os::unix::ffi::OsStrExt, time::Duration};

const TTL: Duration = Duration::from_secs(1);

/// Default FS tree objectid (`BTRFS_FS_TREE_OBJECTID`).
const FS_TREE_OBJECTID: u64 = 5;

pub struct BtrfsFuse {
    fs: Filesystem<File>,
    blksize: u32,
}

impl BtrfsFuse {
    /// Bootstrap the filesystem from an open image file or block device.
    pub fn open(file: File) -> Result<Self> {
        let fs = Filesystem::open(file)?;
        let blksize = fs.blksize();
        Ok(Self { fs, blksize })
    }
}

fn fuse_inode(ino: u64) -> Inode {
    Inode {
        subvol: SubvolId(FS_TREE_OBJECTID),
        ino: inode::fuse_to_btrfs(ino),
    }
}

fn to_file_type(kind: FileKind) -> FileType {
    match kind {
        FileKind::RegularFile => FileType::RegularFile,
        FileKind::Directory => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
        FileKind::BlockDevice => FileType::BlockDevice,
        FileKind::CharDevice => FileType::CharDevice,
        FileKind::NamedPipe => FileType::NamedPipe,
        FileKind::Socket => FileType::Socket,
    }
}

fn to_file_attr(fuse_ino: u64, stat: &Stat) -> FileAttr {
    FileAttr {
        ino: INodeNo(fuse_ino),
        size: stat.size,
        blocks: stat.blocks,
        atime: stat.atime,
        mtime: stat.mtime,
        ctime: stat.ctime,
        crtime: stat.btime,
        kind: to_file_type(stat.kind),
        perm: stat.perm,
        nlink: stat.nlink,
        uid: stat.uid,
        gid: stat.gid,
        rdev: stat.rdev,
        blksize: stat.blksize,
        flags: 0,
    }
}

impl FuserFilesystem for BtrfsFuse {
    fn lookup(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        let parent_ino = fuse_inode(parent.0);
        match self.fs.lookup(parent_ino, name.as_bytes()) {
            Ok(Some((ino, item))) => {
                let fuse_ino = inode::btrfs_to_fuse(ino.ino);
                let stat = Stat::from_inode(ino, &item, self.blksize);
                reply.entry(
                    &TTL,
                    &to_file_attr(fuse_ino, &stat),
                    Generation(0),
                );
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
        let target = fuse_inode(ino.0);
        match self.fs.getattr(target) {
            Ok(Some(stat)) => reply.attr(&TTL, &to_file_attr(ino.0, &stat)),
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
        let dir_ino = fuse_inode(ino.0);
        let entries = match self.fs.readdir(dir_ino, offset) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("readdir ino={} offset={offset}: {e}", ino.0);
                reply.error(Errno::EIO);
                return;
            }
        };
        for entry in entries {
            let child_ino = INodeNo(inode::btrfs_to_fuse(entry.ino.ino));
            if reply.add(
                child_ino,
                entry.offset,
                to_file_type(entry.kind),
                OsStr::from_bytes(&entry.name),
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let target = fuse_inode(ino.0);
        match self.fs.readlink(target) {
            Ok(Some(t)) => reply.data(&t),
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
        let target = fuse_inode(ino.0);
        match self.fs.read(target, offset, size) {
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
        let target = fuse_inode(ino.0);
        let names = match self.fs.xattr_list(target) {
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
        let target = fuse_inode(ino.0);
        match self.fs.xattr_get(target, name.as_bytes()) {
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
        let s = self.fs.statfs();
        reply.statfs(
            s.blocks, s.bfree, s.bavail, 0, 0, s.bsize, s.namelen, s.frsize,
        );
    }
}

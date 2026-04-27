//! `BtrfsFuse`: a thin `fuser::Filesystem` adapter on top of [`btrfs_fs`].
//!
//! All filesystem semantics live in the [`btrfs_fs`] crate. This module
//! is responsible for the FUSE protocol mapping only:
//!
//! - inode-number translation (FUSE root = 1 ⇄ btrfs root dir = 256),
//! - converting [`btrfs_fs::Stat`] → [`fuser::FileAttr`] and
//!   [`btrfs_fs::FileKind`] → [`fuser::FileType`],
//! - spawning a tokio task per FUSE callback that owns the `Reply*`,
//!   awaits the async filesystem op, and replies from the task. The
//!   FUSE worker thread returns immediately, so concurrent FUSE
//!   callbacks don't serialise on a single in-flight I/O.

use crate::inode;
use anyhow::{Context, Result};
use btrfs_fs::{FileKind, Filesystem, Inode, Stat, SubvolId};
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem as FuserFilesystem,
    Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, ReplyStatfs, ReplyXattr, Request,
};
use std::{ffi::OsStr, fs::File, io, os::unix::ffi::OsStrExt, time::Duration};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_secs(1);

pub struct BtrfsFuse {
    fs: Filesystem<File>,
    blksize: u32,
    /// Subvolume that the FUSE root inode (`1`) maps onto. This is
    /// whatever `Filesystem` was opened with — the default `FS_TREE`
    /// for `BtrfsFuse::open`, or a user-selected subvolume for
    /// `BtrfsFuse::open_subvol`.
    mount_subvol: SubvolId,
    /// Tokio runtime used to drive async [`Filesystem`] ops. Each FUSE
    /// callback `spawn`s a task here; the FUSE worker thread itself
    /// returns immediately.
    runtime: Runtime,
}

impl BtrfsFuse {
    /// Bootstrap the filesystem from an open image file or block device,
    /// using the default subvolume (`FS_TREE`, id 5) as the mount root.
    pub fn open(file: File) -> Result<Self> {
        Self::from_filesystem(Filesystem::open(file)?)
    }

    /// Bootstrap the filesystem with a non-default subvolume as the
    /// mount root. The id must come from a previous call to
    /// [`btrfs_fs::Filesystem::list_subvolumes`].
    pub fn open_subvol(file: File, subvol: btrfs_fs::SubvolId) -> Result<Self> {
        Self::from_filesystem(Filesystem::open_subvol(file, subvol)?)
    }

    fn from_filesystem(fs: Filesystem<File>) -> Result<Self> {
        let blksize = fs.blksize();
        let mount_subvol = fs.default_subvol();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("btrfs-fuse-worker")
            .build()
            .context("failed to build tokio runtime")?;
        Ok(Self {
            fs,
            blksize,
            mount_subvol,
            runtime,
        })
    }

    /// Translate a FUSE inode (always `1` for the mount root) into a
    /// btrfs [`Inode`] in the active mount subvolume.
    fn fuse_inode(&self, ino: u64) -> Inode {
        Inode {
            subvol: self.mount_subvol,
            ino: inode::fuse_to_btrfs(ino),
        }
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
        let parent_ino = self.fuse_inode(parent.0);
        let name = name.as_bytes().to_vec();
        let fs = self.fs.clone();
        let blksize = self.blksize;
        self.runtime.spawn(async move {
            match fs.lookup(parent_ino, &name).await {
                Ok(Some((ino, item))) => {
                    let fuse_ino = inode::btrfs_to_fuse(ino.ino);
                    let stat = Stat::from_inode(ino, &item, blksize);
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
                        parent_ino.ino,
                        String::from_utf8_lossy(&name),
                    );
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        let target = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.getattr(target).await {
                Ok(Some(stat)) => {
                    reply.attr(&TTL, &to_file_attr(fuse_ino, &stat));
                }
                Ok(None) => reply.error(Errno::ENOENT),
                Err(e) => {
                    log::warn!("getattr ino={fuse_ino}: {e}");
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let dir_ino = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            let entries = match fs.readdir(dir_ino, offset).await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("readdir ino={fuse_ino} offset={offset}: {e}");
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
        });
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let target = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.readlink(target).await {
                Ok(Some(t)) => reply.data(&t),
                Ok(None) => {
                    log::warn!(
                        "readlink ino={fuse_ino}: no inline extent found"
                    );
                    reply.error(Errno::EIO);
                }
                Err(e) => {
                    log::warn!("readlink ino={fuse_ino}: {e}");
                    reply.error(Errno::EIO);
                }
            }
        });
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
        let target = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.read(target, offset, size).await {
                Ok(data) => reply.data(&data),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    reply.error(Errno::ENOENT);
                }
                Err(e) => {
                    log::warn!(
                        "read ino={fuse_ino} offset={offset} size={size}: {e}"
                    );
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn listxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        size: u32,
        reply: ReplyXattr,
    ) {
        let target = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            let names = match fs.xattr_list(target).await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("listxattr ino={fuse_ino}: {e}");
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
        });
    }

    fn getxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let target = self.fuse_inode(ino.0);
        let fuse_ino = ino.0;
        let name_bytes = name.as_bytes().to_vec();
        let fs = self.fs.clone();
        self.runtime.spawn(async move {
            match fs.xattr_get(target, &name_bytes).await {
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
                        "getxattr ino={fuse_ino} name={}: {e}",
                        String::from_utf8_lossy(&name_bytes),
                    );
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let s = self.fs.statfs();
        reply.statfs(
            s.blocks, s.bfree, s.bavail, 0, 0, s.bsize, s.namelen, s.frsize,
        );
    }
}

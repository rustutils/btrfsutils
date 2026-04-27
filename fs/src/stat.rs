//! POSIX-style inode metadata returned by [`crate::Filesystem::getattr`].

use crate::{FileKind, Inode};
use btrfs_disk::items::{InodeItem, Timespec};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert an on-disk btrfs [`Timespec`] to a [`SystemTime`].
#[must_use]
pub fn to_system_time(ts: &Timespec) -> SystemTime {
    UNIX_EPOCH + Duration::new(ts.sec, ts.nsec)
}

/// POSIX-style file metadata.
///
/// Mirrors the fields a `stat(2)` caller cares about, plus the btrfs
/// btime. The struct is FUSE-independent: a FUSE adapter can build a
/// `fuser::FileAttr` from it with a small mapping, and a non-FUSE
/// embedder (offline tools, tests) can read the fields directly.
#[derive(Debug, Clone, Copy)]
pub struct Stat {
    pub ino: Inode,
    pub kind: FileKind,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    /// btrfs creation time (`otime`), exposed for callers that surface
    /// `birthtime` / `crtime`.
    pub btime: SystemTime,
    /// Permission bits (mode & 0o7777).
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

impl Stat {
    /// Build a [`Stat`] from a parsed [`InodeItem`].
    #[must_use]
    pub fn from_inode(ino: Inode, item: &InodeItem, blksize: u32) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let perm = (item.mode & 0o7777) as u16;
        #[allow(clippy::cast_possible_truncation)]
        // rdev fits in 20 bits (major:12 + minor:8)
        let rdev = item.rdev as u32;
        Self {
            ino,
            kind: FileKind::from_mode(item.mode),
            size: item.size,
            blocks: item.nbytes / 512,
            atime: to_system_time(&item.atime),
            mtime: to_system_time(&item.mtime),
            ctime: to_system_time(&item.ctime),
            btime: to_system_time(&item.otime),
            perm,
            nlink: item.nlink,
            uid: item.uid,
            gid: item.gid,
            rdev,
            blksize,
        }
    }
}

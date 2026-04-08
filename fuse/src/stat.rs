//! Translate `btrfs_disk::items::InodeItem` into `fuser::FileAttr`.

use btrfs_disk::items::{InodeItem, Timespec};
use fuser::{FileAttr, FileType, INodeNo};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a btrfs on-disk `Timespec` to a `SystemTime`.
#[must_use]
pub fn to_system_time(ts: &Timespec) -> SystemTime {
    UNIX_EPOCH + Duration::new(ts.sec, ts.nsec)
}

/// Translate the POSIX mode field's type bits into a `fuser::FileType`.
#[must_use]
pub fn mode_to_kind(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        x if x == libc::S_IFDIR => FileType::Directory,
        x if x == libc::S_IFLNK => FileType::Symlink,
        x if x == libc::S_IFBLK => FileType::BlockDevice,
        x if x == libc::S_IFCHR => FileType::CharDevice,
        x if x == libc::S_IFIFO => FileType::NamedPipe,
        x if x == libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

/// Build a `FileAttr` from a parsed `InodeItem` and the FUSE inode number.
#[must_use]
pub fn make_attr(ino: u64, item: &InodeItem, blksize: u32) -> FileAttr {
    let ino = INodeNo(ino);
    let kind = mode_to_kind(item.mode);
    let perm = (item.mode & 0o7777) as u16;
    let atime = to_system_time(&item.atime);
    let mtime = to_system_time(&item.mtime);
    let ctime = to_system_time(&item.ctime);
    let btime = to_system_time(&item.otime);
    #[allow(clippy::cast_possible_truncation)]
    let rdev = item.rdev as u32; // rdev fits in 20 bits (major:12 + minor:8)
    FileAttr {
        ino,
        size: item.size,
        blocks: item.nbytes / 512,
        atime,
        mtime,
        ctime,
        crtime: btime,
        kind,
        perm,
        nlink: item.nlink,
        uid: item.uid,
        gid: item.gid,
        rdev,
        blksize,
        flags: 0,
    }
}

//! Directory entries and the file-kind enum.
//!
//! [`FileKind`] is the FUSE-independent equivalent of `fuser::FileType`,
//! and [`Entry`] is what [`crate::Filesystem::readdir`] returns for each
//! child of a directory.

use crate::Inode;
use btrfs_disk::items::{DirItem, FileType as BtrfsFileType};

/// Filesystem-level file type, decoupled from any FUSE crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileKind {
    RegularFile,
    Directory,
    Symlink,
    BlockDevice,
    CharDevice,
    NamedPipe,
    Socket,
}

impl FileKind {
    /// Translate a btrfs on-disk `FileType` into a [`FileKind`].
    ///
    /// `Xattr`, `Unknown`, and `Other(_)` are reported as `RegularFile`,
    /// matching how the kernel surfaces them through `readdir`.
    #[must_use]
    pub fn from_btrfs(ft: BtrfsFileType) -> Self {
        match ft {
            BtrfsFileType::Dir => FileKind::Directory,
            BtrfsFileType::Symlink => FileKind::Symlink,
            BtrfsFileType::Blkdev => FileKind::BlockDevice,
            BtrfsFileType::Chrdev => FileKind::CharDevice,
            BtrfsFileType::Fifo => FileKind::NamedPipe,
            BtrfsFileType::Sock => FileKind::Socket,
            BtrfsFileType::RegFile
            | BtrfsFileType::Xattr
            | BtrfsFileType::Unknown
            | BtrfsFileType::Other(_) => FileKind::RegularFile,
        }
    }

    /// Decode the type bits of a POSIX mode field into a [`FileKind`].
    #[must_use]
    pub fn from_mode(mode: u32) -> Self {
        const S_IFMT: u32 = 0o170_000;
        const S_IFDIR: u32 = 0o040_000;
        const S_IFLNK: u32 = 0o120_000;
        const S_IFBLK: u32 = 0o060_000;
        const S_IFCHR: u32 = 0o020_000;
        const S_IFIFO: u32 = 0o010_000;
        const S_IFSOCK: u32 = 0o140_000;
        match mode & S_IFMT {
            S_IFDIR => FileKind::Directory,
            S_IFLNK => FileKind::Symlink,
            S_IFBLK => FileKind::BlockDevice,
            S_IFCHR => FileKind::CharDevice,
            S_IFIFO => FileKind::NamedPipe,
            S_IFSOCK => FileKind::Socket,
            _ => FileKind::RegularFile,
        }
    }
}

/// A single directory entry from [`crate::Filesystem::readdir`].
#[derive(Debug, Clone)]
pub struct Entry {
    pub ino: Inode,
    pub kind: FileKind,
    pub name: Vec<u8>,
    /// Cookie a caller passes back to resume reading strictly after this
    /// entry. Stable as long as the directory layout doesn't change.
    pub offset: u64,
}

impl Entry {
    pub(crate) fn from_dir_item(
        subvol: crate::SubvolId,
        item: &DirItem,
        next_offset: u64,
    ) -> Self {
        Self {
            ino: Inode {
                subvol,
                ino: item.location.objectid,
            },
            kind: FileKind::from_btrfs(item.file_type),
            name: item.name.clone(),
            offset: next_offset,
        }
    }
}

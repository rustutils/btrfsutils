//! Directory readdir helpers.
//!
//! Currently a small translation layer; the actual walk is implemented
//! inline in `fs.rs::readdir` while the shape of the iterator stabilises.

use btrfs_disk::items::{DirItem, FileType as BtrfsFileType};
use fuser::FileType;

/// Translate a btrfs `FileType` to a FUSE `FileType`.
#[must_use]
pub fn translate_file_type(ft: BtrfsFileType) -> FileType {
    match ft {
        BtrfsFileType::Dir => FileType::Directory,
        BtrfsFileType::Symlink => FileType::Symlink,
        BtrfsFileType::Blkdev => FileType::BlockDevice,
        BtrfsFileType::Chrdev => FileType::CharDevice,
        BtrfsFileType::Fifo => FileType::NamedPipe,
        BtrfsFileType::Sock => FileType::Socket,
        BtrfsFileType::RegFile
        | BtrfsFileType::Xattr
        | BtrfsFileType::Unknown
        | BtrfsFileType::Other(_) => FileType::RegularFile,
    }
}

/// A single directory entry collected during readdir.
pub struct Entry {
    pub ino: u64,
    pub kind: FileType,
    pub name: Vec<u8>,
    /// readdir cookie to return to FUSE; the next call will resume strictly
    /// after this offset.
    pub offset: u64,
}

impl Entry {
    #[must_use]
    pub fn from_dir_item(item: &DirItem, key_offset: u64) -> Self {
        Self {
            ino: item.location.objectid,
            kind: translate_file_type(item.file_type),
            name: item.name.clone(),
            offset: key_offset,
        }
    }
}

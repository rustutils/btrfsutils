//! # Typed Rust structs for btrfs tree item payloads
//!
//! Each on-disk item type has a corresponding struct with a `parse` method
//! that reads from a raw byte buffer using safe LE reader helpers. These
//! structs are the public API for item data; display formatting lives in
//! the `cli` crate.

use crate::{
    raw,
    tree::{DiskKey, ObjectId},
    util::raw_crc32c,
};
use bytes::{Buf, BufMut};
use std::{fmt, mem};
use uuid::Uuid;

/// Read a UUID (16 bytes) from a `Buf` cursor, advancing it by 16 bytes.
fn get_uuid(buf: &mut &[u8]) -> Uuid {
    let bytes: [u8; 16] = buf[..16].try_into().unwrap();
    buf.advance(16);
    Uuid::from_bytes(bytes)
}

bitflags::bitflags! {
    /// Block group / chunk type flags: the combination of chunk type
    /// (DATA, SYSTEM, METADATA) and RAID profile stored in on-disk chunk
    /// items and block group items.
    ///
    /// Display produces the dump-tree format: `DATA|DUP`, `METADATA|single`, etc.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct BlockGroupFlags: u64 {
        const DATA     = raw::BTRFS_BLOCK_GROUP_DATA as u64;
        const SYSTEM   = raw::BTRFS_BLOCK_GROUP_SYSTEM as u64;
        const METADATA = raw::BTRFS_BLOCK_GROUP_METADATA as u64;
        const RAID0    = raw::BTRFS_BLOCK_GROUP_RAID0 as u64;
        const RAID1    = raw::BTRFS_BLOCK_GROUP_RAID1 as u64;
        const DUP      = raw::BTRFS_BLOCK_GROUP_DUP as u64;
        const RAID10   = raw::BTRFS_BLOCK_GROUP_RAID10 as u64;
        const RAID5    = raw::BTRFS_BLOCK_GROUP_RAID5 as u64;
        const RAID6    = raw::BTRFS_BLOCK_GROUP_RAID6 as u64;
        const RAID1C3  = raw::BTRFS_BLOCK_GROUP_RAID1C3 as u64;
        const RAID1C4  = raw::BTRFS_BLOCK_GROUP_RAID1C4 as u64;

        /// Explicit "single" marker (bit 48). When no profile bits are
        /// set, the allocation is also single.
        const SINGLE     = raw::BTRFS_AVAIL_ALLOC_BIT_SINGLE;

        /// Pseudo-type used for the global reservation pool.
        const GLOBAL_RSV = raw::BTRFS_SPACE_INFO_GLOBAL_RSV;
    }
}

impl BlockGroupFlags {
    /// Returns the human-readable chunk type name.
    #[must_use]
    pub fn type_name(self) -> &'static str {
        if self.contains(Self::GLOBAL_RSV) {
            return "GlobalReserve";
        }
        let ty = self & (Self::DATA | Self::SYSTEM | Self::METADATA);
        match ty {
            t if t == Self::DATA => "Data",
            t if t == Self::SYSTEM => "System",
            t if t == Self::METADATA => "Metadata",
            t if t == Self::DATA | Self::METADATA => "Data+Metadata",
            _ => "unknown",
        }
    }

    /// Returns the RAID profile name, or `"single"` when no profile bit is set.
    #[must_use]
    pub fn profile_name(self) -> &'static str {
        let profile = self
            & (Self::RAID0
                | Self::RAID1
                | Self::DUP
                | Self::RAID10
                | Self::RAID5
                | Self::RAID6
                | Self::RAID1C3
                | Self::RAID1C4
                | Self::SINGLE);
        match profile {
            p if p == Self::RAID0 => "RAID0",
            p if p == Self::RAID1 => "RAID1",
            p if p == Self::DUP => "DUP",
            p if p == Self::RAID10 => "RAID10",
            p if p == Self::RAID5 => "RAID5",
            p if p == Self::RAID6 => "RAID6",
            p if p == Self::RAID1C3 => "RAID1C3",
            p if p == Self::RAID1C4 => "RAID1C4",
            // Both explicit SINGLE and no-profile-bits mean "single".
            _ => "single",
        }
    }
}

bitflags::bitflags! {
    /// Inode item flags stored in `btrfs_inode_item::flags`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct InodeFlags: u64 {
        const NODATASUM      = raw::BTRFS_INODE_NODATASUM as u64;
        const NODATACOW      = raw::BTRFS_INODE_NODATACOW as u64;
        const READONLY       = raw::BTRFS_INODE_READONLY as u64;
        const NOCOMPRESS     = raw::BTRFS_INODE_NOCOMPRESS as u64;
        const PREALLOC       = raw::BTRFS_INODE_PREALLOC as u64;
        const SYNC           = raw::BTRFS_INODE_SYNC as u64;
        const IMMUTABLE      = raw::BTRFS_INODE_IMMUTABLE as u64;
        const APPEND         = raw::BTRFS_INODE_APPEND as u64;
        const NODUMP         = raw::BTRFS_INODE_NODUMP as u64;
        const NOATIME        = raw::BTRFS_INODE_NOATIME as u64;
        const DIRSYNC        = raw::BTRFS_INODE_DIRSYNC as u64;
        const COMPRESS       = raw::BTRFS_INODE_COMPRESS as u64;
        const ROOT_ITEM_INIT = raw::BTRFS_INODE_ROOT_ITEM_INIT as u64;
        // Preserve unknown bits from the on-disk value.
        const _ = !0;
    }
}

impl fmt::Display for InodeFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const NAMES: &[(InodeFlags, &str)] = &[
            (InodeFlags::NODATASUM, "NODATASUM"),
            (InodeFlags::NODATACOW, "NODATACOW"),
            (InodeFlags::READONLY, "READONLY"),
            (InodeFlags::NOCOMPRESS, "NOCOMPRESS"),
            (InodeFlags::PREALLOC, "PREALLOC"),
            (InodeFlags::SYNC, "SYNC"),
            (InodeFlags::IMMUTABLE, "IMMUTABLE"),
            (InodeFlags::APPEND, "APPEND"),
            (InodeFlags::NODUMP, "NODUMP"),
            (InodeFlags::NOATIME, "NOATIME"),
            (InodeFlags::DIRSYNC, "DIRSYNC"),
            (InodeFlags::COMPRESS, "COMPRESS"),
            (InodeFlags::ROOT_ITEM_INIT, "ROOT_ITEM_INIT"),
        ];
        let known: InodeFlags = NAMES
            .iter()
            .fold(InodeFlags::empty(), |a, &(flag, _)| a | flag);
        let mut parts: Vec<String> = NAMES
            .iter()
            .filter(|&&(flag, _)| self.contains(flag))
            .map(|&(_, name)| name.to_string())
            .collect();
        let unknown = *self & !known;
        if !unknown.is_empty() {
            parts.push(format!("UNKNOWN: 0x{:x}", unknown.bits()));
        }
        if parts.is_empty() {
            write!(f, "none")
        } else {
            write!(f, "{}", parts.join("|"))
        }
    }
}
/// Btrfs timestamp (seconds + nanoseconds since Unix epoch).
#[derive(Debug, Clone, Copy)]
pub struct Timespec {
    /// Seconds since 1970-01-01 00:00:00 UTC.
    pub sec: u64,
    /// Nanosecond component (0..999_999_999).
    pub nsec: u32,
}

impl Timespec {
    fn parse(buf: &mut &[u8]) -> Self {
        Self {
            sec: buf.get_u64_le(),
            nsec: buf.get_u32_le(),
        }
    }
}

/// Compression type for file extents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Zlib,
    Lzo,
    Zstd,
    Unknown(u8),
}

impl CompressionType {
    pub fn from_raw(v: u8) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Zlib,
            2 => Self::Lzo,
            3 => Self::Zstd,
            _ => Self::Unknown(v),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zlib => "zlib",
            Self::Lzo => "lzo",
            Self::Zstd => "zstd",
            Self::Unknown(_) => "unknown",
        }
    }

    pub fn to_raw(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zlib => 1,
            Self::Lzo => 2,
            Self::Zstd => 3,
            Self::Unknown(v) => v,
        }
    }
}

/// File extent type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileExtentType {
    Inline,
    Regular,
    Prealloc,
    Unknown(u8),
}

impl FileExtentType {
    pub fn from_raw(v: u8) -> Self {
        match u32::from(v) {
            raw::BTRFS_FILE_EXTENT_INLINE => Self::Inline,
            raw::BTRFS_FILE_EXTENT_REG => Self::Regular,
            raw::BTRFS_FILE_EXTENT_PREALLOC => Self::Prealloc,
            _ => Self::Unknown(v),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Regular => "regular",
            Self::Prealloc => "prealloc",
            Self::Unknown(_) => "unknown",
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn to_raw(self) -> u8 {
        match self {
            Self::Inline => raw::BTRFS_FILE_EXTENT_INLINE as u8,
            Self::Regular => raw::BTRFS_FILE_EXTENT_REG as u8,
            Self::Prealloc => raw::BTRFS_FILE_EXTENT_PREALLOC as u8,
            Self::Unknown(v) => v,
        }
    }
}

/// Directory entry file type, stored in `btrfs_dir_item::type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Unknown,
    RegFile,
    Dir,
    Chrdev,
    Blkdev,
    Fifo,
    Sock,
    Symlink,
    Xattr,
    Other(u8),
}

impl FileType {
    pub fn from_raw(v: u8) -> Self {
        match u32::from(v) {
            raw::BTRFS_FT_UNKNOWN => Self::Unknown,
            raw::BTRFS_FT_REG_FILE => Self::RegFile,
            raw::BTRFS_FT_DIR => Self::Dir,
            raw::BTRFS_FT_CHRDEV => Self::Chrdev,
            raw::BTRFS_FT_BLKDEV => Self::Blkdev,
            raw::BTRFS_FT_FIFO => Self::Fifo,
            raw::BTRFS_FT_SOCK => Self::Sock,
            raw::BTRFS_FT_SYMLINK => Self::Symlink,
            raw::BTRFS_FT_XATTR => Self::Xattr,
            _ => Self::Other(v),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown | Self::Other(_) => "UNKNOWN",
            Self::RegFile => "FILE",
            Self::Dir => "DIR",
            Self::Chrdev => "CHRDEV",
            Self::Blkdev => "BLKDEV",
            Self::Fifo => "FIFO",
            Self::Sock => "SOCK",
            Self::Symlink => "SYMLINK",
            Self::Xattr => "XATTR",
        }
    }
}

/// Inode metadata, stored as `INODE_ITEM` in the FS tree.
///
/// Contains POSIX attributes (uid, gid, mode, timestamps) plus btrfs-specific
/// fields (flags, sequence number, block group hint).
#[derive(Debug, Clone)]
pub struct InodeItem {
    /// Generation when this inode was created.
    pub generation: u64,
    /// Transaction ID of the last modification.
    pub transid: u64,
    /// Logical file size in bytes.
    pub size: u64,
    /// Total on-disk bytes used (including all copies for RAID).
    pub nbytes: u64,
    /// Block group hint for new allocations.
    pub block_group: u64,
    /// Hard link count.
    pub nlink: u32,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// POSIX file mode (type + permissions).
    pub mode: u32,
    /// Device number (for character/block device inodes).
    pub rdev: u64,
    /// Inode flags (NODATASUM, COMPRESS, etc.).
    pub flags: InodeFlags,
    /// NFS-compatible change sequence number.
    pub sequence: u64,
    /// Last access time.
    pub atime: Timespec,
    /// Last change time (inode metadata).
    pub ctime: Timespec,
    /// Last modification time (file data).
    pub mtime: Timespec,
    /// Creation time.
    pub otime: Timespec,
}

impl InodeItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_inode_item>() {
            return None;
        }
        let mut buf = data;
        Some(Self {
            generation: buf.get_u64_le(),
            transid: buf.get_u64_le(),
            size: buf.get_u64_le(),
            nbytes: buf.get_u64_le(),
            block_group: buf.get_u64_le(),
            nlink: buf.get_u32_le(),
            uid: buf.get_u32_le(),
            gid: buf.get_u32_le(),
            mode: buf.get_u32_le(),
            rdev: buf.get_u64_le(),
            flags: InodeFlags::from_bits_truncate(buf.get_u64_le()),
            sequence: buf.get_u64_le(),
            // Skip reserved[4] (4 x u64 = 32 bytes)
            atime: {
                buf.advance(32);
                Timespec::parse(&mut buf)
            },
            ctime: Timespec::parse(&mut buf),
            mtime: Timespec::parse(&mut buf),
            otime: Timespec::parse(&mut buf),
        })
    }
}

/// Hard link reference from an inode to a directory entry.
///
/// Key: `(inode_number, INODE_REF, parent_dir_inode)`. Multiple refs can be
/// packed into a single item when an inode has several hard links in the same
/// parent directory.
#[derive(Debug, Clone)]
pub struct InodeRef {
    /// Index in the parent directory (matches a `DIR_INDEX` key offset).
    pub index: u64,
    /// Filename component (raw bytes, typically UTF-8).
    pub name: Vec<u8>,
}

impl InodeRef {
    pub fn parse_all(data: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let mut buf = data;
        while buf.remaining() >= 10 {
            let index = buf.get_u64_le();
            let name_len = buf.get_u16_le() as usize;
            if buf.remaining() < name_len {
                break;
            }
            let name = buf[..name_len].to_vec();
            buf.advance(name_len);
            result.push(Self { index, name });
        }
        result
    }
}

/// Extended inode reference, used when the `EXTREF` feature is enabled.
///
/// Unlike `InodeRef`, the parent directory objectid is stored in the struct
/// rather than the key offset, allowing references from different parent
/// directories to coexist.
#[derive(Debug, Clone)]
pub struct InodeExtref {
    /// Parent directory inode number.
    pub parent: u64,
    /// Index in the parent directory.
    pub index: u64,
    /// Filename component (raw bytes, typically UTF-8).
    pub name: Vec<u8>,
}

impl InodeExtref {
    pub fn parse_all(data: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let mut buf = data;
        while buf.remaining() >= 18 {
            let parent = buf.get_u64_le();
            let index = buf.get_u64_le();
            let name_len = buf.get_u16_le() as usize;
            if buf.remaining() < name_len {
                break;
            }
            let name = buf[..name_len].to_vec();
            buf.advance(name_len);
            result.push(Self {
                parent,
                index,
                name,
            });
        }
        result
    }
}

/// Directory entry, stored as `DIR_ITEM` (hashed by name) or `DIR_INDEX`
/// (sequential index) in the FS tree.
///
/// Multiple entries can be packed into a single item when names hash to the
/// same value (for `DIR_ITEM`) or when processing xattrs (`XATTR_ITEM`).
#[derive(Debug, Clone)]
pub struct DirItem {
    /// Key of the target inode (objectid = inode number, type = `INODE_ITEM`).
    pub location: DiskKey,
    /// Transaction ID when this entry was created.
    pub transid: u64,
    /// Type of the referenced inode (file, directory, symlink, etc.).
    pub file_type: FileType,
    /// Filename or xattr name (raw bytes).
    pub name: Vec<u8>,
    /// Xattr value (empty for regular directory entries).
    pub data: Vec<u8>,
}

impl DirItem {
    pub fn parse_all(data: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let dir_item_size = mem::size_of::<raw::btrfs_dir_item>();
        let mut buf = data;

        while buf.remaining() >= dir_item_size {
            let location = DiskKey::parse(buf, 0);
            buf.advance(17); // skip past DiskKey (u64 + u8 + u64)
            let transid = buf.get_u64_le();
            let data_len = buf.get_u16_le() as usize;
            let name_len = buf.get_u16_le() as usize;
            let file_type = FileType::from_raw(buf.get_u8());

            if buf.remaining() < name_len + data_len {
                break;
            }
            let name = buf[..name_len].to_vec();
            buf.advance(name_len);
            let item_data = buf[..data_len].to_vec();
            buf.advance(data_len);
            result.push(Self {
                location,
                transid,
                file_type,
                name,
                data: item_data,
            });
        }
        result
    }
}

bitflags::bitflags! {
    /// Root item flags stored in `btrfs_root_item::flags`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RootItemFlags: u64 {
        const RDONLY = raw::BTRFS_ROOT_SUBVOL_RDONLY as u64;
        const DEAD   = raw::BTRFS_ROOT_SUBVOL_DEAD;
        // Preserve unknown bits from the on-disk value.
        const _ = !0;
    }
}

impl fmt::Display for RootItemFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.contains(Self::RDONLY) {
            write!(f, "RDONLY")
        } else {
            write!(f, "none")
        }
    }
}

/// Root item describing a tree (subvolume, snapshot, or internal tree).
///
/// Stored in the root tree with key `(tree_objectid, ROOT_ITEM, 0)`. Contains
/// the root block pointer, subvolume UUIDs, and transaction timestamps needed
/// for snapshot management and send/receive.
#[derive(Debug, Clone)]
pub struct RootItem {
    /// Generation when this root was last modified.
    pub generation: u64,
    /// Objectid of the root directory inode (always 256 for FS trees).
    pub root_dirid: u64,
    /// Logical bytenr of this tree's root block.
    pub bytenr: u64,
    /// Quota byte limit (0 = unlimited).
    pub byte_limit: u64,
    /// Bytes used by this tree.
    pub bytes_used: u64,
    /// Generation of the last snapshot taken from this subvolume.
    pub last_snapshot: u64,
    /// Root flags (RDONLY for read-only snapshots).
    pub flags: RootItemFlags,
    /// Reference count.
    pub refs: u32,
    /// Progress key for in-progress drop operations.
    pub drop_progress: DiskKey,
    /// Tree level of the drop progress.
    pub drop_level: u8,
    /// B-tree level of this tree's root block.
    pub level: u8,
    /// Extended generation (v2 root items, matches `generation` in practice).
    pub generation_v2: u64,
    /// UUID of this subvolume.
    pub uuid: Uuid,
    /// UUID of the parent subvolume (for snapshots).
    pub parent_uuid: Uuid,
    /// UUID of the subvolume this was received from (for send/receive).
    pub received_uuid: Uuid,
    /// Transaction ID of the last change to this subvolume.
    pub ctransid: u64,
    /// Transaction ID when this subvolume was created.
    pub otransid: u64,
    /// Transaction ID when this subvolume was sent.
    pub stransid: u64,
    /// Transaction ID when this subvolume was received.
    pub rtransid: u64,
    /// Time of the last change.
    pub ctime: Timespec,
    /// Creation time.
    pub otime: Timespec,
    /// Time when sent.
    pub stime: Timespec,
    /// Time when received.
    pub rtime: Timespec,
}

impl RootItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let inode_size = mem::size_of::<raw::btrfs_inode_item>();
        if data.len() < inode_size + 8 {
            return None;
        }

        let mut buf = &data[inode_size..];
        let generation = buf.get_u64_le();
        let root_dirid = buf.get_u64_le();
        let bytenr = buf.get_u64_le();
        let byte_limit = buf.get_u64_le();
        let bytes_used = buf.get_u64_le();
        let last_snapshot = buf.get_u64_le();
        let flags = RootItemFlags::from_bits_truncate(buf.get_u64_le());
        let refs = buf.get_u32_le();

        let dp_off = inode_size + 60;
        let drop_progress = if dp_off + 17 <= data.len() {
            DiskKey::parse(data, dp_off)
        } else {
            DiskKey::parse(&[0; 17], 0)
        };
        let drop_level = if dp_off + 17 < data.len() {
            data[dp_off + 17]
        } else {
            0
        };

        let level_off = mem::offset_of!(raw::btrfs_root_item, level);
        let level = if level_off < data.len() {
            data[level_off]
        } else {
            0
        };
        let generation_v2 = if level_off + 1 + 8 <= data.len() {
            let mut b = &data[level_off + 1..];
            b.get_u64_le()
        } else {
            0
        };

        let uuid_off = mem::offset_of!(raw::btrfs_root_item, uuid);
        let uuid = if uuid_off + 16 <= data.len() {
            let mut b = &data[uuid_off..];
            get_uuid(&mut b)
        } else {
            Uuid::nil()
        };
        let parent_uuid = if uuid_off + 32 <= data.len() {
            let mut b = &data[uuid_off + 16..];
            get_uuid(&mut b)
        } else {
            Uuid::nil()
        };
        let received_uuid = if uuid_off + 48 <= data.len() {
            let mut b = &data[uuid_off + 32..];
            get_uuid(&mut b)
        } else {
            Uuid::nil()
        };

        let ct_off = mem::offset_of!(raw::btrfs_root_item, ctransid);
        let ctransid = if ct_off + 8 <= data.len() {
            let mut b = &data[ct_off..];
            b.get_u64_le()
        } else {
            0
        };
        let otransid = if ct_off + 16 <= data.len() {
            let mut b = &data[ct_off + 8..];
            b.get_u64_le()
        } else {
            0
        };
        let stransid = if ct_off + 24 <= data.len() {
            let mut b = &data[ct_off + 16..];
            b.get_u64_le()
        } else {
            0
        };
        let rtransid = if ct_off + 32 <= data.len() {
            let mut b = &data[ct_off + 24..];
            b.get_u64_le()
        } else {
            0
        };

        let ctime_off = mem::offset_of!(raw::btrfs_root_item, ctime);
        let ts_size = mem::size_of::<raw::btrfs_timespec>();
        let ctime = if ctime_off + ts_size <= data.len() {
            let mut b = &data[ctime_off..];
            Timespec::parse(&mut b)
        } else {
            Timespec { sec: 0, nsec: 0 }
        };
        let otime = if ctime_off + 2 * ts_size <= data.len() {
            let mut b = &data[ctime_off + ts_size..];
            Timespec::parse(&mut b)
        } else {
            Timespec { sec: 0, nsec: 0 }
        };
        let stime = if ctime_off + 3 * ts_size <= data.len() {
            let mut b = &data[ctime_off + 2 * ts_size..];
            Timespec::parse(&mut b)
        } else {
            Timespec { sec: 0, nsec: 0 }
        };
        let rtime = if ctime_off + 4 * ts_size <= data.len() {
            let mut b = &data[ctime_off + 3 * ts_size..];
            Timespec::parse(&mut b)
        } else {
            Timespec { sec: 0, nsec: 0 }
        };

        Some(Self {
            generation,
            root_dirid,
            bytenr,
            byte_limit,
            bytes_used,
            last_snapshot,
            flags,
            refs,
            drop_progress,
            drop_level,
            level,
            generation_v2,
            uuid,
            parent_uuid,
            received_uuid,
            ctransid,
            otransid,
            stransid,
            rtransid,
            ctime,
            otime,
            stime,
            rtime,
        })
    }
}

/// Reference linking a subvolume to its parent directory.
///
/// `ROOT_REF` keys (parent → child) and `ROOT_BACKREF` keys (child → parent)
/// use the same on-disk format.
#[derive(Debug, Clone)]
pub struct RootRef {
    /// Inode number of the directory containing the subvolume entry.
    pub dirid: u64,
    /// Directory sequence number (matches the `DIR_INDEX` offset).
    pub sequence: u64,
    /// Name of the subvolume entry in the parent directory.
    pub name: Vec<u8>,
}

impl RootRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_root_ref>() {
            return None;
        }
        let mut buf = data;
        let dirid = buf.get_u64_le();
        let sequence = buf.get_u64_le();
        let name_len = buf.get_u16_le() as usize;
        let name_start = mem::size_of::<raw::btrfs_root_ref>();
        let name = if name_start + name_len <= data.len() {
            data[name_start..name_start + name_len].to_vec()
        } else {
            Vec::new()
        };
        Some(Self {
            dirid,
            sequence,
            name,
        })
    }
}

/// File extent descriptor, stored as `EXTENT_DATA` in the FS tree.
///
/// Key: `(inode, EXTENT_DATA, file_offset)`. Describes how a range of file
/// bytes maps to on-disk storage. Extents can be inline (data embedded in the
/// item), regular (referencing a disk extent), or prealloc (reserved but
/// unwritten).
#[derive(Debug, Clone)]
pub struct FileExtentItem {
    /// Generation when this extent was allocated.
    pub generation: u64,
    /// Uncompressed size of the data in this extent.
    pub ram_bytes: u64,
    /// Compression algorithm applied to the on-disk data.
    pub compression: CompressionType,
    /// Whether the extent is inline, regular, or preallocated.
    pub extent_type: FileExtentType,
    /// Type-specific extent location.
    pub body: FileExtentBody,
}

/// Body of a file extent: either inline data or a reference to a disk extent.
#[derive(Debug, Clone)]
pub enum FileExtentBody {
    /// Data is stored directly in the tree leaf (small files/tails).
    Inline {
        /// Number of bytes of inline data following the extent header.
        inline_size: usize,
    },
    /// Data is stored in a separate disk extent.
    Regular {
        /// Logical byte address of the extent on disk (0 = hole/sparse).
        disk_bytenr: u64,
        /// Size of the on-disk extent in bytes (compressed size if compressed).
        disk_num_bytes: u64,
        /// Byte offset into the extent where this file range starts.
        offset: u64,
        /// Number of logical file bytes this extent covers.
        num_bytes: u64,
    },
}

impl FileExtentItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 21 {
            return None;
        }
        let mut buf = data;
        let generation = buf.get_u64_le();
        let ram_bytes = buf.get_u64_le();
        let compression = CompressionType::from_raw(buf.get_u8());
        buf.advance(3); // skip encryption, other_encoding
        let extent_type = FileExtentType::from_raw(buf.get_u8());

        let body = if extent_type == FileExtentType::Inline {
            FileExtentBody::Inline {
                inline_size: buf.remaining(),
            }
        } else if buf.remaining() >= 32 {
            FileExtentBody::Regular {
                disk_bytenr: buf.get_u64_le(),
                disk_num_bytes: buf.get_u64_le(),
                offset: buf.get_u64_le(),
                num_bytes: buf.get_u64_le(),
            }
        } else {
            return None;
        };

        Some(Self {
            generation,
            ram_bytes,
            compression,
            extent_type,
            body,
        })
    }
}

/// Compute the hash used for `EXTENT_DATA_REF` keys, matching the kernel's
/// `hash_extent_data_ref()`. Uses two independent CRC32C computations
/// combined into a single u64.
fn extent_data_ref_hash(root: u64, objectid: u64, offset: u64) -> u64 {
    let high_crc = raw_crc32c(!0u32, &root.to_le_bytes());
    let low_crc = raw_crc32c(!0u32, &objectid.to_le_bytes());
    let low_crc = raw_crc32c(low_crc, &offset.to_le_bytes());
    (u64::from(high_crc) << 31) ^ u64::from(low_crc)
}

/// Inline reference types found inside `EXTENT_ITEM`/`METADATA_ITEM`.
#[derive(Debug, Clone)]
pub enum InlineRef {
    TreeBlockBackref {
        ref_offset: u64,
        root: u64,
    },
    SharedBlockBackref {
        ref_offset: u64,
        parent: u64,
    },
    ExtentDataBackref {
        ref_offset: u64,
        root: u64,
        objectid: u64,
        offset: u64,
        count: u32,
    },
    SharedDataBackref {
        ref_offset: u64,
        parent: u64,
        count: u32,
    },
    ExtentOwnerRef {
        ref_offset: u64,
        root: u64,
    },
}

impl InlineRef {
    /// The raw type byte for this inline ref.
    #[allow(clippy::cast_possible_truncation)]
    pub fn raw_type(&self) -> u8 {
        match self {
            Self::TreeBlockBackref { .. } => {
                raw::BTRFS_TREE_BLOCK_REF_KEY as u8
            }
            Self::SharedBlockBackref { .. } => {
                raw::BTRFS_SHARED_BLOCK_REF_KEY as u8
            }
            Self::ExtentDataBackref { .. } => {
                raw::BTRFS_EXTENT_DATA_REF_KEY as u8
            }
            Self::SharedDataBackref { .. } => {
                raw::BTRFS_SHARED_DATA_REF_KEY as u8
            }
            Self::ExtentOwnerRef { .. } => {
                raw::BTRFS_EXTENT_OWNER_REF_KEY as u8
            }
        }
    }

    /// The offset value from the inline ref header.
    pub fn raw_offset(&self) -> u64 {
        match self {
            Self::TreeBlockBackref { ref_offset, .. }
            | Self::SharedBlockBackref { ref_offset, .. }
            | Self::ExtentDataBackref { ref_offset, .. }
            | Self::SharedDataBackref { ref_offset, .. }
            | Self::ExtentOwnerRef { ref_offset, .. } => *ref_offset,
        }
    }
}

bitflags::bitflags! {
    /// Extent item flags stored in `btrfs_extent_item::flags`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ExtentFlags: u64 {
        const DATA         = raw::BTRFS_EXTENT_FLAG_DATA as u64;
        const TREE_BLOCK   = raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64;
        const FULL_BACKREF = raw::BTRFS_BLOCK_FLAG_FULL_BACKREF as u64;
        // Preserve unknown bits from the on-disk value.
        const _ = !0;
    }
}

impl fmt::Display for ExtentFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.contains(Self::DATA) {
            parts.push("DATA");
        }
        if self.contains(Self::TREE_BLOCK) {
            parts.push("TREE_BLOCK");
        }
        if self.contains(Self::FULL_BACKREF) {
            parts.push("FULL_BACKREF");
        }
        write!(f, "{}", parts.join("|"))
    }
}

/// Extent allocation record from the extent tree.
///
/// Tracks reference counts, ownership, and backreferences for a contiguous
/// range of allocated disk space. Used for both data extents (`EXTENT_ITEM`)
/// and metadata blocks (`METADATA_ITEM` with skinny metadata).
#[derive(Debug, Clone)]
pub struct ExtentItem {
    /// Number of references to this extent.
    pub refs: u64,
    /// Generation when this extent was allocated.
    pub generation: u64,
    /// Whether this extent holds data or a tree block.
    pub flags: ExtentFlags,
    /// For non-skinny tree block extents: the first key in the block.
    pub tree_block_key: Option<DiskKey>,
    /// For non-skinny tree block extents: the block's tree level.
    pub tree_block_level: Option<u8>,
    /// For skinny metadata items: the tree level (from the key offset).
    pub skinny_level: Option<u64>,
    /// Inline backreferences packed after the extent header.
    pub inline_refs: Vec<InlineRef>,
}

impl ExtentItem {
    pub fn is_data(&self) -> bool {
        self.flags.contains(ExtentFlags::DATA)
    }

    pub fn is_tree_block(&self) -> bool {
        self.flags.contains(ExtentFlags::TREE_BLOCK)
    }

    pub fn parse(data: &[u8], key: &DiskKey) -> Option<Self> {
        use crate::tree::KeyType;

        if data.len() < mem::size_of::<raw::btrfs_extent_item>() {
            return None;
        }
        let mut buf = data;
        let refs = buf.get_u64_le();
        let generation = buf.get_u64_le();
        let flags = ExtentFlags::from_bits_truncate(buf.get_u64_le());

        let is_tree_block = flags.contains(ExtentFlags::TREE_BLOCK);

        let mut tree_block_key = None;
        let mut tree_block_level = None;
        if is_tree_block
            && key.key_type == KeyType::ExtentItem
            && buf.remaining() > 17
        {
            tree_block_key = Some(DiskKey::parse(buf, 0));
            buf.advance(17); // skip DiskKey
            tree_block_level = Some(buf.get_u8());
        }

        let skinny_level =
            if key.key_type == KeyType::MetadataItem && is_tree_block {
                Some(key.offset)
            } else {
                None
            };

        let mut inline_refs = Vec::new();
        while buf.remaining() > 0 {
            let ref_type = buf.get_u8();
            let ref_offset = if buf.remaining() >= 8 {
                buf.get_u64_le()
            } else {
                0
            };

            match u32::from(ref_type) {
                raw::BTRFS_TREE_BLOCK_REF_KEY => {
                    inline_refs.push(InlineRef::TreeBlockBackref {
                        ref_offset,
                        root: ref_offset,
                    });
                }
                raw::BTRFS_SHARED_BLOCK_REF_KEY => {
                    inline_refs.push(InlineRef::SharedBlockBackref {
                        ref_offset,
                        parent: ref_offset,
                    });
                }
                raw::BTRFS_EXTENT_DATA_REF_KEY => {
                    // EXTENT_DATA_REF has no 8-byte offset field; the struct
                    // starts directly after the type byte. The 8 bytes we
                    // speculatively consumed are actually the first field
                    // (root) of the struct, so reinterpret them.
                    let root = ref_offset; // already read as u64_le
                    if buf.remaining() >= 20 {
                        let oid = buf.get_u64_le();
                        let off = buf.get_u64_le();
                        let count = buf.get_u32_le();
                        // The C tool prints a CRC hash for the display offset;
                        // compute it the same way: hash(root, objectid, offset).
                        let hash = extent_data_ref_hash(root, oid, off);
                        inline_refs.push(InlineRef::ExtentDataBackref {
                            ref_offset: hash,
                            root,
                            objectid: oid,
                            offset: off,
                            count,
                        });
                    } else {
                        break;
                    }
                }
                raw::BTRFS_SHARED_DATA_REF_KEY => {
                    if buf.remaining() >= 4 {
                        let count = buf.get_u32_le();
                        inline_refs.push(InlineRef::SharedDataBackref {
                            ref_offset,
                            parent: ref_offset,
                            count,
                        });
                    } else {
                        break;
                    }
                }
                raw::BTRFS_EXTENT_OWNER_REF_KEY => {
                    inline_refs.push(InlineRef::ExtentOwnerRef {
                        ref_offset,
                        root: ref_offset,
                    });
                }
                _ => break,
            }
        }

        Some(Self {
            refs,
            generation,
            flags,
            tree_block_key,
            tree_block_level,
            skinny_level,
            inline_refs,
        })
    }
}

/// Standalone data extent backreference (non-inline).
///
/// Key: `(extent_bytenr, EXTENT_DATA_REF, hash)`. Records which file inode
/// references a given data extent.
#[derive(Debug, Clone)]
pub struct ExtentDataRef {
    /// Root tree objectid that owns the referencing inode.
    pub root: u64,
    /// Inode number that references this extent.
    pub objectid: u64,
    /// File offset where this extent is referenced.
    pub offset: u64,
    /// Number of references from this (root, objectid, offset) triple.
    pub count: u32,
}

impl ExtentDataRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_extent_data_ref>() {
            return None;
        }
        let mut buf = data;
        Some(Self {
            root: buf.get_u64_le(),
            objectid: buf.get_u64_le(),
            offset: buf.get_u64_le(),
            count: buf.get_u32_le(),
        })
    }
}

/// Shared data extent backreference (for snapshot-shared extents).
///
/// Key: `(extent_bytenr, SHARED_DATA_REF, parent_bytenr)`.
#[derive(Debug, Clone)]
pub struct SharedDataRef {
    /// Number of references from the parent block.
    pub count: u32,
}

impl SharedDataRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let mut buf = data;
        Some(Self {
            count: buf.get_u32_le(),
        })
    }
}

/// Block group descriptor, tracking space usage for a chunk.
///
/// Key: `(logical_offset, BLOCK_GROUP_ITEM, length)`.
#[derive(Debug, Clone)]
pub struct BlockGroupItem {
    /// Bytes used within this block group.
    pub used: u64,
    /// Objectid of the chunk that backs this block group.
    pub chunk_objectid: u64,
    /// Type and RAID profile flags (DATA, METADATA, SYSTEM, DUP, RAID*, etc.).
    pub flags: BlockGroupFlags,
}

impl BlockGroupItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_block_group_item>() {
            return None;
        }
        let mut buf = data;
        Some(Self {
            used: buf.get_u64_le(),
            chunk_objectid: buf.get_u64_le(),
            flags: BlockGroupFlags::from_bits_truncate(buf.get_u64_le()),
        })
    }
}

/// Chunk item mapping logical addresses to physical device locations.
///
/// Key: `(FIRST_CHUNK_TREE, CHUNK_ITEM, logical_offset)`. Each chunk maps a
/// contiguous range of logical addresses to one or more device stripes.
#[derive(Debug, Clone)]
pub struct ChunkItem {
    /// Length of this chunk in bytes.
    pub length: u64,
    /// Owner of this chunk (always `BTRFS_FIRST_CHUNK_TREE_OBJECTID`).
    pub owner: u64,
    /// Stripe length for striped profiles.
    pub stripe_len: u64,
    /// Type and RAID profile flags.
    pub chunk_type: BlockGroupFlags,
    /// I/O alignment requirement.
    pub io_align: u32,
    /// I/O width requirement.
    pub io_width: u32,
    /// Sector size of the underlying devices.
    pub sector_size: u32,
    /// Number of stripes (device copies) for this chunk.
    pub num_stripes: u16,
    /// Number of sub-stripes (for RAID10).
    pub sub_stripes: u16,
    /// Physical device locations for each stripe.
    pub stripes: Vec<ChunkStripe>,
}

/// A single physical stripe within a chunk.
#[derive(Debug, Clone)]
pub struct ChunkStripe {
    /// Device ID where this stripe lives.
    pub devid: u64,
    /// Physical byte offset on the device.
    pub offset: u64,
    /// UUID of the device.
    pub dev_uuid: Uuid,
}

impl ChunkItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
        if data.len() < chunk_base_size {
            return None;
        }
        let mut buf = data;
        let length = buf.get_u64_le();
        let owner = buf.get_u64_le();
        let stripe_len = buf.get_u64_le();
        let chunk_type = BlockGroupFlags::from_bits_truncate(buf.get_u64_le());
        let io_align = buf.get_u32_le();
        let io_width = buf.get_u32_le();
        let sector_size = buf.get_u32_le();
        let num_stripes = buf.get_u16_le();
        let sub_stripes = buf.get_u16_le();
        let stripe_size = mem::size_of::<raw::btrfs_stripe>();
        let mut stripes = Vec::with_capacity(num_stripes as usize);
        let mut sbuf = &data[chunk_base_size..];
        for i in 0..num_stripes as usize {
            let s_off = chunk_base_size + i * stripe_size;
            if s_off + stripe_size > data.len() {
                break;
            }
            let devid = sbuf.get_u64_le();
            let offset = sbuf.get_u64_le();
            let dev_uuid = get_uuid(&mut sbuf);
            stripes.push(ChunkStripe {
                devid,
                offset,
                dev_uuid,
            });
        }
        Some(Self {
            length,
            owner,
            stripe_len,
            chunk_type,
            io_align,
            io_width,
            sector_size,
            num_stripes,
            sub_stripes,
            stripes,
        })
    }
}

/// Device item describing a single device in the filesystem.
///
/// Stored in the device tree and embedded in the superblock. Contains the
/// device's size, usage, and identifying UUIDs.
#[derive(Debug, Clone)]
pub struct DeviceItem {
    /// Unique device ID within this filesystem.
    pub devid: u64,
    /// Total size of the device in bytes.
    pub total_bytes: u64,
    /// Bytes allocated on this device.
    pub bytes_used: u64,
    /// I/O alignment requirement.
    pub io_align: u32,
    /// I/O width requirement.
    pub io_width: u32,
    /// Sector size of this device.
    pub sector_size: u32,
    /// Device type (reserved, always 0).
    pub dev_type: u64,
    /// Generation when this device was last updated.
    pub generation: u64,
    /// Start offset for allocations on this device.
    pub start_offset: u64,
    /// Device group (reserved, always 0).
    pub dev_group: u32,
    /// Seek speed hint (0 = not set).
    pub seek_speed: u8,
    /// Bandwidth hint (0 = not set).
    pub bandwidth: u8,
    /// UUID of this device.
    pub uuid: Uuid,
    /// Filesystem UUID that this device belongs to.
    pub fsid: Uuid,
}

impl DeviceItem {
    /// Serialize the device item to a `BufMut`.
    pub fn write_bytes(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.devid);
        buf.put_u64_le(self.total_bytes);
        buf.put_u64_le(self.bytes_used);
        buf.put_u32_le(self.io_align);
        buf.put_u32_le(self.io_width);
        buf.put_u32_le(self.sector_size);
        buf.put_u64_le(self.dev_type);
        buf.put_u64_le(self.generation);
        buf.put_u64_le(self.start_offset);
        buf.put_u32_le(self.dev_group);
        buf.put_u8(self.seek_speed);
        buf.put_u8(self.bandwidth);
        buf.put_slice(self.uuid.as_bytes());
        buf.put_slice(self.fsid.as_bytes());
    }

    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_dev_item>() {
            return None;
        }
        let mut buf = data;
        let devid = buf.get_u64_le();
        let total_bytes = buf.get_u64_le();
        let bytes_used = buf.get_u64_le();
        let io_align = buf.get_u32_le();
        let io_width = buf.get_u32_le();
        let sector_size = buf.get_u32_le();
        let dev_type = buf.get_u64_le();
        let generation = buf.get_u64_le();
        let start_offset = buf.get_u64_le();
        let dev_group = buf.get_u32_le();
        let seek_speed = buf.get_u8();
        let bandwidth = buf.get_u8();
        let uuid = get_uuid(&mut buf);
        let fsid = get_uuid(&mut buf);
        Some(Self {
            devid,
            total_bytes,
            bytes_used,
            io_align,
            io_width,
            sector_size,
            dev_type,
            generation,
            start_offset,
            dev_group,
            seek_speed,
            bandwidth,
            uuid,
            fsid,
        })
    }
}

/// Device extent, mapping a physical range on a device to a chunk.
///
/// Key: `(devid, DEV_EXTENT, physical_offset)`. The inverse of a chunk
/// stripe: given a device and physical offset, find the owning chunk.
#[derive(Debug, Clone)]
pub struct DeviceExtent {
    /// Objectid of the chunk tree (always 3).
    pub chunk_tree: u64,
    /// Objectid of the owning chunk.
    pub chunk_objectid: u64,
    /// Logical offset of the owning chunk.
    pub chunk_offset: u64,
    /// Length of this device extent in bytes.
    pub length: u64,
    /// UUID of the chunk tree.
    pub chunk_tree_uuid: Uuid,
}

impl DeviceExtent {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_dev_extent>() {
            return None;
        }
        let mut buf = data;
        let chunk_tree = buf.get_u64_le();
        let chunk_objectid = buf.get_u64_le();
        let chunk_offset = buf.get_u64_le();
        let length = buf.get_u64_le();
        let chunk_tree_uuid = get_uuid(&mut buf);
        Some(Self {
            chunk_tree,
            chunk_objectid,
            chunk_offset,
            length,
            chunk_tree_uuid,
        })
    }
}

bitflags::bitflags! {
    /// Free space info flags stored in `btrfs_free_space_info::flags`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct FreeSpaceInfoFlags: u32 {
        const USING_BITMAPS = raw::BTRFS_FREE_SPACE_USING_BITMAPS;
        // Preserve unknown bits from the on-disk value.
        const _ = !0;
    }
}

impl fmt::Display for FreeSpaceInfoFlags {
    // The C reference prints this field as an unsigned decimal integer (%u).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.bits())
    }
}

/// Free space info for a block group in the free space tree.
///
/// Key: `(block_group_offset, FREE_SPACE_INFO, block_group_length)`.
#[derive(Debug, Clone)]
pub struct FreeSpaceInfo {
    /// Number of free extents (or bitmap entries) in this block group.
    pub extent_count: u32,
    /// Flags indicating whether this block group uses bitmaps.
    pub flags: FreeSpaceInfoFlags,
}

impl FreeSpaceInfo {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let mut buf = data;
        Some(Self {
            extent_count: buf.get_u32_le(),
            flags: FreeSpaceInfoFlags::from_bits_truncate(buf.get_u32_le()),
        })
    }
}

/// Quota group status, stored in the quota tree.
///
/// Key: `(0, QGROUP_STATUS, 0)`. Tracks the overall state of quota accounting.
#[derive(Debug, Clone)]
pub struct QgroupStatus {
    /// Qgroup on-disk format version.
    pub version: u64,
    /// Generation when quotas were last consistent.
    pub generation: u64,
    /// Status flags (e.g. rescan in progress).
    pub flags: u64,
    /// Progress objectid for an in-progress rescan.
    pub scan: u64,
    /// Generation when quotas were enabled (kernel 6.8+, absent on older formats).
    pub enable_gen: Option<u64>,
}

impl QgroupStatus {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 32 {
            return None;
        }
        let mut buf = data;
        let version = buf.get_u64_le();
        let generation = buf.get_u64_le();
        let flags = buf.get_u64_le();
        let scan = buf.get_u64_le();
        let enable_gen = if buf.remaining() >= 8 {
            Some(buf.get_u64_le())
        } else {
            None
        };
        Some(Self {
            version,
            generation,
            flags,
            scan,
            enable_gen,
        })
    }
}

/// Quota group accounting info.
///
/// Key: `(level/subvolid, QGROUP_INFO, 0)`. Tracks how much space a qgroup
/// references and how much is exclusive to it.
#[derive(Debug, Clone)]
pub struct QgroupInfo {
    /// Generation when this info was last updated.
    pub generation: u64,
    /// Total bytes referenced by this qgroup (shared + exclusive).
    pub referenced: u64,
    /// Referenced bytes after compression.
    pub referenced_compressed: u64,
    /// Bytes used exclusively by this qgroup.
    pub exclusive: u64,
    /// Exclusive bytes after compression.
    pub exclusive_compressed: u64,
}

impl QgroupInfo {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_qgroup_info_item>() {
            return None;
        }
        let mut buf = data;
        Some(Self {
            generation: buf.get_u64_le(),
            referenced: buf.get_u64_le(),
            referenced_compressed: buf.get_u64_le(),
            exclusive: buf.get_u64_le(),
            exclusive_compressed: buf.get_u64_le(),
        })
    }
}

/// Quota group limits.
///
/// Key: `(level/subvolid, QGROUP_LIMIT, 0)`. Caps referenced and/or exclusive
/// space usage for a qgroup.
#[derive(Debug, Clone)]
pub struct QgroupLimit {
    /// Bitmask of which limits are active.
    pub flags: u64,
    /// Maximum referenced bytes (0 = unlimited).
    pub max_referenced: u64,
    /// Maximum exclusive bytes (0 = unlimited).
    pub max_exclusive: u64,
    /// Reserved referenced bytes.
    pub rsv_referenced: u64,
    /// Reserved exclusive bytes.
    pub rsv_exclusive: u64,
}

impl QgroupLimit {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_qgroup_limit_item>() {
            return None;
        }
        let mut buf = data;
        Some(Self {
            flags: buf.get_u64_le(),
            max_referenced: buf.get_u64_le(),
            max_exclusive: buf.get_u64_le(),
            rsv_referenced: buf.get_u64_le(),
            rsv_exclusive: buf.get_u64_le(),
        })
    }
}

/// Per-device I/O error statistics.
///
/// Key: `(DEV_STATS, PERSISTENT_ITEM, devid)`. Stored as an array of u64
/// counters for write errors, read errors, flush errors, corruption errors,
/// and generation mismatches.
#[derive(Debug, Clone)]
pub struct DeviceStats {
    /// Named counters: `(stat_name, count)`.
    pub values: Vec<(String, u64)>,
}

impl DeviceStats {
    pub fn parse(data: &[u8]) -> Self {
        let stat_names = [
            "write_errs",
            "read_errs",
            "flush_errs",
            "corruption_errs",
            "generation",
        ];
        let mut buf = data;
        let mut values = Vec::new();
        for name in &stat_names {
            if buf.remaining() >= 8 {
                values.push((name.to_string(), buf.get_u64_le()));
            }
        }
        DeviceStats { values }
    }
}

/// UUID tree entry mapping a subvolume UUID to its objectid(s).
///
/// Key: `(upper_half_of_uuid, UUID_KEY_SUBVOL, lower_half_of_uuid)`.
#[derive(Debug, Clone)]
pub struct UuidItem {
    /// Subvolume objectids associated with this UUID.
    pub subvol_ids: Vec<u64>,
}

impl UuidItem {
    pub fn parse(data: &[u8]) -> Self {
        let mut buf = data;
        let mut subvol_ids = Vec::new();
        while buf.remaining() >= 8 {
            subvol_ids.push(buf.get_u64_le());
        }
        Self { subvol_ids }
    }
}

/// Parsed item payload: the typed result of parsing a leaf item's raw data
/// based on its key type.
///
/// Returned by [`parse_item_payload`]. Each variant wraps the corresponding
/// item struct. `Unknown` holds the raw bytes for unrecognized key types.
pub enum ItemPayload {
    InodeItem(InodeItem),
    InodeRef(Vec<InodeRef>),
    InodeExtref(Vec<InodeExtref>),
    DirItem(Vec<DirItem>),
    DirLogItem { end: u64 },
    OrphanItem,
    RootItem(RootItem),
    RootRef(RootRef),
    FileExtentItem(FileExtentItem),
    ExtentCsum { data: Vec<u8> },
    ExtentItem(ExtentItem),
    TreeBlockRef,
    SharedBlockRef,
    ExtentDataRef(ExtentDataRef),
    SharedDataRef(SharedDataRef),
    ExtentOwnerRef { root: u64 },
    BlockGroupItem(BlockGroupItem),
    FreeSpaceInfo(FreeSpaceInfo),
    FreeSpaceExtent,
    FreeSpaceBitmap,
    ChunkItem(ChunkItem),
    DeviceItem(DeviceItem),
    DeviceExtent(DeviceExtent),
    QgroupStatus(QgroupStatus),
    QgroupInfo(QgroupInfo),
    QgroupLimit(QgroupLimit),
    QgroupRelation,
    DeviceStats(DeviceStats),
    BalanceItem { flags: u64 },
    DeviceReplace(DeviceReplaceItem),
    UuidItem(UuidItem),
    StringItem(Vec<u8>),
    RaidStripe(RaidStripeItem),
    Unknown(Vec<u8>),
}

/// Device replace status, persisted across reboots.
///
/// Key: `(DEV_REPLACE, PERSISTENT_ITEM, 0)`.
#[derive(Debug, Clone)]
pub struct DeviceReplaceItem {
    /// Device ID of the source device being replaced.
    pub src_devid: u64,
    /// Left cursor position (bytes processed from left).
    pub cursor_left: u64,
    /// Right cursor position.
    pub cursor_right: u64,
    /// Replace mode (continuous = 0 or legacy).
    pub replace_mode: u64,
    /// Current state (not started, started, suspended, etc.).
    pub replace_state: u64,
    /// Unix timestamp when the replace operation started.
    pub time_started: u64,
    /// Unix timestamp when the replace operation completed or was cancelled.
    pub time_stopped: u64,
    /// Number of write errors during replace.
    pub num_write_errors: u64,
    /// Number of uncorrectable read errors during replace.
    pub num_uncorrectable_read_errors: u64,
}

impl DeviceReplaceItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 80 {
            return None;
        }
        let mut buf = data;
        Some(Self {
            src_devid: buf.get_u64_le(),
            cursor_left: buf.get_u64_le(),
            cursor_right: buf.get_u64_le(),
            replace_mode: buf.get_u64_le(),
            replace_state: buf.get_u64_le(),
            time_started: buf.get_u64_le(),
            time_stopped: buf.get_u64_le(),
            num_write_errors: buf.get_u64_le(),
            num_uncorrectable_read_errors: buf.get_u64_le(),
        })
    }
}

/// RAID stripe extent mapping (for the raid-stripe-tree feature).
///
/// Key: `(logical_offset, RAID_STRIPE, length)`.
#[derive(Debug, Clone)]
pub struct RaidStripeItem {
    /// RAID encoding type.
    pub encoding: u64,
    /// Per-device stripe entries.
    pub stripes: Vec<RaidStripeEntry>,
}

/// A single device stripe within a RAID stripe item.
#[derive(Debug, Clone)]
pub struct RaidStripeEntry {
    /// Device ID for this stripe.
    pub devid: u64,
    /// Physical byte offset on the device.
    pub physical: u64,
}

impl RaidStripeItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let mut buf = data;
        let encoding = buf.get_u64_le();
        let mut stripes = Vec::new();
        while buf.remaining() >= 16 {
            stripes.push(RaidStripeEntry {
                devid: buf.get_u64_le(),
                physical: buf.get_u64_le(),
            });
        }
        Some(Self { encoding, stripes })
    }
}

/// Parse an item's raw data into a typed payload based on its key type.
#[allow(clippy::too_many_lines)]
pub fn parse_item_payload(key: &DiskKey, data: &[u8]) -> ItemPayload {
    use crate::tree::KeyType;

    match key.key_type {
        KeyType::InodeItem => match InodeItem::parse(data) {
            Some(v) => ItemPayload::InodeItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::InodeRef => ItemPayload::InodeRef(InodeRef::parse_all(data)),
        KeyType::InodeExtref => {
            ItemPayload::InodeExtref(InodeExtref::parse_all(data))
        }
        KeyType::DirItem | KeyType::DirIndex | KeyType::XattrItem => {
            ItemPayload::DirItem(DirItem::parse_all(data))
        }
        KeyType::DirLogItem | KeyType::DirLogIndex => {
            let end = if data.len() >= 8 {
                let mut buf = data;
                buf.get_u64_le()
            } else {
                0
            };
            ItemPayload::DirLogItem { end }
        }
        KeyType::OrphanItem => ItemPayload::OrphanItem,
        KeyType::RootItem => match RootItem::parse(data) {
            Some(v) => ItemPayload::RootItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::RootRef | KeyType::RootBackref => match RootRef::parse(data) {
            Some(v) => ItemPayload::RootRef(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::ExtentData => match FileExtentItem::parse(data) {
            Some(v) => ItemPayload::FileExtentItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::ExtentCsum => ItemPayload::ExtentCsum {
            data: data.to_vec(),
        },
        KeyType::ExtentItem | KeyType::MetadataItem => {
            match ExtentItem::parse(data, key) {
                Some(v) => ItemPayload::ExtentItem(v),
                None => ItemPayload::Unknown(data.to_vec()),
            }
        }
        KeyType::TreeBlockRef => ItemPayload::TreeBlockRef,
        KeyType::SharedBlockRef => ItemPayload::SharedBlockRef,
        KeyType::ExtentDataRef => match ExtentDataRef::parse(data) {
            Some(v) => ItemPayload::ExtentDataRef(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::SharedDataRef => match SharedDataRef::parse(data) {
            Some(v) => ItemPayload::SharedDataRef(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::ExtentOwnerRef => {
            if data.len() >= 8 {
                let mut buf = data;
                ItemPayload::ExtentOwnerRef {
                    root: buf.get_u64_le(),
                }
            } else {
                ItemPayload::Unknown(data.to_vec())
            }
        }
        KeyType::BlockGroupItem => match BlockGroupItem::parse(data) {
            Some(v) => ItemPayload::BlockGroupItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::FreeSpaceInfo => match FreeSpaceInfo::parse(data) {
            Some(v) => ItemPayload::FreeSpaceInfo(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::FreeSpaceExtent => ItemPayload::FreeSpaceExtent,
        KeyType::FreeSpaceBitmap => ItemPayload::FreeSpaceBitmap,
        KeyType::ChunkItem => match ChunkItem::parse(data) {
            Some(v) => ItemPayload::ChunkItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::DeviceItem => match DeviceItem::parse(data) {
            Some(v) => ItemPayload::DeviceItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::DeviceExtent => match DeviceExtent::parse(data) {
            Some(v) => ItemPayload::DeviceExtent(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::QgroupStatus => match QgroupStatus::parse(data) {
            Some(v) => ItemPayload::QgroupStatus(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::QgroupInfo => match QgroupInfo::parse(data) {
            Some(v) => ItemPayload::QgroupInfo(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::QgroupLimit => match QgroupLimit::parse(data) {
            Some(v) => ItemPayload::QgroupLimit(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::QgroupRelation => ItemPayload::QgroupRelation,
        KeyType::PersistentItem => {
            if key.objectid == u64::from(raw::BTRFS_DEV_STATS_OBJECTID) {
                ItemPayload::DeviceStats(DeviceStats::parse(data))
            } else {
                ItemPayload::Unknown(data.to_vec())
            }
        }
        KeyType::TemporaryItem => {
            if ObjectId::from_raw(key.objectid) == ObjectId::Balance
                && data.len() >= 8
            {
                ItemPayload::BalanceItem {
                    flags: {
                        let mut buf = data;
                        buf.get_u64_le()
                    },
                }
            } else {
                ItemPayload::Unknown(data.to_vec())
            }
        }
        KeyType::DeviceReplace => match DeviceReplaceItem::parse(data) {
            Some(v) => ItemPayload::DeviceReplace(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::UuidKeySubvol | KeyType::UuidKeyReceivedSubvol => {
            ItemPayload::UuidItem(UuidItem::parse(data))
        }
        KeyType::StringItem => ItemPayload::StringItem(data.to_vec()),
        KeyType::RaidStripe => match RaidStripeItem::parse(data) {
            Some(v) => ItemPayload::RaidStripe(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        _ => ItemPayload::Unknown(data.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Enum round-trips ──────────────────────────────────────────────

    #[test]
    fn compression_type_round_trip() {
        for v in 0..=3 {
            let ct = CompressionType::from_raw(v);
            assert_eq!(ct.to_raw(), v);
        }
        assert_eq!(CompressionType::from_raw(0), CompressionType::None);
        assert_eq!(CompressionType::from_raw(1), CompressionType::Zlib);
        assert_eq!(CompressionType::from_raw(2), CompressionType::Lzo);
        assert_eq!(CompressionType::from_raw(3), CompressionType::Zstd);
        assert_eq!(CompressionType::from_raw(99), CompressionType::Unknown(99));
        assert_eq!(CompressionType::Unknown(99).to_raw(), 99);
    }

    #[test]
    fn compression_type_names() {
        assert_eq!(CompressionType::None.name(), "none");
        assert_eq!(CompressionType::Zlib.name(), "zlib");
        assert_eq!(CompressionType::Lzo.name(), "lzo");
        assert_eq!(CompressionType::Zstd.name(), "zstd");
        assert_eq!(CompressionType::Unknown(42).name(), "unknown");
    }

    #[test]
    fn file_extent_type_round_trip() {
        assert_eq!(FileExtentType::from_raw(0), FileExtentType::Inline);
        assert_eq!(FileExtentType::from_raw(1), FileExtentType::Regular);
        assert_eq!(FileExtentType::from_raw(2), FileExtentType::Prealloc);
        assert_eq!(FileExtentType::from_raw(77), FileExtentType::Unknown(77));
        for v in 0..=2 {
            let ft = FileExtentType::from_raw(v);
            assert_eq!(ft.to_raw(), v);
        }
        assert_eq!(FileExtentType::Unknown(77).to_raw(), 77);
    }

    #[test]
    fn file_extent_type_names() {
        assert_eq!(FileExtentType::Inline.name(), "inline");
        assert_eq!(FileExtentType::Regular.name(), "regular");
        assert_eq!(FileExtentType::Prealloc.name(), "prealloc");
        assert_eq!(FileExtentType::Unknown(5).name(), "unknown");
    }

    #[test]
    fn file_type_from_raw_all_variants() {
        assert_eq!(FileType::from_raw(0), FileType::Unknown);
        assert_eq!(FileType::from_raw(1), FileType::RegFile);
        assert_eq!(FileType::from_raw(2), FileType::Dir);
        assert_eq!(FileType::from_raw(3), FileType::Chrdev);
        assert_eq!(FileType::from_raw(4), FileType::Blkdev);
        assert_eq!(FileType::from_raw(5), FileType::Fifo);
        assert_eq!(FileType::from_raw(6), FileType::Sock);
        assert_eq!(FileType::from_raw(7), FileType::Symlink);
        assert_eq!(FileType::from_raw(8), FileType::Xattr);
        assert_eq!(FileType::from_raw(99), FileType::Other(99));
    }

    #[test]
    fn file_type_names() {
        assert_eq!(FileType::Unknown.name(), "UNKNOWN");
        assert_eq!(FileType::RegFile.name(), "FILE");
        assert_eq!(FileType::Dir.name(), "DIR");
        assert_eq!(FileType::Chrdev.name(), "CHRDEV");
        assert_eq!(FileType::Blkdev.name(), "BLKDEV");
        assert_eq!(FileType::Fifo.name(), "FIFO");
        assert_eq!(FileType::Sock.name(), "SOCK");
        assert_eq!(FileType::Symlink.name(), "SYMLINK");
        assert_eq!(FileType::Xattr.name(), "XATTR");
        assert_eq!(FileType::Other(200).name(), "UNKNOWN");
    }

    // ── Simple struct parsers ─────────────────────────────────────────

    #[test]
    fn block_group_item_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000u64.to_le_bytes()); // used
        buf.extend_from_slice(&256u64.to_le_bytes()); // chunk_objectid
        buf.extend_from_slice(
            &(raw::BTRFS_BLOCK_GROUP_DATA as u64).to_le_bytes(),
        );
        let item = BlockGroupItem::parse(&buf).unwrap();
        assert_eq!(item.used, 1000);
        assert_eq!(item.chunk_objectid, 256);
        assert_eq!(item.flags, BlockGroupFlags::DATA);
    }

    #[test]
    fn block_group_item_too_short() {
        assert!(BlockGroupItem::parse(&[0; 23]).is_none());
    }

    #[test]
    fn free_space_info_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u32.to_le_bytes());
        buf.extend_from_slice(&7u32.to_le_bytes());
        let info = FreeSpaceInfo::parse(&buf).unwrap();
        assert_eq!(info.extent_count, 42);
        assert_eq!(info.flags, FreeSpaceInfoFlags::from_bits_truncate(7));
    }

    #[test]
    fn free_space_info_too_short() {
        assert!(FreeSpaceInfo::parse(&[0; 7]).is_none());
    }

    #[test]
    fn dev_extent_parse() {
        let size = mem::size_of::<raw::btrfs_dev_extent>();
        let mut buf = vec![0u8; size];
        buf[0..8].copy_from_slice(&3u64.to_le_bytes()); // chunk_tree
        buf[8..16].copy_from_slice(&256u64.to_le_bytes()); // chunk_objectid
        buf[16..24].copy_from_slice(&0x10000u64.to_le_bytes()); // chunk_offset
        buf[24..32].copy_from_slice(&0x40000u64.to_le_bytes()); // length
        // chunk_tree_uuid at offset 32
        buf[32..48].copy_from_slice(&[0xAB; 16]);
        let de = DeviceExtent::parse(&buf).unwrap();
        assert_eq!(de.chunk_tree, 3);
        assert_eq!(de.chunk_objectid, 256);
        assert_eq!(de.chunk_offset, 0x10000);
        assert_eq!(de.length, 0x40000);
        assert_eq!(de.chunk_tree_uuid.as_bytes(), &[0xAB; 16]);
    }

    #[test]
    fn dev_extent_too_short() {
        let size = mem::size_of::<raw::btrfs_dev_extent>();
        assert!(DeviceExtent::parse(&vec![0u8; size - 1]).is_none());
    }

    #[test]
    fn extent_data_ref_parse() {
        let size = mem::size_of::<raw::btrfs_extent_data_ref>();
        let mut buf = vec![0u8; size];
        buf[0..8].copy_from_slice(&5u64.to_le_bytes()); // root
        buf[8..16].copy_from_slice(&256u64.to_le_bytes()); // objectid
        buf[16..24].copy_from_slice(&0u64.to_le_bytes()); // offset
        buf[24..28].copy_from_slice(&1u32.to_le_bytes()); // count
        let edr = ExtentDataRef::parse(&buf).unwrap();
        assert_eq!(edr.root, 5);
        assert_eq!(edr.objectid, 256);
        assert_eq!(edr.offset, 0);
        assert_eq!(edr.count, 1);
    }

    #[test]
    fn extent_data_ref_too_short() {
        assert!(ExtentDataRef::parse(&[0; 27]).is_none());
    }

    #[test]
    fn shared_data_ref_parse() {
        let buf = 17u32.to_le_bytes();
        let sdr = SharedDataRef::parse(&buf).unwrap();
        assert_eq!(sdr.count, 17);
    }

    #[test]
    fn shared_data_ref_too_short() {
        assert!(SharedDataRef::parse(&[0; 3]).is_none());
    }

    #[test]
    fn qgroup_info_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u64.to_le_bytes()); // generation
        buf.extend_from_slice(&4096u64.to_le_bytes()); // referenced
        buf.extend_from_slice(&4096u64.to_le_bytes()); // referenced_compressed
        buf.extend_from_slice(&2048u64.to_le_bytes()); // exclusive
        buf.extend_from_slice(&2048u64.to_le_bytes()); // exclusive_compressed
        let qi = QgroupInfo::parse(&buf).unwrap();
        assert_eq!(qi.generation, 100);
        assert_eq!(qi.referenced, 4096);
        assert_eq!(qi.referenced_compressed, 4096);
        assert_eq!(qi.exclusive, 2048);
        assert_eq!(qi.exclusive_compressed, 2048);
    }

    #[test]
    fn qgroup_info_too_short() {
        assert!(QgroupInfo::parse(&[0; 39]).is_none());
    }

    #[test]
    fn qgroup_limit_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u64.to_le_bytes()); // flags
        buf.extend_from_slice(&1_000_000u64.to_le_bytes()); // max_referenced
        buf.extend_from_slice(&500_000u64.to_le_bytes()); // max_exclusive
        buf.extend_from_slice(&0u64.to_le_bytes()); // rsv_referenced
        buf.extend_from_slice(&0u64.to_le_bytes()); // rsv_exclusive
        let ql = QgroupLimit::parse(&buf).unwrap();
        assert_eq!(ql.flags, 3);
        assert_eq!(ql.max_referenced, 1_000_000);
        assert_eq!(ql.max_exclusive, 500_000);
        assert_eq!(ql.rsv_referenced, 0);
        assert_eq!(ql.rsv_exclusive, 0);
    }

    #[test]
    fn qgroup_limit_too_short() {
        assert!(QgroupLimit::parse(&[0; 39]).is_none());
    }

    #[test]
    fn qgroup_status_parse_minimal() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // version
        buf.extend_from_slice(&50u64.to_le_bytes()); // generation
        buf.extend_from_slice(&2u64.to_le_bytes()); // flags
        buf.extend_from_slice(&0u64.to_le_bytes()); // scan
        let qs = QgroupStatus::parse(&buf).unwrap();
        assert_eq!(qs.version, 1);
        assert_eq!(qs.generation, 50);
        assert_eq!(qs.flags, 2);
        assert_eq!(qs.scan, 0);
        assert!(qs.enable_gen.is_none());
    }

    #[test]
    fn qgroup_status_parse_with_enable_gen() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&50u64.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes()); // enable_gen
        let qs = QgroupStatus::parse(&buf).unwrap();
        assert_eq!(qs.enable_gen, Some(99));
    }

    #[test]
    fn qgroup_status_too_short() {
        assert!(QgroupStatus::parse(&[0; 31]).is_none());
    }

    #[test]
    fn dev_replace_item_parse() {
        let mut buf = vec![0u8; 80];
        buf[0..8].copy_from_slice(&1u64.to_le_bytes()); // src_devid
        buf[8..16].copy_from_slice(&0x1000u64.to_le_bytes()); // cursor_left
        buf[16..24].copy_from_slice(&0x2000u64.to_le_bytes()); // cursor_right
        buf[24..32].copy_from_slice(&0u64.to_le_bytes()); // replace_mode
        buf[32..40].copy_from_slice(&2u64.to_le_bytes()); // replace_state
        buf[40..48].copy_from_slice(&1700000000u64.to_le_bytes()); // time_started
        buf[48..56].copy_from_slice(&1700000100u64.to_le_bytes()); // time_stopped
        buf[56..64].copy_from_slice(&3u64.to_le_bytes()); // num_write_errors
        buf[64..72].copy_from_slice(&5u64.to_le_bytes()); // num_uncorrectable_read_errors
        let dri = DeviceReplaceItem::parse(&buf).unwrap();
        assert_eq!(dri.src_devid, 1);
        assert_eq!(dri.cursor_left, 0x1000);
        assert_eq!(dri.cursor_right, 0x2000);
        assert_eq!(dri.replace_state, 2);
        assert_eq!(dri.time_started, 1700000000);
        assert_eq!(dri.time_stopped, 1700000100);
        assert_eq!(dri.num_write_errors, 3);
        assert_eq!(dri.num_uncorrectable_read_errors, 5);
    }

    #[test]
    fn dev_replace_item_too_short() {
        assert!(DeviceReplaceItem::parse(&[0; 79]).is_none());
    }

    #[test]
    fn raid_stripe_item_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // encoding
        // stripe 1
        buf.extend_from_slice(&1u64.to_le_bytes()); // devid
        buf.extend_from_slice(&0x10000u64.to_le_bytes()); // physical
        // stripe 2
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&0x20000u64.to_le_bytes());
        let rsi = RaidStripeItem::parse(&buf).unwrap();
        assert_eq!(rsi.encoding, 1);
        assert_eq!(rsi.stripes.len(), 2);
        assert_eq!(rsi.stripes[0].devid, 1);
        assert_eq!(rsi.stripes[0].physical, 0x10000);
        assert_eq!(rsi.stripes[1].devid, 2);
        assert_eq!(rsi.stripes[1].physical, 0x20000);
    }

    #[test]
    fn raid_stripe_item_no_stripes() {
        let buf = 42u64.to_le_bytes();
        let rsi = RaidStripeItem::parse(&buf).unwrap();
        assert_eq!(rsi.encoding, 42);
        assert!(rsi.stripes.is_empty());
    }

    #[test]
    fn raid_stripe_item_too_short() {
        assert!(RaidStripeItem::parse(&[0; 7]).is_none());
    }

    // ── Variable-length parsers ───────────────────────────────────────

    #[test]
    fn inode_ref_parse_single() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_le_bytes()); // index
        buf.extend_from_slice(&4u16.to_le_bytes()); // name_len
        buf.extend_from_slice(b"test");
        let refs = InodeRef::parse_all(&buf);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].index, 42);
        assert_eq!(refs[0].name, b"test");
    }

    #[test]
    fn inode_ref_parse_multiple() {
        let mut buf = Vec::new();
        // entry 1
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(b"abc");
        // entry 2
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(b"xy");
        let refs = InodeRef::parse_all(&buf);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].index, 1);
        assert_eq!(refs[0].name, b"abc");
        assert_eq!(refs[1].index, 2);
        assert_eq!(refs[1].name, b"xy");
    }

    #[test]
    fn inode_ref_parse_truncated() {
        // Header present but name extends past buffer end.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&10u16.to_le_bytes()); // claims 10 bytes
        buf.extend_from_slice(b"abc"); // only 3 available
        let refs = InodeRef::parse_all(&buf);
        assert!(refs.is_empty());
    }

    #[test]
    fn inode_extref_parse_single() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&256u64.to_le_bytes()); // parent
        buf.extend_from_slice(&3u64.to_le_bytes()); // index
        buf.extend_from_slice(&5u16.to_le_bytes()); // name_len
        buf.extend_from_slice(b"hello");
        let refs = InodeExtref::parse_all(&buf);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].parent, 256);
        assert_eq!(refs[0].index, 3);
        assert_eq!(refs[0].name, b"hello");
    }

    #[test]
    fn dir_item_parse_single() {
        let dir_item_size = mem::size_of::<raw::btrfs_dir_item>();
        let mut buf = vec![0u8; dir_item_size];
        // location: DiskKey at offset 0 (17 bytes: u64 objectid + u8 type + u64 offset)
        buf[0..8].copy_from_slice(&256u64.to_le_bytes()); // objectid
        buf[8] = 1; // key type
        buf[9..17].copy_from_slice(&0u64.to_le_bytes()); // offset
        // transid at offset 17
        buf[17..25].copy_from_slice(&100u64.to_le_bytes());
        // data_len at offset 25
        buf[25..27].copy_from_slice(&0u16.to_le_bytes());
        // name_len at offset 27
        buf[27..29].copy_from_slice(&4u16.to_le_bytes());
        // file_type at offset 29
        buf[29] = 1; // FT_REG_FILE
        // Append name
        buf.extend_from_slice(b"file");
        let items = DirItem::parse_all(&buf);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].transid, 100);
        assert_eq!(items[0].file_type, FileType::RegFile);
        assert_eq!(items[0].name, b"file");
        assert!(items[0].data.is_empty());
    }

    #[test]
    fn root_ref_parse() {
        let hdr_size = mem::size_of::<raw::btrfs_root_ref>();
        let mut buf = vec![0u8; hdr_size];
        buf[0..8].copy_from_slice(&256u64.to_le_bytes()); // dirid
        buf[8..16].copy_from_slice(&7u64.to_le_bytes()); // sequence
        buf[16..18].copy_from_slice(&6u16.to_le_bytes()); // name_len
        buf.extend_from_slice(b"subvol");
        let rr = RootRef::parse(&buf).unwrap();
        assert_eq!(rr.dirid, 256);
        assert_eq!(rr.sequence, 7);
        assert_eq!(rr.name, b"subvol");
    }

    #[test]
    fn root_ref_too_short() {
        let hdr_size = mem::size_of::<raw::btrfs_root_ref>();
        assert!(RootRef::parse(&vec![0u8; hdr_size - 1]).is_none());
    }

    #[test]
    fn uuid_item_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&256u64.to_le_bytes());
        buf.extend_from_slice(&257u64.to_le_bytes());
        buf.extend_from_slice(&258u64.to_le_bytes());
        let ui = UuidItem::parse(&buf);
        assert_eq!(ui.subvol_ids, vec![256, 257, 258]);
    }

    #[test]
    fn uuid_item_empty() {
        let ui = UuidItem::parse(&[]);
        assert!(ui.subvol_ids.is_empty());
    }

    #[test]
    fn dev_stats_parse() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // write_errs
        buf.extend_from_slice(&2u64.to_le_bytes()); // read_errs
        buf.extend_from_slice(&3u64.to_le_bytes()); // flush_errs
        buf.extend_from_slice(&4u64.to_le_bytes()); // corruption_errs
        buf.extend_from_slice(&5u64.to_le_bytes()); // generation
        let ds = DeviceStats::parse(&buf);
        assert_eq!(ds.values.len(), 5);
        assert_eq!(ds.values[0], ("write_errs".to_string(), 1));
        assert_eq!(ds.values[1], ("read_errs".to_string(), 2));
        assert_eq!(ds.values[2], ("flush_errs".to_string(), 3));
        assert_eq!(ds.values[3], ("corruption_errs".to_string(), 4));
        assert_eq!(ds.values[4], ("generation".to_string(), 5));
    }

    #[test]
    fn dev_stats_partial() {
        // Only 2 values available.
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u64.to_le_bytes());
        buf.extend_from_slice(&20u64.to_le_bytes());
        let ds = DeviceStats::parse(&buf);
        assert_eq!(ds.values.len(), 2);
        assert_eq!(ds.values[0].1, 10);
        assert_eq!(ds.values[1].1, 20);
    }

    // ── FileExtentItem ────────────────────────────────────────────────

    #[test]
    fn file_extent_item_inline() {
        let mut buf = vec![0u8; 21 + 10]; // 21 header + 10 inline data
        buf[0..8].copy_from_slice(&7u64.to_le_bytes()); // generation
        buf[8..16].copy_from_slice(&10u64.to_le_bytes()); // ram_bytes
        buf[16] = 0; // compression = none
        // bytes 17-19 are encryption/other_encoding (unused)
        buf[20] = 0; // extent_type = inline
        buf[21..31].copy_from_slice(&[0xAA; 10]); // inline data
        let fei = FileExtentItem::parse(&buf).unwrap();
        assert_eq!(fei.generation, 7);
        assert_eq!(fei.ram_bytes, 10);
        assert_eq!(fei.compression, CompressionType::None);
        assert_eq!(fei.extent_type, FileExtentType::Inline);
        match fei.body {
            FileExtentBody::Inline { inline_size } => {
                assert_eq!(inline_size, 10)
            }
            _ => panic!("expected inline body"),
        }
    }

    #[test]
    fn file_extent_item_regular() {
        let mut buf = vec![0u8; 53];
        buf[0..8].copy_from_slice(&100u64.to_le_bytes()); // generation
        buf[8..16].copy_from_slice(&4096u64.to_le_bytes()); // ram_bytes
        buf[16] = 1; // compression = zlib
        buf[20] = 1; // extent_type = regular
        buf[21..29].copy_from_slice(&0x100000u64.to_le_bytes()); // disk_bytenr
        buf[29..37].copy_from_slice(&4096u64.to_le_bytes()); // disk_num_bytes
        buf[37..45].copy_from_slice(&0u64.to_le_bytes()); // offset
        buf[45..53].copy_from_slice(&4096u64.to_le_bytes()); // num_bytes
        let fei = FileExtentItem::parse(&buf).unwrap();
        assert_eq!(fei.generation, 100);
        assert_eq!(fei.compression, CompressionType::Zlib);
        assert_eq!(fei.extent_type, FileExtentType::Regular);
        match fei.body {
            FileExtentBody::Regular {
                disk_bytenr,
                disk_num_bytes,
                offset,
                num_bytes,
            } => {
                assert_eq!(disk_bytenr, 0x100000);
                assert_eq!(disk_num_bytes, 4096);
                assert_eq!(offset, 0);
                assert_eq!(num_bytes, 4096);
            }
            _ => panic!("expected regular body"),
        }
    }

    #[test]
    fn file_extent_item_too_short() {
        assert!(FileExtentItem::parse(&[0; 20]).is_none());
    }

    #[test]
    fn file_extent_item_regular_too_short() {
        // 21 bytes is enough for inline but not for regular.
        let mut buf = vec![0u8; 21];
        buf[20] = 1; // extent_type = regular
        assert!(FileExtentItem::parse(&buf).is_none());
    }

    // ── Helper functions ──────────────────────────────────────────────

    #[test]
    fn raw_crc32c_known_value() {
        // The raw CRC32C of an empty buffer with seed 0 should be 0.
        assert_eq!(raw_crc32c(0, &[]), 0);
        // Verify that raw_crc32c differs from the standard CRC32C.
        // Standard CRC32C of "123456789" is 0xE3069283.
        let raw = raw_crc32c(0, b"123456789");
        let standard = crc32c::crc32c(b"123456789");
        assert_eq!(standard, 0xE3069283);
        assert_ne!(raw, standard);
        // raw_crc32c is deterministic.
        assert_eq!(raw, raw_crc32c(0, b"123456789"));
        // Chaining: raw_crc32c with a nonzero seed produces different results.
        let chained = raw_crc32c(raw, b"more");
        assert_ne!(chained, raw);
    }

    #[test]
    fn extent_data_ref_hash_deterministic() {
        let h1 = extent_data_ref_hash(5, 256, 0);
        let h2 = extent_data_ref_hash(5, 256, 0);
        assert_eq!(h1, h2);
        // Different inputs produce different hashes.
        let h3 = extent_data_ref_hash(5, 256, 4096);
        assert_ne!(h1, h3);
    }

    #[test]
    fn block_group_flags_type_name() {
        assert_eq!(BlockGroupFlags::DATA.type_name(), "Data");
        assert_eq!(BlockGroupFlags::METADATA.type_name(), "Metadata");
        assert_eq!(BlockGroupFlags::SYSTEM.type_name(), "System");
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::METADATA).type_name(),
            "Data+Metadata"
        );
        assert_eq!(BlockGroupFlags::GLOBAL_RSV.type_name(), "GlobalReserve");
    }

    #[test]
    fn block_group_flags_profile_name() {
        assert_eq!(BlockGroupFlags::DATA.profile_name(), "single");
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::DUP).profile_name(),
            "DUP"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID0).profile_name(),
            "RAID0"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1).profile_name(),
            "RAID1"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID10).profile_name(),
            "RAID10"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID5).profile_name(),
            "RAID5"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID6).profile_name(),
            "RAID6"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1C3).profile_name(),
            "RAID1C3"
        );
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1C4).profile_name(),
            "RAID1C4"
        );
    }
}

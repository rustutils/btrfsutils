//! # Typed Rust structs for btrfs tree item payloads
//!
//! Each on-disk item type has a corresponding struct with a `parse` method
//! that reads from a raw byte buffer using safe LE reader helpers. These
//! structs are the public API for item data; display formatting lives in
//! the `cli` crate.

use crate::{
    raw,
    tree::{DiskKey, ObjectId},
    util::{read_le_u16, read_le_u32, read_le_u64, read_uuid},
};
use std::{fmt, mem};
use uuid::Uuid;

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
    }
}

impl BlockGroupFlags {
    /// Returns the RAID profile name, or `"single"` when no profile bit is set.
    pub fn profile_name(self) -> &'static str {
        let profile = self
            & (Self::RAID0
                | Self::RAID1
                | Self::DUP
                | Self::RAID10
                | Self::RAID5
                | Self::RAID6
                | Self::RAID1C3
                | Self::RAID1C4);
        match profile {
            p if p == Self::RAID0 => "RAID0",
            p if p == Self::RAID1 => "RAID1",
            p if p == Self::DUP => "DUP",
            p if p == Self::RAID10 => "RAID10",
            p if p == Self::RAID5 => "RAID5",
            p if p == Self::RAID6 => "RAID6",
            p if p == Self::RAID1C3 => "RAID1C3",
            p if p == Self::RAID1C4 => "RAID1C4",
            _ => "single",
        }
    }
}

impl fmt::Display for BlockGroupFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<&str> = Vec::new();
        if self.contains(Self::DATA) {
            parts.push("DATA");
        }
        if self.contains(Self::SYSTEM) {
            parts.push("SYSTEM");
        }
        if self.contains(Self::METADATA) {
            parts.push("METADATA");
        }
        let profile = self.profile_name();
        if parts.is_empty() {
            write!(f, "{profile}")
        } else {
            parts.push(profile);
            write!(f, "{}", parts.join("|"))
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
/// Btrfs timestamp (seconds + nanoseconds since epoch).
#[derive(Debug, Clone, Copy)]
pub struct Timespec {
    pub sec: u64,
    pub nsec: u32,
}

impl Timespec {
    fn parse(data: &[u8], off: usize) -> Self {
        Self {
            sec: read_le_u64(data, off),
            nsec: read_le_u32(data, off + 8),
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

/// Directory entry file type.
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

#[derive(Debug, Clone)]
pub struct InodeItem {
    pub generation: u64,
    pub transid: u64,
    pub size: u64,
    pub nbytes: u64,
    pub block_group: u64,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub rdev: u64,
    pub flags: InodeFlags,
    pub sequence: u64,
    pub atime: Timespec,
    pub ctime: Timespec,
    pub mtime: Timespec,
    pub otime: Timespec,
}

impl InodeItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_inode_item>() {
            return None;
        }
        let ts_off = mem::offset_of!(raw::btrfs_inode_item, atime);
        let ts_size = mem::size_of::<raw::btrfs_timespec>();
        Some(Self {
            generation: read_le_u64(data, 0),
            transid: read_le_u64(data, 8),
            size: read_le_u64(data, 16),
            nbytes: read_le_u64(data, 24),
            block_group: read_le_u64(data, 32),
            nlink: read_le_u32(data, 40),
            uid: read_le_u32(data, 44),
            gid: read_le_u32(data, 48),
            mode: read_le_u32(data, 52),
            rdev: read_le_u64(data, 56),
            flags: InodeFlags::from_bits_truncate(read_le_u64(data, 64)),
            sequence: read_le_u64(data, 72),
            atime: Timespec::parse(data, ts_off),
            ctime: Timespec::parse(data, ts_off + ts_size),
            mtime: Timespec::parse(data, ts_off + 2 * ts_size),
            otime: Timespec::parse(data, ts_off + 3 * ts_size),
        })
    }
}

#[derive(Debug, Clone)]
pub struct InodeRef {
    pub index: u64,
    pub name: Vec<u8>,
}

impl InodeRef {
    pub fn parse_all(data: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + 10 <= data.len() {
            let index = read_le_u64(data, offset);
            let name_len = read_le_u16(data, offset + 8) as usize;
            offset += 10;
            let name = if offset + name_len <= data.len() {
                data[offset..offset + name_len].to_vec()
            } else {
                break;
            };
            result.push(Self { index, name });
            offset += name_len;
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct InodeExtref {
    pub parent: u64,
    pub index: u64,
    pub name: Vec<u8>,
}

impl InodeExtref {
    pub fn parse_all(data: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + 18 <= data.len() {
            let parent = read_le_u64(data, offset);
            let index = read_le_u64(data, offset + 8);
            let name_len = read_le_u16(data, offset + 16) as usize;
            offset += 18;
            let name = if offset + name_len <= data.len() {
                data[offset..offset + name_len].to_vec()
            } else {
                break;
            };
            result.push(Self {
                parent,
                index,
                name,
            });
            offset += name_len;
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct DirItem {
    pub location: DiskKey,
    pub transid: u64,
    pub file_type: FileType,
    pub name: Vec<u8>,
    pub data: Vec<u8>,
}

impl DirItem {
    pub fn parse_all(buf: &[u8]) -> Vec<Self> {
        let mut result = Vec::new();
        let mut offset = 0usize;
        let dir_item_size = mem::size_of::<raw::btrfs_dir_item>();

        while offset + dir_item_size <= buf.len() {
            let location = DiskKey::parse(buf, offset);
            let transid = read_le_u64(buf, offset + 17);
            let data_len = read_le_u16(buf, offset + 25) as usize;
            let name_len = read_le_u16(buf, offset + 27) as usize;
            let file_type = FileType::from_raw(buf[offset + 29]);
            offset += dir_item_size;

            if offset + name_len + data_len > buf.len() {
                break;
            }
            let name = buf[offset..offset + name_len].to_vec();
            let data =
                buf[offset + name_len..offset + name_len + data_len].to_vec();
            result.push(Self {
                location,
                transid,
                file_type,
                name,
                data,
            });
            offset += name_len + data_len;
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

#[derive(Debug, Clone)]
pub struct RootItem {
    pub generation: u64,
    pub root_dirid: u64,
    pub bytenr: u64,
    pub byte_limit: u64,
    pub bytes_used: u64,
    pub last_snapshot: u64,
    pub flags: RootItemFlags,
    pub refs: u32,
    pub drop_progress: DiskKey,
    pub drop_level: u8,
    pub level: u8,
    pub generation_v2: u64,
    pub uuid: Uuid,
    pub parent_uuid: Uuid,
    pub received_uuid: Uuid,
    pub ctransid: u64,
    pub otransid: u64,
    pub stransid: u64,
    pub rtransid: u64,
    pub ctime: Timespec,
    pub otime: Timespec,
    pub stime: Timespec,
    pub rtime: Timespec,
}

impl RootItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let inode_size = mem::size_of::<raw::btrfs_inode_item>();
        if data.len() < inode_size + 8 {
            return None;
        }

        let dp_off = inode_size + 60;
        let level_off = mem::offset_of!(raw::btrfs_root_item, level);
        let uuid_off = mem::offset_of!(raw::btrfs_root_item, uuid);
        let ct_off = mem::offset_of!(raw::btrfs_root_item, ctransid);
        let ctime_off = mem::offset_of!(raw::btrfs_root_item, ctime);
        let ts_size = mem::size_of::<raw::btrfs_timespec>();

        Some(Self {
            generation: read_le_u64(data, inode_size),
            root_dirid: read_le_u64(data, inode_size + 8),
            bytenr: read_le_u64(data, inode_size + 16),
            byte_limit: read_le_u64(data, inode_size + 24),
            bytes_used: read_le_u64(data, inode_size + 32),
            last_snapshot: read_le_u64(data, inode_size + 40),
            flags: RootItemFlags::from_bits_truncate(read_le_u64(
                data,
                inode_size + 48,
            )),
            refs: read_le_u32(data, inode_size + 56),
            drop_progress: if dp_off + 17 <= data.len() {
                DiskKey::parse(data, dp_off)
            } else {
                DiskKey::parse(&[0; 17], 0)
            },
            drop_level: if dp_off + 17 < data.len() {
                data[dp_off + 17]
            } else {
                0
            },
            level: if level_off < data.len() {
                data[level_off]
            } else {
                0
            },
            generation_v2: if level_off + 1 + 8 <= data.len() {
                read_le_u64(data, level_off + 1)
            } else {
                0
            },
            uuid: if uuid_off + 16 <= data.len() {
                read_uuid(data, uuid_off)
            } else {
                Uuid::nil()
            },
            parent_uuid: if uuid_off + 32 <= data.len() {
                read_uuid(data, uuid_off + 16)
            } else {
                Uuid::nil()
            },
            received_uuid: if uuid_off + 48 <= data.len() {
                read_uuid(data, uuid_off + 32)
            } else {
                Uuid::nil()
            },
            ctransid: if ct_off + 8 <= data.len() {
                read_le_u64(data, ct_off)
            } else {
                0
            },
            otransid: if ct_off + 16 <= data.len() {
                read_le_u64(data, ct_off + 8)
            } else {
                0
            },
            stransid: if ct_off + 24 <= data.len() {
                read_le_u64(data, ct_off + 16)
            } else {
                0
            },
            rtransid: if ct_off + 32 <= data.len() {
                read_le_u64(data, ct_off + 24)
            } else {
                0
            },
            ctime: if ctime_off + ts_size <= data.len() {
                Timespec::parse(data, ctime_off)
            } else {
                Timespec { sec: 0, nsec: 0 }
            },
            otime: if ctime_off + 2 * ts_size <= data.len() {
                Timespec::parse(data, ctime_off + ts_size)
            } else {
                Timespec { sec: 0, nsec: 0 }
            },
            stime: if ctime_off + 3 * ts_size <= data.len() {
                Timespec::parse(data, ctime_off + 2 * ts_size)
            } else {
                Timespec { sec: 0, nsec: 0 }
            },
            rtime: if ctime_off + 4 * ts_size <= data.len() {
                Timespec::parse(data, ctime_off + 3 * ts_size)
            } else {
                Timespec { sec: 0, nsec: 0 }
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct RootRef {
    pub dirid: u64,
    pub sequence: u64,
    pub name: Vec<u8>,
}

impl RootRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_root_ref>() {
            return None;
        }
        let dirid = read_le_u64(data, 0);
        let sequence = read_le_u64(data, 8);
        let name_len = read_le_u16(data, 16) as usize;
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

#[derive(Debug, Clone)]
pub struct FileExtentItem {
    pub generation: u64,
    pub ram_bytes: u64,
    pub compression: CompressionType,
    pub extent_type: FileExtentType,
    pub body: FileExtentBody,
}

#[derive(Debug, Clone)]
pub enum FileExtentBody {
    Inline {
        inline_size: usize,
    },
    Regular {
        disk_bytenr: u64,
        disk_num_bytes: u64,
        offset: u64,
        num_bytes: u64,
    },
}

impl FileExtentItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 21 {
            return None;
        }
        let generation = read_le_u64(data, 0);
        let ram_bytes = read_le_u64(data, 8);
        let compression = CompressionType::from_raw(data[16]);
        let extent_type = FileExtentType::from_raw(data[20]);

        let body = if extent_type == FileExtentType::Inline {
            FileExtentBody::Inline {
                inline_size: data.len() - 21,
            }
        } else if data.len() >= 53 {
            FileExtentBody::Regular {
                disk_bytenr: read_le_u64(data, 21),
                disk_num_bytes: read_le_u64(data, 29),
                offset: read_le_u64(data, 37),
                num_bytes: read_le_u64(data, 45),
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

/// Raw CRC32C matching the kernel's `crc32c()` function: seed is passed
/// through directly with no inversion on input or output.
fn raw_crc32c(seed: u32, data: &[u8]) -> u32 {
    // crc32c::crc32c_append(seed) computes: !crc32c_hw(!seed, data)
    // We want: crc32c_hw(seed, data)
    // So: !crc32c::crc32c_append(!seed, data)
    !crc32c::crc32c_append(!seed, data)
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

#[derive(Debug, Clone)]
pub struct ExtentItem {
    pub refs: u64,
    pub generation: u64,
    pub flags: ExtentFlags,
    pub tree_block_key: Option<DiskKey>,
    pub tree_block_level: Option<u8>,
    pub skinny_level: Option<u64>,
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
        let refs = read_le_u64(data, 0);
        let generation = read_le_u64(data, 8);
        let flags = ExtentFlags::from_bits_truncate(read_le_u64(data, 16));

        let mut offset = mem::size_of::<raw::btrfs_extent_item>();
        let is_tree_block = flags.contains(ExtentFlags::TREE_BLOCK);

        let mut tree_block_key = None;
        let mut tree_block_level = None;
        if is_tree_block
            && key.key_type == KeyType::ExtentItem
            && offset + 17 < data.len()
        {
            tree_block_key = Some(DiskKey::parse(data, offset));
            tree_block_level = Some(data[offset + 17]);
            offset += mem::size_of::<raw::btrfs_tree_block_info>();
        }

        let skinny_level =
            if key.key_type == KeyType::MetadataItem && is_tree_block {
                Some(key.offset)
            } else {
                None
            };

        let mut inline_refs = Vec::new();
        while offset < data.len() {
            let ref_type = data[offset];
            let ref_offset = if offset + 9 <= data.len() {
                read_le_u64(data, offset + 1)
            } else {
                0
            };
            offset += 1 + 8;

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
                    // starts directly after the type byte. Back up the 8 bytes
                    // we speculatively consumed.
                    let struct_start = offset - 8;
                    if struct_start + 28 <= data.len() {
                        let root = read_le_u64(data, struct_start);
                        let oid = read_le_u64(data, struct_start + 8);
                        let off = read_le_u64(data, struct_start + 16);
                        let count = read_le_u32(data, struct_start + 24);
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
                        offset = struct_start + 28;
                    } else {
                        break;
                    }
                }
                raw::BTRFS_SHARED_DATA_REF_KEY => {
                    if offset + 4 <= data.len() {
                        let count = read_le_u32(data, offset);
                        inline_refs.push(InlineRef::SharedDataBackref {
                            ref_offset,
                            parent: ref_offset,
                            count,
                        });
                        offset += 4;
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

#[derive(Debug, Clone)]
pub struct ExtentDataRef {
    pub root: u64,
    pub objectid: u64,
    pub offset: u64,
    pub count: u32,
}

impl ExtentDataRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_extent_data_ref>() {
            return None;
        }
        Some(Self {
            root: read_le_u64(data, 0),
            objectid: read_le_u64(data, 8),
            offset: read_le_u64(data, 16),
            count: read_le_u32(data, 24),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SharedDataRef {
    pub count: u32,
}

impl SharedDataRef {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        Some(Self {
            count: read_le_u32(data, 0),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BlockGroupItem {
    pub used: u64,
    pub chunk_objectid: u64,
    pub flags: BlockGroupFlags,
}

impl BlockGroupItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_block_group_item>() {
            return None;
        }
        Some(Self {
            used: read_le_u64(data, 0),
            chunk_objectid: read_le_u64(data, 8),
            flags: BlockGroupFlags::from_bits_truncate(read_le_u64(data, 16)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ChunkItem {
    pub length: u64,
    pub owner: u64,
    pub stripe_len: u64,
    pub chunk_type: BlockGroupFlags,
    pub io_align: u32,
    pub io_width: u32,
    pub sector_size: u32,
    pub num_stripes: u16,
    pub sub_stripes: u16,
    pub stripes: Vec<ChunkStripe>,
}

#[derive(Debug, Clone)]
pub struct ChunkStripe {
    pub devid: u64,
    pub offset: u64,
    pub dev_uuid: Uuid,
}

impl ChunkItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
        if data.len() < chunk_base_size {
            return None;
        }
        let num_stripes = read_le_u16(data, 44);
        let stripe_size = mem::size_of::<raw::btrfs_stripe>();
        let mut stripes = Vec::with_capacity(num_stripes as usize);
        for i in 0..num_stripes as usize {
            let s_off = chunk_base_size + i * stripe_size;
            if s_off + stripe_size > data.len() {
                break;
            }
            stripes.push(ChunkStripe {
                devid: read_le_u64(data, s_off),
                offset: read_le_u64(data, s_off + 8),
                dev_uuid: read_uuid(data, s_off + 16),
            });
        }
        Some(Self {
            length: read_le_u64(data, 0),
            owner: read_le_u64(data, 8),
            stripe_len: read_le_u64(data, 16),
            chunk_type: BlockGroupFlags::from_bits_truncate(read_le_u64(
                data, 24,
            )),
            io_align: read_le_u32(data, 32),
            io_width: read_le_u32(data, 36),
            sector_size: read_le_u32(data, 40),
            num_stripes,
            sub_stripes: read_le_u16(data, 46),
            stripes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeviceItem {
    pub devid: u64,
    pub total_bytes: u64,
    pub bytes_used: u64,
    pub io_align: u32,
    pub io_width: u32,
    pub sector_size: u32,
    pub dev_type: u64,
    pub generation: u64,
    pub start_offset: u64,
    pub dev_group: u32,
    pub seek_speed: u8,
    pub bandwidth: u8,
    pub uuid: Uuid,
    pub fsid: Uuid,
}

impl DeviceItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_dev_item>() {
            return None;
        }
        Some(Self {
            devid: read_le_u64(data, 0),
            total_bytes: read_le_u64(data, 8),
            bytes_used: read_le_u64(data, 16),
            io_align: read_le_u32(data, 24),
            io_width: read_le_u32(data, 28),
            sector_size: read_le_u32(data, 32),
            dev_type: read_le_u64(data, 36),
            generation: read_le_u64(data, 44),
            start_offset: read_le_u64(data, 52),
            dev_group: read_le_u32(data, 60),
            seek_speed: data[64],
            bandwidth: data[65],
            uuid: read_uuid(data, 66),
            fsid: read_uuid(data, 82),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeviceExtent {
    pub chunk_tree: u64,
    pub chunk_objectid: u64,
    pub chunk_offset: u64,
    pub length: u64,
    pub chunk_tree_uuid: Uuid,
}

impl DeviceExtent {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_dev_extent>() {
            return None;
        }
        Some(Self {
            chunk_tree: read_le_u64(data, 0),
            chunk_objectid: read_le_u64(data, 8),
            chunk_offset: read_le_u64(data, 16),
            length: read_le_u64(data, 24),
            chunk_tree_uuid: read_uuid(data, 32),
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

#[derive(Debug, Clone)]
pub struct FreeSpaceInfo {
    pub extent_count: u32,
    pub flags: FreeSpaceInfoFlags,
}

impl FreeSpaceInfo {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        Some(Self {
            extent_count: read_le_u32(data, 0),
            flags: FreeSpaceInfoFlags::from_bits_truncate(read_le_u32(data, 4)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct QgroupStatus {
    pub version: u64,
    pub generation: u64,
    pub flags: u64,
    pub scan: u64,
    pub enable_gen: Option<u64>,
}

impl QgroupStatus {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 32 {
            return None;
        }
        Some(Self {
            version: read_le_u64(data, 0),
            generation: read_le_u64(data, 8),
            flags: read_le_u64(data, 16),
            scan: read_le_u64(data, 24),
            enable_gen: if data.len() >= 40 {
                Some(read_le_u64(data, 32))
            } else {
                None
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct QgroupInfo {
    pub generation: u64,
    pub referenced: u64,
    pub referenced_compressed: u64,
    pub exclusive: u64,
    pub exclusive_compressed: u64,
}

impl QgroupInfo {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_qgroup_info_item>() {
            return None;
        }
        Some(Self {
            generation: read_le_u64(data, 0),
            referenced: read_le_u64(data, 8),
            referenced_compressed: read_le_u64(data, 16),
            exclusive: read_le_u64(data, 24),
            exclusive_compressed: read_le_u64(data, 32),
        })
    }
}

#[derive(Debug, Clone)]
pub struct QgroupLimit {
    pub flags: u64,
    pub max_referenced: u64,
    pub max_exclusive: u64,
    pub rsv_referenced: u64,
    pub rsv_exclusive: u64,
}

impl QgroupLimit {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_qgroup_limit_item>() {
            return None;
        }
        Some(Self {
            flags: read_le_u64(data, 0),
            max_referenced: read_le_u64(data, 8),
            max_exclusive: read_le_u64(data, 16),
            rsv_referenced: read_le_u64(data, 24),
            rsv_exclusive: read_le_u64(data, 32),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeviceStats {
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
        let mut values = Vec::new();
        for (i, name) in stat_names.iter().enumerate() {
            let off = i * 8;
            if off + 8 <= data.len() {
                values.push((name.to_string(), read_le_u64(data, off)));
            }
        }
        DeviceStats { values }
    }
}

#[derive(Debug, Clone)]
pub struct UuidItem {
    pub subvol_ids: Vec<u64>,
}

impl UuidItem {
    pub fn parse(data: &[u8]) -> Self {
        let mut subvol_ids = Vec::new();
        let mut offset = 0;
        while offset + 8 <= data.len() {
            subvol_ids.push(read_le_u64(data, offset));
            offset += 8;
        }
        Self { subvol_ids }
    }
}

/// Parsed item payload — the result of parsing an item's raw data based on its key type.
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

#[derive(Debug, Clone)]
pub struct DeviceReplaceItem {
    pub src_devid: u64,
    pub cursor_left: u64,
    pub cursor_right: u64,
    pub replace_mode: u64,
    pub replace_state: u64,
    pub time_started: u64,
    pub time_stopped: u64,
    pub num_write_errors: u64,
    pub num_uncorrectable_read_errors: u64,
}

impl DeviceReplaceItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 80 {
            return None;
        }
        Some(Self {
            src_devid: read_le_u64(data, 0),
            cursor_left: read_le_u64(data, 8),
            cursor_right: read_le_u64(data, 16),
            replace_mode: read_le_u64(data, 24),
            replace_state: read_le_u64(data, 32),
            time_started: read_le_u64(data, 40),
            time_stopped: read_le_u64(data, 48),
            num_write_errors: read_le_u64(data, 56),
            num_uncorrectable_read_errors: read_le_u64(data, 64),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RaidStripeItem {
    pub encoding: u64,
    pub stripes: Vec<RaidStripeEntry>,
}

#[derive(Debug, Clone)]
pub struct RaidStripeEntry {
    pub devid: u64,
    pub physical: u64,
}

impl RaidStripeItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let encoding = read_le_u64(data, 0);
        let mut stripes = Vec::new();
        let mut offset = 8;
        while offset + 16 <= data.len() {
            stripes.push(RaidStripeEntry {
                devid: read_le_u64(data, offset),
                physical: read_le_u64(data, offset + 8),
            });
            offset += 16;
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
                read_le_u64(data, 0)
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
                ItemPayload::ExtentOwnerRef {
                    root: read_le_u64(data, 0),
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
                    flags: read_le_u64(data, 0),
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
    fn block_group_flags_data_single() {
        let flags = BlockGroupFlags::DATA;
        assert_eq!(format!("{flags}"), "DATA|single");
    }

    #[test]
    fn block_group_flags_metadata_dup() {
        let flags = BlockGroupFlags::METADATA | BlockGroupFlags::DUP;
        assert_eq!(format!("{flags}"), "METADATA|DUP");
    }

    #[test]
    fn block_group_flags_system_raid1() {
        let flags = BlockGroupFlags::SYSTEM | BlockGroupFlags::RAID1;
        assert_eq!(format!("{flags}"), "SYSTEM|RAID1");
    }

    #[test]
    fn block_group_flags_data_metadata() {
        let flags = BlockGroupFlags::DATA | BlockGroupFlags::METADATA;
        assert_eq!(format!("{flags}"), "DATA|METADATA|single");
    }

    #[test]
    fn block_group_flags_no_type_bits() {
        // Profile only, no DATA/SYSTEM/METADATA bit.
        assert_eq!(format!("{}", BlockGroupFlags::empty()), "single");
        assert_eq!(format!("{}", BlockGroupFlags::RAID0), "RAID0");
    }

    #[test]
    fn block_group_flags_all_profiles() {
        let data = BlockGroupFlags::DATA;
        assert_eq!(format!("{}", data | BlockGroupFlags::RAID5), "DATA|RAID5");
        assert_eq!(format!("{}", data | BlockGroupFlags::RAID6), "DATA|RAID6");
        assert_eq!(
            format!("{}", data | BlockGroupFlags::RAID10),
            "DATA|RAID10"
        );
        assert_eq!(
            format!("{}", data | BlockGroupFlags::RAID1C3),
            "DATA|RAID1C3"
        );
        assert_eq!(
            format!("{}", data | BlockGroupFlags::RAID1C4),
            "DATA|RAID1C4"
        );
    }
}

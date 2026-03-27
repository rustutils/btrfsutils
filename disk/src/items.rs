//! # Items: typed Rust structs for btrfs tree item payloads
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
use std::mem;
use uuid::Uuid;

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
        match v as u32 {
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
        match v as u32 {
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
            Self::Unknown => "UNKNOWN",
            Self::RegFile => "FILE",
            Self::Dir => "DIR",
            Self::Chrdev => "CHRDEV",
            Self::Blkdev => "BLKDEV",
            Self::Fifo => "FIFO",
            Self::Sock => "SOCK",
            Self::Symlink => "SYMLINK",
            Self::Xattr => "XATTR",
            Self::Other(_) => "UNKNOWN",
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
    pub flags: u64,
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
            flags: read_le_u64(data, 64),
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

#[derive(Debug, Clone)]
pub struct RootItem {
    pub generation: u64,
    pub root_dirid: u64,
    pub bytenr: u64,
    pub byte_limit: u64,
    pub bytes_used: u64,
    pub last_snapshot: u64,
    pub flags: u64,
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
            flags: read_le_u64(data, inode_size + 48),
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

    pub fn is_rdonly(&self) -> bool {
        self.flags & raw::BTRFS_ROOT_SUBVOL_RDONLY as u64 != 0
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

/// Inline reference types found inside EXTENT_ITEM/METADATA_ITEM.
#[derive(Debug, Clone)]
pub enum InlineRef {
    TreeBlockBackref {
        root: u64,
    },
    SharedBlockBackref {
        parent: u64,
    },
    ExtentDataBackref {
        root: u64,
        objectid: u64,
        offset: u64,
        count: u32,
    },
    SharedDataBackref {
        parent: u64,
        count: u32,
    },
    ExtentOwnerRef {
        root: u64,
    },
}

#[derive(Debug, Clone)]
pub struct ExtentItem {
    pub refs: u64,
    pub generation: u64,
    pub flags: u64,
    pub tree_block_key: Option<DiskKey>,
    pub tree_block_level: Option<u8>,
    pub skinny_level: Option<u64>,
    pub inline_refs: Vec<InlineRef>,
}

impl ExtentItem {
    pub fn is_data(&self) -> bool {
        self.flags & raw::BTRFS_EXTENT_FLAG_DATA as u64 != 0
    }

    pub fn is_tree_block(&self) -> bool {
        self.flags & raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64 != 0
    }

    pub fn flag_names(&self) -> String {
        let mut names = Vec::new();
        if self.is_data() {
            names.push("DATA");
        }
        if self.is_tree_block() {
            names.push("TREE_BLOCK");
        }
        if names.is_empty() {
            "none".to_string()
        } else {
            names.join("|")
        }
    }

    pub fn parse(data: &[u8], key: &DiskKey) -> Option<Self> {
        use crate::tree::KeyType;

        if data.len() < mem::size_of::<raw::btrfs_extent_item>() {
            return None;
        }
        let refs = read_le_u64(data, 0);
        let generation = read_le_u64(data, 8);
        let flags = read_le_u64(data, 16);

        let mut offset = mem::size_of::<raw::btrfs_extent_item>();
        let is_tree_block =
            flags & raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64 != 0;

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

            match ref_type as u32 {
                raw::BTRFS_TREE_BLOCK_REF_KEY => {
                    inline_refs
                        .push(InlineRef::TreeBlockBackref { root: ref_offset });
                }
                raw::BTRFS_SHARED_BLOCK_REF_KEY => {
                    inline_refs.push(InlineRef::SharedBlockBackref {
                        parent: ref_offset,
                    });
                }
                raw::BTRFS_EXTENT_DATA_REF_KEY => {
                    let ref_start = offset - 8;
                    if ref_start + 28 <= data.len() {
                        let root = read_le_u64(data, ref_start);
                        let oid = read_le_u64(data, ref_start + 8);
                        let off = read_le_u64(data, ref_start + 16);
                        let count = read_le_u32(data, ref_start + 24);
                        inline_refs.push(InlineRef::ExtentDataBackref {
                            root,
                            objectid: oid,
                            offset: off,
                            count,
                        });
                        offset = ref_start + 28;
                    } else {
                        break;
                    }
                }
                raw::BTRFS_SHARED_DATA_REF_KEY => {
                    if offset + 4 <= data.len() {
                        let count = read_le_u32(data, offset);
                        inline_refs.push(InlineRef::SharedDataBackref {
                            parent: ref_offset,
                            count,
                        });
                        offset += 4;
                    } else {
                        break;
                    }
                }
                raw::BTRFS_EXTENT_OWNER_REF_KEY => {
                    inline_refs
                        .push(InlineRef::ExtentOwnerRef { root: ref_offset });
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
    pub flags: u64,
}

impl BlockGroupItem {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < mem::size_of::<raw::btrfs_block_group_item>() {
            return None;
        }
        Some(Self {
            used: read_le_u64(data, 0),
            chunk_objectid: read_le_u64(data, 8),
            flags: read_le_u64(data, 16),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ChunkItem {
    pub length: u64,
    pub owner: u64,
    pub stripe_len: u64,
    pub chunk_type: u64,
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
            chunk_type: read_le_u64(data, 24),
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
pub struct DevItem {
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

impl DevItem {
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
pub struct DevExtent {
    pub chunk_tree: u64,
    pub chunk_objectid: u64,
    pub chunk_offset: u64,
    pub length: u64,
    pub chunk_tree_uuid: Uuid,
}

impl DevExtent {
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

#[derive(Debug, Clone)]
pub struct FreeSpaceInfo {
    pub extent_count: u32,
    pub flags: u32,
}

impl FreeSpaceInfo {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        Some(Self {
            extent_count: read_le_u32(data, 0),
            flags: read_le_u32(data, 4),
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
pub struct DevStats {
    pub values: Vec<(String, u64)>,
}

impl DevStats {
    pub fn parse(data: &[u8]) -> Self {
        let stat_names = [
            "write_errs",
            "read_errs",
            "flush_errs",
            "corruption_errs",
            "generation_errs",
        ];
        let mut values = Vec::new();
        for (i, name) in stat_names.iter().enumerate() {
            let off = i * 8;
            if off + 8 <= data.len() {
                values.push((name.to_string(), read_le_u64(data, off)));
            }
        }
        DevStats { values }
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
    DevItem(DevItem),
    DevExtent(DevExtent),
    QgroupStatus(QgroupStatus),
    QgroupInfo(QgroupInfo),
    QgroupLimit(QgroupLimit),
    QgroupRelation,
    DevStats(DevStats),
    BalanceItem { flags: u64 },
    DevReplace(DevReplaceItem),
    UuidItem(UuidItem),
    StringItem(Vec<u8>),
    RaidStripe(RaidStripeItem),
    Unknown(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct DevReplaceItem {
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

impl DevReplaceItem {
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
        KeyType::DevItem => match DevItem::parse(data) {
            Some(v) => ItemPayload::DevItem(v),
            None => ItemPayload::Unknown(data.to_vec()),
        },
        KeyType::DevExtent => match DevExtent::parse(data) {
            Some(v) => ItemPayload::DevExtent(v),
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
            if key.objectid == raw::BTRFS_DEV_STATS_OBJECTID as u64 {
                ItemPayload::DevStats(DevStats::parse(data))
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
        KeyType::DevReplace => match DevReplaceItem::parse(data) {
            Some(v) => ItemPayload::DevReplace(v),
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

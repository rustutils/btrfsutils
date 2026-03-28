//! # Tree: parsing btrfs B-tree nodes and leaves from raw blocks
//!
//! A btrfs filesystem is organized as a collection of B-trees. Each tree is
//! stored as a hierarchy of blocks (nodesize bytes each, typically 16 KiB).
//! Internal nodes contain key pointers to child blocks; leaves contain items
//! with their associated data payloads.
//!
//! This module provides typed Rust structs for all tree block components and
//! enums for key types and well-known object IDs, with safe LE parsing from
//! raw byte buffers.

use crate::{
    raw,
    util::{read_le_u32, read_le_u64, read_uuid},
};
use std::{fmt, mem};
use uuid::Uuid;

/// Btrfs item key type, identifying what kind of item a key refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    InodeItem,
    InodeRef,
    InodeExtref,
    XattrItem,
    VerityDescItem,
    VerityMerkleItem,
    OrphanItem,
    DirLogItem,
    DirLogIndex,
    DirItem,
    DirIndex,
    ExtentData,
    ExtentCsum,
    RootItem,
    RootBackref,
    RootRef,
    ExtentItem,
    MetadataItem,
    ExtentOwnerRef,
    TreeBlockRef,
    ExtentDataRef,
    SharedBlockRef,
    SharedDataRef,
    BlockGroupItem,
    FreeSpaceInfo,
    FreeSpaceExtent,
    FreeSpaceBitmap,
    DevExtent,
    DevItem,
    ChunkItem,
    RaidStripe,
    QgroupStatus,
    QgroupInfo,
    QgroupLimit,
    QgroupRelation,
    /// BTRFS_BALANCE_ITEM_KEY and BTRFS_TEMPORARY_ITEM_KEY share value 248
    TemporaryItem,
    /// BTRFS_DEV_STATS_KEY and BTRFS_PERSISTENT_ITEM_KEY share value 249
    PersistentItem,
    DevReplace,
    UuidKeySubvol,
    UuidKeyReceivedSubvol,
    StringItem,
    Unknown(u8),
}

impl KeyType {
    /// Convert a raw on-disk key type byte to a `KeyType` variant.
    pub fn from_raw(value: u8) -> Self {
        match u32::from(value) {
            raw::BTRFS_INODE_ITEM_KEY => Self::InodeItem,
            raw::BTRFS_INODE_REF_KEY => Self::InodeRef,
            raw::BTRFS_INODE_EXTREF_KEY => Self::InodeExtref,
            raw::BTRFS_XATTR_ITEM_KEY => Self::XattrItem,
            raw::BTRFS_VERITY_DESC_ITEM_KEY => Self::VerityDescItem,
            raw::BTRFS_VERITY_MERKLE_ITEM_KEY => Self::VerityMerkleItem,
            raw::BTRFS_ORPHAN_ITEM_KEY => Self::OrphanItem,
            raw::BTRFS_DIR_LOG_ITEM_KEY => Self::DirLogItem,
            raw::BTRFS_DIR_LOG_INDEX_KEY => Self::DirLogIndex,
            raw::BTRFS_DIR_ITEM_KEY => Self::DirItem,
            raw::BTRFS_DIR_INDEX_KEY => Self::DirIndex,
            raw::BTRFS_EXTENT_DATA_KEY => Self::ExtentData,
            raw::BTRFS_EXTENT_CSUM_KEY => Self::ExtentCsum,
            raw::BTRFS_ROOT_ITEM_KEY => Self::RootItem,
            raw::BTRFS_ROOT_BACKREF_KEY => Self::RootBackref,
            raw::BTRFS_ROOT_REF_KEY => Self::RootRef,
            raw::BTRFS_EXTENT_ITEM_KEY => Self::ExtentItem,
            raw::BTRFS_METADATA_ITEM_KEY => Self::MetadataItem,
            raw::BTRFS_EXTENT_OWNER_REF_KEY => Self::ExtentOwnerRef,
            raw::BTRFS_TREE_BLOCK_REF_KEY => Self::TreeBlockRef,
            raw::BTRFS_EXTENT_DATA_REF_KEY => Self::ExtentDataRef,
            raw::BTRFS_SHARED_BLOCK_REF_KEY => Self::SharedBlockRef,
            raw::BTRFS_SHARED_DATA_REF_KEY => Self::SharedDataRef,
            raw::BTRFS_BLOCK_GROUP_ITEM_KEY => Self::BlockGroupItem,
            raw::BTRFS_FREE_SPACE_INFO_KEY => Self::FreeSpaceInfo,
            raw::BTRFS_FREE_SPACE_EXTENT_KEY => Self::FreeSpaceExtent,
            raw::BTRFS_FREE_SPACE_BITMAP_KEY => Self::FreeSpaceBitmap,
            raw::BTRFS_DEV_EXTENT_KEY => Self::DevExtent,
            raw::BTRFS_DEV_ITEM_KEY => Self::DevItem,
            raw::BTRFS_CHUNK_ITEM_KEY => Self::ChunkItem,
            raw::BTRFS_RAID_STRIPE_KEY => Self::RaidStripe,
            raw::BTRFS_QGROUP_STATUS_KEY => Self::QgroupStatus,
            raw::BTRFS_QGROUP_INFO_KEY => Self::QgroupInfo,
            raw::BTRFS_QGROUP_LIMIT_KEY => Self::QgroupLimit,
            raw::BTRFS_QGROUP_RELATION_KEY => Self::QgroupRelation,
            // 248 = BTRFS_BALANCE_ITEM_KEY = BTRFS_TEMPORARY_ITEM_KEY
            raw::BTRFS_TEMPORARY_ITEM_KEY => Self::TemporaryItem,
            // 249 = BTRFS_DEV_STATS_KEY = BTRFS_PERSISTENT_ITEM_KEY
            raw::BTRFS_PERSISTENT_ITEM_KEY => Self::PersistentItem,
            raw::BTRFS_DEV_REPLACE_KEY => Self::DevReplace,
            raw::BTRFS_UUID_KEY_SUBVOL => Self::UuidKeySubvol,
            raw::BTRFS_UUID_KEY_RECEIVED_SUBVOL => Self::UuidKeyReceivedSubvol,
            raw::BTRFS_STRING_ITEM_KEY => Self::StringItem,
            _ => Self::Unknown(value),
        }
    }

    /// Return the raw u8 key type value.
    pub fn to_raw(self) -> u8 {
        match self {
            Self::InodeItem => raw::BTRFS_INODE_ITEM_KEY as u8,
            Self::InodeRef => raw::BTRFS_INODE_REF_KEY as u8,
            Self::InodeExtref => raw::BTRFS_INODE_EXTREF_KEY as u8,
            Self::XattrItem => raw::BTRFS_XATTR_ITEM_KEY as u8,
            Self::VerityDescItem => raw::BTRFS_VERITY_DESC_ITEM_KEY as u8,
            Self::VerityMerkleItem => raw::BTRFS_VERITY_MERKLE_ITEM_KEY as u8,
            Self::OrphanItem => raw::BTRFS_ORPHAN_ITEM_KEY as u8,
            Self::DirLogItem => raw::BTRFS_DIR_LOG_ITEM_KEY as u8,
            Self::DirLogIndex => raw::BTRFS_DIR_LOG_INDEX_KEY as u8,
            Self::DirItem => raw::BTRFS_DIR_ITEM_KEY as u8,
            Self::DirIndex => raw::BTRFS_DIR_INDEX_KEY as u8,
            Self::ExtentData => raw::BTRFS_EXTENT_DATA_KEY as u8,
            Self::ExtentCsum => raw::BTRFS_EXTENT_CSUM_KEY as u8,
            Self::RootItem => raw::BTRFS_ROOT_ITEM_KEY as u8,
            Self::RootBackref => raw::BTRFS_ROOT_BACKREF_KEY as u8,
            Self::RootRef => raw::BTRFS_ROOT_REF_KEY as u8,
            Self::ExtentItem => raw::BTRFS_EXTENT_ITEM_KEY as u8,
            Self::MetadataItem => raw::BTRFS_METADATA_ITEM_KEY as u8,
            Self::ExtentOwnerRef => raw::BTRFS_EXTENT_OWNER_REF_KEY as u8,
            Self::TreeBlockRef => raw::BTRFS_TREE_BLOCK_REF_KEY as u8,
            Self::ExtentDataRef => raw::BTRFS_EXTENT_DATA_REF_KEY as u8,
            Self::SharedBlockRef => raw::BTRFS_SHARED_BLOCK_REF_KEY as u8,
            Self::SharedDataRef => raw::BTRFS_SHARED_DATA_REF_KEY as u8,
            Self::BlockGroupItem => raw::BTRFS_BLOCK_GROUP_ITEM_KEY as u8,
            Self::FreeSpaceInfo => raw::BTRFS_FREE_SPACE_INFO_KEY as u8,
            Self::FreeSpaceExtent => raw::BTRFS_FREE_SPACE_EXTENT_KEY as u8,
            Self::FreeSpaceBitmap => raw::BTRFS_FREE_SPACE_BITMAP_KEY as u8,
            Self::DevExtent => raw::BTRFS_DEV_EXTENT_KEY as u8,
            Self::DevItem => raw::BTRFS_DEV_ITEM_KEY as u8,
            Self::ChunkItem => raw::BTRFS_CHUNK_ITEM_KEY as u8,
            Self::RaidStripe => raw::BTRFS_RAID_STRIPE_KEY as u8,
            Self::QgroupStatus => raw::BTRFS_QGROUP_STATUS_KEY as u8,
            Self::QgroupInfo => raw::BTRFS_QGROUP_INFO_KEY as u8,
            Self::QgroupLimit => raw::BTRFS_QGROUP_LIMIT_KEY as u8,
            Self::QgroupRelation => raw::BTRFS_QGROUP_RELATION_KEY as u8,
            Self::TemporaryItem => raw::BTRFS_TEMPORARY_ITEM_KEY as u8,
            Self::PersistentItem => raw::BTRFS_PERSISTENT_ITEM_KEY as u8,
            Self::DevReplace => raw::BTRFS_DEV_REPLACE_KEY as u8,
            Self::UuidKeySubvol => raw::BTRFS_UUID_KEY_SUBVOL as u8,
            Self::UuidKeyReceivedSubvol => {
                raw::BTRFS_UUID_KEY_RECEIVED_SUBVOL as u8
            }
            Self::StringItem => raw::BTRFS_STRING_ITEM_KEY as u8,
            Self::Unknown(v) => v,
        }
    }
}

impl fmt::Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InodeItem => write!(f, "INODE_ITEM"),
            Self::InodeRef => write!(f, "INODE_REF"),
            Self::InodeExtref => write!(f, "INODE_EXTREF"),
            Self::XattrItem => write!(f, "XATTR_ITEM"),
            Self::VerityDescItem => write!(f, "VERITY_DESC_ITEM"),
            Self::VerityMerkleItem => write!(f, "VERITY_MERKLE_ITEM"),
            Self::OrphanItem => write!(f, "ORPHAN_ITEM"),
            Self::DirLogItem => write!(f, "DIR_LOG_ITEM"),
            Self::DirLogIndex => write!(f, "DIR_LOG_INDEX"),
            Self::DirItem => write!(f, "DIR_ITEM"),
            Self::DirIndex => write!(f, "DIR_INDEX"),
            Self::ExtentData => write!(f, "EXTENT_DATA"),
            Self::ExtentCsum => write!(f, "EXTENT_CSUM"),
            Self::RootItem => write!(f, "ROOT_ITEM"),
            Self::RootBackref => write!(f, "ROOT_BACKREF"),
            Self::RootRef => write!(f, "ROOT_REF"),
            Self::ExtentItem => write!(f, "EXTENT_ITEM"),
            Self::MetadataItem => write!(f, "METADATA_ITEM"),
            Self::ExtentOwnerRef => write!(f, "EXTENT_OWNER_REF"),
            Self::TreeBlockRef => write!(f, "TREE_BLOCK_REF"),
            Self::ExtentDataRef => write!(f, "EXTENT_DATA_REF"),
            Self::SharedBlockRef => write!(f, "SHARED_BLOCK_REF"),
            Self::SharedDataRef => write!(f, "SHARED_DATA_REF"),
            Self::BlockGroupItem => write!(f, "BLOCK_GROUP_ITEM"),
            Self::FreeSpaceInfo => write!(f, "FREE_SPACE_INFO"),
            Self::FreeSpaceExtent => write!(f, "FREE_SPACE_EXTENT"),
            Self::FreeSpaceBitmap => write!(f, "FREE_SPACE_BITMAP"),
            Self::DevExtent => write!(f, "DEV_EXTENT"),
            Self::DevItem => write!(f, "DEV_ITEM"),
            Self::ChunkItem => write!(f, "CHUNK_ITEM"),
            Self::RaidStripe => write!(f, "RAID_STRIPE"),
            Self::QgroupStatus => write!(f, "QGROUP_STATUS"),
            Self::QgroupInfo => write!(f, "QGROUP_INFO"),
            Self::QgroupLimit => write!(f, "QGROUP_LIMIT"),
            Self::QgroupRelation => write!(f, "QGROUP_RELATION"),
            Self::TemporaryItem => write!(f, "TEMPORARY_ITEM"),
            Self::PersistentItem => write!(f, "PERSISTENT_ITEM"),
            Self::DevReplace => write!(f, "DEV_REPLACE"),
            Self::UuidKeySubvol => write!(f, "UUID_KEY_SUBVOL"),
            Self::UuidKeyReceivedSubvol => {
                write!(f, "UUID_KEY_RECEIVED_SUBVOL")
            }
            Self::StringItem => write!(f, "STRING_ITEM"),
            Self::Unknown(v) => write!(f, "UNKNOWN.{v}"),
        }
    }
}

/// Well-known btrfs object IDs used as tree IDs, namespace roots, and
/// special-purpose objectids in item keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectId {
    RootTree,
    ExtentTree,
    ChunkTree,
    DevTree,
    FsTree,
    RootTreeDir,
    CsumTree,
    QuotaTree,
    UuidTree,
    FreeSpaceTree,
    BlockGroupTree,
    RaidStripeTree,
    RemapTree,
    DevStats,
    Balance,
    Orphan,
    TreeLog,
    TreeLogFixup,
    TreeReloc,
    DataRelocTree,
    ExtentCsum,
    FreeSpace,
    FreeIno,
    CsumChange,
    Multiple,
    /// First user-accessible objectid (256)
    FirstFree,
    /// A numeric objectid that doesn't match any well-known value.
    Id(u64),
}

impl ObjectId {
    /// Convert a raw 64-bit objectid to an `ObjectId` variant.
    pub fn from_raw(value: u64) -> Self {
        // Positive well-known objectids (bindgen produces u32 for these)
        match value {
            v if v == raw::BTRFS_ROOT_TREE_OBJECTID as u64 => Self::RootTree,
            v if v == raw::BTRFS_EXTENT_TREE_OBJECTID as u64 => {
                Self::ExtentTree
            }
            v if v == raw::BTRFS_CHUNK_TREE_OBJECTID as u64 => Self::ChunkTree,
            v if v == raw::BTRFS_DEV_TREE_OBJECTID as u64 => Self::DevTree,
            v if v == raw::BTRFS_FS_TREE_OBJECTID as u64 => Self::FsTree,
            v if v == raw::BTRFS_ROOT_TREE_DIR_OBJECTID as u64 => {
                Self::RootTreeDir
            }
            v if v == raw::BTRFS_CSUM_TREE_OBJECTID as u64 => Self::CsumTree,
            v if v == raw::BTRFS_QUOTA_TREE_OBJECTID as u64 => Self::QuotaTree,
            v if v == raw::BTRFS_UUID_TREE_OBJECTID as u64 => Self::UuidTree,
            v if v == raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64 => {
                Self::FreeSpaceTree
            }
            v if v == raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID as u64 => {
                Self::BlockGroupTree
            }
            v if v == raw::BTRFS_RAID_STRIPE_TREE_OBJECTID as u64 => {
                Self::RaidStripeTree
            }
            v if v == raw::BTRFS_REMAP_TREE_OBJECTID as u64 => Self::RemapTree,
            // Negative objectids: cast i32 to u64 to get the kernel representation
            v if v == raw::BTRFS_BALANCE_OBJECTID as u64 => Self::Balance,
            v if v == raw::BTRFS_ORPHAN_OBJECTID as u64 => Self::Orphan,
            v if v == raw::BTRFS_TREE_LOG_OBJECTID as u64 => Self::TreeLog,
            v if v == raw::BTRFS_TREE_LOG_FIXUP_OBJECTID as u64 => {
                Self::TreeLogFixup
            }
            v if v == raw::BTRFS_TREE_RELOC_OBJECTID as u64 => Self::TreeReloc,
            v if v == raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64 => {
                Self::DataRelocTree
            }
            v if v == raw::BTRFS_EXTENT_CSUM_OBJECTID as u64 => {
                Self::ExtentCsum
            }
            v if v == raw::BTRFS_FREE_SPACE_OBJECTID as u64 => Self::FreeSpace,
            v if v == raw::BTRFS_FREE_INO_OBJECTID as u64 => Self::FreeIno,
            v if v == raw::BTRFS_CSUM_CHANGE_OBJECTID as u64 => {
                Self::CsumChange
            }
            v if v == raw::BTRFS_MULTIPLE_OBJECTIDS as u64 => Self::Multiple,
            _ => Self::Id(value),
        }
    }

    /// Return the raw u64 objectid value.
    pub fn to_raw(self) -> u64 {
        match self {
            Self::RootTree => raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            Self::ExtentTree => raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            Self::ChunkTree => raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
            Self::DevTree => raw::BTRFS_DEV_TREE_OBJECTID as u64,
            Self::FsTree => raw::BTRFS_FS_TREE_OBJECTID as u64,
            Self::RootTreeDir => raw::BTRFS_ROOT_TREE_DIR_OBJECTID as u64,
            Self::CsumTree => raw::BTRFS_CSUM_TREE_OBJECTID as u64,
            Self::QuotaTree => raw::BTRFS_QUOTA_TREE_OBJECTID as u64,
            Self::UuidTree => raw::BTRFS_UUID_TREE_OBJECTID as u64,
            Self::FreeSpaceTree => raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64,
            Self::BlockGroupTree => raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID as u64,
            Self::RaidStripeTree => raw::BTRFS_RAID_STRIPE_TREE_OBJECTID as u64,
            Self::RemapTree => raw::BTRFS_REMAP_TREE_OBJECTID as u64,
            Self::DevStats => raw::BTRFS_DEV_STATS_OBJECTID as u64,
            Self::Balance => raw::BTRFS_BALANCE_OBJECTID as u64,
            Self::Orphan => raw::BTRFS_ORPHAN_OBJECTID as u64,
            Self::TreeLog => raw::BTRFS_TREE_LOG_OBJECTID as u64,
            Self::TreeLogFixup => raw::BTRFS_TREE_LOG_FIXUP_OBJECTID as u64,
            Self::TreeReloc => raw::BTRFS_TREE_RELOC_OBJECTID as u64,
            Self::DataRelocTree => raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
            Self::ExtentCsum => raw::BTRFS_EXTENT_CSUM_OBJECTID as u64,
            Self::FreeSpace => raw::BTRFS_FREE_SPACE_OBJECTID as u64,
            Self::FreeIno => raw::BTRFS_FREE_INO_OBJECTID as u64,
            Self::CsumChange => raw::BTRFS_CSUM_CHANGE_OBJECTID as u64,
            Self::Multiple => raw::BTRFS_MULTIPLE_OBJECTIDS as u64,
            Self::FirstFree => raw::BTRFS_FIRST_FREE_OBJECTID as u64,
            Self::Id(v) => v,
        }
    }

    /// Display an objectid with context-dependent disambiguation.
    ///
    /// Some objectid values have different meanings depending on the key type.
    /// For example, objectid 1 is `ROOT_TREE` in general but `DEV_ITEMS`
    /// when used with `DEV_ITEM_KEY`, and `0` is `DEV_STATS` when used
    /// with `PERSISTENT_ITEM_KEY`.
    pub fn display_with_type(self, key_type: KeyType) -> String {
        let raw = self.to_raw();
        // Special disambiguations from the C reference print_objectid()
        if raw == raw::BTRFS_DEV_ITEMS_OBJECTID as u64
            && key_type == KeyType::DevItem
        {
            return "DEV_ITEMS".to_string();
        }
        if raw == raw::BTRFS_DEV_STATS_OBJECTID as u64
            && key_type == KeyType::PersistentItem
        {
            return "DEV_STATS".to_string();
        }
        if raw == raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64
            && key_type == KeyType::ChunkItem
        {
            return "FIRST_CHUNK_TREE".to_string();
        }
        // DEV_EXTENT objectids are device IDs, not tree IDs — print as numbers
        if key_type == KeyType::DevExtent {
            return raw.to_string();
        }
        self.to_string()
    }

    /// Parse a tree name string (for CLI `-t` flag) into an ObjectId.
    pub fn from_tree_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "root" => Some(Self::RootTree),
            "extent" => Some(Self::ExtentTree),
            "chunk" => Some(Self::ChunkTree),
            "device" | "dev" => Some(Self::DevTree),
            "fs" => Some(Self::FsTree),
            "root_tree_dir" => Some(Self::RootTreeDir),
            "csum" | "checksum" => Some(Self::CsumTree),
            "quota" => Some(Self::QuotaTree),
            "uuid" => Some(Self::UuidTree),
            "free_space" | "free-space" => Some(Self::FreeSpaceTree),
            "block_group" | "block-group" => Some(Self::BlockGroupTree),
            "raid_stripe" | "raid-stripe" => Some(Self::RaidStripeTree),
            "remap" => Some(Self::RemapTree),
            "tree_log" | "tree-log" => Some(Self::TreeLog),
            "tree_log_fixup" | "tree-log-fixup" => Some(Self::TreeLogFixup),
            "tree_reloc" | "tree-reloc" => Some(Self::TreeReloc),
            "data_reloc" | "data-reloc" => Some(Self::DataRelocTree),
            _ => name.parse::<u64>().ok().map(Self::from_raw),
        }
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootTree => write!(f, "ROOT_TREE"),
            Self::ExtentTree => write!(f, "EXTENT_TREE"),
            Self::ChunkTree => write!(f, "CHUNK_TREE"),
            Self::DevTree => write!(f, "DEV_TREE"),
            Self::FsTree => write!(f, "FS_TREE"),
            Self::RootTreeDir => write!(f, "ROOT_TREE_DIR"),
            Self::CsumTree => write!(f, "CSUM_TREE"),
            Self::QuotaTree => write!(f, "QUOTA_TREE"),
            Self::UuidTree => write!(f, "UUID_TREE"),
            Self::FreeSpaceTree => write!(f, "FREE_SPACE_TREE"),
            Self::BlockGroupTree => write!(f, "BLOCK_GROUP_TREE"),
            Self::RaidStripeTree => write!(f, "RAID_STRIPE_TREE"),
            Self::RemapTree => write!(f, "REMAP_TREE"),
            Self::DevStats => write!(f, "DEV_STATS"),
            Self::Balance => write!(f, "BALANCE"),
            Self::Orphan => write!(f, "ORPHAN"),
            Self::TreeLog => write!(f, "TREE_LOG"),
            Self::TreeLogFixup => write!(f, "TREE_LOG_FIXUP"),
            Self::TreeReloc => write!(f, "TREE_RELOC"),
            Self::DataRelocTree => write!(f, "DATA_RELOC_TREE"),
            Self::ExtentCsum => write!(f, "EXTENT_CSUM"),
            Self::FreeSpace => write!(f, "FREE_SPACE"),
            Self::FreeIno => write!(f, "FREE_INO"),
            Self::CsumChange => write!(f, "CSUM_CHANGE"),
            Self::Multiple => write!(f, "MULTIPLE"),
            Self::FirstFree => write!(f, "256"),
            Self::Id(v) => write!(f, "{v}"),
        }
    }
}

/// A parsed btrfs disk key (objectid, type, offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskKey {
    pub objectid: u64,
    pub key_type: KeyType,
    pub offset: u64,
}

impl DiskKey {
    /// Parse a disk key from `buf` at byte offset `off`.
    /// The on-disk layout is: objectid (le64), type (u8), offset (le64) = 17 bytes.
    pub fn parse(buf: &[u8], off: usize) -> Self {
        Self {
            objectid: read_le_u64(buf, off),
            key_type: KeyType::from_raw(buf[off + 8]),
            offset: read_le_u64(buf, off + 9),
        }
    }
}

/// Format a key as `(OBJECTID TYPE OFFSET)` matching the C reference output.
///
/// Special formatting rules:
/// - Qgroup keys: objectid and offset are formatted as `LEVEL/SUBVOLID`
/// - UUID keys: objectid is formatted as `0x<16-char-hex>`
/// - Offset of u64::MAX is formatted as `-1`
pub fn format_key(key: &DiskKey) -> String {
    let objectid_str = format_key_objectid(key);
    let type_str = format_key_type(key);
    let offset_str = format_key_offset(key);
    format!("({objectid_str} {type_str} {offset_str})")
}

fn format_key_objectid(key: &DiskKey) -> String {
    match key.key_type {
        KeyType::QgroupRelation
        | KeyType::QgroupInfo
        | KeyType::QgroupLimit => {
            let level = key.objectid >> 48;
            let subvolid = key.objectid & ((1u64 << 48) - 1);
            format!("{level}/{subvolid}")
        }
        KeyType::UuidKeySubvol | KeyType::UuidKeyReceivedSubvol => {
            format!("0x{:016x}", key.objectid)
        }
        _ => {
            let oid = ObjectId::from_raw(key.objectid);
            oid.display_with_type(key.key_type)
        }
    }
}

fn format_key_type(key: &DiskKey) -> String {
    // Special case: type 0 with FREE_SPACE objectid means UNTYPED
    if key.key_type == KeyType::Unknown(0)
        && key.objectid == raw::BTRFS_FREE_SPACE_OBJECTID as u64
    {
        return "UNTYPED".to_string();
    }
    key.key_type.to_string()
}

fn format_key_offset(key: &DiskKey) -> String {
    match key.key_type {
        KeyType::QgroupRelation
        | KeyType::QgroupStatus
        | KeyType::QgroupInfo
        | KeyType::QgroupLimit => {
            let level = key.offset >> 48;
            let subvolid = key.offset & ((1u64 << 48) - 1);
            format!("{level}/{subvolid}")
        }
        KeyType::UuidKeySubvol | KeyType::UuidKeyReceivedSubvol => {
            format!("0x{:016x}", key.offset)
        }
        _ => format!("{}", key.offset),
    }
}

/// Parsed header of a btrfs tree block (shared by nodes and leaves).
#[derive(Debug, Clone)]
pub struct Header {
    pub csum: [u8; 32],
    pub fsid: Uuid,
    pub bytenr: u64,
    pub flags: u64,
    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
    pub owner: u64,
    pub nritems: u32,
    pub level: u8,
}

/// Size of the on-disk header in bytes.
const HEADER_SIZE: usize = mem::size_of::<raw::btrfs_header>();

impl Header {
    /// Parse a tree block header from the start of `buf`.
    pub fn parse(buf: &[u8]) -> Self {
        assert!(
            buf.len() >= HEADER_SIZE,
            "buffer too small for btrfs_header: {} < {HEADER_SIZE}",
            buf.len()
        );
        let mut csum = [0u8; 32];
        csum.copy_from_slice(&buf[0..32]);
        Self {
            csum,
            fsid: read_uuid(buf, 32),
            bytenr: read_le_u64(buf, 48),
            flags: read_le_u64(buf, 56),
            chunk_tree_uuid: read_uuid(buf, 64),
            generation: read_le_u64(buf, 80),
            owner: read_le_u64(buf, 88),
            nritems: read_le_u32(buf, 96),
            level: buf[100],
        }
    }

    /// Return the backref revision from the flags field.
    pub fn backref_rev(&self) -> u64 {
        self.flags >> raw::BTRFS_BACKREF_REV_SHIFT
    }

    /// Return the flags with the backref revision bits masked out.
    pub fn block_flags(&self) -> u64 {
        self.flags
            & !((raw::BTRFS_BACKREF_REV_MAX as u64 - 1)
                << raw::BTRFS_BACKREF_REV_SHIFT)
    }
}

/// Format header flags as a human-readable string (e.g. "WRITTEN|RELOC").
pub fn format_header_flags(flags: u64) -> String {
    let mut names = Vec::new();
    if flags & raw::BTRFS_HEADER_FLAG_WRITTEN as u64 != 0 {
        names.push("WRITTEN");
    }
    if flags & raw::BTRFS_HEADER_FLAG_RELOC as u64 != 0 {
        names.push("RELOC");
    }
    let known = raw::BTRFS_HEADER_FLAG_WRITTEN as u64
        | raw::BTRFS_HEADER_FLAG_RELOC as u64;
    let unknown = flags & !known;
    if unknown != 0 {
        names.push("UNKNOWN");
    }
    if names.is_empty() {
        "0x0".to_string()
    } else {
        names.join("|")
    }
}

/// A key pointer from an internal tree node, pointing to a child block.
#[derive(Debug, Clone, Copy)]
pub struct KeyPtr {
    pub key: DiskKey,
    pub blockptr: u64,
    pub generation: u64,
}

/// Size of a key pointer on disk.
const KEY_PTR_SIZE: usize = mem::size_of::<raw::btrfs_key_ptr>();

impl KeyPtr {
    /// Parse a key pointer from `buf` at byte offset `off`.
    fn parse(buf: &[u8], off: usize) -> Self {
        Self {
            key: DiskKey::parse(buf, off),
            blockptr: read_le_u64(buf, off + 17),
            generation: read_le_u64(buf, off + 25),
        }
    }
}

/// A leaf item descriptor: key + offset/size into the leaf's data area.
#[derive(Debug, Clone, Copy)]
pub struct Item {
    pub key: DiskKey,
    /// Byte offset of this item's data, relative to the end of the item array
    /// (i.e. relative to `HEADER_SIZE + nritems * ITEM_SIZE`... but actually
    /// it's an offset from the start of the leaf data area in the C code,
    /// which starts right after the header). See `Leaf::item_data()`.
    pub offset: u32,
    /// Size of this item's data in bytes.
    pub size: u32,
}

/// Size of an item descriptor on disk.
const ITEM_SIZE: usize = mem::size_of::<raw::btrfs_item>();

impl Item {
    /// Parse an item descriptor from `buf` at byte offset `off`.
    fn parse(buf: &[u8], off: usize) -> Self {
        Self {
            key: DiskKey::parse(buf, off),
            offset: read_le_u32(buf, off + 17),
            size: read_le_u32(buf, off + 21),
        }
    }
}

/// A parsed btrfs tree block: either an internal node or a leaf.
pub enum TreeBlock {
    /// Internal node (level > 0): contains key pointers to child blocks.
    Node { header: Header, ptrs: Vec<KeyPtr> },
    /// Leaf node (level == 0): contains items with data payloads.
    Leaf {
        header: Header,
        items: Vec<Item>,
        /// The full block data, so item formatters can extract payloads.
        data: Vec<u8>,
    },
}

impl TreeBlock {
    /// Parse a tree block from a nodesize-length buffer.
    pub fn parse(buf: &[u8]) -> Self {
        let header = Header::parse(buf);
        let nritems = header.nritems as usize;

        if header.level > 0 {
            let mut ptrs = Vec::with_capacity(nritems);
            for i in 0..nritems {
                let off = HEADER_SIZE + i * KEY_PTR_SIZE;
                ptrs.push(KeyPtr::parse(buf, off));
            }
            Self::Node { header, ptrs }
        } else {
            let mut items = Vec::with_capacity(nritems);
            for i in 0..nritems {
                let off = HEADER_SIZE + i * ITEM_SIZE;
                items.push(Item::parse(buf, off));
            }
            Self::Leaf {
                header,
                items,
                data: buf.to_vec(),
            }
        }
    }

    /// Return a reference to the header.
    pub fn header(&self) -> &Header {
        match self {
            Self::Node { header, .. } | Self::Leaf { header, .. } => header,
        }
    }

    /// For a leaf block, get the data slice for item at `index`.
    ///
    /// The item's `offset` field is relative to the start of the data area,
    /// which begins right after the header. So the absolute offset in the
    /// block buffer is `HEADER_SIZE + item.offset`.
    pub fn item_data(&self, index: usize) -> Option<&[u8]> {
        match self {
            Self::Leaf { items, data, .. } => {
                let item = items.get(index)?;
                let start = HEADER_SIZE + item.offset as usize;
                let end = start + item.size as usize;
                if end <= data.len() {
                    Some(&data[start..end])
                } else {
                    None
                }
            }
            Self::Node { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to build a minimal LE buffer for a disk key
    fn make_disk_key(objectid: u64, key_type: u8, offset: u64) -> [u8; 17] {
        let mut buf = [0u8; 17];
        buf[0..8].copy_from_slice(&objectid.to_le_bytes());
        buf[8] = key_type;
        buf[9..17].copy_from_slice(&offset.to_le_bytes());
        buf
    }

    // Helper to build a minimal tree block header
    fn make_header(
        bytenr: u64,
        generation: u64,
        owner: u64,
        nritems: u32,
        level: u8,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_SIZE];
        // csum: leave as zeros
        // fsid: leave as zeros
        buf[48..56].copy_from_slice(&bytenr.to_le_bytes());
        // flags: WRITTEN
        buf[56..64].copy_from_slice(
            &(raw::BTRFS_HEADER_FLAG_WRITTEN as u64).to_le_bytes(),
        );
        // chunk_tree_uuid: leave as zeros
        buf[80..88].copy_from_slice(&generation.to_le_bytes());
        buf[88..96].copy_from_slice(&owner.to_le_bytes());
        buf[96..100].copy_from_slice(&nritems.to_le_bytes());
        buf[100] = level;
        buf
    }

    #[test]
    fn key_type_round_trip() {
        for raw_val in 0..=255u8 {
            let kt = KeyType::from_raw(raw_val);
            assert_eq!(kt.to_raw(), raw_val);
        }
    }

    #[test]
    fn key_type_display() {
        assert_eq!(KeyType::InodeItem.to_string(), "INODE_ITEM");
        assert_eq!(KeyType::ChunkItem.to_string(), "CHUNK_ITEM");
        assert_eq!(KeyType::Unknown(99).to_string(), "UNKNOWN.99");
    }

    #[test]
    fn objectid_round_trip() {
        let cases: &[u64] = &[
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            256,
            1000,
            // Negative objectids cast to u64
            raw::BTRFS_BALANCE_OBJECTID as u64,
            raw::BTRFS_ORPHAN_OBJECTID as u64,
            raw::BTRFS_TREE_LOG_OBJECTID as u64,
            raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
        ];
        for &v in cases {
            let oid = ObjectId::from_raw(v);
            assert_eq!(oid.to_raw(), v, "round-trip failed for {v}");
        }
    }

    #[test]
    fn objectid_display() {
        assert_eq!(ObjectId::RootTree.to_string(), "ROOT_TREE");
        assert_eq!(ObjectId::FsTree.to_string(), "FS_TREE");
        assert_eq!(ObjectId::Id(256).to_string(), "256");
        assert_eq!(ObjectId::Id(u64::MAX).to_string(), "18446744073709551615");
    }

    #[test]
    fn objectid_display_with_type() {
        // objectid 1 is normally ROOT_TREE but DEV_ITEMS with DevItem key
        let oid = ObjectId::from_raw(1);
        assert_eq!(oid.display_with_type(KeyType::RootItem), "ROOT_TREE");
        assert_eq!(oid.display_with_type(KeyType::DevItem), "DEV_ITEMS");
    }

    #[test]
    fn objectid_from_tree_name() {
        assert_eq!(ObjectId::from_tree_name("root"), Some(ObjectId::RootTree));
        assert_eq!(
            ObjectId::from_tree_name("CHUNK"),
            Some(ObjectId::ChunkTree)
        );
        assert_eq!(
            ObjectId::from_tree_name("free-space"),
            Some(ObjectId::FreeSpaceTree)
        );
        assert_eq!(ObjectId::from_tree_name("5"), Some(ObjectId::FsTree));
        assert_eq!(ObjectId::from_tree_name("256"), Some(ObjectId::Id(256)));
        assert_eq!(ObjectId::from_tree_name("nosuch"), None);
    }

    #[test]
    fn parse_disk_key() {
        let buf = make_disk_key(42, raw::BTRFS_INODE_ITEM_KEY as u8, 100);
        let key = DiskKey::parse(&buf, 0);
        assert_eq!(key.objectid, 42);
        assert_eq!(key.key_type, KeyType::InodeItem);
        assert_eq!(key.offset, 100);
    }

    #[test]
    fn parse_header() {
        let buf = make_header(65536, 7, 5, 10, 0);
        let hdr = Header::parse(&buf);
        assert_eq!(hdr.bytenr, 65536);
        assert_eq!(hdr.generation, 7);
        assert_eq!(hdr.owner, 5);
        assert_eq!(hdr.nritems, 10);
        assert_eq!(hdr.level, 0);
        assert_eq!(hdr.block_flags(), raw::BTRFS_HEADER_FLAG_WRITTEN as u64);
    }

    #[test]
    fn format_header_flags_written() {
        assert_eq!(
            format_header_flags(raw::BTRFS_HEADER_FLAG_WRITTEN as u64),
            "WRITTEN"
        );
    }

    #[test]
    fn format_header_flags_multiple() {
        let flags = raw::BTRFS_HEADER_FLAG_WRITTEN as u64
            | raw::BTRFS_HEADER_FLAG_RELOC as u64;
        assert_eq!(format_header_flags(flags), "WRITTEN|RELOC");
    }

    #[test]
    fn parse_leaf_block() {
        let nodesize = 4096usize;
        let nritems = 2u32;
        let mut buf = vec![0u8; nodesize];

        // Write header (level 0 = leaf)
        let hdr = make_header(65536, 7, 5, nritems, 0);
        buf[..HEADER_SIZE].copy_from_slice(&hdr);

        // Write two item descriptors
        // Item 0: key=(256, INODE_ITEM, 0), offset=3800, size=160
        let key0 = make_disk_key(256, raw::BTRFS_INODE_ITEM_KEY as u8, 0);
        let item0_off = HEADER_SIZE;
        buf[item0_off..item0_off + 17].copy_from_slice(&key0);
        buf[item0_off + 17..item0_off + 21]
            .copy_from_slice(&3800u32.to_le_bytes());
        buf[item0_off + 21..item0_off + 25]
            .copy_from_slice(&160u32.to_le_bytes());

        // Item 1: key=(256, DIR_ITEM, 100), offset=3700, size=50
        let key1 = make_disk_key(256, raw::BTRFS_DIR_ITEM_KEY as u8, 100);
        let item1_off = HEADER_SIZE + ITEM_SIZE;
        buf[item1_off..item1_off + 17].copy_from_slice(&key1);
        buf[item1_off + 17..item1_off + 21]
            .copy_from_slice(&3700u32.to_le_bytes());
        buf[item1_off + 21..item1_off + 25]
            .copy_from_slice(&50u32.to_le_bytes());

        // Write some recognizable data at the item data offsets
        // Item 0 data at HEADER_SIZE + 3800
        let data0_start = HEADER_SIZE + 3800;
        buf[data0_start] = 0xAA;
        // Item 1 data at HEADER_SIZE + 3700
        let data1_start = HEADER_SIZE + 3700;
        buf[data1_start] = 0xBB;

        let block = TreeBlock::parse(&buf);

        match &block {
            TreeBlock::Leaf { header, items, .. } => {
                assert_eq!(header.level, 0);
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].key.key_type, KeyType::InodeItem);
                assert_eq!(items[1].key.key_type, KeyType::DirItem);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }

        let data0 = block.item_data(0).unwrap();
        assert_eq!(data0[0], 0xAA);
        assert_eq!(data0.len(), 160);

        let data1 = block.item_data(1).unwrap();
        assert_eq!(data1[0], 0xBB);
        assert_eq!(data1.len(), 50);
    }

    #[test]
    fn parse_node_block() {
        let nodesize = 4096usize;
        let nritems = 3u32;
        let mut buf = vec![0u8; nodesize];

        // Write header (level 1 = node)
        let hdr = make_header(131072, 10, 2, nritems, 1);
        buf[..HEADER_SIZE].copy_from_slice(&hdr);

        // Write three key pointers
        for i in 0..3u64 {
            let off = HEADER_SIZE + i as usize * KEY_PTR_SIZE;
            let key = make_disk_key(i + 1, raw::BTRFS_ROOT_ITEM_KEY as u8, 0);
            buf[off..off + 17].copy_from_slice(&key);
            let blockptr = (i + 1) * 65536;
            buf[off + 17..off + 25].copy_from_slice(&blockptr.to_le_bytes());
            let generation = 10 - i;
            buf[off + 25..off + 33].copy_from_slice(&generation.to_le_bytes());
        }

        let block = TreeBlock::parse(&buf);

        match &block {
            TreeBlock::Node { header, ptrs } => {
                assert_eq!(header.level, 1);
                assert_eq!(header.bytenr, 131072);
                assert_eq!(ptrs.len(), 3);
                assert_eq!(ptrs[0].blockptr, 65536);
                assert_eq!(ptrs[1].blockptr, 131072);
                assert_eq!(ptrs[2].blockptr, 196608);
                assert_eq!(ptrs[0].generation, 10);
                assert_eq!(ptrs[2].generation, 8);
            }
            TreeBlock::Leaf { .. } => panic!("expected node"),
        }

        // item_data should return None for nodes
        assert!(block.item_data(0).is_none());
    }

    #[test]
    fn format_key_basic() {
        let key = DiskKey {
            objectid: 256,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        assert_eq!(format_key(&key), "(256 INODE_ITEM 0)");
    }

    #[test]
    fn format_key_well_known_objectid() {
        let key = DiskKey {
            objectid: raw::BTRFS_FS_TREE_OBJECTID as u64,
            key_type: KeyType::RootItem,
            offset: u64::MAX,
        };
        assert_eq!(
            format_key(&key),
            "(FS_TREE ROOT_ITEM 18446744073709551615)"
        );
    }

    #[test]
    fn format_key_qgroup() {
        let key = DiskKey {
            objectid: 0, // level=0, subvolid=0
            key_type: KeyType::QgroupInfo,
            offset: (0u64 << 48) | 256, // level=0, subvolid=256
        };
        assert_eq!(format_key(&key), "(0/0 QGROUP_INFO 0/256)");
    }

    #[test]
    fn format_key_uuid() {
        let key = DiskKey {
            objectid: 0xdeadbeef12345678,
            key_type: KeyType::UuidKeySubvol,
            offset: 0xabcdef0123456789,
        };
        assert_eq!(
            format_key(&key),
            "(0xdeadbeef12345678 UUID_KEY_SUBVOL 0xabcdef0123456789)"
        );
    }

    #[test]
    fn format_key_dev_items() {
        let key = DiskKey {
            objectid: 1,
            key_type: KeyType::DevItem,
            offset: 1,
        };
        assert_eq!(format_key(&key), "(DEV_ITEMS DEV_ITEM 1)");
    }
}

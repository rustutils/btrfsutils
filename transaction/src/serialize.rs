//! # Item type serialization (to bytes)
//!
//! Serialization functions for tree item payloads. While `btrfs-disk` handles
//! parsing (bytes to typed structs), this module handles the reverse: converting
//! typed data back to on-disk little-endian byte representations for insertion
//! into leaves.
//!
//! Each function writes directly into a `Vec<u8>` using `BufMut` for
//! consistency with the disk crate's `write_bytes` pattern.

use crate::extent_buffer::write_disk_key;
use btrfs_disk::{
    items::{BlockGroupItem, RootItem, RootItemFlags, Timespec},
    tree::DiskKey,
};
use bytes::BufMut;
use uuid::Uuid;

/// On-disk size of `btrfs_inode_item` (160 bytes).
///
/// The kernel header defines this as a packed struct. We write it as 160 bytes
/// of zeros for internal trees (root tree entries don't use the inode), or
/// serialize the actual fields for FS tree inodes.
const INODE_ITEM_SIZE: usize = 160;

/// On-disk size of a `btrfs_root_item` (439 bytes, padded to match the C
/// struct which is 496 bytes including reserved space).
const ROOT_ITEM_SIZE: usize = 439;

/// On-disk size of a `btrfs_block_group_item` (24 bytes).
const BLOCK_GROUP_ITEM_SIZE: usize = 24;

/// Serialize a `RootItem` to its on-disk byte representation.
///
/// The on-disk format starts with a 160-byte `btrfs_inode_item` (all zeros for
/// internal tree root items), followed by the root item fields. The total is
/// 439 bytes of defined fields. The C struct is padded to 496 bytes with a
/// 64-byte reserved region plus padding; we write the full 496 bytes to match.
#[must_use]
pub fn root_item_to_bytes(item: &RootItem) -> Vec<u8> {
    let mut buf = Vec::with_capacity(ROOT_ITEM_SIZE + INODE_ITEM_SIZE + 64);

    // btrfs_inode_item (160 bytes) — zeroed for internal tree root items
    buf.extend_from_slice(&[0u8; INODE_ITEM_SIZE]);

    // Root item fields after the inode
    buf.put_u64_le(item.generation);
    buf.put_u64_le(item.root_dirid);
    buf.put_u64_le(item.bytenr);
    buf.put_u64_le(item.byte_limit);
    buf.put_u64_le(item.bytes_used);
    buf.put_u64_le(item.last_snapshot);
    buf.put_u64_le(item.flags.bits());
    buf.put_u32_le(item.refs);

    // drop_progress (btrfs_disk_key, 17 bytes)
    let key_off = buf.len();
    buf.extend_from_slice(&[0u8; 17]);
    write_disk_key(&mut buf[key_off..], 0, &item.drop_progress);

    // drop_level
    buf.put_u8(item.drop_level);

    // level
    buf.put_u8(item.level);

    // generation_v2
    buf.put_u64_le(item.generation_v2);

    // UUIDs (16 bytes each)
    buf.extend_from_slice(item.uuid.as_bytes());
    buf.extend_from_slice(item.parent_uuid.as_bytes());
    buf.extend_from_slice(item.received_uuid.as_bytes());

    // Transaction IDs
    buf.put_u64_le(item.ctransid);
    buf.put_u64_le(item.otransid);
    buf.put_u64_le(item.stransid);
    buf.put_u64_le(item.rtransid);

    // Timestamps (12 bytes each: 8-byte sec + 4-byte nsec)
    write_timespec(&mut buf, &item.ctime);
    write_timespec(&mut buf, &item.otime);
    write_timespec(&mut buf, &item.stime);
    write_timespec(&mut buf, &item.rtime);

    // Reserved (64 bytes of zeros to reach 496 bytes total)
    let current = buf.len();
    let target = INODE_ITEM_SIZE + 336; // 160 + 336 = 496
    if current < target {
        buf.resize(target, 0);
    }

    buf
}

/// Serialize a `BlockGroupItem` to its 24-byte on-disk representation.
#[must_use]
pub fn block_group_item_to_bytes(item: &BlockGroupItem) -> Vec<u8> {
    let mut buf = Vec::with_capacity(BLOCK_GROUP_ITEM_SIZE);
    buf.put_u64_le(item.used);
    buf.put_u64_le(item.chunk_objectid);
    buf.put_u64_le(item.flags.bits());
    buf
}

/// Size of a skinny metadata extent item with one `TREE_BLOCK_REF` inline backref.
///
/// Layout: extent item header (24) + inline ref type (1) + offset (8) = 33 bytes.
pub const METADATA_EXTENT_ITEM_SIZE: usize = 33;

/// Serialize a metadata extent item (METADATA_ITEM) with a single
/// `TREE_BLOCK_REF` inline backref.
///
/// This is the on-disk format for a tree block extent when the
/// `SKINNY_METADATA` incompat flag is set (modern default). The key is
/// `(bytenr, METADATA_ITEM=169, level)`.
///
/// Data layout (33 bytes):
/// - Extent item header (24 bytes): refs (u64) + generation (u64) + flags (u64)
/// - Inline backref (9 bytes): type (u8, `TREE_BLOCK_REF`=176) + offset (u64, root_id)
#[must_use]
pub fn metadata_extent_item_to_bytes(
    refs: u64,
    generation: u64,
    root_id: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(METADATA_EXTENT_ITEM_SIZE);

    // Extent item header
    buf.put_u64_le(refs);
    buf.put_u64_le(generation);
    buf.put_u64_le(btrfs_disk::items::ExtentFlags::TREE_BLOCK.bits());

    // Inline TREE_BLOCK_REF: type byte + root_id as offset
    buf.put_u8(btrfs_disk::tree::KeyType::TreeBlockRef.to_raw());
    buf.put_u64_le(root_id);

    debug_assert_eq!(buf.len(), METADATA_EXTENT_ITEM_SIZE);
    buf
}

/// Serialize a `Timespec` to 12 bytes (8-byte sec + 4-byte nsec).
fn write_timespec(buf: &mut Vec<u8>, ts: &Timespec) {
    buf.put_u64_le(ts.sec);
    buf.put_u32_le(ts.nsec);
}

/// Parameters for creating an inode item.
pub struct InodeItemArgs {
    /// Generation and transid.
    pub generation: u64,
    /// Logical file size.
    pub size: u64,
    /// On-disk bytes used.
    pub nbytes: u64,
    /// Hard link count.
    pub nlink: u32,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// POSIX file mode (type + permissions).
    pub mode: u32,
    /// Timestamp for atime/ctime/mtime/otime.
    pub time: Timespec,
}

/// Serialize a `btrfs_inode_item` (160 bytes).
#[must_use]
pub fn inode_item_to_bytes(args: &InodeItemArgs) -> Vec<u8> {
    let mut buf = Vec::with_capacity(INODE_ITEM_SIZE);
    buf.put_u64_le(args.generation);
    buf.put_u64_le(args.generation); // transid = generation
    buf.put_u64_le(args.size);
    buf.put_u64_le(args.nbytes);
    buf.put_u64_le(0); // block_group
    buf.put_u32_le(args.nlink);
    buf.put_u32_le(args.uid);
    buf.put_u32_le(args.gid);
    buf.put_u32_le(args.mode);
    buf.put_u64_le(0); // rdev
    buf.put_u64_le(0); // flags
    buf.put_u64_le(0); // sequence
    buf.extend_from_slice(&[0u8; 32]); // reserved
    for _ in 0..4 {
        write_timespec(&mut buf, &args.time);
    }
    debug_assert_eq!(buf.len(), INODE_ITEM_SIZE);
    buf
}

/// Serialize a `btrfs_dir_item` (directory entry).
///
/// On-disk layout: location key (17) + transid (8) + data_len (2) +
/// name_len (2) + type (1) + name + data = 30 + name.len() + data.len().
#[must_use]
pub fn dir_item_to_bytes(
    location: &DiskKey,
    transid: u64,
    file_type: u8,
    name: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(30 + name.len());
    // location key
    let key_off = buf.len();
    buf.extend_from_slice(&[0u8; 17]);
    write_disk_key(&mut buf[key_off..], 0, location);
    buf.put_u64_le(transid);
    buf.put_u16_le(0); // data_len (no xattr data for regular entries)
    buf.put_u16_le(name.len() as u16);
    buf.put_u8(file_type);
    buf.extend_from_slice(name);
    buf
}

/// Serialize a `btrfs_inode_ref`.
///
/// On-disk layout: index (8) + name_len (2) + name.
#[must_use]
pub fn inode_ref_to_bytes(index: u64, name: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10 + name.len());
    buf.put_u64_le(index);
    buf.put_u16_le(name.len() as u16);
    buf.extend_from_slice(name);
    buf
}

/// Create a minimal `RootItem` suitable for internal trees (not subvolumes).
///
/// Sets generation, bytenr, level, and refs=1. All other fields are zeroed/nil.
#[must_use]
pub fn make_internal_root_item(
    generation: u64,
    bytenr: u64,
    level: u8,
) -> RootItem {
    RootItem {
        generation,
        root_dirid: 0,
        bytenr,
        byte_limit: 0,
        bytes_used: 0,
        last_snapshot: 0,
        flags: RootItemFlags::empty(),
        refs: 1,
        drop_progress: DiskKey {
            objectid: 0,
            key_type: btrfs_disk::tree::KeyType::from_raw(0),
            offset: 0,
        },
        drop_level: 0,
        level,
        generation_v2: generation,
        uuid: Uuid::nil(),
        parent_uuid: Uuid::nil(),
        received_uuid: Uuid::nil(),
        ctransid: 0,
        otransid: 0,
        stransid: 0,
        rtransid: 0,
        ctime: Timespec { sec: 0, nsec: 0 },
        otime: Timespec { sec: 0, nsec: 0 },
        stime: Timespec { sec: 0, nsec: 0 },
        rtime: Timespec { sec: 0, nsec: 0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::items::{BlockGroupFlags, RootItem as DiskRootItem};

    #[test]
    fn root_item_round_trip() {
        let original = make_internal_root_item(42, 65536, 0);
        let bytes = root_item_to_bytes(&original);

        // Parse it back with btrfs-disk's parser
        let parsed = DiskRootItem::parse(&bytes).expect("parse failed");
        assert_eq!(parsed.generation, 42);
        assert_eq!(parsed.bytenr, 65536);
        assert_eq!(parsed.level, 0);
        assert_eq!(parsed.refs, 1);
        assert_eq!(parsed.generation_v2, 42);
    }

    #[test]
    fn root_item_with_uuids() {
        let mut item = make_internal_root_item(10, 131072, 1);
        item.uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        item.ctransid = 5;
        item.otransid = 3;

        let bytes = root_item_to_bytes(&item);
        let parsed = DiskRootItem::parse(&bytes).unwrap();
        assert_eq!(parsed.uuid, item.uuid);
        assert_eq!(parsed.ctransid, 5);
        assert_eq!(parsed.otransid, 3);
        assert_eq!(parsed.level, 1);
    }

    #[test]
    fn block_group_item_round_trip() {
        let bg = BlockGroupItem {
            used: 1024 * 1024,
            chunk_objectid: 256,
            flags: BlockGroupFlags::METADATA | BlockGroupFlags::DUP,
        };
        let bytes = block_group_item_to_bytes(&bg);
        assert_eq!(bytes.len(), BLOCK_GROUP_ITEM_SIZE);

        let parsed = BlockGroupItem::parse(&bytes).unwrap();
        assert_eq!(parsed.used, bg.used);
        assert_eq!(parsed.chunk_objectid, bg.chunk_objectid);
        assert_eq!(parsed.flags, bg.flags);
    }

    #[test]
    fn root_item_size() {
        let item = make_internal_root_item(1, 0, 0);
        let bytes = root_item_to_bytes(&item);
        // Should be 496 bytes (160 inode + 336 root item fields + reserved)
        assert_eq!(bytes.len(), 496);
    }

    #[test]
    fn inode_item_round_trip() {
        use btrfs_disk::items::InodeItem;

        let bytes = inode_item_to_bytes(&InodeItemArgs {
            generation: 7,
            size: 42,
            nbytes: 4096,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            mode: 0o100644,
            time: Timespec {
                sec: 1000,
                nsec: 500,
            },
        });
        assert_eq!(bytes.len(), INODE_ITEM_SIZE);

        let parsed = InodeItem::parse(&bytes).unwrap();
        assert_eq!(parsed.generation, 7);
        assert_eq!(parsed.transid, 7);
        assert_eq!(parsed.size, 42);
        assert_eq!(parsed.nbytes, 4096);
        assert_eq!(parsed.nlink, 1);
        assert_eq!(parsed.uid, 1000);
        assert_eq!(parsed.gid, 1000);
        assert_eq!(parsed.mode, 0o100644);
    }

    #[test]
    fn dir_item_round_trip() {
        use btrfs_disk::items::DirItem;

        let location = DiskKey {
            objectid: 257,
            key_type: btrfs_disk::tree::KeyType::InodeItem,
            offset: 0,
        };
        let bytes = dir_item_to_bytes(
            &location,
            7,
            btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
            b"hello.txt",
        );

        let items = DirItem::parse_all(&bytes);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].location.objectid, 257);
        assert_eq!(items[0].transid, 7);
        assert_eq!(items[0].name, b"hello.txt");
    }

    #[test]
    fn inode_ref_round_trip() {
        use btrfs_disk::items::InodeRef;

        let bytes = inode_ref_to_bytes(2, b"hello.txt");
        let refs = InodeRef::parse_all(&bytes);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].index, 2);
        assert_eq!(refs[0].name, b"hello.txt");
    }
}

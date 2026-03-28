//! # Item serializers: produce on-disk byte payloads for btrfs tree items
//!
//! Each function serializes a specific item type into a `Vec<u8>` suitable
//! for passing to `LeafBuilder::push`. Field positions are derived from the
//! bindgen structs in `btrfs_disk::raw` via `offset_of!` and `size_of`.

use crate::tree::Key;
use btrfs_disk::{
    raw,
    util::{write_le_u16, write_le_u32, write_le_u64, write_uuid},
};
use std::mem;
use uuid::Uuid;

/// Serialize a ROOT_ITEM.
///
/// A root item describes a tree root: its block address, generation, and
/// metadata. The item starts with an embedded inode_item (for the root
/// directory inode), followed by the tree-specific fields.
pub fn root_item(generation: u64, bytenr: u64, root_dirid: u64) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_root_item>();
    let mut buf = vec![0u8; size];
    let inode_size = mem::size_of::<raw::btrfs_inode_item>();

    // Embedded inode_item: set generation, nlink=1, mode=040755 (directory)
    write_le_u64(&mut buf, 0, generation); // inode.generation
    write_le_u32(&mut buf, 40, 1); // inode.nlink
    write_le_u32(&mut buf, 52, 0o40755); // inode.mode

    // Root-specific fields (after the embedded inode)
    write_le_u64(&mut buf, inode_size, generation);
    write_le_u64(&mut buf, inode_size + 8, root_dirid);
    write_le_u64(&mut buf, inode_size + 16, bytenr);
    // byte_limit, bytes_used, last_snapshot, flags, refs: all zero
    write_le_u32(&mut buf, inode_size + 56, 1); // refs = 1

    // drop_progress key (17 bytes) at inode_size + 60: zero
    // drop_level at inode_size + 77: zero
    // level at offset_of!(btrfs_root_item, level): zero

    // generation_v2 follows level
    let level_off = mem::offset_of!(raw::btrfs_root_item, level);
    write_le_u64(&mut buf, level_off + 1, generation); // generation_v2

    buf
}

/// Serialize a ROOT_ITEM for the chunk tree.
///
/// The chunk tree root item is special: it stores the chunk tree generation
/// in its `generation` field and sets the `bytenr` to the chunk tree block.
/// Same structure as a normal root item otherwise.
pub fn chunk_tree_root_item(generation: u64, bytenr: u64) -> Vec<u8> {
    root_item(
        generation,
        bytenr,
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
    )
}

/// Serialize an EXTENT_ITEM for a tree block (metadata extent).
///
/// For skinny metadata (the default), the item is just the 24-byte
/// btrfs_extent_item header. For non-skinny, it also includes the
/// btrfs_tree_block_info (17 + 1 bytes).
pub fn extent_item(refs: u64, generation: u64, skinny: bool) -> Vec<u8> {
    let base_size = mem::size_of::<raw::btrfs_extent_item>();
    let extra = if skinny {
        0
    } else {
        mem::size_of::<raw::btrfs_tree_block_info>()
    };
    let size = base_size + extra;
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, refs);
    write_le_u64(&mut buf, 8, generation);
    write_le_u64(&mut buf, 16, raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64);

    buf
}

/// Serialize a BLOCK_GROUP_ITEM.
pub fn block_group_item(used: u64, chunk_objectid: u64, flags: u64) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_block_group_item>();
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, used);
    write_le_u64(&mut buf, 8, chunk_objectid);
    write_le_u64(&mut buf, 16, flags);

    buf
}

/// Serialize a DEV_ITEM.
pub fn dev_item(
    devid: u64,
    total_bytes: u64,
    bytes_used: u64,
    sector_size: u32,
    dev_uuid: &Uuid,
    fsid: &Uuid,
) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_dev_item>();
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, devid);
    write_le_u64(&mut buf, 8, total_bytes);
    write_le_u64(&mut buf, 16, bytes_used);
    write_le_u32(&mut buf, 24, sector_size); // io_align
    write_le_u32(&mut buf, 28, sector_size); // io_width
    write_le_u32(&mut buf, 32, sector_size); // sector_size
    // dev_type at 36: 0
    // generation at 44: 0
    // start_offset at 52: 0
    // dev_group at 60: 0
    // seek_speed at 64: 0
    // bandwidth at 65: 0
    write_uuid(&mut buf, 66, dev_uuid);
    write_uuid(&mut buf, 82, fsid);

    buf
}

/// Serialize a CHUNK_ITEM with one stripe.
pub fn chunk_item_single(
    length: u64,
    owner: u64,
    chunk_type: u64,
    sector_size: u32,
    stripe_devid: u64,
    stripe_offset: u64,
    stripe_dev_uuid: &Uuid,
) -> Vec<u8> {
    let base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();
    let size = base_size + stripe_size;
    let mut buf = vec![0u8; size];

    /// 64 KiB — default stripe length for btrfs chunks.
    /// From kernel-shared/volumes.h: BTRFS_STRIPE_LEN
    const STRIPE_LEN: u64 = 64 * 1024;

    write_le_u64(&mut buf, 0, length);
    write_le_u64(&mut buf, 8, owner);
    write_le_u64(&mut buf, 16, STRIPE_LEN);
    write_le_u64(&mut buf, 24, chunk_type);
    write_le_u32(&mut buf, 32, sector_size); // io_align
    write_le_u32(&mut buf, 36, sector_size); // io_width
    write_le_u32(&mut buf, 40, sector_size); // sector_size
    write_le_u16(&mut buf, 44, 1); // num_stripes
    write_le_u16(&mut buf, 46, 0); // sub_stripes

    // Stripe 0
    write_le_u64(&mut buf, base_size, stripe_devid);
    write_le_u64(&mut buf, base_size + 8, stripe_offset);
    write_uuid(&mut buf, base_size + 16, stripe_dev_uuid);

    buf
}

/// Serialize a DEV_EXTENT.
pub fn dev_extent(
    chunk_tree: u64,
    chunk_objectid: u64,
    chunk_offset: u64,
    length: u64,
    chunk_tree_uuid: &Uuid,
) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_dev_extent>();
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, chunk_tree);
    write_le_u64(&mut buf, 8, chunk_objectid);
    write_le_u64(&mut buf, 16, chunk_offset);
    write_le_u64(&mut buf, 24, length);
    write_uuid(&mut buf, 32, chunk_tree_uuid);

    buf
}

/// Serialize a DEV_STATS_ITEM (all counters zero).
pub fn dev_stats_zeroed() -> Vec<u8> {
    // 5 u64 counters: write_errs, read_errs, flush_errs, corruption_errs, generation
    vec![0u8; 5 * 8]
}

/// Serialize a FREE_SPACE_INFO.
pub fn free_space_info(extent_count: u32, flags: u32) -> Vec<u8> {
    let mut buf = vec![0u8; 8];
    write_le_u32(&mut buf, 0, extent_count);
    write_le_u32(&mut buf, 4, flags);
    buf
}

/// Serialize an INODE_ITEM for a root directory.
///
/// Creates a directory inode (mode 040755) with nlink=1 and the given
/// generation and timestamps.
pub fn inode_item_dir(generation: u64, nbytes: u64, now_sec: u64) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_inode_item>();
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, generation); // generation
    // transid at 8: 0 (set by kernel on first write)
    // size at 16: 0 (empty directory)
    write_le_u64(&mut buf, 24, nbytes); // nbytes
    // block_group at 32: 0
    write_le_u32(&mut buf, 40, 1); // nlink
    // uid at 44: 0
    // gid at 48: 0
    write_le_u32(&mut buf, 52, 0o40755); // mode = S_IFDIR | 0755
    // rdev at 56: 0
    // flags at 64: 0
    // sequence at 72: 0

    // Timestamps: atime, ctime, mtime, otime
    let ts_off = mem::offset_of!(raw::btrfs_inode_item, atime);
    let ts_size = mem::size_of::<raw::btrfs_timespec>();
    for i in 0..4 {
        write_le_u64(&mut buf, ts_off + i * ts_size, now_sec);
        write_le_u32(&mut buf, ts_off + i * ts_size + 8, 0);
    }

    buf
}

/// Serialize an INODE_REF item.
///
/// Contains the directory entry index and the name of the entry
/// pointing to this inode.
pub fn inode_ref(index: u64, name: &[u8]) -> Vec<u8> {
    let size = 8 + 2 + name.len(); // index(8) + name_len(2) + name
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, index);
    write_le_u16(&mut buf, 8, name.len() as u16);
    buf[10..10 + name.len()].copy_from_slice(name);

    buf
}

/// Serialize a DiskKey into 17 bytes (for embedding in other items).
pub fn disk_key(key: &Key) -> Vec<u8> {
    let mut buf = vec![0u8; 17];
    key.write_to(&mut buf, 0);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::items;

    #[test]
    fn roundtrip_block_group_item() {
        let data = block_group_item(
            4096,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
        );
        let parsed = items::BlockGroupItem::parse(&data).unwrap();
        assert_eq!(parsed.used, 4096);
        assert_eq!(
            parsed.chunk_objectid,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64
        );
        assert_eq!(parsed.flags, raw::BTRFS_BLOCK_GROUP_SYSTEM as u64);
    }

    #[test]
    fn roundtrip_dev_item() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let fsid =
            Uuid::parse_str("cafebabe-cafe-babe-cafe-babecafebabe").unwrap();
        let data = dev_item(1, 1_000_000_000, 4_000_000, 4096, &uuid, &fsid);
        let parsed = items::DevItem::parse(&data).unwrap();
        assert_eq!(parsed.devid, 1);
        assert_eq!(parsed.total_bytes, 1_000_000_000);
        assert_eq!(parsed.bytes_used, 4_000_000);
        assert_eq!(parsed.io_align, 4096);
        assert_eq!(parsed.io_width, 4096);
        assert_eq!(parsed.sector_size, 4096);
        assert_eq!(parsed.uuid, uuid);
        assert_eq!(parsed.fsid, fsid);
    }

    #[test]
    fn roundtrip_chunk_item() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let data = chunk_item_single(
            4 * 1024 * 1024,
            raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
            4096,
            1,
            0x100000,
            &uuid,
        );
        let parsed = items::ChunkItem::parse(&data).unwrap();
        assert_eq!(parsed.length, 4 * 1024 * 1024);
        assert_eq!(parsed.owner, raw::BTRFS_EXTENT_TREE_OBJECTID as u64);
        assert_eq!(parsed.chunk_type, raw::BTRFS_BLOCK_GROUP_SYSTEM as u64);
        assert_eq!(parsed.num_stripes, 1);
        assert_eq!(parsed.stripes.len(), 1);
        assert_eq!(parsed.stripes[0].devid, 1);
        assert_eq!(parsed.stripes[0].offset, 0x100000);
        assert_eq!(parsed.stripes[0].dev_uuid, uuid);
    }

    #[test]
    fn roundtrip_dev_extent() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let data = dev_extent(
            raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
            0x100000,
            4 * 1024 * 1024,
            &uuid,
        );
        let parsed = items::DevExtent::parse(&data).unwrap();
        assert_eq!(parsed.chunk_tree, raw::BTRFS_CHUNK_TREE_OBJECTID as u64);
        assert_eq!(
            parsed.chunk_objectid,
            raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64
        );
        assert_eq!(parsed.chunk_offset, 0x100000);
        assert_eq!(parsed.length, 4 * 1024 * 1024);
        assert_eq!(parsed.chunk_tree_uuid, uuid);
    }

    #[test]
    fn roundtrip_free_space_info() {
        let data = free_space_info(3, 0);
        let parsed = items::FreeSpaceInfo::parse(&data).unwrap();
        assert_eq!(parsed.extent_count, 3);
        assert_eq!(parsed.flags, 0);
    }

    #[test]
    fn roundtrip_root_item() {
        let data =
            root_item(1, 0x100000, raw::BTRFS_FIRST_FREE_OBJECTID as u64);
        let parsed = items::RootItem::parse(&data).unwrap();
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.bytenr, 0x100000);
        assert_eq!(parsed.root_dirid, raw::BTRFS_FIRST_FREE_OBJECTID as u64);
        assert_eq!(parsed.refs, 1);
        assert_eq!(parsed.generation_v2, 1);
    }

    #[test]
    fn extent_item_skinny_size() {
        let data = extent_item(1, 1, true);
        assert_eq!(data.len(), mem::size_of::<raw::btrfs_extent_item>());
    }

    #[test]
    fn extent_item_non_skinny_size() {
        let data = extent_item(1, 1, false);
        assert_eq!(
            data.len(),
            mem::size_of::<raw::btrfs_extent_item>()
                + mem::size_of::<raw::btrfs_tree_block_info>()
        );
    }

    #[test]
    fn dev_stats_zeroed_size() {
        let data = dev_stats_zeroed();
        assert_eq!(data.len(), 40);
        assert!(data.iter().all(|&b| b == 0));
    }
}

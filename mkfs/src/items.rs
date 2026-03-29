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
pub fn root_item(
    generation: u64,
    bytenr: u64,
    root_dirid: u64,
    nodesize: u32,
) -> Vec<u8> {
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
    // byte_limit at inode_size + 24: 0
    write_le_u64(&mut buf, inode_size + 32, nodesize as u64); // bytes_used
    // last_snapshot, flags: zero
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
pub fn chunk_tree_root_item(
    generation: u64,
    bytenr: u64,
    nodesize: u32,
) -> Vec<u8> {
    root_item(
        generation,
        bytenr,
        raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID as u64,
        nodesize,
    )
}

/// Serialize an EXTENT_ITEM for a tree block (metadata extent) with an
/// inline TREE_BLOCK_REF.
///
/// For skinny metadata (the default), the layout is:
///   24 bytes btrfs_extent_item + 9 bytes inline ref (type + root_objectid)
/// For non-skinny, it also includes btrfs_tree_block_info before the ref.
///
/// The inline ref eliminates the need for a separate TREE_BLOCK_REF item
/// and is required by `btrfs check`.
pub fn extent_item(
    refs: u64,
    generation: u64,
    skinny: bool,
    owner_root: u64,
) -> Vec<u8> {
    let base_size = mem::size_of::<raw::btrfs_extent_item>();
    let tree_block_info_size = if skinny {
        0
    } else {
        mem::size_of::<raw::btrfs_tree_block_info>()
    };
    // Inline ref: 1 byte type + 8 bytes offset
    let inline_ref_size = 9;
    let size = base_size + tree_block_info_size + inline_ref_size;
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, refs);
    write_le_u64(&mut buf, 8, generation);
    write_le_u64(&mut buf, 16, raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64);

    // Inline TREE_BLOCK_REF
    let ref_off = base_size + tree_block_info_size;
    buf[ref_off] = raw::BTRFS_TREE_BLOCK_REF_KEY as u8;
    write_le_u64(&mut buf, ref_off + 1, owner_root);

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

use crate::layout::StripeInfo;

/// Serialize a CHUNK_ITEM with the given stripes.
///
/// For non-bootstrap chunks, `io_align` and `io_width` should be
/// `STRIPE_LEN` (64K). The bootstrap system chunk uses `sector_size`
/// instead (see `chunk_item_bootstrap`).
pub fn chunk_item(
    length: u64,
    owner: u64,
    chunk_type: u64,
    io_align: u32,
    io_width: u32,
    sector_size: u32,
    stripes: &[StripeInfo],
) -> Vec<u8> {
    let base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_entry_size = mem::size_of::<raw::btrfs_stripe>();
    let size = base_size + stripes.len() * stripe_entry_size;
    let mut buf = vec![0u8; size];

    write_le_u64(&mut buf, 0, length);
    write_le_u64(&mut buf, 8, owner);
    write_le_u64(&mut buf, 16, crate::layout::STRIPE_LEN);
    write_le_u64(&mut buf, 24, chunk_type);
    write_le_u32(&mut buf, 32, io_align);
    write_le_u32(&mut buf, 36, io_width);
    write_le_u32(&mut buf, 40, sector_size);
    write_le_u16(&mut buf, 44, stripes.len() as u16);
    write_le_u16(&mut buf, 46, 0); // sub_stripes

    for (i, stripe) in stripes.iter().enumerate() {
        let off = base_size + i * stripe_entry_size;
        write_le_u64(&mut buf, off, stripe.devid);
        write_le_u64(&mut buf, off + 8, stripe.offset);
        write_uuid(&mut buf, off + 16, &stripe.dev_uuid);
    }

    buf
}

/// Serialize the bootstrap system CHUNK_ITEM (uses sectorsize for io_align/io_width).
pub fn chunk_item_bootstrap(
    length: u64,
    owner: u64,
    chunk_type: u64,
    sector_size: u32,
    stripe: &StripeInfo,
) -> Vec<u8> {
    chunk_item(
        length,
        owner,
        chunk_type,
        sector_size,
        sector_size,
        sector_size,
        &[StripeInfo {
            devid: stripe.devid,
            offset: stripe.offset,
            dev_uuid: stripe.dev_uuid,
        }],
    )
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
        assert_eq!(parsed.flags, items::BlockGroupFlags::SYSTEM);
    }

    #[test]
    fn roundtrip_dev_item() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let fsid =
            Uuid::parse_str("cafebabe-cafe-babe-cafe-babecafebabe").unwrap();
        let data = dev_item(1, 1_000_000_000, 4_000_000, 4096, &uuid, &fsid);
        let parsed = items::DeviceItem::parse(&data).unwrap();
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
        let stripe = StripeInfo {
            devid: 1,
            offset: 0x100000,
            dev_uuid: uuid,
        };
        let data = chunk_item_bootstrap(
            4 * 1024 * 1024,
            raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_SYSTEM as u64,
            4096,
            &stripe,
        );
        let parsed = items::ChunkItem::parse(&data).unwrap();
        assert_eq!(parsed.length, 4 * 1024 * 1024);
        assert_eq!(parsed.owner, raw::BTRFS_EXTENT_TREE_OBJECTID as u64);
        assert_eq!(parsed.chunk_type, items::BlockGroupFlags::SYSTEM);
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
        let parsed = items::DeviceExtent::parse(&data).unwrap();
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
        assert_eq!(parsed.flags, items::FreeSpaceInfoFlags::empty());
    }

    #[test]
    fn roundtrip_root_item() {
        let data = root_item(
            1,
            0x100000,
            raw::BTRFS_FIRST_FREE_OBJECTID as u64,
            16384,
        );
        let parsed = items::RootItem::parse(&data).unwrap();
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.bytenr, 0x100000);
        assert_eq!(parsed.root_dirid, raw::BTRFS_FIRST_FREE_OBJECTID as u64);
        assert_eq!(parsed.refs, 1);
        assert_eq!(parsed.generation_v2, 1);
    }

    #[test]
    fn extent_item_skinny_size() {
        let data = extent_item(1, 1, true, 5);
        // 24 bytes extent_item + 9 bytes inline TREE_BLOCK_REF
        assert_eq!(data.len(), mem::size_of::<raw::btrfs_extent_item>() + 9);
    }

    #[test]
    fn extent_item_non_skinny_size() {
        let data = extent_item(1, 1, false, 5);
        // 24 bytes extent_item + 18 bytes tree_block_info + 9 bytes inline ref
        assert_eq!(
            data.len(),
            mem::size_of::<raw::btrfs_extent_item>()
                + mem::size_of::<raw::btrfs_tree_block_info>()
                + 9
        );
    }

    #[test]
    fn dev_stats_zeroed_size() {
        let data = dev_stats_zeroed();
        assert_eq!(data.len(), 40);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn roundtrip_chunk_item_dup() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let stripes = [
            StripeInfo {
                devid: 1,
                offset: 5 * 1024 * 1024,
                dev_uuid: uuid,
            },
            StripeInfo {
                devid: 1,
                offset: 5 * 1024 * 1024 + 32 * 1024 * 1024,
                dev_uuid: uuid,
            },
        ];
        let data = chunk_item(
            32 * 1024 * 1024,
            raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_METADATA as u64
                | raw::BTRFS_BLOCK_GROUP_DUP as u64,
            crate::layout::STRIPE_LEN as u32,
            crate::layout::STRIPE_LEN as u32,
            4096,
            &stripes,
        );
        let parsed = items::ChunkItem::parse(&data).unwrap();
        assert_eq!(parsed.length, 32 * 1024 * 1024);
        assert_eq!(parsed.num_stripes, 2);
        assert_eq!(parsed.stripes.len(), 2);
        assert_eq!(parsed.stripes[0].devid, 1);
        assert_eq!(parsed.stripes[0].offset, 5 * 1024 * 1024);
        assert_eq!(parsed.stripes[1].devid, 1);
        assert_eq!(
            parsed.stripes[1].offset,
            5 * 1024 * 1024 + 32 * 1024 * 1024
        );
        assert_eq!(parsed.io_align, crate::layout::STRIPE_LEN as u32);
        assert_eq!(parsed.io_width, crate::layout::STRIPE_LEN as u32);
    }

    #[test]
    fn roundtrip_chunk_item_non_bootstrap_single() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let stripes = [StripeInfo {
            devid: 1,
            offset: 69 * 1024 * 1024,
            dev_uuid: uuid,
        }];
        let data = chunk_item(
            64 * 1024 * 1024,
            raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            raw::BTRFS_BLOCK_GROUP_DATA as u64,
            crate::layout::STRIPE_LEN as u32,
            crate::layout::STRIPE_LEN as u32,
            4096,
            &stripes,
        );
        let parsed = items::ChunkItem::parse(&data).unwrap();
        assert_eq!(parsed.length, 64 * 1024 * 1024);
        assert_eq!(parsed.num_stripes, 1);
        assert_eq!(parsed.io_align, crate::layout::STRIPE_LEN as u32);
        assert_eq!(parsed.io_width, crate::layout::STRIPE_LEN as u32);
    }
}

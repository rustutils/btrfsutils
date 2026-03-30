//! # Item serializers: produce on-disk byte payloads for btrfs tree items
//!
//! Each function serializes a specific item type into a `Vec<u8>` suitable
//! for passing to `LeafBuilder::push`. Field positions are derived from the
//! bindgen structs in `btrfs_disk::raw` via `offset_of!` and `size_of`.

use crate::tree::Key;
use btrfs_disk::raw;
use bytes::BufMut;
use std::mem;
use uuid::Uuid;

/// Write a UUID (16 bytes) to a `BufMut`.
fn put_uuid(buf: &mut impl BufMut, uuid: &Uuid) {
    buf.put_slice(uuid.as_bytes());
}

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
    let inode_size = mem::size_of::<raw::btrfs_inode_item>();
    let mut buf = Vec::with_capacity(size);

    // Embedded inode_item: generation, then zeros until nlink at 40
    buf.put_u64_le(generation); // inode.generation
    buf.put_bytes(0, 32); // transid..block_group (offsets 8..40)
    buf.put_u32_le(1); // inode.nlink
    buf.put_bytes(0, 8); // uid, gid (offsets 44..52)
    buf.put_u32_le(0o40755); // inode.mode
    buf.put_bytes(0, inode_size - 56); // rdev..otime

    // Root-specific fields (after the embedded inode)
    buf.put_u64_le(generation); // generation
    buf.put_u64_le(root_dirid); // root_dirid
    buf.put_u64_le(bytenr); // bytenr
    buf.put_u64_le(0); // byte_limit
    buf.put_u64_le(nodesize as u64); // bytes_used
    buf.put_u64_le(0); // last_snapshot
    buf.put_u64_le(0); // flags
    buf.put_u32_le(1); // refs = 1

    // drop_progress key (17 bytes) + drop_level (1 byte) + level (1 byte)
    let level_off = mem::offset_of!(raw::btrfs_root_item, level);
    let pad_to_level = level_off - buf.len();
    buf.put_bytes(0, pad_to_level);
    buf.put_u8(0); // level

    buf.put_u64_le(generation); // generation_v2

    // Pad rest with zeros (uuid, parent_uuid, received_uuid, ctransid, etc.)
    buf.resize(size, 0);
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
    let tree_block_info_size = if skinny {
        0
    } else {
        mem::size_of::<raw::btrfs_tree_block_info>()
    };
    let mut buf = Vec::new();

    buf.put_u64_le(refs);
    buf.put_u64_le(generation);
    buf.put_u64_le(raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64);

    // Zero-fill tree_block_info (non-skinny only)
    buf.put_bytes(0, tree_block_info_size);

    // Inline TREE_BLOCK_REF
    buf.put_u8(raw::BTRFS_TREE_BLOCK_REF_KEY as u8);
    buf.put_u64_le(owner_root);

    buf
}

/// Serialize a BLOCK_GROUP_ITEM.
pub fn block_group_item(used: u64, chunk_objectid: u64, flags: u64) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(mem::size_of::<raw::btrfs_block_group_item>());
    buf.put_u64_le(used);
    buf.put_u64_le(chunk_objectid);
    buf.put_u64_le(flags);
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
    let mut buf = Vec::with_capacity(size);

    buf.put_u64_le(devid);
    buf.put_u64_le(total_bytes);
    buf.put_u64_le(bytes_used);
    buf.put_u32_le(sector_size); // io_align
    buf.put_u32_le(sector_size); // io_width
    buf.put_u32_le(sector_size); // sector_size
    buf.put_bytes(0, 30); // dev_type(8)+generation(8)+start_offset(8)+dev_group(4)+seek_speed(1)+bandwidth(1)
    put_uuid(&mut buf, dev_uuid);
    put_uuid(&mut buf, fsid);

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
    let mut buf = Vec::new();

    buf.put_u64_le(length);
    buf.put_u64_le(owner);
    buf.put_u64_le(crate::layout::STRIPE_LEN);
    buf.put_u64_le(chunk_type);
    buf.put_u32_le(io_align);
    buf.put_u32_le(io_width);
    buf.put_u32_le(sector_size);
    buf.put_u16_le(stripes.len() as u16);
    buf.put_u16_le(0); // sub_stripes

    for stripe in stripes {
        buf.put_u64_le(stripe.devid);
        buf.put_u64_le(stripe.offset);
        put_uuid(&mut buf, &stripe.dev_uuid);
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
    let mut buf = Vec::with_capacity(mem::size_of::<raw::btrfs_dev_extent>());
    buf.put_u64_le(chunk_tree);
    buf.put_u64_le(chunk_objectid);
    buf.put_u64_le(chunk_offset);
    buf.put_u64_le(length);
    put_uuid(&mut buf, chunk_tree_uuid);
    buf
}

/// Serialize a DEV_STATS_ITEM (all counters zero).
pub fn dev_stats_zeroed() -> Vec<u8> {
    // 5 u64 counters: write_errs, read_errs, flush_errs, corruption_errs, generation
    vec![0u8; 5 * 8]
}

/// Serialize a FREE_SPACE_INFO.
pub fn free_space_info(extent_count: u32, flags: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.put_u32_le(extent_count);
    buf.put_u32_le(flags);
    buf
}

/// Serialize an INODE_ITEM for a root directory.
///
/// Creates a directory inode (mode 040755) with nlink=1 and the given
/// generation and timestamps.
pub fn inode_item_dir(generation: u64, nbytes: u64, now_sec: u64) -> Vec<u8> {
    let size = mem::size_of::<raw::btrfs_inode_item>();
    let mut buf = Vec::with_capacity(size);

    buf.put_u64_le(generation); // generation
    buf.put_u64_le(0); // transid
    buf.put_u64_le(0); // size
    buf.put_u64_le(nbytes); // nbytes
    buf.put_u64_le(0); // block_group
    buf.put_u32_le(1); // nlink
    buf.put_u32_le(0); // uid
    buf.put_u32_le(0); // gid
    buf.put_u32_le(0o40755); // mode = S_IFDIR | 0755
    buf.put_u64_le(0); // rdev
    buf.put_u64_le(0); // flags
    buf.put_u64_le(0); // sequence
    buf.put_bytes(0, 32); // reserved[4]

    // Timestamps: atime, ctime, mtime, otime
    for _ in 0..4 {
        buf.put_u64_le(now_sec);
        buf.put_u32_le(0); // nsec
    }

    buf
}

/// Serialize an INODE_REF item.
///
/// Contains the directory entry index and the name of the entry
/// pointing to this inode.
pub fn inode_ref(index: u64, name: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 2 + name.len());
    buf.put_u64_le(index);
    buf.put_u16_le(name.len() as u16);
    buf.put_slice(name);
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

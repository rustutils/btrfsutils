//! # Tree block builder: construct btrfs leaf nodes from items
//!
//! Provides `LeafBuilder`, a safe abstraction for constructing btrfs tree
//! leaf blocks. Items must be pushed in sorted key order. The builder handles
//! all offset bookkeeping: item descriptors grow forward from the header,
//! item data grows backward from the end of the block.

use btrfs_disk::{
    raw,
    util::{write_le_u32, write_le_u64, write_uuid},
};
use std::mem;
use uuid::Uuid;

/// Size of the on-disk tree block header (101 bytes).
const HEADER_SIZE: usize = mem::size_of::<raw::btrfs_header>();

/// Size of a leaf item descriptor on disk (25 bytes).
const ITEM_SIZE: usize = mem::size_of::<raw::btrfs_item>();

/// A key for an on-disk item: (objectid, type, offset).
///
/// Items must be inserted in ascending key order. The ordering is
/// lexicographic: objectid first, then type, then offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub objectid: u64,
    pub key_type: u8,
    pub offset: u64,
}

impl Key {
    pub fn new(objectid: u64, key_type: u8, offset: u64) -> Self {
        Self {
            objectid,
            key_type,
            offset,
        }
    }

    /// Serialize this key into 17 bytes at `buf[off..]`.
    pub fn write_to(&self, buf: &mut [u8], off: usize) {
        write_le_u64(buf, off, self.objectid);
        buf[off + 8] = self.key_type;
        write_le_u64(buf, off + 9, self.offset);
    }
}

/// Builds a btrfs leaf block (level 0 tree node) from individual items.
///
/// Usage:
/// ```ignore
/// let mut leaf = LeafBuilder::new(builder_args);
/// leaf.push(key1, &data1)?;
/// leaf.push(key2, &data2)?;
/// let block = leaf.finish();
/// ```
///
/// Items must be pushed in ascending key order. The builder enforces this
/// and returns an error if keys are out of order.
pub struct LeafBuilder {
    buf: Vec<u8>,
    /// Number of items inserted so far.
    nritems: u32,
    /// Byte offset of the next item descriptor (grows forward from HEADER_SIZE).
    item_offset: usize,
    /// Byte offset of the next item's data end (grows backward from nodesize).
    data_end: usize,
    /// The last key inserted, for sort-order enforcement.
    last_key: Option<Key>,
}

/// Parameters for constructing a leaf block header.
pub struct LeafHeader {
    pub fsid: Uuid,
    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
    /// Tree that owns this block (e.g. BTRFS_ROOT_TREE_OBJECTID).
    pub owner: u64,
    /// Logical byte address of this block on disk.
    pub bytenr: u64,
}

impl LeafBuilder {
    /// Create a new leaf builder for a block of `nodesize` bytes.
    pub fn new(nodesize: u32, header: &LeafHeader) -> Self {
        let mut buf = vec![0u8; nodesize as usize];

        // Write header fields (csum and nritems are finalized in finish()).
        let flags = (raw::BTRFS_MIXED_BACKREF_REV as u64)
            << raw::BTRFS_BACKREF_REV_SHIFT
            | raw::BTRFS_HEADER_FLAG_WRITTEN as u64;

        write_uuid(&mut buf, 32, &header.fsid);
        write_le_u64(&mut buf, 48, header.bytenr);
        write_le_u64(&mut buf, 56, flags);
        write_uuid(&mut buf, 64, &header.chunk_tree_uuid);
        write_le_u64(&mut buf, 80, header.generation);
        write_le_u64(&mut buf, 88, header.owner);
        // nritems at offset 96: written in finish()
        // level at offset 100: 0 for leaf (already zero)

        Self {
            buf,
            nritems: 0,
            item_offset: HEADER_SIZE,
            data_end: nodesize as usize,
            last_key: None,
        }
    }

    /// Available space for more items (item descriptors + data payloads).
    pub fn space_left(&self) -> usize {
        self.data_end.saturating_sub(self.item_offset + ITEM_SIZE)
    }

    /// Push an item with the given key and data payload.
    ///
    /// Returns an error if the key is not greater than the previous key,
    /// or if there is not enough space in the leaf.
    pub fn push(&mut self, key: Key, data: &[u8]) -> Result<(), LeafError> {
        if let Some(last) = self.last_key
            && key <= last
        {
            return Err(LeafError::KeyOrder { last, got: key });
        }

        let needed = ITEM_SIZE + data.len();
        if self.item_offset + needed > self.data_end {
            return Err(LeafError::Full {
                needed,
                available: self.space_left(),
            });
        }

        // Write item data (grows backward from end of block).
        self.data_end -= data.len();
        self.buf[self.data_end..self.data_end + data.len()]
            .copy_from_slice(data);

        // Write item descriptor: key (17 bytes) + offset (4) + size (4).
        // The offset field is relative to the end of the header.
        let data_offset = (self.data_end - HEADER_SIZE) as u32;
        key.write_to(&mut self.buf, self.item_offset);
        write_le_u32(&mut self.buf, self.item_offset + 17, data_offset);
        write_le_u32(&mut self.buf, self.item_offset + 21, data.len() as u32);

        self.item_offset += ITEM_SIZE;
        self.nritems += 1;
        self.last_key = Some(key);
        Ok(())
    }

    /// Push an item with an empty data payload (e.g. TREE_BLOCK_REF_KEY).
    pub fn push_empty(&mut self, key: Key) -> Result<(), LeafError> {
        self.push(key, &[])
    }

    /// Finalize the leaf block: write nritems to the header and return the
    /// raw block bytes.
    ///
    /// The checksum field (bytes 0..32) is left zeroed — the caller must
    /// compute and fill it before writing to disk.
    pub fn finish(mut self) -> Vec<u8> {
        write_le_u32(&mut self.buf, 96, self.nritems);
        self.buf
    }

    /// Number of items inserted so far.
    pub fn len(&self) -> u32 {
        self.nritems
    }

    /// Whether no items have been inserted.
    pub fn is_empty(&self) -> bool {
        self.nritems == 0
    }
}

/// Errors that can occur while building a leaf.
#[derive(Debug)]
pub enum LeafError {
    /// Keys must be pushed in strictly ascending order.
    KeyOrder { last: Key, got: Key },
    /// Not enough space in the leaf for this item.
    Full { needed: usize, available: usize },
}

impl std::fmt::Display for LeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeafError::KeyOrder { last, got } => {
                write!(
                    f,
                    "key out of order: ({}, {}, {}) >= ({}, {}, {})",
                    last.objectid,
                    last.key_type,
                    last.offset,
                    got.objectid,
                    got.key_type,
                    got.offset,
                )
            }
            LeafError::Full { needed, available } => {
                write!(
                    f,
                    "leaf full: need {needed} bytes, only {available} available"
                )
            }
        }
    }
}

impl std::error::Error for LeafError {}

/// Size of a key-pointer entry in an internal node (17 + 8 + 8 = 33 bytes).
const KEY_PTR_SIZE: usize = 17 + mem::size_of::<u64>() + mem::size_of::<u64>();

/// Parameters for constructing an internal node header.
pub struct NodeHeader {
    pub fsid: Uuid,
    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
    pub owner: u64,
    pub bytenr: u64,
    pub level: u8,
}

/// Builds a btrfs internal node (level > 0) from key-pointer pairs.
///
/// Each entry maps a key to a child block pointer and generation.
/// Entries must be pushed in ascending key order.
pub struct NodeBuilder {
    buf: Vec<u8>,
    nritems: u32,
    ptr_offset: usize,
    last_key: Option<Key>,
}

impl NodeBuilder {
    /// Create a new node builder for a block of `nodesize` bytes at the given level.
    pub fn new(nodesize: u32, header: &NodeHeader) -> Self {
        let mut buf = vec![0u8; nodesize as usize];

        let flags = (raw::BTRFS_MIXED_BACKREF_REV as u64)
            << raw::BTRFS_BACKREF_REV_SHIFT
            | raw::BTRFS_HEADER_FLAG_WRITTEN as u64;

        write_uuid(&mut buf, 32, &header.fsid);
        write_le_u64(&mut buf, 48, header.bytenr);
        write_le_u64(&mut buf, 56, flags);
        write_uuid(&mut buf, 64, &header.chunk_tree_uuid);
        write_le_u64(&mut buf, 80, header.generation);
        write_le_u64(&mut buf, 88, header.owner);
        // nritems at offset 96: written in finish()
        buf[100] = header.level;

        Self {
            buf,
            nritems: 0,
            ptr_offset: HEADER_SIZE,
            last_key: None,
        }
    }

    /// Available space for more key-pointer entries.
    pub fn space_left(&self) -> usize {
        (self.buf.len() - self.ptr_offset) / KEY_PTR_SIZE
    }

    /// Push a key-pointer entry pointing to a child block.
    pub fn push(
        &mut self,
        key: Key,
        blockptr: u64,
        generation: u64,
    ) -> Result<(), LeafError> {
        if let Some(last) = self.last_key
            && key <= last
        {
            return Err(LeafError::KeyOrder { last, got: key });
        }

        if self.ptr_offset + KEY_PTR_SIZE > self.buf.len() {
            return Err(LeafError::Full {
                needed: KEY_PTR_SIZE,
                available: self.buf.len() - self.ptr_offset,
            });
        }

        key.write_to(&mut self.buf, self.ptr_offset);
        write_le_u64(&mut self.buf, self.ptr_offset + 17, blockptr);
        write_le_u64(&mut self.buf, self.ptr_offset + 25, generation);

        self.ptr_offset += KEY_PTR_SIZE;
        self.nritems += 1;
        self.last_key = Some(key);
        Ok(())
    }

    /// Finalize the node: write nritems and return the raw block bytes.
    pub fn finish(mut self) -> Vec<u8> {
        write_le_u32(&mut self.buf, 96, self.nritems);
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::tree::{Header, TreeBlock};

    fn test_header() -> LeafHeader {
        LeafHeader {
            fsid: Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef")
                .unwrap(),
            chunk_tree_uuid: Uuid::parse_str(
                "cafebabe-cafe-babe-cafe-babecafebabe",
            )
            .unwrap(),
            generation: 1,
            owner: raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            bytenr: 0x100000,
        }
    }

    #[test]
    fn empty_leaf() {
        let leaf = LeafBuilder::new(4096, &test_header());
        assert_eq!(leaf.len(), 0);
        assert!(leaf.is_empty());
        let buf = leaf.finish();
        assert_eq!(buf.len(), 4096);

        let block = TreeBlock::parse(&buf);
        match block {
            TreeBlock::Leaf { header, items, .. } => {
                assert_eq!(header.nritems, 0);
                assert_eq!(header.level, 0);
                assert_eq!(header.generation, 1);
                assert_eq!(header.owner, raw::BTRFS_ROOT_TREE_OBJECTID as u64);
                assert_eq!(header.bytenr, 0x100000);
                assert!(items.is_empty());
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn single_item() {
        let mut leaf = LeafBuilder::new(4096, &test_header());
        let data = [0x42u8; 24];
        let key = Key::new(1, raw::BTRFS_ROOT_ITEM_KEY as u8, 5);
        leaf.push(key, &data).unwrap();
        let buf = leaf.finish();

        let block = TreeBlock::parse(&buf);
        match block {
            TreeBlock::Leaf {
                header,
                items,
                data: raw_data,
            } => {
                assert_eq!(header.nritems, 1);
                assert_eq!(items.len(), 1);
                let item = &items[0];
                assert_eq!(item.key.objectid, 1);
                assert_eq!(
                    item.key.key_type,
                    btrfs_disk::tree::KeyType::RootItem
                );
                assert_eq!(item.key.offset, 5);
                assert_eq!(item.size, 24);
                // Verify the actual data content
                let start = HEADER_SIZE + item.offset as usize;
                assert_eq!(&raw_data[start..start + 24], &[0x42u8; 24]);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn multiple_items_sorted() {
        let mut leaf = LeafBuilder::new(4096, &test_header());
        leaf.push(Key::new(1, 132, 2), &[0xAA; 8]).unwrap();
        leaf.push(Key::new(1, 132, 5), &[0xBB; 16]).unwrap();
        leaf.push(Key::new(2, 132, 1), &[0xCC; 4]).unwrap();
        let buf = leaf.finish();

        let block = TreeBlock::parse(&buf);
        match block {
            TreeBlock::Leaf { header, items, .. } => {
                assert_eq!(header.nritems, 3);
                assert_eq!(items[0].key.objectid, 1);
                assert_eq!(items[0].key.offset, 2);
                assert_eq!(items[0].size, 8);
                assert_eq!(items[1].key.objectid, 1);
                assert_eq!(items[1].key.offset, 5);
                assert_eq!(items[1].size, 16);
                assert_eq!(items[2].key.objectid, 2);
                assert_eq!(items[2].key.offset, 1);
                assert_eq!(items[2].size, 4);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn key_order_enforced() {
        let mut leaf = LeafBuilder::new(4096, &test_header());
        leaf.push(Key::new(2, 132, 5), &[]).unwrap();
        let err = leaf.push(Key::new(1, 132, 10), &[]).unwrap_err();
        assert!(matches!(err, LeafError::KeyOrder { .. }));
    }

    #[test]
    fn duplicate_key_rejected() {
        let mut leaf = LeafBuilder::new(4096, &test_header());
        leaf.push(Key::new(1, 132, 5), &[]).unwrap();
        let err = leaf.push(Key::new(1, 132, 5), &[]).unwrap_err();
        assert!(matches!(err, LeafError::KeyOrder { .. }));
    }

    #[test]
    fn leaf_full_error() {
        // With nodesize 256 and header 101, we have 155 bytes for items+data.
        // Each item descriptor is 25 bytes. So we can fit at most a few items.
        let mut leaf = LeafBuilder::new(256, &test_header());
        // Fill it up: 155 bytes / 25 bytes per empty item = 6 items max
        for i in 0..6 {
            leaf.push(Key::new(i, 132, 0), &[]).unwrap();
        }
        let err = leaf.push(Key::new(100, 132, 0), &[]).unwrap_err();
        assert!(matches!(err, LeafError::Full { .. }));
    }

    #[test]
    fn empty_data_item() {
        let mut leaf = LeafBuilder::new(4096, &test_header());
        leaf.push_empty(Key::new(1, raw::BTRFS_TREE_BLOCK_REF_KEY as u8, 2))
            .unwrap();
        let buf = leaf.finish();

        let block = TreeBlock::parse(&buf);
        match block {
            TreeBlock::Leaf { items, .. } => {
                assert_eq!(items[0].size, 0);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn header_fields_correct() {
        let hdr = test_header();
        let leaf = LeafBuilder::new(16384, &hdr);
        let buf = leaf.finish();
        let parsed = Header::parse(&buf);

        assert_eq!(parsed.fsid, hdr.fsid);
        assert_eq!(parsed.chunk_tree_uuid, hdr.chunk_tree_uuid);
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.owner, raw::BTRFS_ROOT_TREE_OBJECTID as u64);
        assert_eq!(parsed.bytenr, 0x100000);
        assert_eq!(parsed.level, 0);
        assert_eq!(parsed.backref_rev(), raw::BTRFS_MIXED_BACKREF_REV as u64);
    }

    fn test_node_header(level: u8) -> NodeHeader {
        NodeHeader {
            fsid: Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef")
                .unwrap(),
            chunk_tree_uuid: Uuid::parse_str(
                "cafebabe-cafe-babe-cafe-babecafebabe",
            )
            .unwrap(),
            generation: 1,
            owner: raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            bytenr: 0x200000,
            level,
        }
    }

    #[test]
    fn node_builder_basic() {
        let mut node = NodeBuilder::new(4096, &test_node_header(1));
        node.push(Key::new(1, 132, 0), 0x100000, 1).unwrap();
        node.push(Key::new(100, 132, 0), 0x104000, 1).unwrap();
        let buf = node.finish();

        let block = TreeBlock::parse(&buf);
        match block {
            TreeBlock::Node { header, ptrs, .. } => {
                assert_eq!(header.level, 1);
                assert_eq!(header.nritems, 2);
                assert_eq!(header.bytenr, 0x200000);
                assert_eq!(ptrs.len(), 2);
                assert_eq!(ptrs[0].key.objectid, 1);
                assert_eq!(ptrs[0].blockptr, 0x100000);
                assert_eq!(ptrs[1].key.objectid, 100);
                assert_eq!(ptrs[1].blockptr, 0x104000);
            }
            TreeBlock::Leaf { .. } => panic!("expected node"),
        }
    }

    #[test]
    fn node_builder_key_order_enforced() {
        let mut node = NodeBuilder::new(4096, &test_node_header(1));
        node.push(Key::new(10, 132, 0), 0x100000, 1).unwrap();
        let err = node.push(Key::new(5, 132, 0), 0x104000, 1).unwrap_err();
        assert!(matches!(err, LeafError::KeyOrder { .. }));
    }
}

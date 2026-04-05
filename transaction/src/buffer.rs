//! # Mutable tree block wrapper
//!
//! An `ExtentBuffer` wraps a `nodesize`-length byte buffer representing a
//! single btrfs tree block (node or leaf). It provides typed accessors for
//! reading and writing header fields, item descriptors, key pointers, and raw
//! item data regions.
//!
//! Unlike `btrfs_disk::tree::TreeBlock` which is a parsed, immutable snapshot,
//! `ExtentBuffer` keeps the raw bytes and mutates them in place, which is what
//! the write path needs for COW and item manipulation.

use btrfs_disk::{
    tree::{DiskKey, KeyType, TreeBlock},
    util::{csum_tree_block, write_disk_key},
};
use bytes::{Buf, BufMut};
use uuid::Uuid;

/// Size of the on-disk tree block header (101 bytes).
pub const HEADER_SIZE: usize = 101;

/// Size of an item descriptor in a leaf (25 bytes): key (17) + offset (4) + size (4).
pub const ITEM_SIZE: usize = 25;

/// Size of a key pointer in an internal node (33 bytes): key (17) + blockptr (8) + generation (8).
pub const KEY_PTR_SIZE: usize = 33;

/// Size of an on-disk key (17 bytes): objectid (8) + type (1) + offset (8).
pub const DISK_KEY_SIZE: usize = 17;

/// Maximum B-tree depth (8 levels, 0-indexed: levels 0 through 7).
pub const BTRFS_MAX_LEVEL: usize = 8;

/// A mutable tree block backed by a byte buffer.
///
/// Provides field-level read/write access to the 101-byte header, item
/// descriptors (for leaves), and key pointers (for nodes). The buffer is
/// always exactly `nodesize` bytes.
#[derive(Clone)]
pub struct ExtentBuffer {
    data: Vec<u8>,
    /// Logical byte address of this block.
    logical: u64,
}

impl ExtentBuffer {
    /// Create an `ExtentBuffer` from raw bytes at the given logical address.
    ///
    /// # Panics
    ///
    /// Panics if `data` is empty.
    #[must_use]
    pub fn from_raw(data: Vec<u8>, logical: u64) -> Self {
        assert!(!data.is_empty(), "ExtentBuffer: empty data");
        Self { data, logical }
    }

    /// Create a zeroed `ExtentBuffer` of `nodesize` bytes at the given logical address.
    #[must_use]
    pub fn new_zeroed(nodesize: u32, logical: u64) -> Self {
        Self {
            data: vec![0u8; nodesize as usize],
            logical,
        }
    }

    /// Return the logical byte address of this block.
    #[must_use]
    pub fn logical(&self) -> u64 {
        self.logical
    }

    /// Set the logical byte address of this block.
    pub fn set_logical(&mut self, logical: u64) {
        self.logical = logical;
    }

    /// Return the nodesize (length of the buffer).
    #[must_use]
    pub fn nodesize(&self) -> u32 {
        self.data.len() as u32
    }

    /// Return a reference to the raw byte buffer.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Return a mutable reference to the raw byte buffer.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Parse this buffer into a `TreeBlock` for read-only inspection.
    #[must_use]
    pub fn as_tree_block(&self) -> TreeBlock {
        TreeBlock::parse(&self.data)
    }

    // --- Header field readers ---

    /// Read the generation field from the header.
    #[must_use]
    pub fn generation(&self) -> u64 {
        let mut b = &self.data[80..88];
        b.get_u64_le()
    }

    /// Read the owner (tree ID) field from the header.
    #[must_use]
    pub fn owner(&self) -> u64 {
        let mut b = &self.data[88..96];
        b.get_u64_le()
    }

    /// Read the nritems field from the header.
    #[must_use]
    pub fn nritems(&self) -> u32 {
        let mut b = &self.data[96..100];
        b.get_u32_le()
    }

    /// Read the level field from the header.
    #[must_use]
    pub fn level(&self) -> u8 {
        self.data[100]
    }

    /// Read the bytenr field from the header.
    #[must_use]
    pub fn bytenr(&self) -> u64 {
        let mut b = &self.data[48..56];
        b.get_u64_le()
    }

    /// Read the flags field from the header.
    #[must_use]
    pub fn flags(&self) -> u64 {
        let mut b = &self.data[56..64];
        b.get_u64_le()
    }

    /// Read the fsid from the header.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is shorter than 48 bytes.
    #[must_use]
    pub fn fsid(&self) -> Uuid {
        Uuid::from_bytes(self.data[32..48].try_into().unwrap())
    }

    /// Read the `chunk_tree_uuid` from the header.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is shorter than 80 bytes.
    #[must_use]
    pub fn chunk_tree_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.data[64..80].try_into().unwrap())
    }

    // --- Header field writers ---

    /// Write the generation field.
    pub fn set_generation(&mut self, generation: u64) {
        (&mut self.data[80..88]).put_u64_le(generation);
    }

    /// Write the owner (tree ID) field.
    pub fn set_owner(&mut self, owner: u64) {
        (&mut self.data[88..96]).put_u64_le(owner);
    }

    /// Write the nritems field.
    pub fn set_nritems(&mut self, nritems: u32) {
        (&mut self.data[96..100]).put_u32_le(nritems);
    }

    /// Write the level field.
    pub fn set_level(&mut self, level: u8) {
        self.data[100] = level;
    }

    /// Write the bytenr field.
    pub fn set_bytenr(&mut self, bytenr: u64) {
        (&mut self.data[48..56]).put_u64_le(bytenr);
    }

    /// Write the flags field.
    pub fn set_flags(&mut self, flags: u64) {
        (&mut self.data[56..64]).put_u64_le(flags);
    }

    /// Write the fsid.
    pub fn set_fsid(&mut self, fsid: &Uuid) {
        self.data[32..48].copy_from_slice(fsid.as_bytes());
    }

    /// Write the `chunk_tree_uuid`.
    pub fn set_chunk_tree_uuid(&mut self, uuid: &Uuid) {
        self.data[64..80].copy_from_slice(uuid.as_bytes());
    }

    // --- Leaf item accessors ---

    /// Read the key of the item at the given slot index in a leaf.
    ///
    /// # Panics
    ///
    /// Panics if `slot` is out of bounds.
    #[must_use]
    pub fn item_key(&self, slot: usize) -> DiskKey {
        let off = HEADER_SIZE + slot * ITEM_SIZE;
        DiskKey::parse(&self.data, off)
    }

    /// Read the data offset field of the item at the given slot.
    /// This offset is relative to byte 101 (immediately after the header).
    #[must_use]
    pub fn item_offset(&self, slot: usize) -> u32 {
        let off = HEADER_SIZE + slot * ITEM_SIZE + DISK_KEY_SIZE;
        let mut b = &self.data[off..off + 4];
        b.get_u32_le()
    }

    /// Read the data size field of the item at the given slot.
    #[must_use]
    pub fn item_size(&self, slot: usize) -> u32 {
        let off = HEADER_SIZE + slot * ITEM_SIZE + DISK_KEY_SIZE + 4;
        let mut b = &self.data[off..off + 4];
        b.get_u32_le()
    }

    /// Return the absolute byte offset within the block where item data starts.
    /// The item's `offset` field is relative to byte 101 (`HEADER_SIZE`).
    #[must_use]
    pub fn item_data_offset(&self, slot: usize) -> usize {
        HEADER_SIZE + self.item_offset(slot) as usize
    }

    /// Return a slice of the item's data payload.
    #[must_use]
    pub fn item_data(&self, slot: usize) -> &[u8] {
        let start = self.item_data_offset(slot);
        let size = self.item_size(slot) as usize;
        &self.data[start..start + size]
    }

    /// Return a mutable slice of the item's data payload.
    pub fn item_data_mut(&mut self, slot: usize) -> &mut [u8] {
        let start = self.item_data_offset(slot);
        let size = self.item_size(slot) as usize;
        &mut self.data[start..start + size]
    }

    /// Write the key for an item at the given slot.
    pub fn set_item_key(&mut self, slot: usize, key: &DiskKey) {
        let off = HEADER_SIZE + slot * ITEM_SIZE;
        write_disk_key(&mut self.data, off, key);
    }

    /// Write the data offset for an item at the given slot.
    pub fn set_item_offset(&mut self, slot: usize, offset: u32) {
        let off = HEADER_SIZE + slot * ITEM_SIZE + DISK_KEY_SIZE;
        (&mut self.data[off..off + 4]).put_u32_le(offset);
    }

    /// Write the data size for an item at the given slot.
    pub fn set_item_size(&mut self, slot: usize, size: u32) {
        let off = HEADER_SIZE + slot * ITEM_SIZE + DISK_KEY_SIZE + 4;
        (&mut self.data[off..off + 4]).put_u32_le(size);
    }

    // --- Node key pointer accessors ---

    /// Read the key of the key pointer at the given slot in a node.
    #[must_use]
    pub fn key_ptr_key(&self, slot: usize) -> DiskKey {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE;
        DiskKey::parse(&self.data, off)
    }

    /// Read the blockptr of the key pointer at the given slot.
    #[must_use]
    pub fn key_ptr_blockptr(&self, slot: usize) -> u64 {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE + DISK_KEY_SIZE;
        let mut b = &self.data[off..off + 8];
        b.get_u64_le()
    }

    /// Read the generation of the key pointer at the given slot.
    #[must_use]
    pub fn key_ptr_generation(&self, slot: usize) -> u64 {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE + DISK_KEY_SIZE + 8;
        let mut b = &self.data[off..off + 8];
        b.get_u64_le()
    }

    /// Write the key for a key pointer at the given slot.
    pub fn set_key_ptr_key(&mut self, slot: usize, key: &DiskKey) {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE;
        write_disk_key(&mut self.data, off, key);
    }

    /// Write the blockptr for a key pointer at the given slot.
    pub fn set_key_ptr_blockptr(&mut self, slot: usize, blockptr: u64) {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE + DISK_KEY_SIZE;
        (&mut self.data[off..off + 8]).put_u64_le(blockptr);
    }

    /// Write the generation for a key pointer at the given slot.
    pub fn set_key_ptr_generation(&mut self, slot: usize, generation: u64) {
        let off = HEADER_SIZE + slot * KEY_PTR_SIZE + DISK_KEY_SIZE + 8;
        (&mut self.data[off..off + 8]).put_u64_le(generation);
    }

    /// Write a complete key pointer at the given slot.
    pub fn set_key_ptr(
        &mut self,
        slot: usize,
        key: &DiskKey,
        blockptr: u64,
        generation: u64,
    ) {
        self.set_key_ptr_key(slot, key);
        self.set_key_ptr_blockptr(slot, blockptr);
        self.set_key_ptr_generation(slot, generation);
    }

    // --- Leaf space management ---

    /// Compute the free space in a leaf block.
    ///
    /// Free space is the gap between the end of the item descriptor array and
    /// the start of the first item's data (which grows backward from the end
    /// of the block).
    #[must_use]
    pub fn leaf_free_space(&self) -> u32 {
        let nritems = self.nritems() as usize;
        if nritems == 0 {
            // All space after the header is free
            return self.nodesize() - HEADER_SIZE as u32;
        }
        let items_end = (HEADER_SIZE + nritems * ITEM_SIZE) as u32;
        let data_start = self.leaf_data_end();
        data_start.saturating_sub(items_end)
    }

    /// Return the absolute byte offset of the first (lowest-offset) data byte
    /// in the leaf. This is `HEADER_SIZE + item[nritems-1].offset` (since the
    /// last item has the lowest offset, as data grows backward).
    ///
    /// For an empty leaf, returns `nodesize`.
    #[must_use]
    pub fn leaf_data_end(&self) -> u32 {
        let nritems = self.nritems();
        if nritems == 0 {
            return self.nodesize();
        }
        // The last item has the smallest offset (data grows backward)
        HEADER_SIZE as u32 + self.item_offset(nritems as usize - 1)
    }

    // --- Checksum ---

    /// Recompute the CRC32C checksum and write it into the header.
    pub fn update_checksum(&mut self) {
        csum_tree_block(&mut self.data);
    }

    // --- Bulk data operations ---

    /// Copy a range of bytes within this buffer.
    ///
    /// Equivalent to `memmove`: handles overlapping regions correctly.
    pub fn copy_within(&mut self, src: core::ops::Range<usize>, dest: usize) {
        self.data.copy_within(src, dest);
    }

    /// Fill a range with zeros.
    pub fn zero_range(&mut self, offset: usize, len: usize) {
        self.data[offset..offset + len].fill(0);
    }

    /// Return true if this is a leaf (level == 0).
    #[must_use]
    pub fn is_leaf(&self) -> bool {
        self.level() == 0
    }

    /// Return true if this is an internal node (level > 0).
    #[must_use]
    pub fn is_node(&self) -> bool {
        self.level() > 0
    }

    /// Maximum number of key pointers that can fit in this node.
    #[must_use]
    pub fn max_key_ptrs(&self) -> u32 {
        (self.nodesize() - HEADER_SIZE as u32) / KEY_PTR_SIZE as u32
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for ExtentBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtentBuffer")
            .field("logical", &self.logical)
            .field("level", &self.level())
            .field("nritems", &self.nritems())
            .field("generation", &self.generation())
            .field("owner", &self.owner())
            .finish()
    }
}

/// Compare two `DiskKey` values as a `(objectid, type, offset)` tuple.
///
/// Returns `Ordering::Less`, `Equal`, or `Greater`.
#[must_use]
pub fn key_cmp(a: &DiskKey, b: &DiskKey) -> std::cmp::Ordering {
    a.objectid
        .cmp(&b.objectid)
        .then_with(|| a.key_type.to_raw().cmp(&b.key_type.to_raw()))
        .then_with(|| a.offset.cmp(&b.offset))
}

/// The minimum possible key.
#[must_use]
pub fn min_key() -> DiskKey {
    DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    }
}

/// The maximum possible key.
#[must_use]
pub fn max_key() -> DiskKey {
    DiskKey {
        objectid: u64::MAX,
        key_type: KeyType::from_raw(u8::MAX),
        offset: u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_leaf(
        nodesize: u32,
        nritems: u32,
        generation: u64,
        owner: u64,
    ) -> ExtentBuffer {
        let mut eb = ExtentBuffer::new_zeroed(nodesize, 65536);
        eb.set_generation(generation);
        eb.set_owner(owner);
        eb.set_nritems(nritems);
        eb.set_level(0);
        eb.set_bytenr(65536);
        eb
    }

    fn make_node(
        nodesize: u32,
        nritems: u32,
        level: u8,
        generation: u64,
    ) -> ExtentBuffer {
        let mut eb = ExtentBuffer::new_zeroed(nodesize, 131072);
        eb.set_generation(generation);
        eb.set_owner(2);
        eb.set_nritems(nritems);
        eb.set_level(level);
        eb.set_bytenr(131072);
        eb
    }

    #[test]
    fn header_round_trip() {
        let mut eb = ExtentBuffer::new_zeroed(16384, 65536);
        eb.set_generation(42);
        eb.set_owner(5);
        eb.set_nritems(10);
        eb.set_level(0);
        eb.set_bytenr(65536);
        eb.set_flags(1);

        assert_eq!(eb.generation(), 42);
        assert_eq!(eb.owner(), 5);
        assert_eq!(eb.nritems(), 10);
        assert_eq!(eb.level(), 0);
        assert_eq!(eb.bytenr(), 65536);
        assert_eq!(eb.flags(), 1);
        assert_eq!(eb.logical(), 65536);
        assert_eq!(eb.nodesize(), 16384);
        assert!(eb.is_leaf());
        assert!(!eb.is_node());
    }

    #[test]
    fn uuid_round_trip() {
        let mut eb = ExtentBuffer::new_zeroed(16384, 0);
        let fsid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let ctu =
            Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        eb.set_fsid(&fsid);
        eb.set_chunk_tree_uuid(&ctu);
        assert_eq!(eb.fsid(), fsid);
        assert_eq!(eb.chunk_tree_uuid(), ctu);
    }

    #[test]
    fn item_accessors() {
        let mut eb = make_leaf(16384, 2, 7, 5);
        let key0 = DiskKey {
            objectid: 256,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let key1 = DiskKey {
            objectid: 256,
            key_type: KeyType::DirItem,
            offset: 100,
        };

        // Item 0: data at end of block, size 160
        let data_off_0 = 16384 - HEADER_SIZE as u32 - 160;
        eb.set_item_key(0, &key0);
        eb.set_item_offset(0, data_off_0);
        eb.set_item_size(0, 160);

        // Item 1: data before item 0's data, size 50
        let data_off_1 = data_off_0 - 50;
        eb.set_item_key(1, &key1);
        eb.set_item_offset(1, data_off_1);
        eb.set_item_size(1, 50);

        // Verify reads
        let k0 = eb.item_key(0);
        assert_eq!(k0.objectid, 256);
        assert_eq!(k0.key_type, KeyType::InodeItem);
        assert_eq!(k0.offset, 0);
        assert_eq!(eb.item_offset(0), data_off_0);
        assert_eq!(eb.item_size(0), 160);

        let k1 = eb.item_key(1);
        assert_eq!(k1.objectid, 256);
        assert_eq!(k1.key_type, KeyType::DirItem);
        assert_eq!(k1.offset, 100);
        assert_eq!(eb.item_offset(1), data_off_1);
        assert_eq!(eb.item_size(1), 50);

        // Verify data slices
        assert_eq!(eb.item_data(0).len(), 160);
        assert_eq!(eb.item_data(1).len(), 50);

        // Write and read back data
        eb.item_data_mut(0)[0] = 0xAA;
        eb.item_data_mut(1)[0] = 0xBB;
        assert_eq!(eb.item_data(0)[0], 0xAA);
        assert_eq!(eb.item_data(1)[0], 0xBB);
    }

    #[test]
    fn key_ptr_accessors() {
        let mut eb = make_node(16384, 3, 1, 10);

        for i in 0..3u64 {
            let key = DiskKey {
                objectid: i + 1,
                key_type: KeyType::RootItem,
                offset: 0,
            };
            eb.set_key_ptr(i as usize, &key, (i + 1) * 65536, 10 - i);
        }

        for i in 0..3u64 {
            let k = eb.key_ptr_key(i as usize);
            assert_eq!(k.objectid, i + 1);
            assert_eq!(k.key_type, KeyType::RootItem);
            assert_eq!(eb.key_ptr_blockptr(i as usize), (i + 1) * 65536);
            assert_eq!(eb.key_ptr_generation(i as usize), 10 - i);
        }
    }

    #[test]
    fn leaf_free_space_empty() {
        let eb = make_leaf(16384, 0, 1, 5);
        assert_eq!(eb.leaf_free_space(), 16384 - HEADER_SIZE as u32);
    }

    #[test]
    fn leaf_free_space_with_items() {
        let mut eb = make_leaf(4096, 1, 1, 5);
        // One item with 100 bytes of data, placed at the end
        let data_off = 4096 - HEADER_SIZE as u32 - 100;
        eb.set_item_key(
            0,
            &DiskKey {
                objectid: 1,
                key_type: KeyType::InodeItem,
                offset: 0,
            },
        );
        eb.set_item_offset(0, data_off);
        eb.set_item_size(0, 100);

        // Free space = data_start - items_end
        // items_end = 101 + 1 * 25 = 126
        // data_start = 101 + data_off
        let expected = (HEADER_SIZE as u32 + data_off)
            - (HEADER_SIZE as u32 + ITEM_SIZE as u32);
        assert_eq!(eb.leaf_free_space(), expected);
    }

    #[test]
    fn key_comparison() {
        let a = DiskKey {
            objectid: 1,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let b = DiskKey {
            objectid: 2,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        assert_eq!(key_cmp(&a, &b), std::cmp::Ordering::Less);
        assert_eq!(key_cmp(&b, &a), std::cmp::Ordering::Greater);
        assert_eq!(key_cmp(&a, &a), std::cmp::Ordering::Equal);

        // Same objectid, different type
        let c = DiskKey {
            objectid: 1,
            key_type: KeyType::DirItem,
            offset: 0,
        };
        assert_eq!(key_cmp(&a, &c), std::cmp::Ordering::Less);
    }

    #[test]
    fn min_max_keys() {
        assert_eq!(key_cmp(&min_key(), &max_key()), std::cmp::Ordering::Less);
        assert_eq!(key_cmp(&min_key(), &min_key()), std::cmp::Ordering::Equal);
    }

    #[test]
    fn checksum_round_trip() {
        let mut eb = make_leaf(4096, 0, 1, 5);
        eb.set_bytenr(65536);
        eb.update_checksum();
        // Verify that the checksum region is non-zero
        assert_ne!(&eb.as_bytes()[0..4], &[0, 0, 0, 0]);
        // Re-checksum should be idempotent
        let csum1: [u8; 4] = eb.as_bytes()[0..4].try_into().unwrap();
        eb.update_checksum();
        let csum2: [u8; 4] = eb.as_bytes()[0..4].try_into().unwrap();
        assert_eq!(csum1, csum2);
    }

    #[test]
    fn clone_independence() {
        let mut eb = make_leaf(4096, 0, 1, 5);
        let eb2 = eb.clone();
        eb.set_generation(999);
        assert_eq!(eb.generation(), 999);
        assert_eq!(eb2.generation(), 1);
    }

    #[test]
    fn as_tree_block_parse() {
        let mut eb = make_leaf(4096, 1, 7, 5);
        eb.set_bytenr(65536);
        let key = DiskKey {
            objectid: 256,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let data_off = 4096 - HEADER_SIZE as u32 - 160;
        eb.set_item_key(0, &key);
        eb.set_item_offset(0, data_off);
        eb.set_item_size(0, 160);

        let tb = eb.as_tree_block();
        match &tb {
            TreeBlock::Leaf { header, items, .. } => {
                assert_eq!(header.generation, 7);
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].key.objectid, 256);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn copy_within_and_zero() {
        let mut eb = ExtentBuffer::new_zeroed(256, 0);
        eb.as_bytes_mut()[10] = 0xAA;
        eb.as_bytes_mut()[11] = 0xBB;
        eb.copy_within(10..12, 20);
        assert_eq!(eb.as_bytes()[20], 0xAA);
        assert_eq!(eb.as_bytes()[21], 0xBB);
        eb.zero_range(20, 2);
        assert_eq!(eb.as_bytes()[20], 0);
        assert_eq!(eb.as_bytes()[21], 0);
    }
}

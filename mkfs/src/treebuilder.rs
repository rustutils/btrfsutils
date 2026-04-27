//! # Multi-leaf tree builder
//!
//! Packs sorted items into btrfs tree blocks (leaves + internal
//! nodes). Used by mkfs's bootstrap path (the four always-present
//! trees in [`crate::mkfs::make_btrfs`]) when a tree has too many
//! items for a single leaf. Post-bootstrap and `--rootdir` go
//! through the transaction crate, which has its own search/insert/
//! split pipeline; this module is bootstrap-only and would be
//! deletable if the bootstrap migrated to a `Filesystem::create`
//! transaction-crate primitive.

use crate::tree::{Key, LeafBuilder, LeafHeader, NodeBuilder, NodeHeader};
use uuid::Uuid;

/// Byte offset of the `bytenr` field within a tree block header.
const BYTENR_OFFSET: usize = 48;

/// Size of a key-pointer entry in internal nodes (17 + 8 + 8 bytes).
const KEY_PTR_SIZE: usize = 33;

/// Size of the tree block header.
const HEADER_SIZE: usize = 101;

/// Size of a leaf item descriptor.
const ITEM_SIZE: usize = 25;

/// Configuration for building a multi-block tree.
pub struct TreeBuilder {
    pub nodesize: u32,
    pub owner: u64,
    pub fsid: Uuid,
    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
}

/// A completed tree: one or more blocks forming a valid B-tree.
pub struct TreeBlocks {
    /// Level of the root block (0 = single leaf, 1+ = has internal nodes).
    pub root_level: u8,
    /// All blocks in the tree, leaves first then nodes bottom-up.
    /// The last block is the root.
    pub blocks: Vec<TreeBlockBuf>,
}

/// A single tree block with metadata needed for address assignment.
pub struct TreeBlockBuf {
    /// B-tree level (0 = leaf).
    pub level: u8,
    /// First key in this block (for parent node entries).
    pub first_key: Key,
    /// Raw block bytes. The bytenr field (offset 48) is zeroed and must
    /// be patched by the caller after address assignment.
    pub buf: Vec<u8>,
    /// For internal nodes: indices into `TreeBlocks::blocks` of child blocks.
    /// Empty for leaves.
    pub child_indices: Vec<usize>,
}

impl TreeBuilder {
    /// Build a complete tree from sorted items.
    ///
    /// Items must be in strictly ascending key order. Returns a set of tree
    /// blocks (leaves + internal nodes) with placeholder bytenr fields that
    /// the caller must patch after assigning logical addresses.
    ///
    /// If `items` is empty, returns a single empty leaf.
    ///
    /// # Panics
    ///
    /// Panics if a single item is too large to fit in an empty leaf.
    #[must_use]
    pub fn build(&self, items: &[(Key, Vec<u8>)]) -> TreeBlocks {
        if items.is_empty() {
            let leaf = LeafBuilder::new(self.nodesize, &self.leaf_header());
            return TreeBlocks {
                root_level: 0,
                blocks: vec![TreeBlockBuf {
                    level: 0,
                    first_key: Key::new(0, 0, 0),
                    buf: leaf.finish(),
                    child_indices: Vec::new(),
                }],
            };
        }

        // Pack items into leaves.
        let mut blocks: Vec<TreeBlockBuf> = Vec::new();
        let mut leaf = LeafBuilder::new(self.nodesize, &self.leaf_header());
        let mut first_key = items[0].0;

        for (key, data) in items {
            let needed = ITEM_SIZE + data.len();
            if leaf.space_left() < needed && !leaf.is_empty() {
                // Current leaf is full — finalize it and start a new one.
                blocks.push(TreeBlockBuf {
                    level: 0,
                    first_key,
                    buf: leaf.finish(),
                    child_indices: Vec::new(),
                });
                leaf = LeafBuilder::new(self.nodesize, &self.leaf_header());
                first_key = *key;
            }
            leaf.push(*key, data)
                .expect("item too large for empty leaf");
        }

        // Finalize the last leaf.
        blocks.push(TreeBlockBuf {
            level: 0,
            first_key,
            buf: leaf.finish(),
            child_indices: Vec::new(),
        });

        // If everything fits in one leaf, we're done.
        if blocks.len() == 1 {
            return TreeBlocks {
                root_level: 0,
                blocks,
            };
        }

        // Build internal node levels until we have a single root.
        let max_ptrs = (self.nodesize as usize - HEADER_SIZE) / KEY_PTR_SIZE;
        let mut level: u8 = 1;
        let mut child_start = 0;
        let mut child_count = blocks.len();

        while child_count > 1 {
            let mut new_nodes: Vec<TreeBlockBuf> = Vec::new();
            let mut i = child_start;
            let child_end = child_start + child_count;

            while i < child_end {
                let batch_end = (i + max_ptrs).min(child_end);
                // If this would leave a tiny remainder, balance it.
                let batch_end = if child_end - batch_end > 0
                    && child_end - batch_end < max_ptrs / 4
                {
                    // Split more evenly: take half of what's left.
                    i + (child_end - i) / 2
                } else {
                    batch_end
                };

                let node_first_key = blocks[i].first_key;
                let mut node =
                    NodeBuilder::new(self.nodesize, &self.node_header(level));
                let mut children = Vec::new();

                for (j, block) in blocks[i..batch_end].iter().enumerate() {
                    node.push(
                        block.first_key,
                        0, // placeholder blockptr — patched later
                        self.generation,
                    )
                    .expect("too many children for internal node");
                    children.push(i + j);
                }

                new_nodes.push(TreeBlockBuf {
                    level,
                    first_key: node_first_key,
                    buf: node.finish(),
                    child_indices: children,
                });

                i = batch_end;
            }

            child_start = blocks.len();
            child_count = new_nodes.len();
            blocks.extend(new_nodes);
            level += 1;
        }

        TreeBlocks {
            root_level: level - 1,
            blocks,
        }
    }

    /// Assign logical addresses to all blocks and patch bytenr fields
    /// in headers and internal node pointers.
    ///
    /// `alloc` is called once per block and must return the logical address.
    pub fn assign_addresses(
        tree: &mut TreeBlocks,
        mut alloc: impl FnMut() -> u64,
    ) {
        let mut addrs: Vec<u64> = Vec::with_capacity(tree.blocks.len());

        // Allocate addresses for all blocks.
        for _ in 0..tree.blocks.len() {
            addrs.push(alloc());
        }

        // Patch bytenr in each block header.
        for (i, block) in tree.blocks.iter_mut().enumerate() {
            let addr = addrs[i];
            block.buf[BYTENR_OFFSET..BYTENR_OFFSET + 8]
                .copy_from_slice(&addr.to_le_bytes());
        }

        // Patch blockptr fields in internal nodes.
        for i in 0..tree.blocks.len() {
            if tree.blocks[i].child_indices.is_empty() {
                continue;
            }
            let children: Vec<usize> = tree.blocks[i].child_indices.clone();
            for (slot, &child_idx) in children.iter().enumerate() {
                let ptr_offset = HEADER_SIZE + slot * KEY_PTR_SIZE + 17;
                let child_addr = addrs[child_idx];
                tree.blocks[i].buf[ptr_offset..ptr_offset + 8]
                    .copy_from_slice(&child_addr.to_le_bytes());
            }
        }
    }

    /// Create a copy of this builder with a different owner tree objectid.
    #[must_use]
    pub fn clone_with_owner(&self, owner: u64) -> Self {
        Self {
            nodesize: self.nodesize,
            owner,
            fsid: self.fsid,
            chunk_tree_uuid: self.chunk_tree_uuid,
            generation: self.generation,
        }
    }

    fn leaf_header(&self) -> LeafHeader {
        LeafHeader {
            fsid: self.fsid,
            chunk_tree_uuid: self.chunk_tree_uuid,
            generation: self.generation,
            owner: self.owner,
            bytenr: 0,
        }
    }

    fn node_header(&self, level: u8) -> NodeHeader {
        NodeHeader {
            fsid: self.fsid,
            chunk_tree_uuid: self.chunk_tree_uuid,
            generation: self.generation,
            owner: self.owner,
            bytenr: 0,
            level,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::tree::TreeBlock;

    fn test_builder() -> TreeBuilder {
        TreeBuilder {
            nodesize: 4096,
            owner: 5,
            fsid: Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef")
                .unwrap(),
            chunk_tree_uuid: Uuid::parse_str(
                "cafebabe-cafe-babe-cafe-babecafebabe",
            )
            .unwrap(),
            generation: 1,
        }
    }

    #[test]
    fn empty_items_single_leaf() {
        let builder = test_builder();
        let tree = builder.build(&[]);
        assert_eq!(tree.root_level, 0);
        assert_eq!(tree.blocks.len(), 1);
        match TreeBlock::parse(&tree.blocks[0].buf) {
            TreeBlock::Leaf { header, items, .. } => {
                assert_eq!(header.nritems, 0);
                assert_eq!(items.len(), 0);
            }
            TreeBlock::Node { .. } => panic!("expected leaf"),
        }
    }

    #[test]
    fn few_items_single_leaf() {
        let builder = test_builder();
        let items: Vec<(Key, Vec<u8>)> = (0..5)
            .map(|i| (Key::new(i, 1, 0), vec![0u8; 100]))
            .collect();
        let tree = builder.build(&items);
        assert_eq!(tree.root_level, 0);
        assert_eq!(tree.blocks.len(), 1);
    }

    #[test]
    fn many_items_multi_leaf() {
        let builder = test_builder();
        // With 4096 nodesize: usable = 4096 - 101 = 3995 bytes.
        // Each item: 25 (descriptor) + 100 (data) = 125 bytes.
        // Items per leaf: 3995 / 125 = 31.
        // 100 items → need 4 leaves → 1 internal node.
        let items: Vec<(Key, Vec<u8>)> = (0..100)
            .map(|i| (Key::new(i, 1, 0), vec![0u8; 100]))
            .collect();
        let tree = builder.build(&items);

        assert_eq!(tree.root_level, 1);
        // Should have 4 leaves + 1 node = 5 blocks.
        let leaf_count = tree.blocks.iter().filter(|b| b.level == 0).count();
        let node_count = tree.blocks.iter().filter(|b| b.level == 1).count();
        assert!(leaf_count >= 3);
        assert_eq!(node_count, 1);

        // Verify the root node points to all leaves.
        let root = tree.blocks.last().unwrap();
        assert_eq!(root.level, 1);
        assert_eq!(root.child_indices.len(), leaf_count);
    }

    #[test]
    fn address_assignment_patches_bytenr() {
        let builder = test_builder();
        let items: Vec<(Key, Vec<u8>)> = (0..100)
            .map(|i| (Key::new(i, 1, 0), vec![0u8; 100]))
            .collect();
        let mut tree = builder.build(&items);

        let mut next_addr = 0x500000u64;
        TreeBuilder::assign_addresses(&mut tree, || {
            let addr = next_addr;
            next_addr += 4096;
            addr
        });

        // Verify all blocks have correct bytenr.
        for (i, block) in tree.blocks.iter().enumerate() {
            let expected = 0x500000 + (i as u64) * 4096;
            let actual = u64::from_le_bytes(
                block.buf[BYTENR_OFFSET..BYTENR_OFFSET + 8]
                    .try_into()
                    .unwrap(),
            );
            assert_eq!(actual, expected);
        }

        // Verify internal node pointers are patched.
        let root = tree.blocks.last().unwrap();
        if root.level > 0 {
            let parsed = TreeBlock::parse(&root.buf);
            match parsed {
                TreeBlock::Node { ptrs, .. } => {
                    for (slot, ptr) in ptrs.iter().enumerate() {
                        let child_idx = root.child_indices[slot];
                        let expected_addr =
                            0x500000 + (child_idx as u64) * 4096;
                        assert_eq!(ptr.blockptr, expected_addr);
                    }
                }
                TreeBlock::Leaf { .. } => panic!("expected node"),
            }
        }
    }
}

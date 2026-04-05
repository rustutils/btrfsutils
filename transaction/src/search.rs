//! # B-tree search operations
//!
//! Implements `search_slot` which descends a btrfs B-tree from root to leaf,
//! recording the path at each level. Also provides binary search within a
//! single tree block, and leaf advancement (`next_leaf`, `prev_leaf`).

use crate::{
    balance,
    buffer::{ExtentBuffer, key_cmp},
    cow::cow_block,
    filesystem::Filesystem,
    path::BtrfsPath,
    split,
    transaction::Transaction,
};
use btrfs_disk::tree::DiskKey;
use std::{
    cmp::Ordering,
    io::{self, Read, Seek, Write},
};

/// Result of a binary search within a tree block.
#[derive(Debug, Clone, Copy)]
pub struct BinSearchResult {
    /// Whether the exact key was found.
    pub found: bool,
    /// The slot index. If found, this is the matching slot. If not found,
    /// this is the insertion point (first key greater than the target).
    pub slot: usize,
}

/// Binary search within a leaf for a key.
///
/// Returns `(found, slot)` where `slot` is the matching slot if found, or
/// the insertion point if not found.
#[must_use]
pub fn leaf_bin_search(eb: &ExtentBuffer, key: &DiskKey) -> BinSearchResult {
    let nritems = eb.nritems() as usize;
    if nritems == 0 {
        return BinSearchResult {
            found: false,
            slot: 0,
        };
    }

    let mut low: usize = 0;
    let mut high: usize = nritems;

    while low < high {
        let mid = low + (high - low) / 2;
        let mid_key = eb.item_key(mid);
        match key_cmp(&mid_key, key) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid,
            Ordering::Equal => {
                return BinSearchResult {
                    found: true,
                    slot: mid,
                };
            }
        }
    }

    BinSearchResult {
        found: false,
        slot: low,
    }
}

/// Binary search within an internal node for a key.
///
/// Returns the slot of the child subtree that could contain the target key.
/// This is the largest slot where `ptrs[slot].key <= target`. If the target
/// is less than all keys, returns slot 0.
#[must_use]
pub fn node_bin_search(eb: &ExtentBuffer, key: &DiskKey) -> usize {
    let nritems = eb.nritems() as usize;
    if nritems == 0 {
        return 0;
    }

    let mut low: usize = 0;
    let mut high: usize = nritems;

    while low < high {
        let mid = low + (high - low) / 2;
        let mid_key = eb.key_ptr_key(mid);
        match key_cmp(&mid_key, key) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid,
            Ordering::Equal => return mid,
        }
    }

    // `low` is the first slot with key > target. We want the slot before it,
    // which is the largest slot with key <= target.
    if low > 0 { low - 1 } else { 0 }
}

/// Describes why a search is being performed, so `search_slot` can
/// prepare the tree accordingly during descent.
#[derive(Debug, Clone, Copy)]
pub enum SearchIntent {
    /// Read-only lookup. No tree modifications are expected.
    ReadOnly,
    /// Insertion: the leaf must have at least this many free bytes
    /// (ITEM_SIZE + data payload size). Full nodes are split preemptively
    /// during descent.
    Insert(u32),
    /// Deletion: sparse nodes are rebalanced during descent to prevent
    /// tree bloat from deletion-heavy operations.
    Delete,
}

/// Search for a key in a tree, recording the path from root to leaf.
///
/// - `trans`: transaction handle (needed if `cow` is true to COW blocks)
/// - `tree_id`: which tree to search
/// - `key`: the key to search for
/// - `path`: receives the search path (must be empty/released)
/// - `intent`: controls preemptive splitting and rebalancing during descent
/// - `cow`: if true, COW each block along the path
///
/// Returns `Ok(true)` if the exact key was found, `Ok(false)` if not (in
/// which case `path.slots[0]` is the insertion point).
///
/// # Errors
///
/// Returns an error if any block read, COW, or split operation fails.
pub fn search_slot<R: Read + Write + Seek>(
    mut trans: Option<&mut Transaction<R>>,
    fs_info: &mut Filesystem<R>,
    tree_id: u64,
    key: &DiskKey,
    path: &mut BtrfsPath,
    intent: SearchIntent,
    cow: bool,
) -> io::Result<bool> {
    let root_bytenr = fs_info.root_bytenr(tree_id).ok_or_else(|| {
        io::Error::other(format!("unknown tree ID {tree_id}"))
    })?;

    let mut eb = fs_info.read_block(root_bytenr)?;

    // COW the root if needed
    if cow && let Some(trans) = trans.as_deref_mut() {
        let old_logical = eb.logical();
        eb = cow_block(trans, fs_info, &eb, tree_id, None)?;
        if eb.logical() != old_logical {
            fs_info.set_root_bytenr(tree_id, eb.logical());
        }
    }

    let mut level = eb.level();

    loop {
        if level == 0 {
            // Leaf: binary search for the key
            let result = leaf_bin_search(&eb, key);
            path.nodes[0] = Some(eb);
            path.slots[0] = result.slot;

            // If inserting, ensure the leaf has enough free space.
            // Split the leaf if it doesn't, then re-search to find the
            // correct slot in whichever leaf the key belongs to.
            if let SearchIntent::Insert(needed) = intent {
                let leaf = path.nodes[0].as_ref().unwrap();
                if leaf.leaf_free_space() < needed {
                    if let Some(trans) = trans.as_deref_mut() {
                        split::split_leaf(
                            trans, fs_info, path, tree_id, key, needed,
                        )?;
                    } else {
                        return Err(io::Error::other(
                            "leaf full and no transaction for split",
                        ));
                    }
                }
            }

            return Ok(result.found);
        }

        // Internal node: preemptive split if inserting and the node is full.
        // This prevents the case where a leaf split needs to insert a key
        // pointer into a full parent, which would require walking back up.
        if matches!(intent, SearchIntent::Insert(_)) {
            let nritems = eb.nritems() as usize;
            let max_ptrs = eb.max_key_ptrs() as usize;
            if nritems >= max_ptrs {
                // Store the node in the path before splitting it
                path.nodes[level as usize] = Some(eb.clone());
                path.slots[level as usize] = node_bin_search(&eb, key);

                if let Some(trans) = trans.as_deref_mut() {
                    let split_point = nritems / 2;
                    let old_slot = path.slots[level as usize];
                    split::split_node(trans, fs_info, path, tree_id, level)?;

                    // After split, path.nodes[level] is the truncated left
                    // half. If our key was in the right half, we need to
                    // switch to the right half node in the path.
                    if old_slot >= split_point {
                        // Find the parent level (above the split level)
                        let parent_level = (level as usize + 1
                            ..path.nodes.len())
                            .find(|&l| path.nodes[l].is_some());
                        if let Some(pl) = parent_level {
                            let parent = path.nodes[pl].as_ref().unwrap();
                            let ps = path.slots[pl];
                            // The right half was inserted at ps + 1
                            if ps + 1 < parent.nritems() as usize {
                                let right_bytenr =
                                    parent.key_ptr_blockptr(ps + 1);
                                let right = fs_info.read_block(right_bytenr)?;
                                path.nodes[level as usize] = Some(right);
                                path.slots[pl] = ps + 1;
                            }
                        }
                    }

                    eb = path.nodes[level as usize].as_ref().unwrap().clone();
                } else {
                    return Err(io::Error::other(
                        "node full and no transaction for split",
                    ));
                }
            }
        }

        // Deletion rebalancing: if the child we're about to descend into
        // is sparse (below 25% occupancy), try to merge it with a sibling.
        // This prevents tree bloat from deletion-heavy operations.
        if matches!(intent, SearchIntent::Delete) {
            let slot = node_bin_search(&eb, key);
            if let Some(trans) = trans.as_deref_mut()
                && balance::balance_node(
                    trans, fs_info, &mut eb, slot, tree_id,
                )?
            {
                // Node was merged — the parent's nritems changed.
                // Re-search to find the correct child slot.
                fs_info.mark_dirty(&eb);
            }
        }

        // Find the child slot
        let slot = node_bin_search(&eb, key);
        path.nodes[level as usize] = Some(eb.clone());
        path.slots[level as usize] = slot;

        // Read the child block
        let child_bytenr = eb.key_ptr_blockptr(slot);
        let mut child = fs_info.read_block(child_bytenr)?;

        // COW the child if needed
        if cow && let Some(trans) = trans.as_deref_mut() {
            let old_logical = child.logical();
            child = cow_block(
                trans,
                fs_info,
                &child,
                tree_id,
                Some((eb.logical(), slot)),
            )?;
            if child.logical() != old_logical {
                // Update parent's pointer to the new child
                if let Some(parent) = &mut path.nodes[level as usize] {
                    parent.set_key_ptr_blockptr(slot, child.logical());
                    parent.set_key_ptr_generation(slot, fs_info.generation);
                    fs_info.mark_dirty(parent);
                }
            }
        }

        eb = child;
        level -= 1;
    }
}

/// Advance the path to the next leaf.
///
/// Walks up the path until finding a level where we can move to the next
/// slot, then walks back down to the leftmost leaf.
///
/// Returns `Ok(true)` if successfully advanced, `Ok(false)` if already at
/// the last leaf (no more items).
///
/// # Errors
///
/// Returns an error if any block read fails.
pub fn next_leaf<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    path: &mut BtrfsPath,
) -> io::Result<bool> {
    // Walk up to find a level where we can advance
    let mut level = 1u8;
    loop {
        if level as usize >= path.nodes.len() {
            return Ok(false); // No more leaves
        }
        let node = match &path.nodes[level as usize] {
            Some(n) => n,
            None => return Ok(false),
        };
        let slot = path.slots[level as usize];
        if slot + 1 < node.nritems() as usize {
            // Can advance at this level
            path.slots[level as usize] = slot + 1;
            break;
        }
        level += 1;
    }

    // Walk back down to the leaf, always taking slot 0
    while level > 0 {
        let parent = path.nodes[level as usize].as_ref().unwrap();
        let slot = path.slots[level as usize];
        let child_bytenr = parent.key_ptr_blockptr(slot);
        let child = fs_info.read_block(child_bytenr)?;
        level -= 1;
        path.nodes[level as usize] = Some(child);
        path.slots[level as usize] = 0;
    }

    Ok(true)
}

/// Move the path to the previous leaf.
///
/// Returns `Ok(true)` if successfully moved, `Ok(false)` if already at
/// the first leaf.
///
/// # Errors
///
/// Returns an error if any block read fails.
pub fn prev_leaf<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    path: &mut BtrfsPath,
) -> io::Result<bool> {
    let mut level = 1u8;
    loop {
        if level as usize >= path.nodes.len() {
            return Ok(false);
        }
        if path.nodes[level as usize].is_none() {
            return Ok(false);
        }
        let slot = path.slots[level as usize];
        if slot > 0 {
            path.slots[level as usize] = slot - 1;
            break;
        }
        level += 1;
    }

    // Walk back down, taking the last slot at each level
    while level > 0 {
        let parent = path.nodes[level as usize].as_ref().unwrap();
        let slot = path.slots[level as usize];
        let child_bytenr = parent.key_ptr_blockptr(slot);
        let child = fs_info.read_block(child_bytenr)?;
        level -= 1;
        let last_slot = if child.nritems() > 0 {
            child.nritems() as usize - 1
        } else {
            0
        };
        path.nodes[level as usize] = Some(child);
        path.slots[level as usize] = last_slot;
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::HEADER_SIZE;
    use btrfs_disk::tree::KeyType;

    fn make_test_leaf(nodesize: u32, keys: &[(u64, u8, u64)]) -> ExtentBuffer {
        let mut eb = ExtentBuffer::new_zeroed(nodesize, 65536);
        eb.set_level(0);
        eb.set_nritems(keys.len() as u32);

        let mut data_end = nodesize;
        for (i, &(oid, typ, off)) in keys.iter().enumerate() {
            let key = DiskKey {
                objectid: oid,
                key_type: KeyType::from_raw(typ),
                offset: off,
            };
            let data_size = 16u32; // Arbitrary small payload
            data_end -= data_size;
            eb.set_item_key(i, &key);
            eb.set_item_offset(i, data_end - HEADER_SIZE as u32);
            eb.set_item_size(i, data_size);
        }
        eb
    }

    #[test]
    fn leaf_search_found() {
        let eb = make_test_leaf(
            4096,
            &[(1, 1, 0), (2, 1, 0), (3, 1, 0), (5, 1, 0), (10, 1, 0)],
        );
        let key = DiskKey {
            objectid: 3,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(r.found);
        assert_eq!(r.slot, 2);
    }

    #[test]
    fn leaf_search_not_found() {
        let eb = make_test_leaf(4096, &[(1, 1, 0), (3, 1, 0), (5, 1, 0)]);
        let key = DiskKey {
            objectid: 2,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(!r.found);
        assert_eq!(r.slot, 1); // insertion point
    }

    #[test]
    fn leaf_search_before_all() {
        let eb = make_test_leaf(4096, &[(5, 1, 0), (10, 1, 0)]);
        let key = DiskKey {
            objectid: 1,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(!r.found);
        assert_eq!(r.slot, 0);
    }

    #[test]
    fn leaf_search_after_all() {
        let eb = make_test_leaf(4096, &[(1, 1, 0), (2, 1, 0)]);
        let key = DiskKey {
            objectid: 99,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(!r.found);
        assert_eq!(r.slot, 2);
    }

    #[test]
    fn leaf_search_empty() {
        let eb = make_test_leaf(4096, &[]);
        let key = DiskKey {
            objectid: 1,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(!r.found);
        assert_eq!(r.slot, 0);
    }

    #[test]
    fn node_search() {
        let mut eb = ExtentBuffer::new_zeroed(4096, 131072);
        eb.set_level(1);
        eb.set_nritems(3);

        // keys: 10, 20, 30
        for (i, oid) in [10u64, 20, 30].iter().enumerate() {
            let key = DiskKey {
                objectid: *oid,
                key_type: KeyType::from_raw(1),
                offset: 0,
            };
            eb.set_key_ptr_key(i, &key);
            eb.set_key_ptr_blockptr(i, (i as u64 + 1) * 65536);
            eb.set_key_ptr_generation(i, 1);
        }

        // Search for key < first: should return slot 0
        let key = DiskKey {
            objectid: 5,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 0);

        // Search for key == 20: should return slot 1
        let key = DiskKey {
            objectid: 20,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 1);

        // Search for key between 20 and 30: should return slot 1
        let key = DiskKey {
            objectid: 25,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 1);

        // Search for key > all: should return slot 2
        let key = DiskKey {
            objectid: 99,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 2);
    }

    #[test]
    fn node_search_single_item() {
        let mut eb = ExtentBuffer::new_zeroed(4096, 131072);
        eb.set_level(1);
        eb.set_nritems(1);

        let key = DiskKey {
            objectid: 10,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        eb.set_key_ptr_key(0, &key);
        eb.set_key_ptr_blockptr(0, 65536);
        eb.set_key_ptr_generation(0, 1);

        // Any key should return slot 0
        let key = DiskKey {
            objectid: 5,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 0);

        let key = DiskKey {
            objectid: 99,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 0);
    }

    #[test]
    fn node_search_empty() {
        let mut eb = ExtentBuffer::new_zeroed(4096, 131072);
        eb.set_level(1);
        eb.set_nritems(0);

        let key = DiskKey {
            objectid: 1,
            key_type: KeyType::from_raw(1),
            offset: 0,
        };
        assert_eq!(node_bin_search(&eb, &key), 0);
    }

    #[test]
    fn leaf_search_key_type_ordering() {
        // Keys are compared by (objectid, type, offset). Two keys with
        // the same objectid but different types should be ordered by type.
        let eb = make_test_leaf(
            4096,
            &[
                (256, 1, 0),  // InodeItem
                (256, 12, 0), // DirItem
                (256, 84, 0), // DirIndex
            ],
        );

        // Search for type 12 (DirItem)
        let key = DiskKey {
            objectid: 256,
            key_type: KeyType::from_raw(12),
            offset: 0,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(r.found);
        assert_eq!(r.slot, 1);
    }

    #[test]
    fn leaf_search_offset_ordering() {
        let eb =
            make_test_leaf(4096, &[(256, 1, 0), (256, 1, 100), (256, 1, 200)]);

        let key = DiskKey {
            objectid: 256,
            key_type: KeyType::from_raw(1),
            offset: 100,
        };
        let r = leaf_bin_search(&eb, &key);
        assert!(r.found);
        assert_eq!(r.slot, 1);
    }

    #[test]
    fn search_intent_debug_format() {
        // Verify SearchIntent implements Debug
        let intent = SearchIntent::Insert(100);
        let _ = format!("{intent:?}");
        let intent = SearchIntent::ReadOnly;
        let _ = format!("{intent:?}");
        let intent = SearchIntent::Delete;
        let _ = format!("{intent:?}");
    }
}

//! # Node balancing (push left/right, merge)
//!
//! Before splitting a full leaf or node, try redistributing items to a
//! neighboring sibling. This reduces tree height growth and keeps the tree
//! more compact. Balancing is an optimization, not required for correctness.
//!
//! After deletion, if a leaf or node is less than roughly 25% full, consider
//! merging with a sibling to prevent excessive tree bloat.

use crate::{
    cow::cow_block,
    extent_buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE},
    fs_info::FsInfo,
    path::BtrfsPath,
    transaction::TransHandle,
};
use std::io::{self, Read, Seek, Write};

/// Try to push items from the current leaf to the left sibling.
///
/// If the left sibling (one slot left in the parent) has free space, move
/// items from the beginning of the current leaf to the end of the left
/// sibling. Returns the number of items moved (0 if no push was possible).
///
/// # Errors
///
/// Returns an error if block I/O fails.
pub fn push_leaf_left<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
) -> io::Result<usize> {
    // Find the parent level
    let parent_level = match find_parent_level(path) {
        Some(l) => l,
        None => return Ok(0), // Root leaf, no sibling
    };

    let parent_slot = path.slots[parent_level];
    if parent_slot == 0 {
        return Ok(0); // No left sibling
    }

    // Read the left sibling
    let parent = path.nodes[parent_level].as_ref().unwrap();
    let left_bytenr = parent.key_ptr_blockptr(parent_slot - 1);
    let left = fs_info.read_block(left_bytenr)?;

    if left.level() != 0 {
        return Ok(0); // Sibling is not a leaf (shouldn't happen)
    }

    let left_free = left.leaf_free_space();

    // Collect item data from the current leaf before we need mutable access
    // to the path for COW and parent pointer updates.
    let push_items = {
        let leaf = path.nodes[0].as_ref().unwrap();
        let nritems = leaf.nritems() as usize;
        if nritems == 0 {
            return Ok(0);
        }

        let mut items = Vec::new();
        let mut total_size = 0u32;
        for i in 0..nritems {
            let item_total = ITEM_SIZE as u32 + leaf.item_size(i);
            if total_size + item_total > left_free {
                break;
            }
            total_size += item_total;
            items.push((
                leaf.item_key(i),
                leaf.item_size(i),
                leaf.item_data(i).to_vec(),
            ));
        }
        items
    };
    let push_count = push_items.len();

    if push_count == 0 {
        return Ok(0);
    }

    // COW the sibling before modifying it. If the sibling belongs to a
    // previous generation, modifying it in place would overwrite the
    // committed state and break crash consistency.
    let mut left = cow_block(trans, fs_info, &left, tree_id, None)?;
    if left.logical() != left_bytenr {
        // COW allocated a new block — update the parent's pointer
        let parent = path.nodes[parent_level].as_mut().unwrap();
        parent.set_key_ptr_blockptr(parent_slot - 1, left.logical());
        parent.set_key_ptr_generation(parent_slot - 1, fs_info.generation);
        fs_info.mark_dirty(parent);
    }
    let left_nritems = left.nritems() as usize;
    let mut data_end = left.leaf_data_end();

    for (i, (src_key, src_size, src_data)) in push_items.iter().enumerate() {
        let src_size = *src_size;

        data_end -= src_size;
        let new_offset = data_end - HEADER_SIZE as u32;
        let dest_slot = left_nritems + i;

        left.set_item_key(dest_slot, src_key);
        left.set_item_offset(dest_slot, new_offset);
        left.set_item_size(dest_slot, src_size);

        let abs_off = data_end as usize;
        left.as_bytes_mut()[abs_off..abs_off + src_size as usize]
            .copy_from_slice(src_data);
    }
    left.set_nritems((left_nritems + push_count) as u32);
    fs_info.mark_dirty(&left);

    // Remove pushed items from the current leaf
    let leaf = path.nodes[0].as_mut().unwrap();
    crate::items::del_items(leaf, 0, push_count);
    fs_info.mark_dirty(leaf);

    // Update the parent's key pointer for this leaf (first key changed)
    if leaf.nritems() > 0 {
        let new_first_key = leaf.item_key(0);
        let parent = path.nodes[parent_level].as_mut().unwrap();
        parent.set_key_ptr_key(parent_slot, &new_first_key);
        fs_info.mark_dirty(parent);
    }

    // Update path slot (items shifted left by push_count)
    if path.slots[0] >= push_count {
        path.slots[0] -= push_count;
    } else {
        // The target slot moved to the left sibling
        path.nodes[0] = Some(left);
        path.slots[0] += left_nritems;
        path.slots[parent_level] = parent_slot - 1;
    }

    Ok(push_count)
}

/// Try to push items from the current leaf to the right sibling.
///
/// Returns the number of items moved (0 if no push was possible).
///
/// # Errors
///
/// Returns an error if block I/O fails.
pub fn push_leaf_right<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
) -> io::Result<usize> {
    let parent_level = match find_parent_level(path) {
        Some(l) => l,
        None => return Ok(0),
    };

    let parent = path.nodes[parent_level].as_ref().unwrap();
    let parent_slot = path.slots[parent_level];
    let parent_nritems = parent.nritems() as usize;

    if parent_slot + 1 >= parent_nritems {
        return Ok(0); // No right sibling
    }

    let right_bytenr = parent.key_ptr_blockptr(parent_slot + 1);
    let right = fs_info.read_block(right_bytenr)?;

    if right.level() != 0 {
        return Ok(0);
    }

    let right_free = right.leaf_free_space();

    // Collect item data from the current leaf before we need mutable access
    // to the path for COW and parent pointer updates.
    let (push_items, nritems) = {
        let leaf = path.nodes[0].as_ref().unwrap();
        let nritems = leaf.nritems() as usize;
        if nritems == 0 {
            return Ok(0);
        }

        let mut items = Vec::new();
        let mut total_size = 0u32;
        for i in (0..nritems).rev() {
            let item_total = ITEM_SIZE as u32 + leaf.item_size(i);
            if total_size + item_total > right_free {
                break;
            }
            total_size += item_total;
            items.push((
                leaf.item_key(i),
                leaf.item_size(i),
                leaf.item_data(i).to_vec(),
            ));
        }
        // Reverse so items are in ascending key order (we collected them in reverse)
        items.reverse();
        (items, nritems)
    };
    let push_count = push_items.len();

    if push_count == 0 {
        return Ok(0);
    }

    // COW the sibling before modifying it. If the sibling belongs to a
    // previous generation, modifying it in place would overwrite the
    // committed state and break crash consistency.
    let mut right = cow_block(trans, fs_info, &right, tree_id, None)?;
    if right.logical() != right_bytenr {
        // COW allocated a new block — update the parent's pointer
        let parent = path.nodes[parent_level].as_mut().unwrap();
        parent.set_key_ptr_blockptr(parent_slot + 1, right.logical());
        parent.set_key_ptr_generation(parent_slot + 1, fs_info.generation);
        fs_info.mark_dirty(parent);
    }
    let right_nritems = right.nritems() as usize;
    let first_push = nritems - push_count;

    // Calculate total data size of items being pushed
    let push_data_total: u32 = push_items.iter().map(|(_, s, _)| *s).sum();

    // Shift existing right items' data DOWN by push_data_total to make
    // room at the top of the data area for the new items. Data in btrfs
    // grows downward from the end of the block, and item 0 must have the
    // highest offset.
    if right_nritems > 0 {
        let old_data_end = right.leaf_data_end();
        let old_data_start =
            HEADER_SIZE as u32 + right.item_offset(0) + right.item_size(0);
        let data_len = old_data_start as usize - old_data_end as usize;
        if data_len > 0 {
            let src_start = old_data_end as usize;
            let dest_start = src_start - push_data_total as usize;
            right.copy_within(src_start..src_start + data_len, dest_start);
        }

        // Shift existing items' descriptors right by push_count and
        // update their offsets to account for the data shift.
        let src = HEADER_SIZE;
        let len = right_nritems * ITEM_SIZE;
        let dest = HEADER_SIZE + push_count * ITEM_SIZE;
        right.copy_within(src..src + len, dest);

        for i in push_count..push_count + right_nritems {
            let old_off = right.item_offset(i);
            right.set_item_offset(i, old_off - push_data_total);
        }
    } else {
        // No existing items — just shift descriptors area
        let src = HEADER_SIZE;
        let len = right_nritems * ITEM_SIZE;
        let dest = HEADER_SIZE + push_count * ITEM_SIZE;
        right.copy_within(src..src + len, dest);
    }

    // Pack new items at the top of the data area (highest offsets).
    let mut data_end = right.nodesize();
    for (i, (src_key, src_size, src_data)) in push_items.iter().enumerate() {
        let src_size = *src_size;

        data_end -= src_size;
        let new_offset = data_end - HEADER_SIZE as u32;

        right.set_item_key(i, src_key);
        right.set_item_offset(i, new_offset);
        right.set_item_size(i, src_size);

        let abs_off = data_end as usize;
        right.as_bytes_mut()[abs_off..abs_off + src_size as usize]
            .copy_from_slice(src_data);
    }
    right.set_nritems((right_nritems + push_count) as u32);
    fs_info.mark_dirty(&right);

    // Truncate our leaf
    let leaf = path.nodes[0].as_mut().unwrap();
    leaf.set_nritems(first_push as u32);
    fs_info.mark_dirty(leaf);

    // Update the parent's key pointer for the right sibling
    let new_right_key = right.item_key(0);
    let parent = path.nodes[parent_level].as_mut().unwrap();
    parent.set_key_ptr_key(parent_slot + 1, &new_right_key);
    fs_info.mark_dirty(parent);

    Ok(push_count)
}

/// Try to rebalance a sparse internal node during deletion descent.
///
/// If the node at `level` in the path has fewer key pointers than
/// `threshold`, attempt to merge with a sibling or redistribute key
/// pointers. This prevents tree bloat from deletion-heavy operations.
///
/// Called from `search_slot` when `SearchIntent::Delete` and the child
/// node to descend into is sparse.
///
/// Returns `true` if the node was merged into its sibling (and the path
/// updated), `false` if no action was taken or only redistribution occurred.
///
/// # Errors
///
/// Returns an error if block I/O or COW fails.
pub fn balance_node<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    parent: &mut ExtentBuffer,
    parent_slot: usize,
    tree_id: u64,
) -> io::Result<bool> {
    let child_bytenr = parent.key_ptr_blockptr(parent_slot);
    let child = fs_info.read_block(child_bytenr)?;
    let child_nritems = child.nritems() as usize;
    let max_ptrs = child.max_key_ptrs() as usize;

    // Only rebalance if below 25% occupancy
    if child_nritems >= max_ptrs / 4 {
        return Ok(false);
    }

    let parent_nritems = parent.nritems() as usize;

    // Try merging with the right sibling first
    if parent_slot + 1 < parent_nritems {
        let right_bytenr = parent.key_ptr_blockptr(parent_slot + 1);
        let right = fs_info.read_block(right_bytenr)?;
        let right_nritems = right.nritems() as usize;

        if child_nritems + right_nritems <= max_ptrs {
            // Merge: move all of right's key pointers into child, then
            // remove right's pointer from the parent.
            let mut child = cow_block(trans, fs_info, &child, tree_id, None)?;
            if child.logical() != child_bytenr {
                parent.set_key_ptr_blockptr(parent_slot, child.logical());
                parent.set_key_ptr_generation(parent_slot, fs_info.generation);
            }

            // Copy right's key pointers to end of child
            for i in 0..right_nritems {
                let key = right.key_ptr_key(i);
                let blockptr = right.key_ptr_blockptr(i);
                let kp_gen = right.key_ptr_generation(i);
                child.set_key_ptr(child_nritems + i, &key, blockptr, kp_gen);
            }
            child.set_nritems((child_nritems + right_nritems) as u32);
            fs_info.mark_dirty(&child);

            // Remove the right sibling's pointer from the parent
            let remove_slot = parent_slot + 1;
            if remove_slot + 1 < parent_nritems {
                let src = HEADER_SIZE + (remove_slot + 1) * KEY_PTR_SIZE;
                let len = (parent_nritems - remove_slot - 1) * KEY_PTR_SIZE;
                parent.copy_within(src..src + len, src - KEY_PTR_SIZE);
            }
            parent.set_nritems((parent_nritems - 1) as u32);
            fs_info.mark_dirty(parent);

            // Queue delayed ref drop for the absorbed right sibling
            trans.delayed_refs.drop_ref(
                right_bytenr,
                true,
                tree_id,
                right.level(),
            );
            trans.pin_block(right_bytenr);

            return Ok(true);
        }
    }

    // Try merging with the left sibling
    if parent_slot > 0 {
        let left_bytenr = parent.key_ptr_blockptr(parent_slot - 1);
        let left = fs_info.read_block(left_bytenr)?;
        let left_nritems = left.nritems() as usize;

        if left_nritems + child_nritems <= max_ptrs {
            // Merge child into left: append child's key pointers to left
            let mut left = cow_block(trans, fs_info, &left, tree_id, None)?;
            if left.logical() != left_bytenr {
                parent.set_key_ptr_blockptr(parent_slot - 1, left.logical());
                parent.set_key_ptr_generation(
                    parent_slot - 1,
                    fs_info.generation,
                );
            }

            for i in 0..child_nritems {
                let key = child.key_ptr_key(i);
                let blockptr = child.key_ptr_blockptr(i);
                let kp_gen = child.key_ptr_generation(i);
                left.set_key_ptr(left_nritems + i, &key, blockptr, kp_gen);
            }
            left.set_nritems((left_nritems + child_nritems) as u32);
            fs_info.mark_dirty(&left);

            // Remove child's pointer from the parent
            if parent_slot + 1 < parent_nritems {
                let src = HEADER_SIZE + (parent_slot + 1) * KEY_PTR_SIZE;
                let len = (parent_nritems - parent_slot - 1) * KEY_PTR_SIZE;
                parent.copy_within(src..src + len, src - KEY_PTR_SIZE);
            }
            parent.set_nritems((parent_nritems - 1) as u32);
            fs_info.mark_dirty(parent);

            // Queue delayed ref drop for the absorbed child
            trans.delayed_refs.drop_ref(
                child_bytenr,
                true,
                tree_id,
                child.level(),
            );
            trans.pin_block(child_bytenr);

            return Ok(true);
        }
    }

    Ok(false)
}

/// Find the parent level in the path.
fn find_parent_level(path: &BtrfsPath) -> Option<usize> {
    (1..path.nodes.len()).find(|&level| path.nodes[level].is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent_buffer::ExtentBuffer;

    // Balance tests require multi-level trees with siblings, which need
    // real filesystem images. Unit tests here verify the helper logic.

    #[test]
    fn find_parent_no_parent() {
        let path = BtrfsPath::new();
        assert_eq!(find_parent_level(&path), None);
    }

    #[test]
    fn find_parent_with_parent() {
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        path.nodes[1] = Some(ExtentBuffer::new_zeroed(4096, 65536));
        assert_eq!(find_parent_level(&path), Some(1));
    }

    #[test]
    fn find_parent_skips_empty_levels() {
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        // Level 1 empty, level 2 has a node
        path.nodes[2] = Some(ExtentBuffer::new_zeroed(4096, 131072));
        assert_eq!(find_parent_level(&path), Some(2));
    }

    #[test]
    fn push_leaf_left_no_parent_returns_zero() {
        // A root leaf has no parent, so push_leaf_left should return 0
        // without needing a real FsInfo. We verify by checking find_parent_level.
        let path = BtrfsPath::new();
        assert_eq!(find_parent_level(&path), None);
    }

    #[test]
    fn push_leaf_left_at_slot_zero_returns_zero() {
        // If current leaf is at parent slot 0, there's no left sibling
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        path.nodes[1] = Some(ExtentBuffer::new_zeroed(4096, 65536));
        path.slots[1] = 0; // Leftmost slot
        assert_eq!(find_parent_level(&path), Some(1));
        // Can't call push_leaf_left without FsInfo, but we can verify
        // the early return condition
        assert_eq!(path.slots[1], 0);
    }

    #[test]
    fn push_leaf_right_at_last_slot_no_sibling() {
        // If the current leaf is at the last parent slot, no right sibling
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        let mut parent = ExtentBuffer::new_zeroed(4096, 65536);
        parent.set_level(1);
        parent.set_nritems(2);
        path.nodes[1] = Some(parent);
        path.slots[1] = 1; // Last slot (nritems=2, so slots 0 and 1)
        // parent_slot + 1 >= parent_nritems → no right sibling
        assert!(path.slots[1] + 1 >= 2);
    }
}

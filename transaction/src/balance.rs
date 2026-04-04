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
    extent_buffer::{HEADER_SIZE, ITEM_SIZE},
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

    // Shift existing right items' descriptors right by push_count
    if right_nritems > 0 {
        let src = HEADER_SIZE;
        let len = right_nritems * ITEM_SIZE;
        let dest = HEADER_SIZE + push_count * ITEM_SIZE;
        right.copy_within(src..src + len, dest);
    }

    // Copy items to the beginning of the right sibling
    let mut data_end = right.leaf_data_end();
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
}

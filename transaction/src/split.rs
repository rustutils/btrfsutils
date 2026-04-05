//! # Node and leaf splitting
//!
//! When a leaf is too full for an insertion, it must be split into two leaves
//! with roughly half the items each. Similarly, when a node is too full for
//! a new key pointer, it must be split. Splits may cascade up to the root,
//! potentially increasing tree height.

use crate::{
    balance,
    extent_buffer::{
        ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE, key_cmp,
    },
    fs_info::FsInfo,
    path::BtrfsPath,
    transaction::TransHandle,
};
use btrfs_disk::tree::DiskKey;
use std::io::{self, Read, Seek, Write};

/// Split a leaf that is too full for an insertion.
///
/// Allocates a new leaf, moves roughly half the items to it, and inserts a
/// key pointer in the parent node. The path is updated to point to the correct
/// leaf for the insertion key.
///
/// # Errors
///
/// Returns an error if block allocation or I/O fails.
pub fn split_leaf<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
    key: &DiskKey,
    data_size: u32,
) -> io::Result<()> {
    // Before splitting, try redistributing items to a sibling. This is
    // cheaper than allocating a new leaf and reduces tree growth.
    if balance::push_leaf_right(trans, fs_info, path, tree_id)? > 0 {
        let leaf = path.nodes[0].as_ref().unwrap();
        if leaf.leaf_free_space() >= data_size {
            return Ok(());
        }
    }
    if balance::push_leaf_left(trans, fs_info, path, tree_id)? > 0 {
        let leaf = path.nodes[0].as_ref().unwrap();
        if leaf.leaf_free_space() >= data_size {
            return Ok(());
        }
    }

    let leaf = path.nodes[0]
        .as_ref()
        .ok_or_else(|| io::Error::other("split_leaf: no leaf in path"))?;
    let nritems = leaf.nritems() as usize;
    let nodesize = leaf.nodesize();

    // Find a data-aware split point. Items have variable sizes, so splitting
    // at nritems/2 can produce very unbalanced leaves. Instead, sum the actual
    // byte usage and find the item where roughly half the total bytes are on
    // each side. Each item consumes ITEM_SIZE (descriptor) + data_size bytes.
    let total_bytes: u32 = (0..nritems)
        .map(|i| ITEM_SIZE as u32 + leaf.item_size(i))
        .sum();
    let half = total_bytes / 2;
    let mut running = 0u32;
    let mut split = nritems / 2; // fallback
    for i in 0..nritems {
        running += ITEM_SIZE as u32 + leaf.item_size(i);
        if running >= half {
            // Split after item i: items [0..=i] stay, [i+1..] move.
            // Ensure at least one item on each side.
            split = (i + 1).clamp(1, nritems - 1);
            break;
        }
    }

    // Allocate a new leaf and queue an extent ref for it
    let new_logical = trans.alloc_tree_block(fs_info, tree_id, 0)?;
    let mut new_leaf = ExtentBuffer::new_zeroed(nodesize, new_logical);
    new_leaf.set_bytenr(new_logical);
    new_leaf.set_level(0);
    new_leaf.set_generation(fs_info.generation);
    new_leaf.set_owner(leaf.owner());
    new_leaf.set_fsid(&leaf.fsid());
    new_leaf.set_chunk_tree_uuid(&leaf.chunk_tree_uuid());
    new_leaf.set_flags(leaf.flags());
    new_leaf.set_nritems(0);

    // Move items [split..nritems) to the new leaf
    let leaf = path.nodes[0].as_ref().unwrap();
    let move_count = nritems - split;
    let mut new_data_end = nodesize;

    for i in 0..move_count {
        let src_slot = split + i;
        let src_key = leaf.item_key(src_slot);
        let src_size = leaf.item_size(src_slot);
        let src_data = leaf.item_data(src_slot).to_vec();

        new_data_end -= src_size;
        let new_offset = new_data_end - HEADER_SIZE as u32;

        new_leaf.set_item_key(i, &src_key);
        new_leaf.set_item_offset(i, new_offset);
        new_leaf.set_item_size(i, src_size);
        new_leaf.as_bytes_mut()
            [new_data_end as usize..new_data_end as usize + src_size as usize]
            .copy_from_slice(&src_data);
    }
    new_leaf.set_nritems(move_count as u32);

    // Truncate the original leaf
    let old_leaf = path.nodes[0].as_mut().unwrap();
    old_leaf.set_nritems(split as u32);
    fs_info.mark_dirty(old_leaf);
    fs_info.mark_dirty(&new_leaf);

    // Insert a key pointer in the parent node for the new leaf
    let new_leaf_first_key = new_leaf.item_key(0);
    insert_ptr_in_parent(
        trans,
        fs_info,
        path,
        tree_id,
        &new_leaf_first_key,
        new_logical,
    )?;

    // Update the path to point to the correct leaf for insertion
    if key_cmp(key, &new_leaf_first_key) != std::cmp::Ordering::Less {
        // The insertion key goes into the new leaf
        let slot_in_new = crate::search::leaf_bin_search(&new_leaf, key).slot;
        path.nodes[0] = Some(new_leaf);
        path.slots[0] = slot_in_new;
    }

    Ok(())
}

/// Split an internal node that is too full for a new key pointer.
///
/// # Errors
///
/// Returns an error if block allocation or I/O fails.
pub fn split_node<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
    level: u8,
) -> io::Result<()> {
    let node = path.nodes[level as usize]
        .as_ref()
        .ok_or_else(|| io::Error::other("split_node: no node at level"))?;
    let nritems = node.nritems() as usize;
    let nodesize = node.nodesize();
    let split = nritems / 2;

    // Allocate new node and queue an extent ref for it
    let new_logical = trans.alloc_tree_block(fs_info, tree_id, level)?;
    let mut new_node = ExtentBuffer::new_zeroed(nodesize, new_logical);
    new_node.set_bytenr(new_logical);
    new_node.set_level(level);
    new_node.set_generation(fs_info.generation);
    new_node.set_owner(node.owner());
    new_node.set_fsid(&node.fsid());
    new_node.set_chunk_tree_uuid(&node.chunk_tree_uuid());
    new_node.set_flags(node.flags());

    // Copy key pointers [split..nritems) to new node
    let node = path.nodes[level as usize].as_ref().unwrap();
    let move_count = nritems - split;
    for i in 0..move_count {
        let src_slot = split + i;
        let key = node.key_ptr_key(src_slot);
        let blockptr = node.key_ptr_blockptr(src_slot);
        let kp_gen = node.key_ptr_generation(src_slot);
        new_node.set_key_ptr(i, &key, blockptr, kp_gen);
    }
    new_node.set_nritems(move_count as u32);

    // Truncate original node
    let old_node = path.nodes[level as usize].as_mut().unwrap();
    old_node.set_nritems(split as u32);
    fs_info.mark_dirty(old_node);
    fs_info.mark_dirty(&new_node);

    // Insert key pointer in parent
    let new_node_first_key = new_node.key_ptr_key(0);
    insert_ptr_in_parent(
        trans,
        fs_info,
        path,
        tree_id,
        &new_node_first_key,
        new_logical,
    )?;

    Ok(())
}

/// Insert a key pointer in the parent node for a newly split child.
///
/// If the parent is full, splits it recursively. If the root splits,
/// creates a new root.
fn insert_ptr_in_parent<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
    key: &DiskKey,
    child_logical: u64,
) -> io::Result<()> {
    // Find the parent level
    let _child_level = path.nodes[0].as_ref().map(|n| n.level()).unwrap_or(0);

    // Walk up to find the level of the parent
    // Actually, for a leaf split, parent is level 1. For a node split at level L,
    // parent is level L+1. But the caller should handle this through the path.
    // We need to find the right parent level.
    let parent_level = find_parent_level(path);

    if parent_level.is_none() {
        // We're splitting the root — create a new root
        return create_new_root(
            trans,
            fs_info,
            path,
            tree_id,
            key,
            child_logical,
        );
    }
    let parent_level = parent_level.unwrap();

    // Check if parent has room
    {
        let parent = path.nodes[parent_level]
            .as_ref()
            .ok_or_else(|| io::Error::other("insert_ptr: parent missing"))?;
        let parent_nritems = parent.nritems() as usize;
        let max_ptrs = parent.max_key_ptrs() as usize;
        if parent_nritems >= max_ptrs {
            // Need to split the parent first, then retry
            split_node(trans, fs_info, path, tree_id, parent_level as u8)?;
            return insert_ptr_in_parent(
                trans,
                fs_info,
                path,
                tree_id,
                key,
                child_logical,
            );
        }
    }

    let parent = path.nodes[parent_level].as_mut().unwrap();
    let parent_nritems = parent.nritems() as usize;

    // Find insertion slot
    let slot = path.slots[parent_level] + 1;

    // Shift existing key pointers right
    if slot < parent_nritems {
        let src = HEADER_SIZE + slot * KEY_PTR_SIZE;
        let len = (parent_nritems - slot) * KEY_PTR_SIZE;
        parent.copy_within(src..src + len, src + KEY_PTR_SIZE);
    }

    // Write the new key pointer
    parent.set_key_ptr(slot, key, child_logical, fs_info.generation);
    parent.set_nritems(parent_nritems as u32 + 1);
    fs_info.mark_dirty(parent);

    Ok(())
}

/// Find the parent level for inserting a new key pointer.
/// Returns None if the path's root is at the top (need new root).
fn find_parent_level(path: &BtrfsPath) -> Option<usize> {
    (1..path.nodes.len()).find(|&level| path.nodes[level].is_some())
}

/// Create a new root node when the old root splits.
fn create_new_root<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
    right_key: &DiskKey,
    right_logical: u64,
) -> io::Result<()> {
    // The current root is at the highest occupied level in the path
    let old_root_level = {
        let mut lvl = 0;
        for (i, node) in path.nodes.iter().enumerate() {
            if node.is_some() {
                lvl = i;
            }
        }
        lvl
    };
    let old_root = path.nodes[old_root_level].as_ref().unwrap();
    let old_root_logical = old_root.logical();
    let old_root_key = if old_root.is_leaf() {
        old_root.item_key(0)
    } else {
        old_root.key_ptr_key(0)
    };

    // Allocate new root and queue an extent ref for it
    let new_level = old_root.level() + 1;
    let new_logical = trans.alloc_tree_block(fs_info, tree_id, new_level)?;
    let mut new_root =
        ExtentBuffer::new_zeroed(old_root.nodesize(), new_logical);
    new_root.set_bytenr(new_logical);
    new_root.set_level(new_level);
    new_root.set_generation(fs_info.generation);
    new_root.set_owner(old_root.owner());
    new_root.set_fsid(&old_root.fsid());
    new_root.set_chunk_tree_uuid(&old_root.chunk_tree_uuid());
    new_root.set_flags(old_root.flags());
    new_root.set_nritems(2);

    // Pointer 0: old root (left child)
    new_root.set_key_ptr(
        0,
        &old_root_key,
        old_root_logical,
        fs_info.generation,
    );
    // Pointer 1: new split (right child)
    new_root.set_key_ptr(1, right_key, right_logical, fs_info.generation);

    fs_info.mark_dirty(&new_root);
    fs_info.set_root_bytenr(tree_id, new_logical);

    // Update the path
    path.nodes[new_level as usize] = Some(new_root);
    path.slots[new_level as usize] = 0;

    Ok(())
}

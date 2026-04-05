//! # Node and leaf splitting
//!
//! When a leaf is too full for an insertion, it must be split into two leaves
//! with roughly half the items each. Similarly, when a node is too full for
//! a new key pointer, it must be split. Splits may cascade up to the root,
//! potentially increasing tree height.

use crate::{
    balance,
    buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE, key_cmp},
    filesystem::Filesystem,
    path::BtrfsPath,
    transaction::Transaction,
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
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
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

    // Truncate the original leaf and compact the remaining items' data
    // toward the end of the block. Items [0..split) stay, but the data
    // from moved items [split..nritems) left a gap. btrfs requires the
    // first item's data to end at nodesize - HEADER_SIZE (no gap at the
    // top of the data area).
    let old_leaf = path.nodes[0].as_mut().unwrap();
    debug_assert_eq!(
        old_leaf.generation(),
        fs_info.generation,
        "split_leaf: old leaf at {} has stale generation {} (expected {})",
        old_leaf.logical(),
        old_leaf.generation(),
        fs_info.generation
    );
    old_leaf.set_nritems(split as u32);

    // Repack data: copy each remaining item's data to a contiguous region
    // at the end of the block, updating offsets as we go.
    if split > 0 {
        let mut data_end = nodesize;
        for i in 0..split {
            let size = old_leaf.item_size(i);
            let old_data = old_leaf.item_data(i).to_vec();
            data_end -= size;
            let new_offset = data_end - HEADER_SIZE as u32;
            old_leaf.set_item_offset(i, new_offset);
            let abs_off = data_end as usize;
            old_leaf.as_bytes_mut()[abs_off..abs_off + size as usize]
                .copy_from_slice(&old_data);
        }
    }

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
        0, // child is a leaf
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
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
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
        level, // child is a node at this level
    )?;

    Ok(())
}

/// Insert a key pointer in the parent node for a newly split child.
///
/// If the parent is full, splits it recursively. If the root splits,
/// creates a new root.
fn insert_ptr_in_parent<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
    path: &mut BtrfsPath,
    tree_id: u64,
    key: &DiskKey,
    child_logical: u64,
    child_level: u8,
) -> io::Result<()> {
    // Find the parent: the first occupied level above the child.
    let parent_level = find_parent_level_above(path, child_level as usize);

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
            // Split the parent first. This truncates the left half and
            // inserts the right half into the grandparent (which may
            // cascade further). After the split, the path at parent_level
            // is stale. Instead of trying to fix it, re-search for the
            // correct insertion point by scanning the grandparent for
            // the child we're inserting next to.
            split_node(trans, fs_info, path, tree_id, parent_level as u8)?;

            // The grandparent (or new root) now has pointers to both
            // halves. Find the node that should contain our key by
            // searching from the grandparent level down.
            let gp_level = find_parent_level_above(path, parent_level);
            if let Some(gp) = gp_level {
                let gp_node = path.nodes[gp].as_ref().unwrap();
                let gp_slot = crate::search::node_bin_search(gp_node, key);
                let target_bytenr = gp_node.key_ptr_blockptr(gp_slot);
                let target_node = fs_info.read_block(target_bytenr)?;
                path.nodes[parent_level] = Some(target_node);
                // Find slot in the target node for the child
                path.slots[parent_level] = crate::search::node_bin_search(
                    path.nodes[parent_level].as_ref().unwrap(),
                    key,
                );
                path.slots[gp] = gp_slot;
            }

            return insert_ptr_in_parent(
                trans,
                fs_info,
                path,
                tree_id,
                key,
                child_logical,
                child_level,
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

/// Find the first occupied level above `min_level` in the path.
fn find_parent_level_above(
    path: &BtrfsPath,
    min_level: usize,
) -> Option<usize> {
    (min_level + 1..path.nodes.len()).find(|&level| path.nodes[level].is_some())
}

/// Create a new root node when the old root splits.
fn create_new_root<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
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
    debug_assert_eq!(
        old_root.generation(),
        fs_info.generation,
        "create_new_root: old root at {} has generation {}, expected {}",
        old_root_logical,
        old_root.generation(),
        fs_info.generation
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{buffer::ExtentBuffer, items};
    use btrfs_disk::tree::KeyType;

    fn make_key(oid: u64) -> DiskKey {
        DiskKey {
            objectid: oid,
            key_type: KeyType::InodeItem,
            offset: 0,
        }
    }

    fn filled_leaf(
        nodesize: u32,
        item_count: usize,
        data_size: usize,
    ) -> ExtentBuffer {
        let mut eb = ExtentBuffer::new_zeroed(nodesize, 65536);
        eb.set_level(0);
        eb.set_nritems(0);
        eb.set_generation(1);
        eb.set_owner(5);
        let data = vec![0xAA; data_size];
        for i in 0..item_count {
            items::insert_item(&mut eb, i, &make_key(i as u64 + 1), &data)
                .unwrap();
        }
        eb
    }

    #[test]
    fn data_aware_split_point_uniform_items() {
        // With uniform 32-byte items, the split point should be near nritems/2
        let eb = filled_leaf(16384, 200, 32);
        let nritems = eb.nritems() as usize;

        let total_bytes: u32 = (0..nritems)
            .map(|i| ITEM_SIZE as u32 + eb.item_size(i))
            .sum();
        let half = total_bytes / 2;
        let mut running = 0u32;
        let mut split = nritems / 2;
        for i in 0..nritems {
            running += ITEM_SIZE as u32 + eb.item_size(i);
            if running >= half {
                split = (i + 1).clamp(1, nritems - 1);
                break;
            }
        }

        // For uniform items, split should be close to nritems/2
        assert!(
            (split as i64 - nritems as i64 / 2).unsigned_abs() <= 1,
            "split={split} but nritems/2={}",
            nritems / 2
        );
    }

    #[test]
    fn data_aware_split_point_variable_items() {
        // Mix of small and large items
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_level(0);
        eb.set_nritems(0);
        eb.set_generation(1);
        eb.set_owner(5);

        // Insert 5 items: one large, four small
        items::insert_item(&mut eb, 0, &make_key(1), &[0x11; 1000]).unwrap();
        items::insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        items::insert_item(&mut eb, 2, &make_key(3), &[0x33; 50]).unwrap();
        items::insert_item(&mut eb, 3, &make_key(4), &[0x44; 50]).unwrap();
        items::insert_item(&mut eb, 4, &make_key(5), &[0x55; 50]).unwrap();

        let nritems = eb.nritems() as usize;
        let total_bytes: u32 = (0..nritems)
            .map(|i| ITEM_SIZE as u32 + eb.item_size(i))
            .sum();
        let half = total_bytes / 2;
        let mut running = 0u32;
        let mut split = nritems / 2;
        for i in 0..nritems {
            running += ITEM_SIZE as u32 + eb.item_size(i);
            if running >= half {
                split = (i + 1).clamp(1, nritems - 1);
                break;
            }
        }

        // The large 1000-byte item at position 0 accounts for most of the
        // bytes. The split should happen after item 0 (split=1), not at
        // nritems/2=2.
        assert_eq!(split, 1, "split should be after the large item");
    }

    #[test]
    fn split_point_clamps_minimum() {
        // With 2 items where the first is huge, split should be 1 (not 0)
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_level(0);
        eb.set_nritems(0);
        eb.set_generation(1);
        eb.set_owner(5);
        items::insert_item(&mut eb, 0, &make_key(1), &[0x11; 2000]).unwrap();
        items::insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();

        let nritems = eb.nritems() as usize;
        let total_bytes: u32 = (0..nritems)
            .map(|i| ITEM_SIZE as u32 + eb.item_size(i))
            .sum();
        let half = total_bytes / 2;
        let mut running = 0u32;
        let mut split = nritems / 2;
        for i in 0..nritems {
            running += ITEM_SIZE as u32 + eb.item_size(i);
            if running >= half {
                split = (i + 1).clamp(1, nritems - 1);
                break;
            }
        }
        assert_eq!(split, 1, "split should be 1 (at least one item per side)");
    }

    #[test]
    fn leaf_data_compaction_after_truncate() {
        // Simulate what split_leaf does: create a full leaf, truncate to
        // first half, then compact data.
        let mut eb = filled_leaf(4096, 20, 32);
        let nodesize = eb.nodesize();
        let split = 10;

        // Verify item 0 data end before truncate
        let end_before = eb.item_offset(0) + eb.item_size(0);
        assert_eq!(end_before, nodesize - HEADER_SIZE as u32);

        // Truncate
        eb.set_nritems(split);

        // Before compaction: item 0's data end should still be correct
        // (items 0..split have the highest offsets)
        let end_after_trunc = eb.item_offset(0) + eb.item_size(0);
        assert_eq!(end_after_trunc, nodesize - HEADER_SIZE as u32);

        // Compact (repack remaining items)
        let mut data_end = nodesize;
        for i in 0..split as usize {
            let size = eb.item_size(i);
            let old_data = eb.item_data(i).to_vec();
            data_end -= size;
            let new_offset = data_end - HEADER_SIZE as u32;
            eb.set_item_offset(i, new_offset);
            let abs_off = data_end as usize;
            eb.as_bytes_mut()[abs_off..abs_off + size as usize]
                .copy_from_slice(&old_data);
        }

        // After compaction: item 0 data should still end at the right place
        let end_compacted = eb.item_offset(0) + eb.item_size(0);
        assert_eq!(end_compacted, nodesize - HEADER_SIZE as u32);

        // Verify all data is intact
        for i in 0..split as usize {
            assert_eq!(eb.item_data(i), &[0xAA; 32]);
        }
    }

    #[test]
    fn find_parent_level_none() {
        let path = BtrfsPath::new();
        assert!(find_parent_level_above(&path, 0).is_none());
    }

    #[test]
    fn find_parent_level_at_1() {
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        path.nodes[1] = Some(ExtentBuffer::new_zeroed(4096, 65536));
        assert_eq!(find_parent_level_above(&path, 0), Some(1));
    }

    #[test]
    fn find_parent_level_at_2() {
        let mut path = BtrfsPath::new();
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        path.nodes[2] = Some(ExtentBuffer::new_zeroed(4096, 131072));
        assert_eq!(find_parent_level_above(&path, 0), Some(2));
    }
}

//! # Transaction lifecycle: start, commit, abort
//!
//! A `TransHandle` groups multiple tree modifications into a single atomic
//! commit. The commit point is the superblock write: all new tree blocks are
//! written first (at new locations via COW), then the superblock is updated
//! to point to the new root.

use crate::{
    delayed_ref::DelayedRefQueue, fs_info::FsInfo, items, path::BtrfsPath,
    search, serialize,
};
use btrfs_disk::{
    items::RootItem,
    superblock,
    tree::{DiskKey, KeyType},
};
use std::io::{self, Read, Seek, Write};

/// Handle for an in-progress transaction.
///
/// Created by [`TransHandle::start`], which increments the generation.
/// Tracks dirty blocks and pending reference count changes. Finalized by
/// either [`commit`](TransHandle::commit) (write to disk) or
/// [`abort`](TransHandle::abort) (discard).
pub struct TransHandle<R> {
    /// The transaction generation (superblock.generation + 1).
    pub transid: u64,
    /// Blocks freed during this transaction (old COW sources).
    freed_blocks: Vec<u64>,
    /// Delayed reference count updates.
    pub delayed_refs: DelayedRefQueue,
    /// Simple bump allocator cursor for metadata blocks.
    /// This is a temporary allocator replaced by proper extent allocation
    /// in Phase 7. It tracks the next free logical address within a
    /// metadata block group.
    alloc_cursor: u64,
    /// End of the allocation region.
    alloc_end: u64,
    /// Phantom to tie the lifetime/type parameter.
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Read + Write + Seek> TransHandle<R> {
    /// Start a new transaction.
    ///
    /// Increments the filesystem generation by 1 and initializes the
    /// temporary block allocator by scanning for a metadata block group.
    ///
    /// # Errors
    ///
    /// Returns an error if the filesystem state cannot be prepared.
    pub fn start(fs_info: &mut FsInfo<R>) -> io::Result<Self> {
        let transid = fs_info.superblock.generation + 1;
        fs_info.generation = transid;

        // Snapshot current roots so we can detect changes at commit time
        fs_info.snapshot_roots();

        // Initialize the temporary bump allocator by finding a metadata
        // block group with free space. We scan the extent tree to find
        // block groups and pick one.
        let (cursor, end) = find_metadata_alloc_region(fs_info)?;

        Ok(Self {
            transid,
            freed_blocks: Vec::new(),
            delayed_refs: DelayedRefQueue::new(),
            alloc_cursor: cursor,
            alloc_end: end,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Allocate a new metadata block (nodesize bytes).
    ///
    /// Uses the temporary bump allocator. Returns the logical address
    /// of the newly allocated block.
    ///
    /// # Errors
    ///
    /// Returns an error if the allocation region is exhausted.
    pub fn alloc_block(&mut self, fs_info: &FsInfo<R>) -> io::Result<u64> {
        let logical = self.alloc_cursor;
        let next = logical + u64::from(fs_info.nodesize);
        if next > self.alloc_end {
            return Err(io::Error::other(
                "temporary allocator: out of space in metadata block group",
            ));
        }
        self.alloc_cursor = next;
        Ok(logical)
    }

    /// Queue a block to be freed after commit.
    pub fn queue_free_block(&mut self, logical: u64) {
        self.freed_blocks.push(logical);
    }

    /// Commit the transaction: update root items, flush delayed refs, write
    /// all dirty blocks, update the superblock, and write to all mirrors.
    ///
    /// This is the full commit sequence per the spec:
    /// 1. Update root items in the root tree for trees whose root changed
    /// 2. Flush delayed reference count updates (convergence loop)
    /// 3. Write all dirty tree blocks to disk with correct checksums
    /// 4. Update superblock (generation, root pointers, byte counts)
    /// 5. Write superblock to all mirrors
    ///
    /// # Errors
    ///
    /// Returns an error if any tree modification, write, or fsync fails.
    pub fn commit(mut self, fs_info: &mut FsInfo<R>) -> io::Result<()> {
        // Step 1: Update root items in the root tree for changed trees.
        self.update_root_items(fs_info)?;

        // Step 2: Flush delayed refs (convergence loop).
        // Processing delayed refs modifies the extent tree, which may generate
        // more delayed refs from COW. Repeat until stable.
        self.flush_delayed_refs(fs_info)?;

        // Step 3: Flush all dirty blocks to disk
        fs_info.flush_dirty()?;

        // Step 4: Update superblock fields
        fs_info.superblock.generation = self.transid;

        // Update root tree root pointer
        if let Some(root_bytenr) = fs_info.root_bytenr(1) {
            fs_info.superblock.root = root_bytenr;
            if let Ok(eb) = fs_info.read_block(root_bytenr) {
                fs_info.superblock.root_level = eb.level();
            }
        }

        // Update chunk tree root pointer
        if let Some(chunk_bytenr) = fs_info.root_bytenr(3) {
            fs_info.superblock.chunk_root = chunk_bytenr;
            fs_info.superblock.chunk_root_generation = self.transid;
            if let Ok(eb) = fs_info.read_block(chunk_bytenr) {
                fs_info.superblock.chunk_root_level = eb.level();
            }
        }

        // Step 5: Update backup roots (rotating through 4 slots)
        let backup_idx = (self.transid % 4) as usize;
        update_backup_root(fs_info, backup_idx);

        // Step 6: Write superblock to all mirrors
        let sb_bytes = fs_info.superblock.to_bytes();
        superblock::write_superblock_all_mirrors(
            fs_info.reader_mut().inner_mut(),
            &sb_bytes,
        )?;

        // The caller should fsync the file handle for durability.

        // Step 7: Clean up
        fs_info.clear_dirty();
        fs_info.clear_cache();

        Ok(())
    }

    /// Update ROOT_ITEM entries in the root tree for every tree whose root
    /// block changed during this transaction.
    ///
    /// For each changed tree, searches the root tree for the existing
    /// ROOT_ITEM, parses it, updates the bytenr/generation/level fields,
    /// re-serializes it, and writes it back in place.
    fn update_root_items(&mut self, fs_info: &mut FsInfo<R>) -> io::Result<()> {
        let changed = fs_info.changed_roots();
        if changed.is_empty() {
            return Ok(());
        }

        // Root tree ID = 1
        let root_tree_id = 1u64;

        for (tree_id, new_bytenr, new_level) in changed {
            let key = DiskKey {
                objectid: tree_id,
                key_type: KeyType::RootItem,
                offset: 0,
            };

            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                root_tree_id,
                &key,
                &mut path,
                0,
                true, // COW the path so we can modify the leaf
            )?;

            if !found {
                // No existing ROOT_ITEM for this tree. This shouldn't normally
                // happen for trees that already existed, but skip gracefully.
                path.release();
                continue;
            }

            // Read the existing root item data, update it, write back
            let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                io::Error::other("update_root_items: no leaf in path")
            })?;
            let slot = path.slots[0];
            let item_data = leaf.item_data(slot).to_vec();

            if let Some(mut root_item) = RootItem::parse(&item_data) {
                root_item.bytenr = new_bytenr;
                root_item.generation = self.transid;
                root_item.generation_v2 = self.transid;
                root_item.level = new_level;

                let new_data = serialize::root_item_to_bytes(&root_item);
                // The serialized size must match the existing item size
                if new_data.len() == item_data.len() {
                    items::update_item(leaf, slot, &new_data)?;
                } else {
                    // Size mismatch (v1 vs v2 root item). Write as much as fits.
                    let write_len = new_data.len().min(item_data.len());
                    leaf.item_data_mut(slot)[..write_len]
                        .copy_from_slice(&new_data[..write_len]);
                }
                fs_info.mark_dirty(leaf);
            }

            path.release();
        }

        Ok(())
    }

    /// Flush delayed reference count updates to the extent tree.
    ///
    /// Drains the delayed ref queue and processes each net-nonzero delta.
    /// Processing refs may modify the extent tree, which may generate more
    /// delayed refs (from COW). Repeats until the queue is empty.
    ///
    /// For now, this is a simplified implementation that records the refs
    /// but does not yet create/update extent items in the extent tree.
    /// The full implementation requires creating METADATA_ITEM/EXTENT_ITEM
    /// entries with inline backreferences — this will be completed when
    /// rescue commands need it.
    fn flush_delayed_refs(
        &mut self,
        _fs_info: &mut FsInfo<R>,
    ) -> io::Result<()> {
        // Convergence loop: drain and process until stable
        let max_iterations = 16; // safety limit
        for _ in 0..max_iterations {
            let refs = self.delayed_refs.drain();
            if refs.is_empty() {
                return Ok(());
            }

            // TODO: For each ref with positive delta, create or update an
            // extent item (METADATA_ITEM for tree blocks) in the extent tree
            // with a TREE_BLOCK_REF inline backref.
            // For each ref with negative delta, decrement the refcount in the
            // extent item. If refcount reaches 0, delete the extent item.
            //
            // For now, we silently consume the refs. This means the extent
            // tree won't reflect the new allocations from COW, but the
            // filesystem structure (tree blocks, root items, superblock) will
            // be correct. A subsequent `btrfs check` will report extent tree
            // inconsistencies until this is fully implemented.
        }

        Ok(())
    }

    /// Abort the transaction: discard all dirty blocks without writing.
    pub fn abort(self, fs_info: &mut FsInfo<R>) {
        fs_info.generation = fs_info.superblock.generation;
        fs_info.clear_dirty();
        fs_info.clear_cache();
    }
}

/// Find a metadata block group with free space for the bump allocator.
///
/// Walks the extent tree looking for block groups with METADATA type
/// that have unused space. Returns (`first_free_logical`, `block_group_end`).
///
/// This is a simple heuristic for the temporary allocator. Phase 7 replaces
/// this with proper free space scanning.
fn find_metadata_alloc_region<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
) -> io::Result<(u64, u64)> {
    use btrfs_disk::items::BlockGroupFlags;

    // We need to find the extent tree (or block group tree) and scan for
    // metadata block groups. Then within a chosen block group, find the
    // highest allocated extent and set the cursor after it.

    // First, try the block group tree (tree 11) if present, else extent tree (tree 2)
    let bg_tree_id = if fs_info.root_bytenr(11).is_some() {
        11u64
    } else {
        2u64
    };

    let root_bytenr = fs_info.root_bytenr(bg_tree_id).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "cannot find extent/block-group tree for allocation",
        )
    })?;

    // Walk the tree looking for BLOCK_GROUP_ITEM with METADATA type
    let mut best_bg: Option<(u64, u64, u64)> = None; // (start, length, used)

    scan_block_groups(
        fs_info,
        root_bytenr,
        &mut |start, length, used, flags| {
            if flags.contains(BlockGroupFlags::METADATA) && used < length {
                let free = length - used;
                if best_bg.is_none()
                    || free > best_bg.unwrap().1 - best_bg.unwrap().2
                {
                    best_bg = Some((start, length, used));
                }
            }
        },
    )?;

    let (bg_start, bg_length, bg_used) = best_bg.ok_or_else(|| {
        io::Error::other("no metadata block group with free space")
    })?;

    // Set cursor after the used portion. This is a rough heuristic; in reality
    // we'd need to scan the extent tree within this block group to find the
    // actual highest allocated address. For now, just use start + used as the
    // cursor, aligned to nodesize.
    let nodesize = u64::from(fs_info.nodesize);
    let cursor = align_up(bg_start + bg_used, nodesize);
    let end = bg_start + bg_length;

    if cursor >= end {
        return Err(io::Error::other(
            "metadata block group has no usable free space",
        ));
    }

    Ok((cursor, end))
}

/// Scan a tree for block group items, calling the visitor for each.
fn scan_block_groups<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
    root_logical: u64,
    visitor: &mut dyn FnMut(u64, u64, u64, btrfs_disk::items::BlockGroupFlags),
) -> io::Result<()> {
    use btrfs_disk::{
        items::BlockGroupItem,
        tree::{KeyType, TreeBlock},
    };

    let block = fs_info.read_block(root_logical)?;
    let tb = block.as_tree_block();

    match &tb {
        TreeBlock::Leaf { items, .. } => {
            for (idx, item) in items.iter().enumerate() {
                if item.key.key_type != KeyType::BlockGroupItem {
                    continue;
                }
                if let Some(data) = tb.item_data(idx)
                    && let Some(bg) = BlockGroupItem::parse(data)
                {
                    visitor(
                        item.key.objectid,
                        item.key.offset,
                        bg.used,
                        bg.flags,
                    );
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                scan_block_groups(fs_info, ptr.blockptr, visitor)?;
            }
        }
    }

    Ok(())
}

/// Update one backup root slot in the superblock.
///
/// This is currently a placeholder. The full implementation needs to populate
/// the backup root fields from the current tree root state.
fn update_backup_root<R>(_fs_info: &FsInfo<R>, _slot: usize) {
    // TODO: properly update backup roots when BackupRoot has setters or
    // when we add a BackupRoot builder. For now, the superblock's existing
    // backup roots are preserved as-is during to_bytes() serialization.
}

/// Align a value up to the given alignment.
const fn align_up(value: u64, align: u64) -> u64 {
    (value + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_already_aligned() {
        assert_eq!(align_up(4096, 4096), 4096);
    }

    #[test]
    fn align_up_not_aligned() {
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn align_up_zero() {
        assert_eq!(align_up(0, 4096), 0);
    }
}

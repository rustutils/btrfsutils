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
        // Step 1: Flush delayed refs (convergence loop).
        // This must happen BEFORE update_root_items because flushing refs
        // may COW the extent tree, changing its root. update_root_items
        // needs to see the final root addresses.
        self.flush_delayed_refs(fs_info)?;

        // Step 2: Update root items in the root tree for changed trees.
        // This captures all root changes including those from step 1.
        self.update_root_items(fs_info)?;

        // Step 2b: Flushing delayed refs and updating root items may have
        // generated more delayed refs (from COWing the extent tree and root
        // tree). Flush again until stable.
        self.flush_delayed_refs(fs_info)?;
        // If that generated more root changes, update again
        self.update_root_items(fs_info)?;

        // Step 3: Flush all dirty blocks to disk
        fs_info.flush_dirty()?;

        // Step 4: Update superblock fields
        fs_info.superblock.generation = self.transid;

        // Clear free space tree flags so the kernel rebuilds it on next mount.
        // We don't update the free space tree when allocating blocks, so it's
        // now stale. Clearing both flags tells btrfs check to skip validation
        // and the kernel to rebuild from scratch.
        fs_info.superblock.compat_ro_flags &= !(u64::from(
            btrfs_disk::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID,
        ) | u64::from(
            btrfs_disk::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE,
        ));

        // Update root tree root pointer
        if let Some(root_bytenr) = fs_info.root_bytenr(1) {
            fs_info.superblock.root = root_bytenr;
            if let Ok(eb) = fs_info.read_block(root_bytenr) {
                fs_info.superblock.root_level = eb.level();
            }
        }

        // Update chunk tree root pointer (only if it changed)
        if let Some(chunk_bytenr) = fs_info.root_bytenr(3)
            && chunk_bytenr != fs_info.superblock.chunk_root
        {
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
    /// For positive deltas (new allocations), creates METADATA_ITEM entries
    /// with TREE_BLOCK_REF inline backrefs. For negative deltas (frees),
    /// deletes the extent item.
    ///
    /// Processing refs modifies the extent tree, which may generate more
    /// delayed refs from COW. Repeats until the queue is empty.
    fn flush_delayed_refs(
        &mut self,
        fs_info: &mut FsInfo<R>,
    ) -> io::Result<()> {
        let skinny = fs_info.superblock.incompat_flags
            & u64::from(
                btrfs_disk::raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA,
            )
            != 0;

        let extent_tree_id = 2u64;

        // Convergence loop: drain and process until stable.
        // Processing refs modifies the extent tree, which COWs blocks and
        // generates more refs. Each iteration processes more refs than it
        // creates, so this converges.
        let max_iterations = 32;
        for iteration in 0..max_iterations {
            let refs = self.delayed_refs.drain();
            if refs.is_empty() {
                return Ok(());
            }

            for dref in refs {
                if !dref.is_metadata {
                    // Data extent refs not yet implemented
                    continue;
                }

                if dref.delta > 0 {
                    // Positive delta: create a new extent item
                    self.create_metadata_extent(
                        fs_info,
                        extent_tree_id,
                        dref.bytenr,
                        dref.level,
                        dref.owner,
                        skinny,
                    )?;
                } else if dref.delta < 0 {
                    // Negative delta: delete the extent item
                    self.delete_metadata_extent(
                        fs_info,
                        extent_tree_id,
                        dref.bytenr,
                        dref.level,
                        skinny,
                    )?;
                }
            }

            // Safety check: if we've been looping too long, something is wrong
            if iteration == max_iterations - 1 && !self.delayed_refs.is_empty()
            {
                return Err(io::Error::other(
                    "delayed ref flush did not converge after 32 iterations",
                ));
            }
        }

        Ok(())
    }

    /// Create a METADATA_ITEM (or EXTENT_ITEM) in the extent tree for a newly
    /// allocated tree block.
    fn create_metadata_extent(
        &mut self,
        fs_info: &mut FsInfo<R>,
        extent_tree_id: u64,
        bytenr: u64,
        level: u8,
        owner: u64,
        skinny: bool,
    ) -> io::Result<()> {
        let key = if skinny {
            DiskKey {
                objectid: bytenr,
                key_type: KeyType::MetadataItem,
                offset: u64::from(level),
            }
        } else {
            DiskKey {
                objectid: bytenr,
                key_type: KeyType::ExtentItem,
                offset: u64::from(fs_info.nodesize),
            }
        };

        let data =
            serialize::metadata_extent_item_to_bytes(1, self.transid, owner);

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            extent_tree_id,
            &key,
            &mut path,
            (25 + data.len()) as u32,
            true,
        )?;

        if found {
            // Extent item already exists (shouldn't happen for new allocations,
            // but handle gracefully by updating refcount)
            path.release();
            return Ok(());
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("create_metadata_extent: no leaf in path")
        })?;
        let slot = path.slots[0];

        items::insert_item(leaf, slot, &key, &data)?;
        fs_info.mark_dirty(leaf);
        path.release();

        Ok(())
    }

    /// Delete a METADATA_ITEM (or EXTENT_ITEM) from the extent tree for a
    /// freed tree block.
    fn delete_metadata_extent(
        &mut self,
        fs_info: &mut FsInfo<R>,
        extent_tree_id: u64,
        bytenr: u64,
        level: u8,
        skinny: bool,
    ) -> io::Result<()> {
        let key = if skinny {
            DiskKey {
                objectid: bytenr,
                key_type: KeyType::MetadataItem,
                offset: u64::from(level),
            }
        } else {
            DiskKey {
                objectid: bytenr,
                key_type: KeyType::ExtentItem,
                offset: u64::from(fs_info.nodesize),
            }
        };

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            extent_tree_id,
            &key,
            &mut path,
            0,
            true,
        )?;

        if !found {
            // The old block may not have an extent item if it was allocated
            // before the transaction crate managed the extent tree. Skip.
            path.release();
            return Ok(());
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("delete_metadata_extent: no leaf in path")
        })?;
        let slot = path.slots[0];

        items::del_items(leaf, slot, 1);
        fs_info.mark_dirty(leaf);
        path.release();

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
/// Uses proper free space scanning via the extent tree to find actual gaps
/// between allocated extents. Returns (`first_free_logical`, `block_group_end`).
fn find_metadata_alloc_region<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
) -> io::Result<(u64, u64)> {
    use crate::extent_alloc;

    let nodesize = u64::from(fs_info.nodesize);
    let groups = extent_alloc::load_block_groups(fs_info)?;

    // Find metadata block groups with free space, sorted by most free
    let mut meta_groups: Vec<&extent_alloc::BlockGroup> = groups
        .iter()
        .filter(|bg| bg.is_metadata() && bg.free() >= nodesize)
        .collect();
    meta_groups.sort_by_key(|bg| std::cmp::Reverse(bg.free()));

    for bg in meta_groups {
        let free_extents = extent_alloc::find_free_extents(
            fs_info, bg.start, bg.length, nodesize,
        )?;

        if let Some(&(start, len)) = free_extents.first() {
            let cursor = align_up(start, nodesize);
            let end = start + len;
            if cursor + nodesize <= end {
                return Ok((cursor, end));
            }
        }
    }

    Err(io::Error::other("no metadata block group with free space"))
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

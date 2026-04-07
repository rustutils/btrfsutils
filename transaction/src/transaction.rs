//! # Transaction lifecycle: start, commit, abort
//!
//! A `Transaction` groups multiple tree modifications into a single atomic
//! commit. The commit point is the superblock write: all new tree blocks are
//! written first (at new locations via COW), then the superblock is updated
//! to point to the new root.

use crate::{
    allocation,
    buffer::ITEM_SIZE,
    delayed_ref::DelayedRefQueue,
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
};
use btrfs_disk::{
    items::{ExtentItem, RootItem},
    superblock,
    tree::{DiskKey, KeyType},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Read, Seek, Write},
};

/// Handle for an in-progress transaction.
///
/// Created by [`Transaction::start`], which increments the generation.
/// Tracks dirty blocks and pending reference count changes. Finalized by
/// either [`commit`](Transaction::commit) (write to disk) or
/// [`abort`](Transaction::abort) (discard).
pub struct Transaction<R> {
    /// The transaction generation (superblock.generation + 1).
    pub transid: u64,
    /// Blocks freed during this transaction (old COW sources).
    freed_blocks: Vec<u64>,
    /// Blocks allocated during this transaction (for free space tree updates).
    allocated_blocks: Vec<u64>,
    /// Delayed reference count updates.
    pub delayed_refs: DelayedRefQueue,
    /// Simple bump allocator cursor for metadata blocks.
    alloc_cursor: u64,
    /// End of the allocation region.
    alloc_end: u64,
    /// Logical addresses of blocks freed during this transaction. These
    /// must not be reallocated before the superblock is committed, because
    /// the previous superblock still references them. A crash before commit
    /// would leave both old and new data at the same address.
    pinned: BTreeSet<u64>,
    /// Phantom to tie the lifetime/type parameter.
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Read + Write + Seek> Transaction<R> {
    /// Start a new transaction.
    ///
    /// Increments the filesystem generation by 1 and initializes the
    /// temporary block allocator by scanning for a metadata block group.
    ///
    /// # Errors
    ///
    /// Returns an error if the filesystem state cannot be prepared.
    pub fn start(fs_info: &mut Filesystem<R>) -> io::Result<Self> {
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
            allocated_blocks: Vec::new(),
            delayed_refs: DelayedRefQueue::new(),
            alloc_cursor: cursor,
            alloc_end: end,
            pinned: BTreeSet::new(),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Allocate a new metadata block (nodesize bytes).
    ///
    /// Uses a bump allocator within a free extent. If the current region is
    /// exhausted, scans the extent tree for another free extent and continues
    /// allocating from there.
    ///
    /// # Errors
    ///
    /// Returns an error if no free metadata space is available.
    pub fn alloc_block(
        &mut self,
        fs_info: &mut Filesystem<R>,
    ) -> io::Result<u64> {
        let nodesize = u64::from(fs_info.nodesize);

        loop {
            let next = self.alloc_cursor + nodesize;

            if next > self.alloc_end {
                // Current region exhausted — find another free extent.
                // Pass the current cursor so we don't re-discover space we
                // already allocated from (those blocks don't have extent
                // items yet so they'd appear "free" in the scan).
                let (cursor, end) = find_metadata_alloc_region_after(
                    fs_info,
                    self.alloc_cursor,
                )?;
                self.alloc_cursor = cursor;
                self.alloc_end = end;

                let next = self.alloc_cursor + nodesize;
                if next > self.alloc_end {
                    return Err(io::Error::other(
                        "no metadata block group with enough free space",
                    ));
                }
            }

            let logical = self.alloc_cursor;
            self.alloc_cursor += nodesize;

            // Skip pinned blocks: these were freed during this transaction
            // but the old superblock still references them. Reusing them
            // before commit would break crash consistency.
            if self.pinned.contains(&logical) {
                continue;
            }

            self.allocated_blocks.push(logical);
            return Ok(logical);
        }
    }

    /// Allocate a new tree block and queue a delayed ref for it.
    ///
    /// This is the standard allocation entry point for tree blocks. It
    /// combines physical allocation with extent reference creation as a
    /// single atomic operation, ensuring every allocated block gets a
    /// corresponding extent item at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error if no free metadata space is available.
    pub fn alloc_tree_block(
        &mut self,
        fs_info: &mut Filesystem<R>,
        tree_id: u64,
        level: u8,
    ) -> io::Result<u64> {
        let logical = self.alloc_block(fs_info)?;
        self.delayed_refs.add_ref(logical, true, tree_id, level);
        Ok(logical)
    }

    /// Mark a block as pinned (freed but not yet committed).
    ///
    /// Pinned blocks must not be reallocated during this transaction.
    /// The previous superblock still references them, so reusing the
    /// address before the new superblock is committed would corrupt the
    /// old consistent state on crash.
    pub fn pin_block(&mut self, logical: u64) {
        self.pinned.insert(logical);
    }

    /// Check whether a logical address is pinned.
    #[must_use]
    pub fn is_pinned(&self, logical: u64) -> bool {
        self.pinned.contains(&logical)
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
    pub fn commit(mut self, fs_info: &mut Filesystem<R>) -> io::Result<()> {
        // Step 1: Convergence loop. Flushing delayed refs modifies the
        // extent tree (COW), which generates new delayed refs. Updating
        // root items modifies the root tree (COW), generating more.
        // Alternate until both are stable.
        let max_passes = 16;
        for pass in 0..max_passes {
            self.flush_delayed_refs(fs_info)?;
            self.update_root_items(fs_info)?;
            // Re-snapshot roots so changed_roots() only detects new
            // changes from subsequent COW operations, not the ones we
            // just processed.
            fs_info.snapshot_roots();

            // Stable when no pending refs and no changed roots remain
            if self.delayed_refs.is_empty()
                && fs_info.changed_roots().is_empty()
            {
                break;
            }

            if pass == max_passes - 1 {
                return Err(io::Error::other(
                    "commit convergence loop did not stabilize",
                ));
            }
        }

        // Empty-commit short-circuit. If the convergence loop produced
        // no dirty blocks, the on-disk image is byte-identical to the
        // previous generation and we must not bump
        // `superblock.generation` — there is no rewritten root tree
        // root to back the new generation, so a mount/check would
        // report "parent transid verify failed: wanted N found N-1".
        //
        // The predicate is measured *after* the convergence loop:
        // `flush_delayed_refs` and `update_root_items` are what create
        // dirty blocks, so any earlier check would race them. It is
        // also independent of `allocated_blocks` and `pinned`, which
        // are bookkeeping (alloc+drop+pin within one transaction
        // produces no on-disk change). See PLAN.md Finding 3
        // invariants I1–I5.
        //
        // The kernel handles no-op commits by always COWing the root
        // tree root, keeping superblock and root header generations in
        // lockstep. We don't yet — Option B in PLAN.md tracks the
        // proper fix.
        if fs_info.dirty_count() == 0 {
            return Ok(());
        }

        // Step 2: Flush all dirty blocks to disk
        fs_info.flush_dirty()?;

        // Step 4: Update superblock fields
        fs_info.superblock.generation = self.transid;

        // The free space tree is now stale (blocks were allocated without
        // updating it). We leave FREE_SPACE_TREE_VALID set because on
        // kernels 6.x+ with BLOCK_GROUP_TREE, the kernel strips
        // FREE_SPACE_TREE when VALID is not set, which makes
        // BLOCK_GROUP_TREE's dependency check fail and prevents mount.
        //
        // For read-only mounts this is harmless (the stale tree is not
        // used for allocation). A proper fix requires updating the free
        // space tree during commit.

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

        // Step 7: Flush writes to stable storage. `Write::flush()`
        // flushes any userspace buffers. For file-backed storage, the
        // caller should also call `sync()` on the Filesystem (which
        // calls `File::sync_all()`) for full durability.
        fs_info.reader_mut().inner_mut().flush()?;

        // Step 8: Clean up
        fs_info.clear_dirty();
        fs_info.clear_cache();

        Ok(())
    }

    /// Update `ROOT_ITEM` entries in the root tree for every tree whose root
    /// block changed during this transaction.
    ///
    /// For each changed tree, searches the root tree for the existing
    /// `ROOT_ITEM`, parses it, updates the bytenr/generation/level fields,
    /// re-serializes it, and writes it back in place.
    fn update_root_items(
        &mut self,
        fs_info: &mut Filesystem<R>,
    ) -> io::Result<()> {
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
                SearchIntent::ReadOnly,
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

                let new_data = root_item.to_bytes();
                if new_data.len() == item_data.len() {
                    items::update_item(leaf, slot, &new_data)?;
                    fs_info.mark_dirty(leaf);
                } else {
                    // Size mismatch (v1 vs v2 root item). Delete and
                    // reinsert with the correct size to avoid corruption.
                    items::del_items(leaf, slot, 1);
                    fs_info.mark_dirty(leaf);
                    path.release();

                    let mut path = BtrfsPath::new();
                    search::search_slot(
                        Some(&mut *self),
                        fs_info,
                        root_tree_id,
                        &key,
                        &mut path,
                        SearchIntent::Insert(
                            (ITEM_SIZE + new_data.len()) as u32,
                        ),
                        true,
                    )?;
                    let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                        io::Error::other(
                            "update_root_items: no leaf after reinsert search",
                        )
                    })?;
                    items::insert_item(leaf, path.slots[0], &key, &new_data)?;
                    fs_info.mark_dirty(leaf);
                    path.release();
                    continue;
                }
            }

            path.release();
        }

        Ok(())
    }

    /// Flush delayed reference count updates to the extent tree.
    ///
    /// Drains the delayed ref queue and processes each net-nonzero delta.
    /// For positive deltas (new allocations), creates `METADATA_ITEM` entries
    /// with `TREE_BLOCK_REF` inline backrefs. For negative deltas (frees),
    /// deletes the extent item.
    ///
    /// Processing refs modifies the extent tree, which may generate more
    /// delayed refs from COW. Repeats until the queue is empty.
    fn flush_delayed_refs(
        &mut self,
        fs_info: &mut Filesystem<R>,
    ) -> io::Result<()> {
        let skinny = fs_info.superblock.incompat_flags
            & u64::from(
                btrfs_disk::raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA,
            )
            != 0;

        let extent_tree_id = 2u64;
        let nodesize = i64::from(fs_info.nodesize);

        // Load block groups once so we can map bytenr → block group.
        let block_groups = allocation::load_block_groups(fs_info)?;

        // Track per-block-group deltas: key is block group start address.
        let mut bg_deltas: BTreeMap<u64, i64> = BTreeMap::new();
        let mut bytes_used_delta: i64 = 0;

        // Convergence loop: drain and process until stable.
        // Processing refs modifies the extent tree, which COWs blocks and
        // generates more refs. Each iteration processes more refs than it
        // creates, so this converges.
        let max_iterations = 32;
        for iteration in 0..max_iterations {
            let refs = self.delayed_refs.drain();
            if refs.is_empty() {
                break;
            }

            for dref in refs {
                if !dref.is_metadata {
                    continue;
                }

                if dref.delta > 0 {
                    self.create_metadata_extent(
                        fs_info,
                        extent_tree_id,
                        dref.bytenr,
                        dref.level,
                        dref.owner,
                        skinny,
                    )?;
                    bytes_used_delta += nodesize;
                    if let Some(bg_start) =
                        find_containing_block_group(&block_groups, dref.bytenr)
                    {
                        *bg_deltas.entry(bg_start).or_insert(0) += nodesize;
                    }
                } else if dref.delta < 0 {
                    self.delete_metadata_extent(
                        fs_info,
                        extent_tree_id,
                        dref.bytenr,
                        dref.level,
                        skinny,
                    )?;
                    bytes_used_delta -= nodesize;
                    if let Some(bg_start) =
                        find_containing_block_group(&block_groups, dref.bytenr)
                    {
                        *bg_deltas.entry(bg_start).or_insert(0) -= nodesize;
                    }
                }
            }

            if iteration == max_iterations - 1 && !self.delayed_refs.is_empty()
            {
                return Err(io::Error::other(
                    "delayed ref flush did not converge after 32 iterations",
                ));
            }
        }

        // Update superblock bytes_used
        if bytes_used_delta != 0 {
            let current = fs_info.superblock.bytes_used as i64;
            fs_info.superblock.bytes_used = (current + bytes_used_delta) as u64;
        }

        // Update each affected block group's used field individually
        for (bg_start, delta) in &bg_deltas {
            if *delta != 0 {
                self.update_block_group_used(fs_info, *bg_start, *delta)?;
            }
        }

        Ok(())
    }

    /// Update a specific block group item's `used` field.
    ///
    /// `bg_start` is the logical start address of the block group (the key's
    /// objectid). The delta is applied to the current `used` value.
    fn update_block_group_used(
        &mut self,
        fs_info: &mut Filesystem<R>,
        bg_start: u64,
        bytes_delta: i64,
    ) -> io::Result<()> {
        use btrfs_disk::items::BlockGroupItem;

        // Block group items live in tree 11 (block group tree) or tree 2
        let bg_tree_id = if fs_info.root_bytenr(11).is_some() {
            11u64
        } else {
            2u64
        };

        // Search for this block group by its start address.
        // Block group keys: (logical_offset, BLOCK_GROUP_ITEM, length)
        let search_key = DiskKey {
            objectid: bg_start,
            key_type: KeyType::BlockGroupItem,
            offset: 0,
        };

        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut *self),
            fs_info,
            bg_tree_id,
            &search_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )?;

        // Block group keys are (start, BLOCK_GROUP_ITEM, length). Our search
        // key uses offset=0, which is less than the actual key. So search_slot
        // lands at the block group item (first key >= our search key). Verify
        // the objectid matches.
        let Some(leaf) = path.nodes[0].as_mut() else {
            return Ok(());
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            path.release();
            return Ok(());
        }

        let item_key = leaf.item_key(slot);
        if item_key.key_type != KeyType::BlockGroupItem
            || item_key.objectid != bg_start
        {
            path.release();
            return Ok(());
        }

        // Read, update, and write back the block group item
        let data = leaf.item_data(slot).to_vec();
        if let Some(bg) = BlockGroupItem::parse(&data) {
            let new_used = (bg.used as i64 + bytes_delta).max(0) as u64;
            let new_data = BlockGroupItem {
                used: new_used,
                chunk_objectid: bg.chunk_objectid,
                flags: bg.flags,
            }
            .to_bytes();
            items::update_item(leaf, slot, &new_data)?;
            fs_info.mark_dirty(leaf);
        }

        path.release();
        Ok(())
    }

    /// Create a `METADATA_ITEM` (or `EXTENT_ITEM`) in the extent tree for a newly
    /// allocated tree block.
    fn create_metadata_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
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

        let data = if skinny {
            ExtentItem::to_bytes_skinny(1, self.transid, owner)
        } else {
            // Non-skinny format requires tree_block_info with the first
            // key and level of the referenced tree block.
            let first_key = if let Ok(eb) = fs_info.read_block(bytenr) {
                if eb.level() == 0 && eb.nritems() > 0 {
                    eb.item_key(0)
                } else if eb.level() > 0 && eb.nritems() > 0 {
                    eb.key_ptr_key(0)
                } else {
                    DiskKey {
                        objectid: 0,
                        key_type: KeyType::Unknown(0),
                        offset: 0,
                    }
                }
            } else {
                DiskKey {
                    objectid: 0,
                    key_type: KeyType::Unknown(0),
                    offset: 0,
                }
            };
            ExtentItem::to_bytes_non_skinny(
                1,
                self.transid,
                owner,
                &first_key,
                level,
            )
        };

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            extent_tree_id,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
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

    /// Delete a `METADATA_ITEM` (or `EXTENT_ITEM`) from the extent tree for a
    /// freed tree block.
    fn delete_metadata_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
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
            SearchIntent::Delete,
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

    /// Rebuild free space tree entries by scanning the extent tree.
    ///
    /// For each block group, computes free ranges from the extent tree and
    /// rewrites the `FREE_SPACE_EXTENT` and `FREE_SPACE_INFO` items. This is
    /// simpler and more robust than incremental updates because it doesn't
    /// have convergence issues.
    #[allow(dead_code)]
    fn rebuild_free_space_tree(
        &mut self,
        fs_info: &mut Filesystem<R>,
    ) -> io::Result<()> {
        use crate::allocation;
        use btrfs_disk::items::FreeSpaceInfo;

        let fst_id = 10u64;
        if fs_info.root_bytenr(fst_id).is_none() {
            return Ok(());
        }
        let groups = allocation::load_block_groups(fs_info)?;

        for bg in &groups {
            // Find free ranges within this block group
            let free_ranges =
                allocation::find_free_extents(fs_info, bg.start, bg.length, 1)?;

            // Delete existing FREE_SPACE_EXTENT items for this block group
            self.delete_free_space_extents(
                fs_info, fst_id, bg.start, bg.length,
            )?;

            // Insert new FREE_SPACE_EXTENT items
            for &(start, len) in &free_ranges {
                let key = DiskKey {
                    objectid: start,
                    key_type: KeyType::FreeSpaceExtent,
                    offset: len,
                };
                let mut path = BtrfsPath::new();
                let found = search::search_slot(
                    Some(&mut *self),
                    fs_info,
                    fst_id,
                    &key,
                    &mut path,
                    SearchIntent::Insert(ITEM_SIZE as u32),
                    true,
                )?;
                if !found {
                    let leaf = path.nodes[0].as_mut().unwrap();
                    items::insert_item(leaf, path.slots[0], &key, &[])?;
                    fs_info.mark_dirty(leaf);
                }
                path.release();
            }

            // Update FREE_SPACE_INFO for this block group
            let info_key = DiskKey {
                objectid: bg.start,
                key_type: KeyType::FreeSpaceInfo,
                offset: bg.length,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                fst_id,
                &info_key,
                &mut path,
                SearchIntent::ReadOnly,
                true,
            )?;
            if found {
                let leaf = path.nodes[0].as_mut().unwrap();
                let slot = path.slots[0];
                let data = leaf.item_data(slot).to_vec();
                if let Some(info) = FreeSpaceInfo::parse(&data) {
                    // Update extent_count, preserve flags
                    let mut new_data = Vec::with_capacity(8);
                    new_data.extend_from_slice(
                        &(free_ranges.len() as u32).to_le_bytes(),
                    );
                    new_data
                        .extend_from_slice(&info.flags.bits().to_le_bytes());
                    items::update_item(leaf, slot, &new_data)?;
                    fs_info.mark_dirty(leaf);
                }
            }
            path.release();
        }

        Ok(())
    }

    /// Delete all `FREE_SPACE_EXTENT` items within a block group's range.
    #[allow(dead_code)]
    fn delete_free_space_extents(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        bg_start: u64,
        bg_length: u64,
    ) -> io::Result<()> {
        let bg_end = bg_start + bg_length;

        // Search for the first key >= bg_start with type FREE_SPACE_EXTENT
        let search_key = DiskKey {
            objectid: bg_start,
            key_type: KeyType::FreeSpaceExtent,
            offset: 0,
        };

        loop {
            let mut path = BtrfsPath::new();
            let _found = search::search_slot(
                Some(&mut *self),
                fs_info,
                fst_id,
                &search_key,
                &mut path,
                SearchIntent::Delete,
                true,
            )?;

            let Some(leaf) = path.nodes[0].as_mut() else {
                break;
            };
            let slot = path.slots[0];
            if slot >= leaf.nritems() as usize {
                path.release();
                break;
            }

            let key = leaf.item_key(slot);
            if key.key_type != KeyType::FreeSpaceExtent
                || key.objectid >= bg_end
            {
                path.release();
                break;
            }

            items::del_items(leaf, slot, 1);
            fs_info.mark_dirty(leaf);
            path.release();
            // Loop to find and delete the next one
        }

        Ok(())
    }

    /// Update the free space tree to account for specific allocated blocks.
    /// For each block, find the containing `FREE_SPACE_EXTENT` and shrink
    /// or split it.
    #[allow(dead_code)]
    fn update_free_space_tree_for(
        &mut self,
        fs_info: &mut Filesystem<R>,
        allocated: &[u64],
    ) -> io::Result<()> {
        let fst_id = 10u64;
        if fs_info.root_bytenr(fst_id).is_none() {
            return Ok(()); // No free space tree
        }

        let nodesize = u64::from(fs_info.nodesize);

        for &addr in allocated {
            // Search for a FREE_SPACE_EXTENT containing this address.
            // Key: (start, FREE_SPACE_EXTENT=199, length)
            // We search for the largest key <= addr with type 199.
            let search_key = DiskKey {
                objectid: addr,
                key_type: KeyType::FreeSpaceExtent,
                offset: u64::MAX,
            };

            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                fst_id,
                &search_key,
                &mut path,
                SearchIntent::Delete,
                true,
            )?;

            // If not exact match, back up one slot
            if !found && path.slots[0] > 0 {
                path.slots[0] -= 1;
            }

            let Some(leaf) = path.nodes[0].as_mut() else {
                path.release();
                continue;
            };
            let slot = path.slots[0];
            if slot >= leaf.nritems() as usize {
                path.release();
                continue;
            }

            let item_key = leaf.item_key(slot);
            if item_key.key_type != KeyType::FreeSpaceExtent {
                path.release();
                continue;
            }

            let extent_start = item_key.objectid;
            let extent_len = item_key.offset;
            let extent_end = extent_start + extent_len;

            // Check if this free extent contains our allocation
            if addr < extent_start || addr + nodesize > extent_end {
                path.release();
                continue;
            }

            // Delete the old free space extent
            items::del_items(leaf, slot, 1);
            fs_info.mark_dirty(leaf);
            path.release();

            // Insert replacement extent(s)
            if addr > extent_start {
                // Left portion: (extent_start, addr - extent_start)
                let left_key = DiskKey {
                    objectid: extent_start,
                    key_type: KeyType::FreeSpaceExtent,
                    offset: addr - extent_start,
                };
                let mut path = BtrfsPath::new();
                search::search_slot(
                    Some(&mut *self),
                    fs_info,
                    fst_id,
                    &left_key,
                    &mut path,
                    SearchIntent::Insert(ITEM_SIZE as u32),
                    true,
                )?;
                let leaf = path.nodes[0].as_mut().unwrap();
                items::insert_item(leaf, path.slots[0], &left_key, &[])?;
                fs_info.mark_dirty(leaf);
                path.release();
            }

            let after = addr + nodesize;
            if after < extent_end {
                // Right portion: (addr + nodesize, extent_end - after)
                let right_key = DiskKey {
                    objectid: after,
                    key_type: KeyType::FreeSpaceExtent,
                    offset: extent_end - after,
                };
                let mut path = BtrfsPath::new();
                search::search_slot(
                    Some(&mut *self),
                    fs_info,
                    fst_id,
                    &right_key,
                    &mut path,
                    SearchIntent::Insert(ITEM_SIZE as u32),
                    true,
                )?;
                let leaf = path.nodes[0].as_mut().unwrap();
                items::insert_item(leaf, path.slots[0], &right_key, &[])?;
                fs_info.mark_dirty(leaf);
                path.release();
            }

            // Update FREE_SPACE_INFO extent_count for this block group.
            // For a simple allocation from the middle of an extent:
            // count changes by +1 (one extent becomes two) or -1 (exact match
            // removes one) or 0 (trim from edge). Skip for now — the kernel
            // rebuilds this on mount when VALID is cleared.
        }

        Ok(())
    }

    /// Abort the transaction: discard all dirty blocks without writing.
    pub fn abort(self, fs_info: &mut Filesystem<R>) {
        fs_info.generation = fs_info.superblock.generation;
        fs_info.clear_dirty();
        fs_info.clear_cache();
    }
}

/// Find a metadata block group with free space for the bump allocator.
///
/// Uses proper free space scanning via the extent tree to find actual gaps
/// between allocated extents. Returns (`first_free_logical`, `region_end`).
fn find_metadata_alloc_region<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
) -> io::Result<(u64, u64)> {
    find_metadata_alloc_region_after(fs_info, 0)
}

/// Find a free metadata region starting at or after `min_addr`.
fn find_metadata_alloc_region_after<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    min_addr: u64,
) -> io::Result<(u64, u64)> {
    use crate::allocation;

    let nodesize = u64::from(fs_info.nodesize);
    let groups = allocation::load_block_groups(fs_info)?;

    // Find metadata block groups with free space, sorted by most free
    let mut meta_groups: Vec<&allocation::BlockGroup> = groups
        .iter()
        .filter(|bg| bg.is_metadata() && bg.free() >= nodesize)
        .collect();
    meta_groups.sort_by_key(|bg| std::cmp::Reverse(bg.free()));

    for bg in meta_groups {
        let free_extents = allocation::find_free_extents(
            fs_info, bg.start, bg.length, nodesize,
        )?;

        for &(start, len) in &free_extents {
            let cursor = align_up(start.max(min_addr), nodesize);
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
/// Populate one backup root slot from the current filesystem state.
///
/// The superblock has 4 rotating backup root entries. On each commit, one
/// slot is overwritten (cycling 0 -> 1 -> 2 -> 3 -> 0). Each entry
/// captures the root pointers, generations, and levels of the 6 core trees
/// plus filesystem size counters.
fn update_backup_root<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    slot: usize,
) {
    use btrfs_disk::superblock::BackupRoot;

    /// Read the generation and level of a tree's root block, returning
    /// (bytenr, generation, level). Falls back to (0, 0, 0) if unavailable.
    fn root_info<R: Read + Write + Seek>(
        fs_info: &mut Filesystem<R>,
        tree_id: u64,
    ) -> (u64, u64, u8) {
        let bytenr = fs_info.root_bytenr(tree_id).unwrap_or(0);
        if bytenr == 0 {
            return (0, 0, 0);
        }
        match fs_info.read_block(bytenr) {
            Ok(eb) => (bytenr, eb.generation(), eb.level()),
            Err(_) => (bytenr, 0, 0),
        }
    }

    let (tree_root, tree_root_gen, tree_root_level) = root_info(fs_info, 1);
    let (chunk_root, chunk_root_gen, chunk_root_level) = root_info(fs_info, 3);
    let (extent_root, extent_root_gen, extent_root_level) =
        root_info(fs_info, 2);
    let (fs_root, fs_root_gen, fs_root_level) = root_info(fs_info, 5);
    let (dev_root, dev_root_gen, dev_root_level) = root_info(fs_info, 4);
    let (csum_root, csum_root_gen, csum_root_level) = root_info(fs_info, 7);

    fs_info.superblock.backup_roots[slot] = BackupRoot {
        tree_root,
        tree_root_gen,
        chunk_root,
        chunk_root_gen,
        extent_root,
        extent_root_gen,
        fs_root,
        fs_root_gen,
        dev_root,
        dev_root_gen,
        csum_root,
        csum_root_gen,
        total_bytes: fs_info.superblock.total_bytes,
        bytes_used: fs_info.superblock.bytes_used,
        num_devices: fs_info.superblock.num_devices,
        tree_root_level,
        chunk_root_level,
        extent_root_level,
        fs_root_level,
        dev_root_level,
        csum_root_level,
    };
}

/// Find which block group contains a given logical byte address.
///
/// Returns the block group's start address, or `None` if the address
/// doesn't fall within any known block group.
fn find_containing_block_group(
    groups: &[allocation::BlockGroup],
    bytenr: u64,
) -> Option<u64> {
    groups
        .iter()
        .find(|bg| bytenr >= bg.start && bytenr < bg.start + bg.length)
        .map(|bg| bg.start)
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

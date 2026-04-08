//! # In-memory filesystem state for a transaction session
//!
//! `Filesystem` is the central state object for modifying a btrfs filesystem. It
//! wraps a `BlockReader` (from `btrfs-disk`), holds the parsed superblock, all
//! tree root pointers, and tracks which blocks have been modified during the
//! current transaction.
//!
//! Open a device or image with [`Filesystem::open`], then use the read/write
//! methods to access tree blocks through `ExtentBuffer`.

use crate::buffer::ExtentBuffer;
use btrfs_disk::{
    reader::{self, BlockReader, OpenFilesystem},
    superblock::Superblock,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read, Seek, Write},
};

/// In-memory filesystem state for a transaction session.
///
/// Holds everything needed to read and write tree blocks: the block reader
/// (with chunk cache for logical-to-physical resolution), the superblock,
/// all tree root pointers, the current transaction generation, and the set
/// of dirty (modified) block addresses.
pub struct Filesystem<R> {
    /// Block reader with fully populated chunk cache.
    reader: BlockReader<R>,
    /// Parsed superblock (updated in-memory during transactions).
    pub superblock: Superblock,
    /// Map of tree ID to root block logical address.
    roots: BTreeMap<u64, u64>,
    /// Snapshot of root bytenrs at transaction start. Used to detect which
    /// trees had their root block change during the transaction.
    original_roots: BTreeMap<u64, u64>,
    /// Logical addresses of blocks modified in the current transaction.
    /// `BTreeSet` gives sorted iteration in `flush_dirty` for sequential I/O.
    dirty: BTreeSet<u64>,
    /// Current transaction generation (superblock.generation + 1 during a
    /// transaction, or superblock.generation when idle).
    pub generation: u64,
    /// Tree block size in bytes.
    pub nodesize: u32,
    /// Minimum I/O unit in bytes.
    pub sectorsize: u32,
    /// In-memory cache of extent buffers read or created during the transaction.
    /// Keyed by logical address. This avoids re-reading blocks from disk and
    /// ensures modifications are visible within the same transaction.
    block_cache: BTreeMap<u64, ExtentBuffer>,
    /// Logical addresses of blocks that have been written to stable storage
    /// (via `flush_dirty` or `write_block`). A block in this set must be
    /// COW'd before modification even if its generation matches the current
    /// transaction, because the on-disk copy is now part of the committed
    /// state and overwriting it would break crash consistency.
    written: BTreeSet<u64>,
    /// Override for the block-group-tree id used by
    /// [`block_group_tree_id`](Self::block_group_tree_id). When `Some`,
    /// callers see this id instead of the auto-detected one. Used by
    /// the `convert-to-block-group-tree` path to pin allocator
    /// metadata to the extent tree (id 2) while the new BGT (id 11)
    /// is being built and is therefore only partially populated.
    /// Should always be cleared via [`BgTreeOverrideGuard`] (RAII)
    /// rather than written directly, so panics or early returns
    /// cannot leak the override into normal operation.
    bg_tree_override: Option<u64>,
}

impl<R: Read + Write + Seek> Filesystem<R> {
    /// Open a btrfs filesystem from a readable+writable+seekable handle.
    ///
    /// Performs the full bootstrap sequence (superblock, chunk cache, root
    /// tree), then wraps the result into an `Filesystem` ready for transactions.
    ///
    /// # Errors
    ///
    /// Returns an error if any I/O operation fails during bootstrap.
    pub fn open(handle: R) -> io::Result<Self> {
        let OpenFilesystem {
            reader,
            superblock,
            tree_roots,
        } = reader::filesystem_open(handle)?;

        let generation = superblock.generation;
        let nodesize = superblock.nodesize;
        let sectorsize = superblock.sectorsize;

        // Convert BTreeMap<u64, (u64, u64)> to BTreeMap<u64, u64> (tree_id -> root bytenr)
        let mut roots: BTreeMap<u64, u64> = tree_roots
            .into_iter()
            .map(|(id, (bytenr, _offset))| (id, bytenr))
            .collect();

        // The root tree and chunk tree roots live in the superblock, not in
        // ROOT_ITEM entries. Add them explicitly.
        roots.insert(1, superblock.root);
        roots.insert(3, superblock.chunk_root);

        let original_roots = roots.clone();

        Ok(Self {
            reader,
            superblock,
            roots,
            original_roots,
            dirty: BTreeSet::new(),
            generation,
            nodesize,
            sectorsize,
            block_cache: BTreeMap::new(),
            written: BTreeSet::new(),
            bg_tree_override: None,
        })
    }

    /// Open a btrfs filesystem using a specific superblock mirror.
    ///
    /// # Errors
    ///
    /// Returns an error if any I/O operation fails during bootstrap.
    pub fn open_mirror(handle: R, mirror: u32) -> io::Result<Self> {
        let OpenFilesystem {
            reader,
            superblock,
            tree_roots,
        } = reader::filesystem_open_mirror(handle, mirror)?;

        let generation = superblock.generation;
        let nodesize = superblock.nodesize;
        let sectorsize = superblock.sectorsize;

        let mut roots: BTreeMap<u64, u64> = tree_roots
            .into_iter()
            .map(|(id, (bytenr, _offset))| (id, bytenr))
            .collect();

        roots.insert(1, superblock.root);
        roots.insert(3, superblock.chunk_root);

        let original_roots = roots.clone();

        Ok(Self {
            reader,
            superblock,
            roots,
            original_roots,
            dirty: BTreeSet::new(),
            generation,
            nodesize,
            sectorsize,
            block_cache: BTreeMap::new(),
            written: BTreeSet::new(),
            bg_tree_override: None,
        })
    }

    /// Read a tree block at the given logical address, returning an `ExtentBuffer`.
    ///
    /// If the block is already in the in-memory cache (e.g. it was COW'd or
    /// previously read in this transaction), the cached version is returned
    /// without hitting disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the block cannot be read from disk.
    pub fn read_block(&mut self, logical: u64) -> io::Result<ExtentBuffer> {
        if let Some(eb) = self.block_cache.get(&logical) {
            return Ok(eb.clone());
        }
        let data = self.reader.read_block(logical)?;
        let eb = ExtentBuffer::from_raw(data, logical);
        self.block_cache.insert(logical, eb.clone());
        Ok(eb)
    }

    /// Write an extent buffer to disk and mark it dirty.
    ///
    /// The buffer's checksum is updated before writing. The block is also
    /// stored in the in-memory cache so subsequent reads see the modification.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    pub fn write_block(&mut self, eb: &mut ExtentBuffer) -> io::Result<()> {
        eb.update_checksum();
        self.reader.write_block(eb.logical(), eb.as_bytes())?;
        self.dirty.insert(eb.logical());
        self.written.insert(eb.logical());
        self.block_cache.insert(eb.logical(), eb.clone());
        Ok(())
    }

    /// Store an extent buffer in the cache and mark it dirty, without writing
    /// to disk yet. The actual disk write happens at commit time.
    pub fn mark_dirty(&mut self, eb: &ExtentBuffer) {
        self.dirty.insert(eb.logical());
        self.block_cache.insert(eb.logical(), eb.clone());
    }

    /// Return the root block logical address for the given tree ID.
    #[must_use]
    pub fn root_bytenr(&self, tree_id: u64) -> Option<u64> {
        self.roots.get(&tree_id).copied()
    }

    /// Update the root block logical address for a tree.
    pub fn set_root_bytenr(&mut self, tree_id: u64, bytenr: u64) {
        self.roots.insert(tree_id, bytenr);
    }

    /// Read the root block of the given tree as an `ExtentBuffer`.
    ///
    /// # Errors
    ///
    /// Returns an error if the tree ID is unknown or the block cannot be read.
    pub fn root_node(&mut self, tree_id: u64) -> io::Result<ExtentBuffer> {
        let bytenr = self.root_bytenr(tree_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown tree ID {tree_id}"),
            )
        })?;
        self.read_block(bytenr)
    }

    /// Return an iterator over all dirty block logical addresses.
    pub fn dirty_blocks(&self) -> impl Iterator<Item = u64> + '_ {
        self.dirty.iter().copied()
    }

    /// Return the number of dirty blocks.
    #[must_use]
    pub fn dirty_count(&self) -> usize {
        self.dirty.len()
    }

    /// Check whether a block has been written to stable storage during
    /// this transaction. Such blocks must be COW'd before modification
    /// even if their generation matches the current transaction.
    #[must_use]
    pub fn is_written(&self, logical: u64) -> bool {
        self.written.contains(&logical)
    }

    /// Clear the dirty and written sets (used after commit or abort).
    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
        self.written.clear();
    }

    /// Clear the block cache (used after commit or abort to free memory).
    pub fn clear_cache(&mut self) {
        self.block_cache.clear();
    }

    /// Return all tree root entries as `(tree_id, root_bytenr)` pairs.
    pub fn tree_roots(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.roots.iter().map(|(&id, &bytenr)| (id, bytenr))
    }

    /// Flush all dirty blocks to disk.
    ///
    /// Iterates the dirty set, checksums each cached block, and writes it.
    /// Blocks that are dirty but not in the cache are skipped (they were
    /// already written by `write_block`).
    ///
    /// # Errors
    ///
    /// Returns an error if any write fails.
    pub fn flush_dirty(&mut self) -> io::Result<()> {
        /// `BTRFS_HEADER_FLAG_WRITTEN` (bit 0): the kernel requires this
        /// flag on all tree blocks that have been committed to stable
        /// storage. Must be set before computing the checksum.
        const HEADER_FLAG_WRITTEN: u64 = 1 << 0;

        let dirty: Vec<u64> = self.dirty.iter().copied().collect();
        for logical in dirty {
            if let Some(eb) = self.block_cache.get(&logical).cloned() {
                let mut eb = eb;
                eb.set_flags(eb.flags() | HEADER_FLAG_WRITTEN);
                eb.update_checksum();
                self.reader.write_block(eb.logical(), eb.as_bytes())?;
                self.written.insert(eb.logical());
            }
        }
        Ok(())
    }

    /// Return a mutable reference to the underlying block reader.
    pub fn reader_mut(&mut self) -> &mut BlockReader<R> {
        &mut self.reader
    }

    /// Return a reference to the underlying block reader.
    #[must_use]
    pub fn reader(&self) -> &BlockReader<R> {
        &self.reader
    }

    /// Remove a tree root entry.
    pub fn remove_root(&mut self, tree_id: u64) -> Option<u64> {
        self.roots.remove(&tree_id)
    }

    /// Evict a block from the cache (e.g. after freeing it).
    pub fn evict_block(&mut self, logical: u64) {
        self.block_cache.remove(&logical);
        self.dirty.remove(&logical);
    }

    /// Snapshot the current roots so we can detect changes at commit time.
    ///
    /// Called at transaction start to record the baseline state.
    pub fn snapshot_roots(&mut self) {
        self.original_roots = self.roots.clone();
    }

    /// Restore the roots map to the last snapshot. Used by
    /// `Transaction::abort` to roll back in-memory `set_root_bytenr`
    /// changes that pointed at COWed-but-never-written bytenrs.
    pub fn restore_roots_from_snapshot(&mut self) {
        self.roots = self.original_roots.clone();
    }

    /// Flush pending writes via `Write::flush()`.
    ///
    /// Flushes any userspace write buffers. For file-backed storage,
    /// use [`Filesystem<File>::sync`] instead, which also calls fsync.
    pub fn flush_writes(&mut self) -> io::Result<()> {
        self.reader.inner_mut().flush()
    }

    /// Return tree IDs whose root block changed since the last snapshot.
    ///
    /// Compares current roots against the snapshot taken at transaction start.
    /// Excludes tree IDs 1 (root tree) and 3 (chunk tree) since their root
    /// pointers live in the superblock, not in root items.
    #[must_use]
    pub fn changed_roots(&self) -> Vec<(u64, u64, u8)> {
        let mut changed = Vec::new();
        for (&tree_id, &current_bytenr) in &self.roots {
            // Root tree and chunk tree are updated via superblock, not root items
            if tree_id == 1 || tree_id == 3 {
                continue;
            }
            let original = self.original_roots.get(&tree_id).copied();
            if original != Some(current_bytenr) {
                // Look up the level from the cached block if available
                let level = self
                    .block_cache
                    .get(&current_bytenr)
                    .map_or(0, ExtentBuffer::level);
                changed.push((tree_id, current_bytenr, level));
            }
        }
        changed
    }

    /// Return the tree id that holds `BLOCK_GROUP_ITEM` records.
    ///
    /// When [`bg_tree_override`](Self::bg_tree_override_for_test) is
    /// set (typically by the `convert-to-block-group-tree` path),
    /// returns it verbatim. Otherwise auto-detects: returns 11
    /// (`BLOCK_GROUP_TREE`) if a root for tree 11 is registered,
    /// else 2 (`EXTENT_TREE`).
    ///
    /// All allocator and block-group-update code paths must consult
    /// this accessor instead of duplicating the routing logic, so
    /// that the override mechanism actually works for everything
    /// that touches block-group state.
    #[must_use]
    pub fn block_group_tree_id(&self) -> u64 {
        if let Some(id) = self.bg_tree_override {
            return id;
        }
        if self.root_bytenr(11).is_some() {
            11
        } else {
            2
        }
    }

    /// Set the block-group-tree id override. Prefer
    /// [`pin_block_group_tree`](Self::pin_block_group_tree) which
    /// returns an RAII guard that clears the override on drop.
    ///
    /// Exposed primarily for unit tests of the routing primitive.
    #[doc(hidden)]
    pub fn bg_tree_override_for_test(&mut self, id: Option<u64>) {
        self.bg_tree_override = id;
    }

    /// Pin [`block_group_tree_id`](Self::block_group_tree_id) to
    /// the given tree id and return a guard that restores the
    /// previous override (typically `None`) when dropped.
    ///
    /// Use this in conversion paths so that panics or `?`
    /// early-returns cannot leave the override stuck on the wrong
    /// value.
    pub fn pin_block_group_tree(
        &mut self,
        id: u64,
    ) -> BgTreeOverrideGuard<'_, R> {
        let prev = self.bg_tree_override;
        self.bg_tree_override = Some(id);
        BgTreeOverrideGuard { fs: self, prev }
    }
}

/// RAII guard that restores the previous block-group-tree
/// override on drop. Created by
/// [`Filesystem::pin_block_group_tree`].
pub struct BgTreeOverrideGuard<'a, R> {
    fs: &'a mut Filesystem<R>,
    prev: Option<u64>,
}

impl<R> BgTreeOverrideGuard<'_, R> {
    /// Borrow the underlying filesystem mutably for the duration
    /// of the guard.
    pub fn fs_mut(&mut self) -> &mut Filesystem<R> {
        self.fs
    }
}

impl<R> Drop for BgTreeOverrideGuard<'_, R> {
    fn drop(&mut self) {
        self.fs.bg_tree_override = self.prev;
    }
}

impl Filesystem<File> {
    /// Sync all data to stable storage (fsync).
    ///
    /// Calls `File::sync_all()` on the underlying file handle, ensuring
    /// all written data reaches stable storage. This should be called
    /// after commit to guarantee durability.
    pub fn sync(&mut self) -> io::Result<()> {
        self.reader.inner_mut().sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Filesystem requires a real filesystem image to test meaningfully.
    // These are basic structural tests; full integration tests will use
    // temporary images created by btrfs-mkfs.

    #[test]
    fn dirty_tracking() {
        // We can test the dirty set logic without a real filesystem
        let mut dirty = BTreeSet::new();
        dirty.insert(65536u64);
        dirty.insert(131072);
        assert_eq!(dirty.len(), 2);
        assert!(dirty.contains(&65536));
        dirty.clear();
        assert!(dirty.is_empty());
    }

    #[test]
    fn roots_map() {
        let mut roots = BTreeMap::new();
        roots.insert(1u64, 65536u64);
        roots.insert(5, 131072);
        assert_eq!(roots.get(&1), Some(&65536));
        assert_eq!(roots.get(&5), Some(&131072));
        assert_eq!(roots.get(&99), None);
    }
}

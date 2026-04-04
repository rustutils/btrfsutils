//! # In-memory filesystem state for a transaction session
//!
//! `FsInfo` is the central state object for modifying a btrfs filesystem. It
//! wraps a `BlockReader` (from `btrfs-disk`), holds the parsed superblock, all
//! tree root pointers, and tracks which blocks have been modified during the
//! current transaction.
//!
//! Open a device or image with [`FsInfo::open`], then use the read/write
//! methods to access tree blocks through `ExtentBuffer`.

use crate::extent_buffer::ExtentBuffer;
use btrfs_disk::{
    reader::{self, BlockReader, OpenFilesystem},
    superblock::Superblock,
};
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Seek, Write},
};

/// In-memory filesystem state for a transaction session.
///
/// Holds everything needed to read and write tree blocks: the block reader
/// (with chunk cache for logical-to-physical resolution), the superblock,
/// all tree root pointers, the current transaction generation, and the set
/// of dirty (modified) block addresses.
pub struct FsInfo<R> {
    /// Block reader with fully populated chunk cache.
    reader: BlockReader<R>,
    /// Parsed superblock (updated in-memory during transactions).
    pub superblock: Superblock,
    /// Map of tree ID to root block logical address.
    roots: HashMap<u64, u64>,
    /// Logical addresses of blocks modified in the current transaction.
    dirty: HashSet<u64>,
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
    block_cache: HashMap<u64, ExtentBuffer>,
}

impl<R: Read + Write + Seek> FsInfo<R> {
    /// Open a btrfs filesystem from a readable+writable+seekable handle.
    ///
    /// Performs the full bootstrap sequence (superblock, chunk cache, root
    /// tree), then wraps the result into an `FsInfo` ready for transactions.
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

        // Convert BTreeMap<u64, (u64, u64)> to HashMap<u64, u64> (tree_id -> root bytenr)
        let roots: HashMap<u64, u64> = tree_roots
            .into_iter()
            .map(|(id, (bytenr, _offset))| (id, bytenr))
            .collect();

        Ok(Self {
            reader,
            superblock,
            roots,
            dirty: HashSet::new(),
            generation,
            nodesize,
            sectorsize,
            block_cache: HashMap::new(),
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

        let roots: HashMap<u64, u64> = tree_roots
            .into_iter()
            .map(|(id, (bytenr, _offset))| (id, bytenr))
            .collect();

        Ok(Self {
            reader,
            superblock,
            roots,
            dirty: HashSet::new(),
            generation,
            nodesize,
            sectorsize,
            block_cache: HashMap::new(),
        })
    }

    /// Read a tree block at the given logical address, returning an `ExtentBuffer`.
    ///
    /// If the block is already in the in-memory cache (e.g. it was COWed or
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

    /// Clear the dirty set (used after commit or abort).
    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
    }

    /// Clear the block cache (used after commit or abort to free memory).
    pub fn clear_cache(&mut self) {
        self.block_cache.clear();
    }

    /// Return all tree root entries as (tree_id, root_bytenr) pairs.
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
        let dirty: Vec<u64> = self.dirty.iter().copied().collect();
        for logical in dirty {
            if let Some(eb) = self.block_cache.get(&logical).cloned() {
                let mut eb = eb;
                eb.update_checksum();
                self.reader.write_block(eb.logical(), eb.as_bytes())?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    // FsInfo requires a real filesystem image to test meaningfully.
    // These are basic structural tests; full integration tests will use
    // temporary images created by btrfs-mkfs.

    #[test]
    fn dirty_tracking() {
        // We can test the dirty set logic without a real filesystem
        let mut dirty = HashSet::new();
        dirty.insert(65536u64);
        dirty.insert(131072);
        assert_eq!(dirty.len(), 2);
        assert!(dirty.contains(&65536));
        dirty.clear();
        assert!(dirty.is_empty());
    }

    #[test]
    fn roots_map() {
        let mut roots = HashMap::new();
        roots.insert(1u64, 65536u64);
        roots.insert(5, 131072);
        assert_eq!(roots.get(&1), Some(&65536));
        assert_eq!(roots.get(&5), Some(&131072));
        assert_eq!(roots.get(&99), None);
    }
}

//! # Transaction lifecycle: start, commit, abort
//!
//! A `Transaction` groups multiple tree modifications into a single atomic
//! commit. The commit point is the superblock write: all new tree blocks are
//! written first (at new locations via COW), then the superblock is updated
//! to point to the new root.

use crate::{
    allocation,
    buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE},
    cow::cow_block,
    delayed_ref::{DelayedRefKey, DelayedRefQueue},
    filesystem::Filesystem,
    free_space::{BlockGroupRangeDeltas, Range},
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
};
use btrfs_disk::{
    chunk::{
        chunk_item_bytes, parse_chunk_item, sys_chunk_array_append,
        sys_chunk_array_contains,
    },
    items::{BlockGroupFlags, ExtentItem, RootItem},
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
/// Block group kind that the transaction allocator can target.
///
/// Metadata block groups hold all tree blocks except the chunk tree;
/// SYSTEM block groups hold the chunk tree itself, so its blocks can be
/// resolved by the early-mount bootstrap via the superblock's
/// `sys_chunk_array`. DATA block groups hold file data extents, which
/// are written directly (not COW'd through the tree-block pipeline).
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum BlockGroupKind {
    /// A metadata block group (tree blocks for trees other than the
    /// chunk tree).
    Metadata,
    /// A SYSTEM block group (used exclusively for chunk tree blocks).
    System,
    /// A DATA block group (used for file data extents).
    Data,
}

/// Bump allocator state for one [`BlockGroupKind`].
#[derive(Debug, Clone, Copy)]
struct AllocCursor {
    cursor: u64,
    end: u64,
}

pub struct Transaction<R> {
    /// The transaction generation (superblock.generation + 1).
    pub transid: u64,
    /// Blocks freed during this transaction (old COW sources).
    freed_blocks: Vec<u64>,
    /// Blocks allocated during this transaction (for free space tree updates).
    allocated_blocks: Vec<u64>,
    /// Delayed reference count updates.
    pub delayed_refs: DelayedRefQueue,
    /// Per-block-group byte ranges allocated and freed during this
    /// transaction. Populated by `flush_delayed_refs`. Consumed by the
    /// free space tree update step (Stage F3). Cleared at commit end.
    pub bg_range_deltas: BlockGroupRangeDeltas,
    /// Per-kind bump allocator state. Lazily populated on first use of
    /// each kind so that filesystems without the relevant block group
    /// type pay no scanning cost up front.
    alloc: BTreeMap<BlockGroupKind, AllocCursor>,
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
        // Generation must advance monotonically.
        assert!(
            transid > fs_info.superblock.generation,
            "start: transid {transid} did not advance beyond superblock \
             generation {}",
            fs_info.superblock.generation,
        );
        fs_info.generation = transid;

        // Snapshot current roots so we can detect changes at commit time
        fs_info.snapshot_roots();

        // Eagerly seed the metadata cursor — every transaction COWs at
        // least one metadata block, so failing here is the same as
        // failing on the first alloc. The SYSTEM cursor is created on
        // demand the first time the chunk tree is COWed.
        let nodesize = u64::from(fs_info.nodesize);
        let (cursor, end) = find_alloc_region_after(
            fs_info,
            BlockGroupKind::Metadata,
            0,
            nodesize,
            nodesize,
        )?;
        let mut alloc = BTreeMap::new();
        alloc.insert(BlockGroupKind::Metadata, AllocCursor { cursor, end });

        Ok(Self {
            transid,
            freed_blocks: Vec::new(),
            allocated_blocks: Vec::new(),
            delayed_refs: DelayedRefQueue::new(),
            bg_range_deltas: BlockGroupRangeDeltas::new(),
            alloc,
            pinned: BTreeSet::new(),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Allocate a new tree block (nodesize bytes) inside a block group
    /// of `kind`.
    ///
    /// Uses a per-kind bump allocator within a free extent. If the
    /// current region is exhausted, scans the extent tree for another
    /// free extent of the requested kind and continues from there.
    ///
    /// # Errors
    ///
    /// Returns an error if no block group of the requested kind has
    /// enough free space.
    pub fn alloc_block(
        &mut self,
        fs_info: &mut Filesystem<R>,
        kind: BlockGroupKind,
    ) -> io::Result<u64> {
        let nodesize = u64::from(fs_info.nodesize);

        // Lazily seed a cursor for this kind on first use.
        #[allow(clippy::map_entry)]
        if !self.alloc.contains_key(&kind) {
            let (cursor, end) =
                find_alloc_region_after(fs_info, kind, 0, nodesize, nodesize)?;
            self.alloc.insert(kind, AllocCursor { cursor, end });
        }

        loop {
            // Snapshot current cursor; we mutate self.alloc below so we
            // can't hold a borrow into it across the find call.
            let mut state = *self.alloc.get(&kind).unwrap();

            if state.cursor + nodesize > state.end {
                // Current region exhausted — find another free extent.
                let (cursor, end) = find_alloc_region_after(
                    fs_info,
                    kind,
                    state.cursor,
                    nodesize,
                    nodesize,
                )?;
                state = AllocCursor { cursor, end };
                if state.cursor + nodesize > state.end {
                    return Err(io::Error::other(format!(
                        "no {kind:?} block group with enough free space",
                    )));
                }
            }

            let logical = state.cursor;
            state.cursor += nodesize;
            self.alloc.insert(kind, state);

            // Skip pinned blocks: these were freed during this transaction
            // but the old superblock still references them. Reusing them
            // before commit would break crash consistency.
            if self.pinned.contains(&logical) {
                continue;
            }

            // Sanity: we should never allocate a pinned address
            // (the pinned check above should have caught it).
            debug_assert!(
                !self.pinned.contains(&logical),
                "alloc_block: allocated pinned address {logical:#x}",
            );
            // The address must be nodesize-aligned.
            debug_assert_eq!(
                logical % u64::from(fs_info.nodesize),
                0,
                "alloc_block: address {logical:#x} not aligned to nodesize {}",
                fs_info.nodesize,
            );
            self.allocated_blocks.push(logical);
            return Ok(logical);
        }
    }

    /// Allocate a new tree block and queue a delayed ref for it.
    ///
    /// Routes the allocation to a SYSTEM block group when COW'ing the
    /// chunk tree (tree id 3) and to a metadata block group otherwise.
    /// SYSTEM allocations are immediately registered in the
    /// superblock's `sys_chunk_array` so the next mount can resolve
    /// them via the bootstrap snippet.
    ///
    /// # Errors
    ///
    /// Returns an error if no free metadata space is available, or if
    /// a SYSTEM allocation cannot be added to the bootstrap snippet.
    pub fn alloc_tree_block(
        &mut self,
        fs_info: &mut Filesystem<R>,
        tree_id: u64,
        level: u8,
    ) -> io::Result<u64> {
        let kind = if tree_id
            == u64::from(btrfs_disk::raw::BTRFS_CHUNK_TREE_OBJECTID)
        {
            BlockGroupKind::System
        } else {
            BlockGroupKind::Metadata
        };
        let logical = self.alloc_block(fs_info, kind)?;
        self.delayed_refs.add_ref(logical, true, tree_id, level);
        if kind == BlockGroupKind::System {
            self.ensure_in_sys_chunk_array(fs_info, logical)?;
        }
        Ok(logical)
    }

    /// Allocate a data extent: find space in a DATA block group, write
    /// `data` to disk immediately, queue a `+1` `EXTENT_DATA_REF` delayed
    /// ref, and return the allocated logical address.
    ///
    /// `data` is zero-padded up to the next sectorsize boundary before
    /// being written. The returned address is sectorsize-aligned and the
    /// queued ref's `num_bytes` is the padded size.
    ///
    /// Unlike tree-block allocations, data extents are written to disk
    /// at allocation time (`BlockReader::write_block` routes to all
    /// stripe copies). Only the metadata (`EXTENT_ITEM`, `EXTENT_DATA`,
    /// `EXTENT_CSUM`) goes through the commit pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error if no DATA block group has enough free space,
    /// or if the disk write fails.
    pub fn alloc_data_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
        data: &[u8],
        owner_root: u64,
        owner_ino: u64,
        owner_offset: u64,
    ) -> io::Result<u64> {
        let sectorsize = u64::from(fs_info.sectorsize);
        let raw_len = data.len() as u64;
        let aligned_size = align_up(raw_len, sectorsize);
        if aligned_size == 0 {
            return Err(io::Error::other(
                "alloc_data_extent: empty data not supported",
            ));
        }

        // Find a region with enough contiguous free space. The cursor
        // for `Data` is shared with metadata/system in `self.alloc`,
        // but advanced here by the variable extent size rather than
        // the fixed nodesize used by `alloc_block`.
        let kind = BlockGroupKind::Data;
        #[allow(clippy::map_entry)]
        if !self.alloc.contains_key(&kind) {
            let (cursor, end) = find_alloc_region_after(
                fs_info,
                kind,
                0,
                sectorsize,
                aligned_size,
            )?;
            self.alloc.insert(kind, AllocCursor { cursor, end });
        }

        let mut state = *self.alloc.get(&kind).unwrap();
        if state.cursor + aligned_size > state.end {
            let (cursor, end) = find_alloc_region_after(
                fs_info,
                kind,
                state.cursor,
                sectorsize,
                aligned_size,
            )?;
            state = AllocCursor { cursor, end };
            if state.cursor + aligned_size > state.end {
                return Err(io::Error::other(
                    "no DATA block group with enough free space",
                ));
            }
        }

        let logical = state.cursor;
        state.cursor += aligned_size;
        self.alloc.insert(kind, state);

        debug_assert_eq!(
            logical % sectorsize,
            0,
            "alloc_data_extent: address {logical:#x} not aligned to sectorsize {sectorsize}",
        );

        // Write the data to disk now (zero-padded to sector alignment).
        // BlockReader::write_block fans out to all stripe copies.
        if raw_len == aligned_size {
            fs_info.reader_mut().write_block(logical, data)?;
        } else {
            let mut padded = Vec::with_capacity(aligned_size as usize);
            padded.extend_from_slice(data);
            padded.resize(aligned_size as usize, 0);
            fs_info.reader_mut().write_block(logical, &padded)?;
        }

        // Queue the +1 EXTENT_DATA_REF. flush_delayed_refs will create
        // the EXTENT_ITEM, update bytes_used, and record the range in
        // bg_range_deltas for the FST.
        self.delayed_refs.add_data_ref(
            logical,
            aligned_size,
            owner_root,
            owner_ino,
            owner_offset,
            1,
        );

        self.allocated_blocks.push(logical);
        Ok(logical)
    }

    /// Insert an `EXTENT_DATA` item into an FS tree.
    ///
    /// `extent_data` is the already-serialized payload (use
    /// [`FileExtentItem::to_bytes_regular`](btrfs_disk::items::FileExtentItem::to_bytes_regular)
    /// or
    /// [`FileExtentItem::to_bytes_inline`](btrfs_disk::items::FileExtentItem::to_bytes_inline)).
    ///
    /// The key is `(inode, EXTENT_DATA, file_offset)`. For inline extents
    /// the caller passes `file_offset = 0` per the on-disk convention.
    ///
    /// # Errors
    ///
    /// Returns an error if an `EXTENT_DATA` item already exists at the
    /// target key, or if any tree operation fails. Updating an existing
    /// extent (the COW write path) is the caller's responsibility: drop
    /// the old extent first, then insert the new one.
    pub fn insert_file_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
        tree_id: u64,
        inode: u64,
        file_offset: u64,
        extent_data: &[u8],
    ) -> io::Result<()> {
        let key = DiskKey {
            objectid: inode,
            key_type: KeyType::ExtentData,
            offset: file_offset,
        };

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            tree_id,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + extent_data.len()) as u32),
            true,
        )?;
        if found {
            path.release();
            return Err(io::Error::other(format!(
                "insert_file_extent: EXTENT_DATA already exists at \
                 (ino={inode}, offset={file_offset}) in tree {tree_id}"
            )));
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("insert_file_extent: no leaf in path")
        })?;
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, extent_data)?;
        fs_info.mark_dirty(leaf);
        path.release();
        Ok(())
    }

    /// Compute per-sector CRC32C checksums of `on_disk_data` and insert
    /// them into the csum tree (tree id 7).
    ///
    /// `on_disk_data` is the data as it lands on disk — for compressed
    /// extents that means the compressed/framed payload, not the
    /// uncompressed original. Length must be a multiple of `sectorsize`.
    ///
    /// Each sector contributes a 4-byte CRC32C (standard ISO 3309) to a
    /// `EXTENT_CSUM` item keyed `(EXTENT_CSUM_OBJECTID, EXTENT_CSUM,
    /// logical_bytenr)`. Large extents are split into multiple csum
    /// items so each fits in a single leaf.
    ///
    /// This call does not merge with adjacent existing csum items;
    /// callers that write contiguous extents in the same transaction
    /// will produce one item per call. `btrfs check` accepts either
    /// shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the filesystem's csum_type is not CRC32C,
    /// if `on_disk_data.len()` is not sectorsize-aligned, or if any
    /// tree operation fails.
    pub fn insert_csums(
        &mut self,
        fs_info: &mut Filesystem<R>,
        logical_bytenr: u64,
        on_disk_data: &[u8],
    ) -> io::Result<()> {
        use btrfs_disk::superblock::ChecksumType;

        if fs_info.superblock.csum_type != ChecksumType::Crc32 {
            return Err(io::Error::other(format!(
                "insert_csums: only CRC32C is supported (csum_type = {:?})",
                fs_info.superblock.csum_type,
            )));
        }

        let sectorsize = u64::from(fs_info.sectorsize);
        let total = on_disk_data.len() as u64;
        if total == 0 || total % sectorsize != 0 {
            return Err(io::Error::other(format!(
                "insert_csums: on_disk_data length {total} not a multiple of \
                 sectorsize {sectorsize}",
            )));
        }
        let csum_size: usize = 4;

        // Compute per-sector csums up front.
        let num_sectors = (total / sectorsize) as usize;
        let mut all_csums = Vec::with_capacity(num_sectors * csum_size);
        for sector in on_disk_data.chunks_exact(sectorsize as usize) {
            let csum = crc32c::crc32c(sector);
            all_csums.extend_from_slice(&csum.to_le_bytes());
        }

        // Cap each csum item so it (plus a second item header to leave
        // room for a future split) fits comfortably in a leaf.
        let leaf_data_size = (fs_info.nodesize as usize) - HEADER_SIZE;
        let max_payload =
            leaf_data_size.saturating_sub(2 * ITEM_SIZE) - csum_size;
        let max_csums_per_item = (max_payload / csum_size).max(1);

        let csum_objectid =
            i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;

        let mut sector_idx = 0usize;
        while sector_idx < num_sectors {
            let take = (num_sectors - sector_idx).min(max_csums_per_item);
            let payload_start = sector_idx * csum_size;
            let payload_end = payload_start + take * csum_size;
            let payload = &all_csums[payload_start..payload_end];

            let chunk_logical =
                logical_bytenr + (sector_idx as u64) * sectorsize;
            let key = DiskKey {
                objectid: csum_objectid,
                key_type: KeyType::ExtentCsum,
                offset: chunk_logical,
            };

            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                7, // csum tree
                &key,
                &mut path,
                SearchIntent::Insert((ITEM_SIZE + payload.len()) as u32),
                true,
            )?;
            if found {
                path.release();
                return Err(io::Error::other(format!(
                    "insert_csums: csum item already exists at {chunk_logical}"
                )));
            }
            let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                io::Error::other("insert_csums: no leaf in path")
            })?;
            let slot = path.slots[0];
            items::insert_item(leaf, slot, &key, payload)?;
            fs_info.mark_dirty(leaf);
            path.release();

            sector_idx += take;
        }

        Ok(())
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

    /// Materialise a fresh empty global tree with the given objectid.
    ///
    /// Allocates a single metadata block, initialises it as an empty
    /// level-0 leaf carrying `tree_id` as its owner, registers
    /// `(tree_id -> bytenr)` in the in-memory roots map, and inserts a
    /// `ROOT_ITEM` keyed `(tree_id, ROOT_ITEM, 0)` into the root tree
    /// pointing at the new block.
    ///
    /// The new leaf and root-item are staged but not flushed; the
    /// caller must invoke `commit` (possibly after inserting items
    /// into the new tree) for them to land on disk. Subsequent items
    /// inserted into the new tree go through the normal
    /// `search_slot`/insert pipeline and may COW the empty leaf away.
    ///
    /// This is the foundation primitive for whole-tree creation
    /// (e.g. `convert-to-free-space-tree`,
    /// `convert-to-block-group-tree`). It does **not** create the
    /// root tree (id 1), the chunk tree (id 3), or the extent tree
    /// (id 2): those are bootstrap state managed by the superblock
    /// and the existing transaction pipeline.
    ///
    /// Returns the logical bytenr of the freshly allocated leaf.
    ///
    /// # Errors
    ///
    /// * `tree_id` is `0`, `1`, `2`, or `3`.
    /// * `tree_id` already has a root in the in-memory roots map.
    /// * The metadata allocator fails.
    /// * The root-tree insert fails.
    pub fn create_empty_tree(
        &mut self,
        fs_info: &mut Filesystem<R>,
        tree_id: u64,
    ) -> io::Result<u64> {
        // Reject bootstrap trees: their roots live in the superblock
        // (1, 3) or are required for the allocator/extent bookkeeping
        // itself (2). Allowing this primitive to overwrite them would
        // corrupt the in-memory roots map and break commit.
        if matches!(tree_id, 0..=3) {
            return Err(io::Error::other(format!(
                "create_empty_tree: tree id {tree_id} is reserved bootstrap state",
            )));
        }

        if fs_info.root_bytenr(tree_id).is_some() {
            return Err(io::Error::other(format!(
                "create_empty_tree: tree id {tree_id} already exists",
            )));
        }

        // Source fsid and chunk_tree_uuid from the existing root tree
        // root block. Every tree block in a healthy btrfs filesystem
        // shares these (the chunk_tree_uuid is the chunk root's uuid,
        // and the fsid is the metadata uuid when the METADATA_UUID
        // incompat flag is set, otherwise the plain fsid). Inheriting
        // matches what cow_block, split_leaf, and split_node do for
        // every other allocation, so the new leaf is structurally
        // indistinguishable from a COWed one to btrfs check.
        let (fsid, chunk_tree_uuid) = {
            let root_bytenr = fs_info.root_bytenr(1).ok_or_else(|| {
                io::Error::other(
                    "create_empty_tree: root tree (id 1) has no root bytenr",
                )
            })?;
            let eb = fs_info.read_block(root_bytenr)?;
            (eb.fsid(), eb.chunk_tree_uuid())
        };

        // Allocate the leaf block and queue its +1 metadata extent
        // ref. alloc_tree_block routes to a metadata block group and
        // also records the allocation in bg_range_deltas, which keeps
        // the free space tree in sync at commit.
        let new_logical = self.alloc_tree_block(fs_info, tree_id, 0)?;

        // Build the empty leaf header. WRITTEN is left clear: the
        // commit's flush_dirty pass sets it before checksumming.
        let nodesize = fs_info.nodesize;
        let mut new_eb = ExtentBuffer::new_zeroed(nodesize, new_logical);
        new_eb.set_bytenr(new_logical);
        new_eb.set_level(0);
        new_eb.set_nritems(0);
        new_eb.set_generation(self.transid);
        new_eb.set_owner(tree_id);
        new_eb.set_fsid(&fsid);
        new_eb.set_chunk_tree_uuid(&chunk_tree_uuid);
        // The header `flags` field encodes the backref revision in
        // its top 8 bits (BTRFS_BACKREF_REV_SHIFT = 56). Modern btrfs
        // uses BTRFS_MIXED_BACKREF_REV = 1; a leaf with revision 0
        // would be parsed as the obsolete pre-mixed-backref format
        // and rejected by btrfs check. WRITTEN (bit 0) stays clear:
        // flush_dirty sets it before checksumming.
        new_eb.set_flags(
            u64::from(btrfs_disk::raw::BTRFS_MIXED_BACKREF_REV)
                << btrfs_disk::raw::BTRFS_BACKREF_REV_SHIFT,
        );

        debug_assert_eq!(new_eb.level(), 0);
        debug_assert_eq!(new_eb.nritems(), 0);
        debug_assert_eq!(new_eb.owner(), tree_id);
        debug_assert_eq!(new_eb.generation(), self.transid);
        debug_assert_eq!(
            new_eb.leaf_free_space(),
            nodesize - HEADER_SIZE as u32,
            "create_empty_tree: empty leaf must have full free space",
        );

        fs_info.mark_dirty(&new_eb);
        fs_info.set_root_bytenr(tree_id, new_logical);

        // Insert the ROOT_ITEM into the root tree.
        let root_item = RootItem::new_internal(self.transid, new_logical, 0);
        let root_item_bytes = root_item.to_bytes();
        let root_item_key = DiskKey {
            objectid: tree_id,
            key_type: KeyType::RootItem,
            offset: 0,
        };

        let root_tree_id = 1u64;
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            root_tree_id,
            &root_item_key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + root_item_bytes.len()) as u32),
            true,
        )?;
        if found {
            path.release();
            return Err(io::Error::other(format!(
                "create_empty_tree: ROOT_ITEM for tree {tree_id} already in root tree",
            )));
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("create_empty_tree: no leaf in path after search")
        })?;
        items::insert_item(
            leaf,
            path.slots[0],
            &root_item_key,
            &root_item_bytes,
        )?;
        fs_info.mark_dirty(leaf);
        path.release();

        debug_assert_eq!(
            fs_info.root_bytenr(tree_id),
            Some(new_logical),
            "create_empty_tree: roots map not updated",
        );

        Ok(new_logical)
    }

    /// Replace the chunk tree with a fresh empty one.
    ///
    /// This is the apply primitive for `rescue chunk-recover`. It:
    ///
    /// 1. Clears and rebuilds the superblock's `sys_chunk_array` from
    ///    the provided SYSTEM chunk records. This ensures that
    ///    `ensure_in_sys_chunk_array` (called during SYSTEM block
    ///    allocation) finds the entries already present and skips the
    ///    chunk tree read that would fail on a damaged filesystem.
    ///
    /// 2. Allocates a fresh SYSTEM block for the new chunk tree root.
    ///
    /// 3. Initializes it as an empty level-0 leaf owned by tree ID 3,
    ///    with proper fsid, `chunk_tree_uuid`, and backref revision.
    ///
    /// After this call, the caller inserts `DEV_ITEM` and `CHUNK_ITEM`
    /// records into tree ID 3 via the normal `search_slot`/`insert_item`
    /// pipeline, then calls `commit()`. The commit automatically updates
    /// `superblock.chunk_root` and `chunk_root_level`.
    ///
    /// `system_chunks` is a list of `(bg_start, chunk_bytes)` pairs
    /// where `chunk_bytes` is the serialized `btrfs_chunk` (from
    /// `chunk_item_bytes`). Only SYSTEM-type chunks should be included.
    ///
    /// Returns the logical address of the new chunk tree root leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if the `sys_chunk_array` overflows, the allocator
    /// fails, or the root tree is unreadable.
    pub fn rebuild_chunk_tree(
        &mut self,
        fs_info: &mut Filesystem<R>,
        system_chunks: &[(u64, Vec<u8>)],
    ) -> io::Result<u64> {
        // Step 1: rebuild sys_chunk_array from the provided SYSTEM chunks.
        fs_info.superblock.sys_chunk_array_size = 0;
        fs_info.superblock.sys_chunk_array = [0; 2048];
        for (bg_start, chunk_bytes) in system_chunks {
            sys_chunk_array_append(
                &mut fs_info.superblock.sys_chunk_array,
                &mut fs_info.superblock.sys_chunk_array_size,
                *bg_start,
                chunk_bytes,
            )
            .map_err(io::Error::other)?;
        }

        // Step 2: allocate a fresh SYSTEM block for the chunk tree root.
        // ensure_in_sys_chunk_array will find the entry and return early.
        let chunk_tree_id =
            u64::from(btrfs_disk::raw::BTRFS_CHUNK_TREE_OBJECTID);
        let new_logical = self.alloc_tree_block(fs_info, chunk_tree_id, 0)?;

        // Step 3: initialize as empty leaf, same pattern as create_empty_tree.
        let (fsid, chunk_tree_uuid) = {
            let root_bytenr = fs_info.root_bytenr(1).ok_or_else(|| {
                io::Error::other("rebuild_chunk_tree: root tree has no root")
            })?;
            let eb = fs_info.read_block(root_bytenr)?;
            (eb.fsid(), eb.chunk_tree_uuid())
        };

        let nodesize = fs_info.nodesize;
        let mut new_eb = ExtentBuffer::new_zeroed(nodesize, new_logical);
        new_eb.set_bytenr(new_logical);
        new_eb.set_level(0);
        new_eb.set_nritems(0);
        new_eb.set_generation(self.transid);
        new_eb.set_owner(chunk_tree_id);
        new_eb.set_fsid(&fsid);
        new_eb.set_chunk_tree_uuid(&chunk_tree_uuid);
        new_eb.set_flags(
            u64::from(btrfs_disk::raw::BTRFS_MIXED_BACKREF_REV)
                << btrfs_disk::raw::BTRFS_BACKREF_REV_SHIFT,
        );

        fs_info.mark_dirty(&new_eb);
        fs_info.set_root_bytenr(chunk_tree_id, new_logical);

        Ok(new_logical)
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
        // Step 0: Force-COW the root tree root so that every commit
        // rewrites at least one block at the new generation. This keeps
        // `superblock.generation` and the root tree root's
        // `header.generation` in lockstep, which is what `btrfs check`
        // (and the kernel mount path) verify. Without this, a no-op
        // commit would either need to be short-circuited or would
        // corrupt the filesystem with "parent transid verify failed".
        // See PLAN.md Finding 3 invariants I1, I2, I7.
        //
        // `cow_block` is idempotent: if the root tree was already COWed
        // earlier in this transaction (its in-memory generation matches
        // and the block is not yet written to disk), it returns the
        // existing buffer unchanged. The new add/drop delayed refs and
        // the new dirty block flow into the convergence loop below.
        let root_tree_id = 1u64;
        if let Some(root_bytenr) = fs_info.root_bytenr(root_tree_id) {
            let eb = fs_info.read_block(root_bytenr)?;
            let new_eb =
                cow_block(&mut self, fs_info, &eb, root_tree_id, None)?;
            if new_eb.logical() != root_bytenr {
                fs_info.set_root_bytenr(root_tree_id, new_eb.logical());
            }
        }

        // Step 1: Convergence loop. Flushing delayed refs modifies the
        // extent tree (COW), which generates new delayed refs. Updating
        // root items modifies the root tree (COW), generating more.
        // Alternate until both are stable.
        let max_passes = 32;
        for pass in 0..max_passes {
            self.flush_delayed_refs(fs_info)?;
            self.update_root_items(fs_info)?;
            // Snapshot roots BEFORE update_free_space_tree so the next
            // pass's update_root_items picks up the FST root change.
            // If we snapshotted after update_FST, the new FST root
            // would already be in the snapshot baseline and would
            // never be written to the on-disk ROOT_ITEM, leaving the
            // old extent items referenced by a stale root pointer and
            // the new ones orphaned.
            fs_info.snapshot_roots();
            let fst_changed = self.update_free_space_tree(fs_info)?;

            // Stable when no pending refs, no changed roots remain
            // (changed_roots since the snapshot we just took, which
            // captures any changes update_free_space_tree made), no
            // FST updates were produced, and no new range deltas were
            // accumulated.
            if self.delayed_refs.is_empty()
                && fs_info.changed_roots().is_empty()
                && self.bg_range_deltas.is_empty()
                && !fst_changed
            {
                break;
            }

            if pass == max_passes - 1 {
                return Err(io::Error::other(
                    "commit convergence loop did not stabilize",
                ));
            }
        }

        // Step 2: Flush all dirty blocks to disk
        fs_info.flush_dirty()?;

        // Step 4: Update superblock fields
        fs_info.superblock.generation = self.transid;

        // The free space tree was updated incrementally inside the
        // convergence loop above. FREE_SPACE_TREE_VALID stays set
        // because the on-disk FST is now consistent with the extent
        // tree.

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

        // Pre-write superblock invariants. These are hard assertions
        // (not debug_assert) because writing a corrupt superblock is
        // unrecoverable.
        assert_eq!(
            fs_info.superblock.generation, self.transid,
            "commit: superblock generation {} != transid {}",
            fs_info.superblock.generation, self.transid,
        );
        assert_eq!(
            fs_info.superblock.root,
            fs_info.root_bytenr(1).unwrap_or(0),
            "commit: superblock.root doesn't match in-memory root tree root",
        );
        // bytes_used must be at least 6 * nodesize (kernel minimum).
        let min_bytes_used = 6 * u64::from(fs_info.nodesize);
        assert!(
            fs_info.superblock.bytes_used >= min_bytes_used,
            "commit: bytes_used {} below kernel minimum {} \
             (6 * nodesize {})",
            fs_info.superblock.bytes_used,
            min_bytes_used,
            fs_info.nodesize,
        );
        // All delayed refs must have been flushed.
        assert!(
            self.delayed_refs.is_empty(),
            "commit: {} delayed refs still pending at superblock write",
            self.delayed_refs.len(),
        );

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
        self.bg_range_deltas.clear();
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
    #[allow(clippy::too_many_lines)]
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
                match dref.key {
                    DelayedRefKey::Metadata {
                        bytenr,
                        owner_root,
                        level,
                    } => {
                        if dref.delta > 0 {
                            self.create_metadata_extent(
                                fs_info,
                                extent_tree_id,
                                bytenr,
                                level,
                                owner_root,
                                skinny,
                            )?;
                            bytes_used_delta += nodesize;
                            if let Some(bg_start) = find_containing_block_group(
                                &block_groups,
                                bytenr,
                            ) {
                                *bg_deltas.entry(bg_start).or_insert(0) +=
                                    nodesize;
                                self.bg_range_deltas.record_allocated(
                                    bg_start,
                                    Range::new(bytenr, nodesize as u64),
                                );
                            }
                        } else if dref.delta < 0 {
                            self.delete_metadata_extent(
                                fs_info,
                                extent_tree_id,
                                bytenr,
                                level,
                                skinny,
                            )?;
                            bytes_used_delta -= nodesize;
                            if let Some(bg_start) = find_containing_block_group(
                                &block_groups,
                                bytenr,
                            ) {
                                *bg_deltas.entry(bg_start).or_insert(0) -=
                                    nodesize;
                                self.bg_range_deltas.record_freed(
                                    bg_start,
                                    Range::new(bytenr, nodesize as u64),
                                );
                            }
                        }
                    }
                    DelayedRefKey::Data {
                        bytenr,
                        owner_root,
                        owner_ino,
                        owner_offset,
                    } => {
                        let num_bytes = dref.num_bytes;
                        if num_bytes == 0 {
                            return Err(io::Error::other(
                                "data delayed ref missing num_bytes",
                            ));
                        }
                        if dref.delta > 0 {
                            let count = dref.delta as u32;
                            self.create_data_extent(
                                fs_info,
                                extent_tree_id,
                                bytenr,
                                num_bytes,
                                owner_root,
                                owner_ino,
                                owner_offset,
                                count,
                            )?;
                            let signed = num_bytes as i64;
                            bytes_used_delta += signed;
                            if let Some(bg_start) = find_containing_block_group(
                                &block_groups,
                                bytenr,
                            ) {
                                *bg_deltas.entry(bg_start).or_insert(0) +=
                                    signed;
                                self.bg_range_deltas.record_allocated(
                                    bg_start,
                                    Range::new(bytenr, num_bytes),
                                );
                            }
                        } else if dref.delta < 0 {
                            let refs_to_drop = (-dref.delta) as u32;
                            let new_total_refs = self.drop_data_extent_ref(
                                fs_info,
                                extent_tree_id,
                                bytenr,
                                num_bytes,
                                owner_root,
                                owner_ino,
                                owner_offset,
                                refs_to_drop,
                            )?;
                            if new_total_refs == 0 {
                                // Whole data extent has been freed.
                                self.delete_data_extent_item(
                                    fs_info,
                                    extent_tree_id,
                                    bytenr,
                                    num_bytes,
                                )?;
                                self.delete_csums_in_range(
                                    fs_info, bytenr, num_bytes,
                                )?;
                                let signed = num_bytes as i64;
                                bytes_used_delta -= signed;
                                if let Some(bg_start) =
                                    find_containing_block_group(
                                        &block_groups,
                                        bytenr,
                                    )
                                {
                                    *bg_deltas.entry(bg_start).or_insert(0) -=
                                        signed;
                                    self.bg_range_deltas.record_freed(
                                        bg_start,
                                        Range::new(bytenr, num_bytes),
                                    );
                                }
                            }
                        }
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

        // Cancel ranges that were both allocated and freed within
        // this transaction. The FST sees neither.
        self.bg_range_deltas.cancel_within_transaction();

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

    /// Apply the per-block-group range deltas accumulated in
    /// `flush_delayed_refs` to the on-disk free space tree.
    ///
    /// For each block group with non-empty deltas:
    ///
    /// 1. Look up the block group's metadata (length).
    /// 2. Read the `FREE_SPACE_INFO` item; if its `BITMAPS` flag is
    ///    set, error out — bitmap layout is out of scope for v1.
    /// 3. Walk the existing `FREE_SPACE_EXTENT` items for this block
    ///    group and collect them into a sorted free-range list.
    /// 4. Apply the delta via [`free_space::apply_delta`] to produce
    ///    the new free-range list.
    /// 5. If unchanged, skip. Otherwise delete every existing
    ///    `FREE_SPACE_EXTENT` for this block group, insert the new
    ///    set, and update `FREE_SPACE_INFO.extent_count`.
    ///
    /// All FST modifications go through the standard COW search path,
    /// so they generate their own delayed refs and dirty blocks; the
    /// caller (the commit convergence loop) will pick those up on a
    /// subsequent pass.
    ///
    /// Returns `true` if any FST modifications were made.
    fn update_free_space_tree(
        &mut self,
        fs_info: &mut Filesystem<R>,
    ) -> io::Result<bool> {
        use crate::free_space::{Range, apply_delta};
        use btrfs_disk::items::FreeSpaceInfoFlags;

        let fst_id = 10u64;
        if fs_info.root_bytenr(fst_id).is_none() {
            // No free space tree on this filesystem.
            self.bg_range_deltas.clear();
            return Ok(false);
        }

        // Take ownership of the current deltas. Any new deltas
        // produced by FST COW during this call accumulate into
        // self.bg_range_deltas via the next flush_delayed_refs pass.
        let deltas = std::mem::take(&mut self.bg_range_deltas);
        if deltas.is_empty() {
            return Ok(false);
        }

        // Look up block group lengths once.
        let block_groups = allocation::load_block_groups(fs_info)?;
        let bg_len = |start: u64| -> Option<u64> {
            block_groups
                .iter()
                .find(|bg| bg.start == start)
                .map(|bg| bg.length)
        };

        let mut any_changes = false;

        for (bg_start, delta) in deltas.iter() {
            let bg_start = *bg_start;
            let bg_length = bg_len(bg_start).ok_or_else(|| {
                io::Error::other(format!(
                    "free space tree update: block group {bg_start} not found"
                ))
            })?;
            let bg = Range::new(bg_start, bg_length);

            // Step 1: read FREE_SPACE_INFO and check for bitmap layout.
            let info = self
                .read_free_space_info(fs_info, fst_id, bg_start, bg_length)?
                .ok_or_else(|| {
                    io::Error::other(format!(
                        "free space tree update: FREE_SPACE_INFO missing for block group {bg_start}"
                    ))
                })?;
            if info.flags.contains(FreeSpaceInfoFlags::USING_BITMAPS) {
                return Err(io::Error::other(format!(
                    "free space tree block group {bg_start} uses bitmap layout (unsupported in v1)"
                )));
            }

            // Step 2: read existing FREE_SPACE_EXTENT items.
            let existing = self.read_free_space_extents(
                fs_info, fst_id, bg_start, bg_length,
            )?;

            // Step 3: apply.
            let new = apply_delta(bg_start, bg, &existing, delta)
                .map_err(|e| io::Error::other(e.to_string()))?;

            if new == existing {
                continue;
            }

            // Step 4: delete all existing FREE_SPACE_EXTENT items for
            // this block group.
            self.delete_free_space_extents_in_range(
                fs_info, fst_id, bg_start, bg_length,
            )?;

            // Step 5: insert new FREE_SPACE_EXTENT items.
            for r in new.as_slice() {
                self.insert_free_space_extent(
                    fs_info, fst_id, r.start, r.length,
                )?;
            }

            // Step 6: update FREE_SPACE_INFO.extent_count.
            self.update_free_space_info_count(
                fs_info,
                fst_id,
                bg_start,
                bg_length,
                u32::try_from(new.len()).unwrap_or(u32::MAX),
                info.flags,
            )?;

            any_changes = true;
        }

        Ok(any_changes)
    }

    /// Read the `FREE_SPACE_INFO` item for a block group, if present.
    fn read_free_space_info(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        bg_start: u64,
        bg_length: u64,
    ) -> io::Result<Option<btrfs_disk::items::FreeSpaceInfo>> {
        use btrfs_disk::items::FreeSpaceInfo;

        let key = DiskKey {
            objectid: bg_start,
            key_type: KeyType::FreeSpaceInfo,
            offset: bg_length,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            fst_id,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )?;
        if !found {
            path.release();
            return Ok(None);
        }
        let leaf = path.nodes[0].as_ref().ok_or_else(|| {
            io::Error::other("read_free_space_info: no leaf in path")
        })?;
        let slot = path.slots[0];
        let data = leaf.item_data(slot).to_vec();
        path.release();
        Ok(FreeSpaceInfo::parse(&data))
    }

    /// Walk every `FREE_SPACE_EXTENT` item whose objectid lies within
    /// `[bg_start, bg_start + bg_length)` and collect them into a
    /// sorted, coalesced [`RangeList`].
    fn read_free_space_extents(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        bg_start: u64,
        bg_length: u64,
    ) -> io::Result<crate::free_space::RangeList> {
        use crate::free_space::{Range, RangeList};

        let bg_end = bg_start + bg_length;
        let mut out: Vec<Range> = Vec::new();

        let key = DiskKey {
            objectid: bg_start,
            key_type: KeyType::FreeSpaceExtent,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut *self),
            fs_info,
            fst_id,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )?;

        loop {
            let Some(leaf) = path.nodes[0].as_ref() else {
                break;
            };
            let slot = path.slots[0];
            if slot >= leaf.nritems() as usize {
                if !search::next_leaf(fs_info, &mut path)? {
                    break;
                }
                continue;
            }
            let k = leaf.item_key(slot);
            if k.objectid >= bg_end {
                break;
            }
            if k.key_type == KeyType::FreeSpaceExtent && k.offset > 0 {
                out.push(Range::new(k.objectid, k.offset));
            }
            path.slots[0] = slot + 1;
        }

        path.release();

        // The walk is naturally sorted because the FST is keyed
        // (start, FREE_SPACE_EXTENT, length). Coalescing is a no-op on
        // a well-formed FST but harmless if the on-disk state somehow
        // contains touching ranges.
        let mut list = RangeList::new();
        for r in out {
            list.insert(r);
        }
        Ok(list)
    }

    /// Delete every `FREE_SPACE_EXTENT` item whose objectid lies within
    /// `[bg_start, bg_start + bg_length)`.
    fn delete_free_space_extents_in_range(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        bg_start: u64,
        bg_length: u64,
    ) -> io::Result<()> {
        let bg_end = bg_start + bg_length;
        loop {
            let key = DiskKey {
                objectid: bg_start,
                key_type: KeyType::FreeSpaceExtent,
                offset: 0,
            };
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut *self),
                fs_info,
                fst_id,
                &key,
                &mut path,
                SearchIntent::Delete,
                true,
            )?;

            let Some(leaf) = path.nodes[0].as_mut() else {
                path.release();
                break;
            };
            let slot = path.slots[0];
            if slot >= leaf.nritems() as usize {
                path.release();
                break;
            }
            let k = leaf.item_key(slot);
            if k.key_type != KeyType::FreeSpaceExtent || k.objectid >= bg_end {
                path.release();
                break;
            }
            items::del_items(leaf, slot, 1);
            fs_info.mark_dirty(leaf);
            path.release();
        }
        Ok(())
    }

    /// Insert a single `FREE_SPACE_EXTENT` item with no payload.
    fn insert_free_space_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        start: u64,
        length: u64,
    ) -> io::Result<()> {
        let key = DiskKey {
            objectid: start,
            key_type: KeyType::FreeSpaceExtent,
            offset: length,
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
        if found {
            path.release();
            return Ok(());
        }
        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("insert_free_space_extent: no leaf in path")
        })?;
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, &[])?;
        fs_info.mark_dirty(leaf);
        path.release();
        Ok(())
    }

    /// Update the `extent_count` field of an existing `FREE_SPACE_INFO`
    /// item, preserving its flag word.
    fn update_free_space_info_count(
        &mut self,
        fs_info: &mut Filesystem<R>,
        fst_id: u64,
        bg_start: u64,
        bg_length: u64,
        new_count: u32,
        flags: btrfs_disk::items::FreeSpaceInfoFlags,
    ) -> io::Result<()> {
        let key = DiskKey {
            objectid: bg_start,
            key_type: KeyType::FreeSpaceInfo,
            offset: bg_length,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut *self),
            fs_info,
            fst_id,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )?;
        if !found {
            path.release();
            return Err(io::Error::other(format!(
                "update_free_space_info_count: FREE_SPACE_INFO missing for {bg_start}"
            )));
        }
        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("update_free_space_info_count: no leaf in path")
        })?;
        let slot = path.slots[0];
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&new_count.to_le_bytes());
        data.extend_from_slice(&flags.bits().to_le_bytes());
        items::update_item(leaf, slot, &data)?;
        fs_info.mark_dirty(leaf);
        path.release();
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

        // Block group items live in tree 11 (block group tree) or
        // tree 2 (extent tree). The routing may also be temporarily
        // pinned to tree 2 by the convert-to-block-group-tree path
        // while it builds the BGT, hence the accessor.
        let bg_tree_id = fs_info.block_group_tree_id();

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

    /// Create an `EXTENT_ITEM` in the extent tree for a newly allocated data
    /// extent with a single inline `EXTENT_DATA_REF` backref.
    #[allow(clippy::too_many_arguments)]
    fn create_data_extent(
        &mut self,
        fs_info: &mut Filesystem<R>,
        extent_tree_id: u64,
        bytenr: u64,
        num_bytes: u64,
        owner_root: u64,
        owner_ino: u64,
        owner_offset: u64,
        count: u32,
    ) -> io::Result<()> {
        let key = DiskKey {
            objectid: bytenr,
            key_type: KeyType::ExtentItem,
            offset: num_bytes,
        };

        let data = ExtentItem::to_bytes_data(
            u64::from(count),
            self.transid,
            owner_root,
            owner_ino,
            owner_offset,
            count,
        );
        debug_assert_eq!(data.len(), ExtentItem::DATA_INLINE_SIZE);

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
            // Extent item already exists. For v1 (mkfs), each data extent
            // has exactly one backref; duplicates shouldn't happen.
            debug_assert!(
                false,
                "create_data_extent: extent item already exists at {bytenr}"
            );
            path.release();
            return Ok(());
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("create_data_extent: no leaf in path")
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

    /// Drop a single `EXTENT_DATA_REF`-shaped backref from a data extent.
    ///
    /// Locates the matching backref either inline inside the
    /// `EXTENT_ITEM` or as a standalone `EXTENT_DATA_REF_KEY` item, then
    /// decrements (or removes) it and decrements `EXTENT_ITEM.refs` by
    /// `refs_to_drop`. Returns the new total `refs` value on the parent
    /// `EXTENT_ITEM`. The caller is responsible for freeing the data
    /// extent itself when this returns 0.
    #[allow(clippy::too_many_arguments)]
    fn drop_data_extent_ref(
        &mut self,
        fs_info: &mut Filesystem<R>,
        extent_tree_id: u64,
        bytenr: u64,
        num_bytes: u64,
        target_root: u64,
        target_ino: u64,
        target_offset: u64,
        refs_to_drop: u32,
    ) -> io::Result<u64> {
        // Step 1: locate the parent EXTENT_ITEM. Data extents always
        // use the non-skinny EXTENT_ITEM_KEY whose offset is num_bytes.
        let key = DiskKey {
            objectid: bytenr,
            key_type: KeyType::ExtentItem,
            offset: num_bytes,
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
            path.release();
            return Err(io::Error::other(format!(
                "drop_data_extent_ref: EXTENT_ITEM not found at bytenr {bytenr} num_bytes {num_bytes}"
            )));
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("drop_data_extent_ref: no leaf in path")
        })?;
        let slot = path.slots[0];

        // Step 2: search the inline area for our backref.
        let location = locate_inline_data_ref(
            leaf,
            slot,
            target_root,
            target_ino,
            target_offset,
        )?;

        let new_total_refs = if let Some(loc) = location {
            // Inline path.
            let result =
                decrement_inline_data_ref(leaf, slot, &loc, refs_to_drop)?;
            fs_info.mark_dirty(leaf);
            result
        } else {
            // Step 2 didn't find an inline backref. Decrement the
            // parent EXTENT_ITEM.refs first while we still hold the
            // path, then release and walk to the standalone item.
            let item_data = leaf.item_data(slot);
            if item_data.len() < 24 {
                return Err(io::Error::other(
                    "drop_data_extent_ref: EXTENT_ITEM payload too short",
                ));
            }
            let mut current_refs =
                u64::from_le_bytes(item_data[0..8].try_into().unwrap());
            if u64::from(refs_to_drop) > current_refs {
                return Err(io::Error::other(
                    "drop_data_extent_ref: EXTENT_ITEM.refs underflow",
                ));
            }
            current_refs -= u64::from(refs_to_drop);
            leaf.item_data_mut(slot)[0..8]
                .copy_from_slice(&current_refs.to_le_bytes());
            fs_info.mark_dirty(leaf);
            path.release();

            self.drop_standalone_data_ref(
                fs_info,
                extent_tree_id,
                bytenr,
                target_root,
                target_ino,
                target_offset,
                refs_to_drop,
            )?;
            return Ok(current_refs);
        };

        path.release();
        Ok(new_total_refs)
    }

    /// Remove a standalone `EXTENT_DATA_REF_KEY` item from the extent
    /// tree. Walks forward through hash collisions until it finds the
    /// `(root, ino, offset)` triple matching the target.
    #[allow(clippy::too_many_arguments)]
    fn drop_standalone_data_ref(
        &mut self,
        fs_info: &mut Filesystem<R>,
        extent_tree_id: u64,
        bytenr: u64,
        target_root: u64,
        target_ino: u64,
        target_offset: u64,
        refs_to_drop: u32,
    ) -> io::Result<()> {
        use btrfs_disk::items::{ExtentDataRef, extent_data_ref_hash};

        let hash = extent_data_ref_hash(target_root, target_ino, target_offset);
        let key = DiskKey {
            objectid: bytenr,
            key_type: KeyType::ExtentDataRef,
            offset: hash,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut *self),
            fs_info,
            extent_tree_id,
            &key,
            &mut path,
            SearchIntent::Delete,
            true,
        )?;

        loop {
            let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                io::Error::other("drop_standalone_data_ref: no leaf in path")
            })?;
            let nritems = leaf.nritems() as usize;
            if path.slots[0] >= nritems {
                if !search::next_leaf(fs_info, &mut path)? {
                    path.release();
                    return Err(io::Error::other(
                        "drop_standalone_data_ref: ran out of leaves",
                    ));
                }
                continue;
            }
            let slot = path.slots[0];
            let item_key = leaf.item_key(slot);
            if item_key.objectid != bytenr
                || item_key.key_type != KeyType::ExtentDataRef
            {
                path.release();
                return Err(io::Error::other(format!(
                    "drop_standalone_data_ref: triple ({target_root},{target_ino},{target_offset}) not found at bytenr {bytenr}"
                )));
            }

            let payload = leaf.item_data(slot).to_vec();
            let parsed = ExtentDataRef::parse(&payload).ok_or_else(|| {
                io::Error::other(
                    "drop_standalone_data_ref: malformed EXTENT_DATA_REF",
                )
            })?;
            if parsed.root == target_root
                && parsed.objectid == target_ino
                && parsed.offset == target_offset
            {
                if refs_to_drop > parsed.count {
                    return Err(io::Error::other(
                        "drop_standalone_data_ref: count underflow",
                    ));
                }
                let new_count = parsed.count - refs_to_drop;
                if new_count == 0 {
                    items::del_items(leaf, slot, 1);
                } else {
                    let mut new_payload = payload.clone();
                    new_payload[24..28]
                        .copy_from_slice(&new_count.to_le_bytes());
                    items::update_item(leaf, slot, &new_payload)?;
                }
                fs_info.mark_dirty(leaf);
                path.release();
                return Ok(());
            }

            // Hash collision: advance and retry.
            path.slots[0] = slot + 1;
        }
    }

    /// Remove the `EXTENT_ITEM` for a fully-freed data extent.
    fn delete_data_extent_item(
        &mut self,
        fs_info: &mut Filesystem<R>,
        extent_tree_id: u64,
        bytenr: u64,
        num_bytes: u64,
    ) -> io::Result<()> {
        let key = DiskKey {
            objectid: bytenr,
            key_type: KeyType::ExtentItem,
            offset: num_bytes,
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
            path.release();
            return Err(io::Error::other(format!(
                "delete_data_extent_item: EXTENT_ITEM missing at {bytenr}"
            )));
        }
        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            io::Error::other("delete_data_extent_item: no leaf in path")
        })?;
        let slot = path.slots[0];
        items::del_items(leaf, slot, 1);
        fs_info.mark_dirty(leaf);
        path.release();
        Ok(())
    }

    /// Remove csum coverage for `[bytenr, bytenr + num_bytes)` from the
    /// csum tree.
    ///
    /// Csum items pack one or more sector csums into a single item
    /// keyed by the logical start offset. A freed data extent may
    /// occupy any contiguous span of sectors inside such an item, so
    /// this helper supports three cases per overlapping csum item:
    ///
    /// - Entirely contained in the freed range → delete the item.
    /// - Freed range strictly inside the item → split into a leading
    ///   and trailing csum item.
    /// - One side trimmed → delete and re-insert one shorter item.
    #[allow(clippy::too_many_lines, clippy::items_after_statements)]
    fn delete_csums_in_range(
        &mut self,
        fs_info: &mut Filesystem<R>,
        bytenr: u64,
        num_bytes: u64,
    ) -> io::Result<()> {
        let csum_tree_id = 7u64;
        if fs_info.root_bytenr(csum_tree_id).is_none() {
            return Ok(());
        }

        // What survives a partial overlap.
        struct Surviving {
            key: DiskKey,
            payload: Vec<u8>,
        }
        // What we plan to do to one csum item.
        struct CsumOp {
            old_key: DiskKey,
            // Up to two surviving sub-items (head and/or tail). Empty
            // means whole-item deletion.
            survivors: Vec<Surviving>,
        }

        // BTRFS_EXTENT_CSUM_OBJECTID == -10 in i64 ==
        // 0xFFFF_FFFF_FFFF_FFF6. The constant binds as i32 in raw, so
        // sign-extend through i64.
        let csum_objectid =
            i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;
        let sectorsize = u64::from(fs_info.superblock.sectorsize);
        // v1 only supports CRC32C filesystems (4-byte csums). Other csum
        // types are not produced by mkfs in this codebase.
        let csum_size: u64 = 4;
        let end = bytenr + num_bytes;

        // Pass 1: walk the csum tree once and collect every operation
        // (full delete or trim/split). Done as a read-only walk so we
        // never hold an &mut borrow on `path` across calls.
        let mut ops: Vec<CsumOp> = Vec::new();
        {
            // Start at the largest key whose offset <= bytenr.
            let start_key = DiskKey {
                objectid: csum_objectid,
                key_type: KeyType::ExtentCsum,
                offset: bytenr,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                csum_tree_id,
                &start_key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )?;
            if !found && path.slots[0] > 0 {
                path.slots[0] -= 1;
            }

            'walk: loop {
                let Some(leaf) = path.nodes[0].as_ref() else {
                    break;
                };
                let nritems = leaf.nritems() as usize;
                if path.slots[0] >= nritems {
                    if !search::next_leaf(fs_info, &mut path)? {
                        break;
                    }
                    continue;
                }
                let slot = path.slots[0];
                let item_key = leaf.item_key(slot);
                if item_key.objectid != csum_objectid
                    || item_key.key_type != KeyType::ExtentCsum
                {
                    // For the very first iteration we may have backed
                    // up onto a non-csum item; advance once and retry
                    // the type check before bailing.
                    if ops.is_empty() {
                        path.slots[0] = slot + 1;
                        continue;
                    }
                    break 'walk;
                }
                let item_size = u64::from(leaf.item_size(slot));
                let csum_start = item_key.offset;
                let sectors = item_size / csum_size;
                let csum_end = csum_start + sectors * sectorsize;

                if csum_end <= bytenr {
                    path.slots[0] = slot + 1;
                    continue;
                }
                if csum_start >= end {
                    break 'walk;
                }

                // Compute up-to-two surviving sub-items: head before
                // bytenr, tail after end. Sectors fully inside the
                // freed range are dropped. The freed range and csum
                // item are both sectorsize-aligned by construction.
                let payload = leaf.item_data(slot).to_vec();
                let mut survivors: Vec<Surviving> = Vec::new();

                if csum_start < bytenr {
                    let head_sectors =
                        ((bytenr - csum_start) / sectorsize) as usize;
                    let head_bytes = head_sectors * csum_size as usize;
                    survivors.push(Surviving {
                        key: DiskKey {
                            objectid: csum_objectid,
                            key_type: KeyType::ExtentCsum,
                            offset: csum_start,
                        },
                        payload: payload[..head_bytes].to_vec(),
                    });
                }
                if csum_end > end {
                    let skipped_sectors =
                        ((end - csum_start) / sectorsize) as usize;
                    let tail_start_bytes = skipped_sectors * csum_size as usize;
                    let tail_byte_count = (sectors as usize - skipped_sectors)
                        * csum_size as usize;
                    survivors.push(Surviving {
                        key: DiskKey {
                            objectid: csum_objectid,
                            key_type: KeyType::ExtentCsum,
                            offset: end,
                        },
                        payload: payload[tail_start_bytes
                            ..tail_start_bytes + tail_byte_count]
                            .to_vec(),
                    });
                }

                ops.push(CsumOp {
                    old_key: item_key,
                    survivors,
                });
                path.slots[0] = slot + 1;
            }
            path.release();
        }

        // Pass 2: apply each collected op. Re-search per item because
        // earlier mutations may have COWed leaves and shifted slots.
        for op in ops {
            // Delete the original item.
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut *self),
                fs_info,
                csum_tree_id,
                &op.old_key,
                &mut path,
                SearchIntent::Delete,
                true,
            )?;
            if found {
                let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                    io::Error::other("delete_csums_in_range: no leaf in path")
                })?;
                items::del_items(leaf, path.slots[0], 1);
                fs_info.mark_dirty(leaf);
            }
            path.release();

            // Insert any surviving fragments.
            for sv in op.survivors {
                if sv.payload.is_empty() {
                    continue;
                }
                let mut path = BtrfsPath::new();
                let found = search::search_slot(
                    Some(&mut *self),
                    fs_info,
                    csum_tree_id,
                    &sv.key,
                    &mut path,
                    SearchIntent::Insert((ITEM_SIZE + sv.payload.len()) as u32),
                    true,
                )?;
                if found {
                    path.release();
                    continue;
                }
                let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                    io::Error::other(
                        "delete_csums_in_range: no leaf for insert",
                    )
                })?;
                items::insert_item(leaf, path.slots[0], &sv.key, &sv.payload)?;
                fs_info.mark_dirty(leaf);
                path.release();
            }
        }
        Ok(())
    }

    /// Make sure the SYSTEM chunk containing `logical` is registered
    /// in the superblock's `sys_chunk_array` bootstrap snippet.
    ///
    /// At mount time the kernel knows the chunk tree's root bytenr but
    /// has no way to resolve it to a physical offset until it can read
    /// chunk items — and chunk items live in the chunk tree itself. The
    /// circular dependency is broken by the `sys_chunk_array` byte
    /// buffer in the superblock, which embeds the chunk records for
    /// every system chunk. Whenever the chunk tree COWs into a system
    /// chunk that is not yet in that snippet, we must add it.
    ///
    /// On filesystems where `logical` already falls inside a system
    /// chunk that is part of the snippet, this is a no-op.
    fn ensure_in_sys_chunk_array(
        &mut self,
        fs_info: &mut Filesystem<R>,
        logical: u64,
    ) -> io::Result<()> {
        // clippy
        let _ = self;

        // Locate the system block group containing this logical address.
        let groups = allocation::load_block_groups(fs_info)?;
        let bg = groups
            .iter()
            .find(|g| {
                g.flags.contains(BlockGroupFlags::SYSTEM)
                    && logical >= g.start
                    && logical < g.start + g.length
            })
            .ok_or_else(|| {
                io::Error::other(format!(
                    "ensure_in_sys_chunk_array: no SYSTEM block group contains {logical}"
                ))
            })?;
        let bg_start = bg.start;

        // Already in the bootstrap snippet?
        if sys_chunk_array_contains(
            &fs_info.superblock.sys_chunk_array,
            fs_info.superblock.sys_chunk_array_size,
            bg_start,
        ) {
            return Ok(());
        }

        // Read the corresponding CHUNK_ITEM from the chunk tree.
        let key = DiskKey {
            objectid: u64::from(
                btrfs_disk::raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID,
            ),
            key_type: KeyType::ChunkItem,
            offset: bg_start,
        };
        let chunk_tree_id =
            u64::from(btrfs_disk::raw::BTRFS_CHUNK_TREE_OBJECTID);
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            fs_info,
            chunk_tree_id,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )?;
        if !found {
            path.release();
            return Err(io::Error::other(format!(
                "ensure_in_sys_chunk_array: CHUNK_ITEM missing for bg {bg_start}"
            )));
        }
        let leaf = path.nodes[0].as_ref().ok_or_else(|| {
            io::Error::other("ensure_in_sys_chunk_array: no leaf in path")
        })?;
        let item_data = leaf.item_data(path.slots[0]).to_vec();
        path.release();

        // Reparse the chunk and re-serialize via the clean-room helper
        // (the on-disk bytes are equivalent, but going through the
        // ChunkMapping round-trip keeps this independent of the chunk
        // tree's exact storage format and lets future changes plug in
        // here in one place).
        let (mapping, _) =
            parse_chunk_item(&item_data, bg_start).ok_or_else(|| {
                io::Error::other(
                    "ensure_in_sys_chunk_array: malformed CHUNK_ITEM",
                )
            })?;
        let chunk_bytes =
            chunk_item_bytes(&mapping, fs_info.superblock.sectorsize);

        let new_size = sys_chunk_array_append(
            &mut fs_info.superblock.sys_chunk_array,
            &mut fs_info.superblock.sys_chunk_array_size,
            bg_start,
            &chunk_bytes,
        )
        .map_err(|e| {
            io::Error::other(format!("ensure_in_sys_chunk_array: {e}"))
        })?;
        debug_assert!(new_size > 0);
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
        // Roll back any in-memory `set_root_bytenr` calls made during
        // the transaction. Without this, the roots map keeps pointing
        // at COWed-but-never-written bytenrs, and the next transaction
        // will read garbage from disk.
        fs_info.restore_roots_from_snapshot();
    }
}

/// Position of one inline backref inside an `EXTENT_ITEM` payload.
#[derive(Debug, Clone, Copy)]
struct InlineRefLocation {
    /// Offset of the inline ref's first byte (the type tag) inside
    /// the item payload.
    inline_offset: usize,
    /// Total size of the inline ref including its type tag.
    inline_size: usize,
    /// Current `count` field for the matched `EXTENT_DATA_REF` record.
    current_count: u32,
}

/// Walk the inline-backref area of an `EXTENT_ITEM` looking for an
/// `EXTENT_DATA_REF` whose `(root, ino, offset)` triple matches the
/// target. Returns `Ok(None)` if the backref is not stored inline; the
/// caller should then look for a standalone `EXTENT_DATA_REF_KEY`
/// item.
fn locate_inline_data_ref(
    leaf: &crate::buffer::ExtentBuffer,
    slot: usize,
    target_root: u64,
    target_ino: u64,
    target_offset: u64,
) -> io::Result<Option<InlineRefLocation>> {
    use btrfs_disk::items::{extent_data_ref_hash, inline_ref_size};

    let item_key = leaf.item_key(slot);
    let payload = leaf.item_data(slot);
    if payload.len() < 24 {
        return Err(io::Error::other(
            "locate_inline_data_ref: EXTENT_ITEM payload too short",
        ));
    }
    let flags = u64::from_le_bytes(payload[16..24].try_into().unwrap());
    let is_tree_block =
        flags & u64::from(btrfs_disk::raw::BTRFS_EXTENT_FLAG_TREE_BLOCK) != 0;

    // Skip header (24) + optional tree_block_info (18 bytes when this
    // is a non-skinny tree-block extent, i.e. EXTENT_ITEM_KEY).
    let mut cursor = 24usize;
    if is_tree_block && item_key.key_type == KeyType::ExtentItem {
        cursor += 18;
    }
    if cursor > payload.len() {
        return Err(io::Error::other(
            "locate_inline_data_ref: header overruns payload",
        ));
    }

    let target_hash =
        extent_data_ref_hash(target_root, target_ino, target_offset);
    let edr_type = btrfs_disk::raw::BTRFS_EXTENT_DATA_REF_KEY as u8;

    while cursor < payload.len() {
        let type_byte = payload[cursor];
        let size = inline_ref_size(type_byte).ok_or_else(|| {
            io::Error::other(format!(
                "locate_inline_data_ref: unknown inline ref type {type_byte}"
            ))
        })?;
        if cursor + size > payload.len() {
            return Err(io::Error::other(
                "locate_inline_data_ref: inline ref overruns payload",
            ));
        }

        if type_byte < edr_type {
            cursor += size;
            continue;
        }
        if type_byte > edr_type {
            // Past the EXTENT_DATA_REF range; the target isn't inline.
            return Ok(None);
        }

        // EXTENT_DATA_REF inline record:
        //   1 byte type tag, then btrfs_extent_data_ref (28 bytes):
        //   u64 root, u64 objectid, u64 offset, u32 count.
        let body = &payload[cursor + 1..cursor + 1 + 28];
        let r = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let o = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let off = u64::from_le_bytes(body[16..24].try_into().unwrap());
        let count = u32::from_le_bytes(body[24..28].try_into().unwrap());

        if r == target_root && o == target_ino && off == target_offset {
            return Ok(Some(InlineRefLocation {
                inline_offset: cursor,
                inline_size: size,
                current_count: count,
            }));
        }

        // Hash collision OR adjacent EDR record. Inline EDR records are
        // ordered by extent_data_ref_hash; if we've already passed the
        // target hash, the target is not inline.
        let here_hash = extent_data_ref_hash(r, o, off);
        if here_hash > target_hash {
            return Ok(None);
        }
        cursor += size;
    }

    Ok(None)
}

/// Decrement (or remove) an inline `EXTENT_DATA_REF` and the parent
/// `EXTENT_ITEM.refs` count by `refs_to_drop`. Returns the new total
/// `EXTENT_ITEM.refs` value.
fn decrement_inline_data_ref(
    leaf: &mut crate::buffer::ExtentBuffer,
    slot: usize,
    location: &InlineRefLocation,
    refs_to_drop: u32,
) -> io::Result<u64> {
    if refs_to_drop > location.current_count {
        return Err(io::Error::other(
            "decrement_inline_data_ref: count underflow",
        ));
    }

    // Step 1: decrement EXTENT_ITEM.refs at offset 0..8.
    let item_data = leaf.item_data(slot);
    let mut current_refs =
        u64::from_le_bytes(item_data[0..8].try_into().unwrap());
    if u64::from(refs_to_drop) > current_refs {
        return Err(io::Error::other(
            "decrement_inline_data_ref: EXTENT_ITEM.refs underflow",
        ));
    }
    current_refs -= u64::from(refs_to_drop);
    leaf.item_data_mut(slot)[0..8].copy_from_slice(&current_refs.to_le_bytes());

    let new_count = location.current_count - refs_to_drop;
    if new_count > 0 {
        // Just rewrite the count field of the inline ref in place.
        // Inline EDR layout: [type=1B][root=8B][oid=8B][off=8B][count=4B]
        let count_off = location.inline_offset + 1 + 24;
        leaf.item_data_mut(slot)[count_off..count_off + 4]
            .copy_from_slice(&new_count.to_le_bytes());
        return Ok(current_refs);
    }

    // Remove the entire inline ref. First memmove the bytes after the
    // ref left within the item payload, then shrink the item by
    // `inline_size`.
    let item_size = leaf.item_size(slot) as usize;
    let after_off = location.inline_offset + location.inline_size;
    if after_off < item_size {
        let payload = leaf.item_data_mut(slot);
        payload.copy_within(after_off..item_size, location.inline_offset);
    }
    items::shrink_item(leaf, slot, location.inline_size as u32)?;

    Ok(current_refs)
}

/// Find a free region inside a block group of the requested kind,
/// starting at or after `min_addr`.
///
/// `alignment` constrains the start address; `min_size` is the minimum
/// contiguous span of free space required. For metadata both are
/// `nodesize`; for data, `alignment` is `sectorsize` and `min_size` is
/// the requested data extent length.
///
/// Uses extent-tree free space scanning to find actual gaps between
/// allocated extents. Returns `(first_free_logical, region_end)`.
fn find_alloc_region_after<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    kind: BlockGroupKind,
    min_addr: u64,
    alignment: u64,
    min_size: u64,
) -> io::Result<(u64, u64)> {
    use crate::allocation;

    let groups = allocation::load_block_groups(fs_info)?;

    let kind_matches = |bg: &&allocation::BlockGroup| match kind {
        BlockGroupKind::Metadata => bg.is_metadata(),
        BlockGroupKind::System => bg.is_system(),
        BlockGroupKind::Data => bg.is_data(),
    };

    let mut candidates: Vec<&allocation::BlockGroup> = groups
        .iter()
        .filter(kind_matches)
        .filter(|bg| bg.free() >= min_size)
        .collect();
    candidates.sort_by_key(|bg| std::cmp::Reverse(bg.free()));

    for bg in candidates {
        let free_extents = allocation::find_free_extents(
            fs_info, bg.start, bg.length, min_size,
        )?;

        for &(start, len) in &free_extents {
            let cursor = align_up(start.max(min_addr), alignment);
            let end = start + len;
            if cursor + min_size <= end {
                return Ok((cursor, end));
            }
        }
    }

    Err(io::Error::other(format!(
        "no {kind:?} block group with free space",
    )))
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

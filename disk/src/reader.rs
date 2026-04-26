//! # Block device reader with logical-to-physical address resolution
//!
//! Provides `BlockReader` which reads btrfs tree blocks by logical address,
//! resolving them through the chunk tree cache. Also provides `filesystem_open`
//! which bootstraps a complete `BlockReader` from a raw block device or image.

use crate::{
    chunk::{self, ChunkTreeCache},
    raw,
    superblock::{self, Superblock},
    tree::{KeyType, TreeBlock},
};
use bytes::Buf;
use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};

/// A block reader that resolves logical addresses through a chunk cache.
///
/// Holds one I/O handle per device, keyed by `devid`. For single-device
/// filesystems the map has a single entry. For RAID1 / RAID1C3 / RAID1C4
/// / RAID10 / DUP, each stripe's `devid` (from the chunk cache) is used
/// to look up the handle. SINGLE and DUP work with a one-entry map.
pub struct BlockReader<R> {
    devices: BTreeMap<u64, R>,
    nodesize: u32,
    chunk_cache: ChunkTreeCache,
}

impl<R> BlockReader<R> {
    /// Create a single-device block reader.
    ///
    /// `devid` is the device id (`superblock.dev_item.devid`) under which
    /// this handle is registered. Stripe lookups for this device must
    /// resolve to this devid.
    pub fn new(
        handle: R,
        devid: u64,
        nodesize: u32,
        chunk_cache: ChunkTreeCache,
    ) -> Self {
        let mut devices = BTreeMap::new();
        devices.insert(devid, handle);
        Self {
            devices,
            nodesize,
            chunk_cache,
        }
    }

    /// Create a multi-device block reader.
    ///
    /// `devices` maps each device id to its I/O handle. Every devid
    /// referenced by the chunk cache must be present.
    #[must_use]
    pub fn new_multi(
        devices: BTreeMap<u64, R>,
        nodesize: u32,
        chunk_cache: ChunkTreeCache,
    ) -> Self {
        Self {
            devices,
            nodesize,
            chunk_cache,
        }
    }

    /// Return the per-devid handle map.
    #[must_use]
    pub fn devices(&self) -> &BTreeMap<u64, R> {
        &self.devices
    }

    /// Return the per-devid handle map mutably.
    ///
    /// Used by transaction commit / sync / flush paths that need to
    /// flush every device. For ordinary reads/writes prefer
    /// [`read_block`](Self::read_block), [`read_data`](Self::read_data),
    /// or [`write_block`](Self::write_block) which route by devid via
    /// the chunk cache.
    pub fn devices_mut(&mut self) -> &mut BTreeMap<u64, R> {
        &mut self.devices
    }

    /// Return the underlying handle for a single-device filesystem.
    ///
    /// Convenience for offline tools (`btrfs-tune`,
    /// `btrfs filesystem resize` on a regular file) that operate on
    /// one device at a time and need raw file access (e.g. `set_len`
    /// or full-file scans).
    ///
    /// # Panics
    ///
    /// Panics if more than one device is open. Multi-device callers
    /// must use [`devices_mut`](Self::devices_mut) and route by
    /// devid explicitly.
    pub fn single_device_mut(&mut self) -> &mut R {
        assert_eq!(
            self.devices.len(),
            1,
            "single_device_mut: filesystem has {} devices, not 1",
            self.devices.len(),
        );
        self.devices.values_mut().next().unwrap()
    }
}

impl<R: Read + Seek> BlockReader<R> {
    /// Read raw bytes at a logical address, resolving to physical via the chunk cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the logical address is unmapped, the resolved
    /// device id is not in the handle map, or the underlying read fails.
    pub fn read_block(&mut self, logical: u64) -> io::Result<Vec<u8>> {
        // Tree blocks are always nodesize <= stripe_len, so a single
        // block lives entirely in one row. Use plan_read so striped
        // profiles (RAID0, RAID10) route to the correct device column;
        // mirrored/SINGLE chunks return one placement on stripes[0]
        // either way.
        self.read_data(logical, self.nodesize as usize)
    }

    /// Read and parse a tree block at a logical address.
    ///
    /// # Errors
    ///
    /// Returns an error if the logical address is unmapped or the underlying read fails.
    pub fn read_tree_block(&mut self, logical: u64) -> io::Result<TreeBlock> {
        let buf = self.read_block(logical)?;
        Ok(TreeBlock::parse(&buf))
    }

    /// Return a reference to the chunk cache.
    #[must_use]
    pub fn chunk_cache(&self) -> &ChunkTreeCache {
        &self.chunk_cache
    }

    /// Return a mutable reference to the chunk cache.
    pub fn chunk_cache_mut(&mut self) -> &mut ChunkTreeCache {
        &mut self.chunk_cache
    }

    /// Return the nodesize.
    #[must_use]
    pub fn nodesize(&self) -> u32 {
        self.nodesize
    }

    /// Read arbitrary data at a logical address (not limited to nodesize).
    ///
    /// Unlike `read_block` which always reads `nodesize` bytes, this reads
    /// exactly `len` bytes. Used for reading file data extents.
    ///
    /// Uses [`ChunkTreeCache::plan_read`](crate::chunk::ChunkTreeCache::plan_read)
    /// internally so reads on striped profiles (RAID0 / RAID10) that span
    /// multiple rows assemble the bytes from the correct devices in
    /// order.
    ///
    /// # Errors
    ///
    /// Returns an error if the logical address is unmapped, the request
    /// extends past the chunk, the chunk uses RAID5/RAID6 (not yet
    /// implemented), or the underlying read fails.
    pub fn read_data(
        &mut self,
        logical: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let placements =
            self.chunk_cache.plan_read(logical, len).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "logical address {logical} not mapped or unsupported profile"
                    ),
                )
            })?;
        let mut buf = vec![0u8; len];
        for p in placements {
            let dev = self.device_handle_mut(p.devid)?;
            dev.seek(SeekFrom::Start(p.physical))?;
            dev.read_exact(&mut buf[p.buf_offset..p.buf_offset + p.len])?;
        }
        Ok(buf)
    }

    /// Look up a device handle by `devid`. Returns a clear error if the
    /// chunk cache references a device that was not opened.
    fn device_handle_mut(&mut self, devid: u64) -> io::Result<&mut R> {
        self.devices.get_mut(&devid).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "device {devid} not open (referenced by the chunk cache)"
                ),
            )
        })
    }
}

impl<R: Read + Write + Seek> BlockReader<R> {
    /// Write raw bytes to a logical address, routing to the correct
    /// per-device locations based on the chunk's RAID profile.
    ///
    /// Uses [`ChunkTreeCache::plan_write`](crate::chunk::ChunkTreeCache::plan_write)
    /// internally. For DUP / RAID1 / RAID1C3 / RAID1C4 every mirror
    /// receives the same bytes; for RAID0 each row goes to exactly one
    /// device; for RAID10 each row goes to one mirror pair. Writes
    /// larger than `stripe_len` on a striped profile are split into
    /// per-row segments automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if the logical address is unmapped, the request
    /// extends past the chunk, any referenced device is not open, the
    /// chunk uses RAID5/RAID6 (not yet implemented), or any underlying
    /// write fails.
    pub fn write_block(&mut self, logical: u64, buf: &[u8]) -> io::Result<()> {
        let placements =
            self.chunk_cache.plan_write(logical, buf.len()).ok_or_else(
                || {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "logical address {logical} not mapped or unsupported profile"
                        ),
                    )
                },
            )?;
        for p in placements {
            let dev = self.device_handle_mut(p.devid)?;
            dev.seek(SeekFrom::Start(p.physical))?;
            dev.write_all(&buf[p.buf_offset..p.buf_offset + p.len])?;
        }
        Ok(())
    }
}

/// Result of opening a btrfs filesystem from a block device or image.
pub struct OpenFilesystem<R> {
    /// Block reader with fully populated chunk cache.
    pub reader: BlockReader<R>,
    /// Parsed primary-device superblock.
    pub superblock: Superblock,
    /// Map of tree ID -> (root block logical address, key offset), from the root tree.
    pub tree_roots: BTreeMap<u64, (u64, u64)>,
    /// Per-device `dev_item` snapshots taken at open time. One entry
    /// per opened device (always at least the primary). The transaction
    /// crate uses these to splice the correct per-device identity into
    /// the superblock when writing it back during commit, so a
    /// multi-device filesystem doesn't get clobbered with the primary
    /// device's `dev_item`.
    pub per_device_dev_items: BTreeMap<u64, crate::items::DeviceItem>,
}

/// Open a btrfs filesystem by bootstrapping from the superblock.
///
/// This performs the full bootstrap sequence:
/// 1. Read the superblock (mirror 0)
/// 2. Seed the chunk cache from the `sys_chunk_array`
/// 3. Read the full chunk tree to complete the cache
/// 4. Read the root tree to collect all tree root pointers
///
/// # Errors
///
/// Returns an error if any I/O operation fails during bootstrap.
pub fn filesystem_open<R: Read + Seek>(
    reader: R,
) -> io::Result<OpenFilesystem<R>> {
    filesystem_open_mirror(reader, 0)
}

/// Open a btrfs filesystem using a specific superblock mirror (0, 1, or 2).
///
/// # Errors
///
/// Returns an error if any I/O operation fails during bootstrap.
pub fn filesystem_open_mirror<R: Read + Seek>(
    reader: R,
    mirror: u32,
) -> io::Result<OpenFilesystem<R>> {
    let mut reader = reader;

    // Step 1: read the superblock
    let sb = superblock::read_superblock(&mut reader, mirror)?;
    let primary_devid = sb.dev_item.devid;
    let mut per_device_dev_items = BTreeMap::new();
    per_device_dev_items.insert(primary_devid, sb.dev_item.clone());

    // Step 2: seed chunk cache from sys_chunk_array
    let chunk_cache = chunk::seed_from_sys_chunk_array(
        &sb.sys_chunk_array,
        sb.sys_chunk_array_size,
    );

    let mut block_reader =
        BlockReader::new(reader, primary_devid, sb.nodesize, chunk_cache);

    // Step 3: read the full chunk tree to complete the cache
    read_chunk_tree(&mut block_reader, sb.chunk_root)?;

    // Step 4: read the root tree to collect tree roots
    let tree_roots = read_root_tree(&mut block_reader, sb.root)?;

    Ok(OpenFilesystem {
        reader: block_reader,
        superblock: sb,
        tree_roots,
        per_device_dev_items,
    })
}

/// Open a multi-device btrfs filesystem from a map of `devid -> handle`.
///
/// All devices in the filesystem must be present in the map; the
/// bootstrap fails with a clear error if the chunk tree references a
/// devid that is not in the map. Each device's superblock is read and
/// its `dev_item.devid` is verified against the map key, and all
/// devices' `fsid` must match.
///
/// The "primary" superblock used for filesystem-wide fields (root,
/// `chunk_root`, generation, etc.) is the one with the lowest devid.
///
/// # Errors
///
/// Returns an error if any superblock cannot be read, any device's
/// superblock disagrees with its map key or with the primary's fsid,
/// or the chunk tree references a devid not in the map.
///
/// # Panics
///
/// Panics if the in-memory device map is empty after the initial
/// non-empty check (an internal invariant violation, not a runtime
/// possibility).
pub fn filesystem_open_multi<R: Read + Seek>(
    devices: BTreeMap<u64, R>,
) -> io::Result<OpenFilesystem<R>> {
    if devices.is_empty() {
        return Err(io::Error::other(
            "filesystem_open_multi: device map is empty",
        ));
    }
    let mut devices = devices;

    // Step 1: read each device's superblock and validate identity.
    let mut per_device_dev_items: BTreeMap<u64, crate::items::DeviceItem> =
        BTreeMap::new();
    let mut superblocks: BTreeMap<u64, Superblock> = BTreeMap::new();
    for (&devid, dev) in &mut devices {
        let sb = superblock::read_superblock(dev, 0)?;
        if sb.dev_item.devid != devid {
            return Err(io::Error::other(format!(
                "device map key {devid} doesn't match superblock dev_item.devid {}",
                sb.dev_item.devid,
            )));
        }
        per_device_dev_items.insert(devid, sb.dev_item.clone());
        superblocks.insert(devid, sb);
    }

    // Step 2: pick the lowest-devid superblock as authoritative for
    // filesystem-wide fields, and validate fsid consistency.
    let primary_sb = superblocks.values().next().unwrap().clone();
    for (devid, sb) in &superblocks {
        if sb.fsid != primary_sb.fsid {
            return Err(io::Error::other(format!(
                "device {devid} fsid {} differs from primary fsid {}",
                sb.fsid, primary_sb.fsid,
            )));
        }
    }

    // Step 3: seed chunk cache from primary's sys_chunk_array.
    let chunk_cache = chunk::seed_from_sys_chunk_array(
        &primary_sb.sys_chunk_array,
        primary_sb.sys_chunk_array_size,
    );

    let mut block_reader =
        BlockReader::new_multi(devices, primary_sb.nodesize, chunk_cache);

    // Step 4: read the full chunk tree to complete the cache.
    read_chunk_tree(&mut block_reader, primary_sb.chunk_root)?;

    // Step 5: validate every devid the chunk cache references is open.
    let mut referenced: std::collections::BTreeSet<u64> =
        std::collections::BTreeSet::new();
    for mapping in block_reader.chunk_cache().iter() {
        for stripe in &mapping.stripes {
            referenced.insert(stripe.devid);
        }
    }
    for devid in &referenced {
        if !block_reader.devices().contains_key(devid) {
            return Err(io::Error::other(format!(
                "chunk tree references devid {devid} but no handle was provided"
            )));
        }
    }

    // Step 6: read the root tree.
    let tree_roots = read_root_tree(&mut block_reader, primary_sb.root)?;

    Ok(OpenFilesystem {
        reader: block_reader,
        superblock: primary_sb,
        tree_roots,
        per_device_dev_items,
    })
}

/// Open a btrfs filesystem using a pre-built chunk cache.
///
/// Skips the chunk tree walk entirely, using the provided cache for
/// all logical-to-physical address resolution. This is the entry point
/// for `rescue chunk-recover --apply`, where the on-disk chunk tree is
/// damaged and the cache has been reconstructed from a raw device scan.
///
/// The root tree is still read normally (it becomes accessible once the
/// correct chunk mappings are in place).
///
/// # Errors
///
/// Returns an error if the superblock read or root tree walk fails.
pub fn filesystem_open_with_cache<R: Read + Seek>(
    reader: R,
    mirror: u32,
    chunk_cache: ChunkTreeCache,
) -> io::Result<OpenFilesystem<R>> {
    let mut reader = reader;
    let sb = superblock::read_superblock(&mut reader, mirror)?;
    let primary_devid = sb.dev_item.devid;
    let mut per_device_dev_items = BTreeMap::new();
    per_device_dev_items.insert(primary_devid, sb.dev_item.clone());
    let mut block_reader =
        BlockReader::new(reader, primary_devid, sb.nodesize, chunk_cache);
    let tree_roots = read_root_tree(&mut block_reader, sb.root)?;

    Ok(OpenFilesystem {
        reader: block_reader,
        superblock: sb,
        tree_roots,
        per_device_dev_items,
    })
}

/// Recursively read the chunk tree to populate the chunk cache.
///
/// Starting from the given root block, walks all leaves and inserts any
/// `CHUNK_ITEM` entries that are not already present in the cache.
///
/// # Errors
///
/// Returns an error if any tree block read fails.
pub fn read_chunk_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
) -> io::Result<()> {
    let block = reader.read_tree_block(root_logical)?;

    match &block {
        TreeBlock::Leaf { items, data, .. } => {
            for item in items {
                if item.key.key_type != KeyType::ChunkItem {
                    continue;
                }
                let item_data = &data[mem::size_of::<raw::btrfs_header>()
                    + item.offset as usize..];
                if let Some((mapping, _)) =
                    chunk::parse_chunk_item(item_data, item.key.offset)
                {
                    // Only insert if not already in cache (sys_chunk_array may
                    // have already seeded some entries)
                    if reader.chunk_cache.lookup(mapping.logical).is_none() {
                        reader.chunk_cache.insert(mapping);
                    }
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                read_chunk_tree(reader, ptr.blockptr)?;
            }
        }
    }

    Ok(())
}

/// Read the root tree to collect all tree root pointers.
///
/// Returns a map of `tree_id` (objectid) -> `(root_bytenr, key_offset)`.
///
/// # Errors
///
/// Returns an error if any tree block read fails.
pub fn read_root_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
) -> io::Result<BTreeMap<u64, (u64, u64)>> {
    let mut tree_roots = BTreeMap::new();
    collect_root_items(reader, root_logical, &mut tree_roots)?;
    Ok(tree_roots)
}

/// Tree traversal order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Traversal {
    /// Breadth-first: print all nodes at level N, then N-1, down to leaves.
    Bfs,
    /// Depth-first: print a node, then recursively its children.
    Dfs,
}

/// Walk a tree starting at `root_logical`, calling `visitor` for each block.
///
/// # Errors
///
/// Returns an error if any tree block cannot be read.
pub fn tree_walk<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    traversal: Traversal,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    match traversal {
        Traversal::Bfs => tree_walk_bfs(reader, root_logical, visitor),
        Traversal::Dfs => tree_walk_dfs(reader, root_logical, visitor),
    }
}

fn tree_walk_dfs<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    let block = reader.read_tree_block(logical)?;
    visitor(&block);

    if let TreeBlock::Node { ptrs, .. } = &block {
        for ptr in ptrs {
            tree_walk_dfs(reader, ptr.blockptr, visitor)?;
        }
    }

    Ok(())
}

fn tree_walk_bfs<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    let root_block = reader.read_tree_block(root_logical)?;
    let root_level = root_block.header().level;
    visitor(&root_block);

    let mut current_level_ptrs: Vec<u64> = match &root_block {
        TreeBlock::Node { ptrs, .. } => {
            ptrs.iter().map(|p| p.blockptr).collect()
        }
        TreeBlock::Leaf { .. } => return Ok(()),
    };

    for _level in (0..root_level).rev() {
        let mut next_level_ptrs = Vec::new();

        for logical in &current_level_ptrs {
            let block = reader.read_tree_block(*logical)?;
            visitor(&block);

            if let TreeBlock::Node { ptrs, .. } = &block {
                next_level_ptrs.extend(ptrs.iter().map(|p| p.blockptr));
            }
        }

        current_level_ptrs = next_level_ptrs;
    }

    Ok(())
}

/// Walk a tree, continuing past individual block read errors.
///
/// Unlike [`tree_walk`], this does not stop when a child block cannot be read.
/// Instead, it calls `on_error` with the logical address and the I/O error,
/// then continues with remaining siblings. The root block failure still
/// propagates since there is nothing to walk.
///
/// # Errors
///
/// Returns an error only if the root block itself cannot be read.
pub fn tree_walk_tolerant<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    visitor: &mut dyn FnMut(&[u8], &TreeBlock),
    on_error: &mut dyn FnMut(u64, &io::Error),
) -> io::Result<()> {
    let buf = reader.read_block(root_logical)?;
    let block = TreeBlock::parse(&buf);
    visitor(&buf, &block);

    if let TreeBlock::Node { ptrs, .. } = &block {
        for ptr in ptrs {
            tree_walk_tolerant_dfs(reader, ptr.blockptr, visitor, on_error);
        }
    }

    Ok(())
}

fn tree_walk_tolerant_dfs<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    visitor: &mut dyn FnMut(&[u8], &TreeBlock),
    on_error: &mut dyn FnMut(u64, &io::Error),
) {
    let buf = match reader.read_block(logical) {
        Ok(b) => b,
        Err(e) => {
            on_error(logical, &e);
            return;
        }
    };
    let block = TreeBlock::parse(&buf);
    visitor(&buf, &block);

    if let TreeBlock::Node { ptrs, .. } = &block {
        for ptr in ptrs {
            tree_walk_tolerant_dfs(reader, ptr.blockptr, visitor, on_error);
        }
    }
}

/// Walk a tree (DFS), allowing the visitor to modify each block in place.
///
/// The visitor receives the raw block buffer and the parsed `TreeBlock`. If it
/// returns `true`, the block is re-checksummed using `csum_type` and written
/// back to disk. This is used by operations that need to patch tree block
/// headers or items (e.g. fsid rewrite, repair).
///
/// # Errors
///
/// Returns an error if the root block cannot be read or any write fails.
pub fn tree_walk_mut<R: Read + Write + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    csum_type: superblock::ChecksumType,
    visitor: &mut dyn FnMut(&mut Vec<u8>, &TreeBlock) -> bool,
) -> io::Result<()> {
    let mut buf = reader.read_block(root_logical)?;
    let block = TreeBlock::parse(&buf);

    let child_ptrs: Vec<u64> = if let TreeBlock::Node { ptrs, .. } = &block {
        ptrs.iter().map(|p| p.blockptr).collect()
    } else {
        Vec::new()
    };

    if visitor(&mut buf, &block) {
        crate::util::csum_tree_block(&mut buf, csum_type);
        reader.write_block(root_logical, &buf)?;
    }

    for ptr in child_ptrs {
        tree_walk_mut(reader, ptr, csum_type, visitor)?;
    }

    Ok(())
}

/// Read a single block and call `visitor` (and optionally walk children with `follow`).
///
/// # Errors
///
/// Returns an error if any tree block cannot be read.
pub fn block_visit<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    follow: bool,
    traversal: Traversal,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    if follow {
        tree_walk(reader, logical, traversal, visitor)
    } else {
        let block = reader.read_tree_block(logical)?;
        visitor(&block);
        Ok(())
    }
}

/// Statistics collected by walking all blocks of a single B-tree.
#[derive(Debug, Clone)]
pub struct TreeStats {
    /// Total number of tree blocks (nodes and leaves).
    pub total_nodes: u64,
    /// Total bytes occupied by tree blocks (`total_nodes × nodesize`).
    pub total_bytes: u64,
    /// Total bytes of inline file data (non-zero only when `find_inline` is true).
    pub total_inline: u64,
    /// Number of non-contiguous jumps between sibling block addresses.
    pub total_seeks: u64,
    /// Seeks where the next sibling is at a higher address.
    pub forward_seeks: u64,
    /// Seeks where the next sibling is at a lower address.
    pub backward_seeks: u64,
    /// Sum of all seek distances in bytes.
    pub total_seek_len: u64,
    /// Largest single seek distance in bytes.
    pub max_seek_len: u64,
    /// Number of contiguous block runs (clusters) counted between seeks.
    pub total_clusters: u64,
    /// Sum of all cluster sizes in bytes.
    pub total_cluster_size: u64,
    /// Smallest cluster size in bytes (`u64::MAX` if no seeks occurred).
    pub min_cluster_size: u64,
    /// Largest cluster size in bytes (initialised to `nodesize`).
    pub max_cluster_size: u64,
    /// Lowest block bytenr seen during the walk.
    pub lowest_bytenr: u64,
    /// Highest block bytenr seen during the walk.
    pub highest_bytenr: u64,
    /// Number of blocks at each level: index 0 = leaves, higher = internal.
    pub node_counts: Vec<u64>,
    /// Tree height (number of levels, root level + 1).
    pub levels: u8,
}

/// Walk `root_logical` collecting [`TreeStats`].
///
/// When `find_inline` is true the walk also counts inline extent data bytes
/// (relevant for subvolume / FS trees which contain `EXTENT_DATA` items).
///
/// # Errors
///
/// Returns an error if any tree block cannot be read.
pub fn tree_stats_collect<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    find_inline: bool,
) -> io::Result<TreeStats> {
    let root_block = reader.read_tree_block(root_logical)?;
    let nodesize = u64::from(reader.nodesize());
    let root_level = root_block.header().level;
    let root_bytenr = root_block.header().bytenr;

    let mut stats = TreeStats {
        total_nodes: 0,
        total_bytes: 0,
        total_inline: 0,
        total_seeks: 0,
        forward_seeks: 0,
        backward_seeks: 0,
        total_seek_len: 0,
        max_seek_len: 0,
        total_clusters: 0,
        total_cluster_size: 0,
        min_cluster_size: u64::MAX,
        max_cluster_size: nodesize,
        lowest_bytenr: root_bytenr,
        highest_bytenr: root_bytenr,
        node_counts: vec![0u64; root_level as usize + 1],
        levels: root_level + 1,
    };

    walk_stats(reader, root_block, &mut stats, find_inline, nodesize)?;
    Ok(stats)
}

/// Recursively walk a tree block, accumulating stats.
fn walk_stats<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    block: TreeBlock,
    stats: &mut TreeStats,
    find_inline: bool,
    nodesize: u64,
) -> io::Result<()> {
    let level = block.header().level;
    let bytenr = block.header().bytenr;

    stats.total_nodes += 1;
    stats.total_bytes += nodesize;
    if (level as usize) < stats.node_counts.len() {
        stats.node_counts[level as usize] += 1;
    }
    if bytenr < stats.lowest_bytenr {
        stats.lowest_bytenr = bytenr;
    }
    if bytenr > stats.highest_bytenr {
        stats.highest_bytenr = bytenr;
    }

    match block {
        TreeBlock::Leaf { items, data, .. } => {
            if find_inline {
                let type_off =
                    mem::offset_of!(raw::btrfs_file_extent_item, type_);
                let inline_hdr_size =
                    mem::offset_of!(raw::btrfs_file_extent_item, disk_bytenr);
                let header_size = mem::size_of::<raw::btrfs_header>();
                for item in &items {
                    if item.key.key_type != KeyType::ExtentData {
                        continue;
                    }
                    let start = header_size + item.offset as usize;
                    if start + type_off >= data.len() {
                        continue;
                    }
                    // BTRFS_FILE_EXTENT_INLINE == 0
                    if data[start + type_off] == 0
                        && item.size as usize > inline_hdr_size
                    {
                        stats.total_inline +=
                            u64::from(item.size) - inline_hdr_size as u64;
                    }
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            let mut last_block = bytenr;
            let mut cluster_size = nodesize;

            for ptr in ptrs {
                let child = reader.read_tree_block(ptr.blockptr)?;
                walk_stats(reader, child, stats, find_inline, nodesize)?;

                let cur = ptr.blockptr;
                if last_block + nodesize == cur {
                    cluster_size += nodesize;
                } else {
                    let distance = cur.abs_diff(last_block + nodesize);
                    stats.total_seeks += 1;
                    stats.total_seek_len += distance;
                    if distance > stats.max_seek_len {
                        stats.max_seek_len = distance;
                    }
                    if cur > last_block + nodesize {
                        stats.forward_seeks += 1;
                    } else {
                        stats.backward_seeks += 1;
                    }
                    if cluster_size != nodesize {
                        stats.total_clusters += 1;
                        stats.total_cluster_size += cluster_size;
                        if cluster_size < stats.min_cluster_size {
                            stats.min_cluster_size = cluster_size;
                        }
                        if cluster_size > stats.max_cluster_size {
                            stats.max_cluster_size = cluster_size;
                        }
                    }
                    cluster_size = nodesize;
                }
                last_block = cur;
            }
        }
    }

    Ok(())
}

fn collect_root_items<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    tree_roots: &mut BTreeMap<u64, (u64, u64)>,
) -> io::Result<()> {
    let block = reader.read_tree_block(logical)?;

    match &block {
        TreeBlock::Leaf { items, data, .. } => {
            // ROOT_ITEM contains btrfs_root_item; the bytenr field gives
            // the root block of that tree.
            let header_size = mem::size_of::<raw::btrfs_header>();
            let root_item_bytenr_offset = {
                // bytenr is after inode (160 bytes) + generation (8) + root_dirid (8)
                // = offset 176 in btrfs_root_item
                mem::offset_of!(raw::btrfs_root_item, bytenr)
            };

            for item in items {
                if item.key.key_type != KeyType::RootItem {
                    continue;
                }
                let item_start = header_size + item.offset as usize;
                if item_start + root_item_bytenr_offset + 8 > data.len() {
                    continue;
                }
                let mut buf = &data[item_start + root_item_bytenr_offset..];
                let bytenr = buf.get_u64_le();
                if bytenr != 0 {
                    tree_roots
                        .insert(item.key.objectid, (bytenr, item.key.offset));
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                collect_root_items(reader, ptr.blockptr, tree_roots)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkMapping, Stripe};
    use std::io::Cursor;
    use uuid::Uuid;

    /// Build a per-device cursor backed by a `len`-byte zero-filled
    /// `Vec<u8>` for use as a fake device.
    fn make_device(len: usize) -> Cursor<Vec<u8>> {
        Cursor::new(vec![0u8; len])
    }

    /// Build a chunk mapping with arbitrary stripes. Each entry is
    /// `(devid, physical_offset)`. The profile defaults to SINGLE for
    /// one-stripe mappings and RAID1 for multi-stripe (matches what
    /// the old `resolve_all`-based tests assumed).
    fn make_mapping(
        logical: u64,
        length: u64,
        stripes: &[(u64, u64)],
    ) -> ChunkMapping {
        let chunk_type = if stripes.len() == 1 {
            0 // SINGLE
        } else {
            // Pick a profile that fans out to all stripes regardless of
            // count: DUP when stripes share a devid, otherwise
            // RAID1 / RAID1C3 / RAID1C4 by mirror count.
            let same_devid = stripes.iter().all(|s| s.0 == stripes[0].0);
            if same_devid {
                u64::from(raw::BTRFS_BLOCK_GROUP_DUP)
            } else {
                u64::from(match stripes.len() {
                    3 => raw::BTRFS_BLOCK_GROUP_RAID1C3,
                    4 => raw::BTRFS_BLOCK_GROUP_RAID1C4,
                    // Default: RAID1 (covers 2 stripes and any
                    // unexpected count — every stripe gets the bytes).
                    _ => raw::BTRFS_BLOCK_GROUP_RAID1,
                })
            }
        };
        ChunkMapping {
            logical,
            length,
            stripe_len: 65536,
            chunk_type,
            num_stripes: stripes.len() as u16,
            sub_stripes: 0,
            stripes: stripes
                .iter()
                .map(|&(devid, offset)| Stripe {
                    devid,
                    offset,
                    dev_uuid: Uuid::nil(),
                })
                .collect(),
        }
    }

    /// Build a `BlockReader` over the supplied per-devid cursors with
    /// the given chunk cache contents and a 4 KiB nodesize.
    fn make_reader(
        devices: &[(u64, usize)],
        mappings: &[ChunkMapping],
    ) -> BlockReader<Cursor<Vec<u8>>> {
        let mut handles = BTreeMap::new();
        for &(devid, len) in devices {
            handles.insert(devid, make_device(len));
        }
        let mut cache = ChunkTreeCache::default();
        for m in mappings {
            cache.insert(m.clone());
        }
        BlockReader::new_multi(handles, 4096, cache)
    }

    #[test]
    fn read_block_routes_to_correct_devid() {
        // Two devices, each carrying distinguishable bytes at the
        // physical offset that the chunk mapping resolves the logical
        // address to. resolve picks stripe[0]; we put devid=2 first
        // in the stripe list to verify routing follows that, not the
        // numerically-lowest devid.
        let mapping =
            make_mapping(1_000_000, 4096, &[(2, 50_000), (1, 20_000)]);
        let mut reader = make_reader(&[(1, 100_000), (2, 100_000)], &[mapping]);

        // Seed each device's cursor with a recognizable byte at the
        // physical offset we'll route to.
        reader.devices_mut().get_mut(&1).unwrap().get_mut()
            [20_000..20_000 + 4096]
            .fill(0xAA);
        reader.devices_mut().get_mut(&2).unwrap().get_mut()
            [50_000..50_000 + 4096]
            .fill(0xBB);

        // resolve picks stripe[0] = (devid=2, phys=50_000), so the
        // read returns 0xBB bytes.
        let buf = reader.read_block(1_000_000).expect("read_block");
        assert_eq!(buf.len(), 4096);
        assert!(buf.iter().all(|&b| b == 0xBB), "expected all 0xBB");
    }

    #[test]
    fn read_data_routes_to_correct_devid() {
        let mapping = make_mapping(1_000_000, 4096, &[(5, 8000)]);
        let mut reader = make_reader(&[(5, 100_000)], &[mapping]);
        reader.devices_mut().get_mut(&5).unwrap().get_mut()[8000..8000 + 100]
            .fill(0xCC);

        let buf = reader.read_data(1_000_000, 100).expect("read_data");
        assert_eq!(buf, vec![0xCC; 100]);
    }

    #[test]
    fn write_block_fans_out_to_all_stripes() {
        // RAID1: 2 devices, write_block must update both at the
        // resolved physical offsets, leaving everything else zero.
        let mapping = make_mapping(2_000_000, 4096, &[(1, 1000), (2, 7000)]);
        let mut reader = make_reader(&[(1, 100_000), (2, 100_000)], &[mapping]);

        let payload = vec![0xDDu8; 4096];
        reader
            .write_block(2_000_000, &payload)
            .expect("write_block");

        let dev1 = reader.devices().get(&1).unwrap().get_ref();
        let dev2 = reader.devices().get(&2).unwrap().get_ref();
        assert_eq!(&dev1[1000..1000 + 4096], &payload[..]);
        assert_eq!(&dev2[7000..7000 + 4096], &payload[..]);
        // Untouched regions still zero on both devices.
        assert!(dev1[..1000].iter().all(|&b| b == 0));
        assert!(dev1[1000 + 4096..].iter().all(|&b| b == 0));
        assert!(dev2[..7000].iter().all(|&b| b == 0));
        assert!(dev2[7000 + 4096..].iter().all(|&b| b == 0));
    }

    #[test]
    fn write_block_fans_out_to_dup_same_devid() {
        // DUP profile: both copies on the same device, at distinct
        // physical offsets. write_block must hit both.
        let mapping = make_mapping(3_000_000, 4096, &[(1, 1000), (1, 50_000)]);
        let mut reader = make_reader(&[(1, 100_000)], &[mapping]);

        let payload = vec![0xEEu8; 4096];
        reader
            .write_block(3_000_000, &payload)
            .expect("write_block");

        let dev = reader.devices().get(&1).unwrap().get_ref();
        assert_eq!(&dev[1000..1000 + 4096], &payload[..]);
        assert_eq!(&dev[50_000..50_000 + 4096], &payload[..]);
    }

    #[test]
    fn write_block_three_devices_raid1c3() {
        let mapping = make_mapping(4_000_000, 4096, &[(1, 0), (2, 0), (3, 0)]);
        let mut reader =
            make_reader(&[(1, 8192), (2, 8192), (3, 8192)], &[mapping]);

        let payload = vec![0xFFu8; 4096];
        reader
            .write_block(4_000_000, &payload)
            .expect("write_block");

        for &devid in &[1u64, 2, 3] {
            let dev = reader.devices().get(&devid).unwrap().get_ref();
            assert_eq!(
                &dev[..4096],
                &payload[..],
                "devid {devid} mirror missing"
            );
        }
    }

    #[test]
    fn read_block_missing_devid_errors() {
        // Chunk cache references devid 9, but the handle map only has
        // devids 1 and 2. Reads must surface a clear error rather than
        // panicking or silently mis-routing.
        let mapping = make_mapping(5_000_000, 4096, &[(9, 0)]);
        let mut reader = make_reader(&[(1, 8192), (2, 8192)], &[mapping]);

        let err = reader.read_block(5_000_000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(
            err.to_string().contains("device 9"),
            "expected error to mention devid 9, got: {err}"
        );
    }

    #[test]
    fn write_block_missing_devid_errors() {
        let mapping = make_mapping(5_000_000, 4096, &[(1, 0), (9, 0)]);
        let mut reader = make_reader(&[(1, 8192)], &[mapping]);

        let err = reader.write_block(5_000_000, &[0u8; 4096]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains("device 9"));
    }

    #[test]
    fn read_block_unmapped_logical_errors() {
        // No chunk for this logical address.
        let mut reader = make_reader(&[(1, 8192)], &[]);
        let err = reader.read_block(1_000_000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains("not mapped"));
    }

    #[test]
    fn new_single_inserts_under_supplied_devid() {
        // BlockReader::new wraps the handle under the explicit devid
        // so multi-device callers can mix it with other handles later.
        let cursor = make_device(8192);
        let cache = ChunkTreeCache::default();
        let reader = BlockReader::new(cursor, 7, 4096, cache);
        assert_eq!(reader.devices().len(), 1);
        assert!(reader.devices().contains_key(&7));
    }

    #[test]
    fn new_multi_with_disjoint_devids() {
        // Sparse devid map (1 and 5 only — devid 3 was removed).
        // Both reads and writes should route correctly.
        let mapping = make_mapping(0, 4096, &[(1, 100), (5, 200)]);
        let mut reader = make_reader(&[(1, 8192), (5, 8192)], &[mapping]);

        let payload = vec![0x77u8; 4096];
        reader.write_block(0, &payload).expect("write_block");
        let dev1 = reader.devices().get(&1).unwrap().get_ref();
        let dev5 = reader.devices().get(&5).unwrap().get_ref();
        assert_eq!(&dev1[100..100 + 4096], &payload[..]);
        assert_eq!(&dev5[200..200 + 4096], &payload[..]);
    }

    #[test]
    #[should_panic(expected = "filesystem has 2 devices")]
    fn single_device_mut_panics_on_multi_device() {
        let mapping = make_mapping(0, 4096, &[(1, 0), (2, 0)]);
        let mut reader = make_reader(&[(1, 4096), (2, 4096)], &[mapping]);
        let _ = reader.single_device_mut();
    }

    #[test]
    fn single_device_mut_returns_handle_for_single_device() {
        let mapping = make_mapping(0, 4096, &[(1, 0)]);
        let mut reader = make_reader(&[(1, 8192)], &[mapping]);
        // Verify it doesn't panic and we can use the returned handle.
        // Write a marker byte and confirm via the map view.
        reader.single_device_mut().get_mut()[42] = 0x99;
        assert_eq!(reader.devices().get(&1).unwrap().get_ref()[42], 0x99);
    }
}

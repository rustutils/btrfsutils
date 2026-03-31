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
pub struct BlockReader<R> {
    reader: R,
    nodesize: u32,
    chunk_cache: ChunkTreeCache,
}

impl<R: Read + Seek> BlockReader<R> {
    /// Read raw bytes at a logical address, resolving to physical via the chunk cache.
    pub fn read_block(&mut self, logical: u64) -> io::Result<Vec<u8>> {
        let physical = self.chunk_cache.resolve(logical).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("logical address {logical} not mapped in chunk cache"),
            )
        })?;
        let mut buf = vec![0u8; self.nodesize as usize];
        self.reader.seek(SeekFrom::Start(physical))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read and parse a tree block at a logical address.
    pub fn read_tree_block(&mut self, logical: u64) -> io::Result<TreeBlock> {
        let buf = self.read_block(logical)?;
        Ok(TreeBlock::parse(&buf))
    }

    /// Return a reference to the chunk cache.
    pub fn chunk_cache(&self) -> &ChunkTreeCache {
        &self.chunk_cache
    }

    /// Return a mutable reference to the chunk cache.
    pub fn chunk_cache_mut(&mut self) -> &mut ChunkTreeCache {
        &mut self.chunk_cache
    }

    /// Return the nodesize.
    pub fn nodesize(&self) -> u32 {
        self.nodesize
    }

    /// Read arbitrary data at a logical address (not limited to nodesize).
    ///
    /// Unlike `read_block` which always reads `nodesize` bytes, this reads
    /// exactly `len` bytes. Used for reading file data extents.
    pub fn read_data(
        &mut self,
        logical: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let physical = self.chunk_cache.resolve(logical).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("logical address {logical} not mapped in chunk cache"),
            )
        })?;
        let mut buf = vec![0u8; len];
        self.reader.seek(SeekFrom::Start(physical))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Return a mutable reference to the underlying I/O handle.
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consume the reader and return the underlying I/O handle.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: Read + Write + Seek> BlockReader<R> {
    /// Write raw bytes to a logical address, resolving to physical via the chunk cache.
    pub fn write_block(&mut self, logical: u64, buf: &[u8]) -> io::Result<()> {
        let physical = self.chunk_cache.resolve(logical).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("logical address {logical} not mapped in chunk cache"),
            )
        })?;
        self.reader.seek(SeekFrom::Start(physical))?;
        self.reader.write_all(buf)?;
        Ok(())
    }
}

/// Result of opening a btrfs filesystem from a block device or image.
pub struct OpenFilesystem<R> {
    /// Block reader with fully populated chunk cache.
    pub reader: BlockReader<R>,
    /// Parsed superblock.
    pub superblock: Superblock,
    /// Map of tree ID -> (root block logical address, key offset), from the root tree.
    pub tree_roots: BTreeMap<u64, (u64, u64)>,
}

/// Open a btrfs filesystem by bootstrapping from the superblock.
///
/// This performs the full bootstrap sequence:
/// 1. Read the superblock (mirror 0)
/// 2. Seed the chunk cache from the `sys_chunk_array`
/// 3. Read the full chunk tree to complete the cache
/// 4. Read the root tree to collect all tree root pointers
pub fn filesystem_open<R: Read + Seek>(
    reader: R,
) -> io::Result<OpenFilesystem<R>> {
    filesystem_open_mirror(reader, 0)
}

/// Open a btrfs filesystem using a specific superblock mirror (0, 1, or 2).
pub fn filesystem_open_mirror<R: Read + Seek>(
    reader: R,
    mirror: u32,
) -> io::Result<OpenFilesystem<R>> {
    let mut reader = reader;

    // Step 1: read the superblock
    let sb = superblock::read_superblock(&mut reader, mirror)?;

    // Step 2: seed chunk cache from sys_chunk_array
    let chunk_cache = chunk::seed_from_sys_chunk_array(
        &sb.sys_chunk_array,
        sb.sys_chunk_array_size,
    );

    let mut block_reader = BlockReader {
        reader,
        nodesize: sb.nodesize,
        chunk_cache,
    };

    // Step 3: read the full chunk tree to complete the cache
    read_chunk_tree(&mut block_reader, sb.chunk_root)?;

    // Step 4: read the root tree to collect tree roots
    let tree_roots = read_root_tree(&mut block_reader, sb.root)?;

    Ok(OpenFilesystem {
        reader: block_reader,
        superblock: sb,
        tree_roots,
    })
}

/// Recursively read the chunk tree to populate the chunk cache.
fn read_chunk_tree<R: Read + Seek>(
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
/// Returns a map of `tree_id` (objectid) -> root block logical address.
fn read_root_tree<R: Read + Seek>(
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

/// Read a single block and call `visitor` (and optionally walk children with `follow`).
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
pub fn tree_stats_collect<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    find_inline: bool,
) -> io::Result<TreeStats> {
    let root_block = reader.read_tree_block(root_logical)?;
    let nodesize = reader.nodesize() as u64;
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
                            item.size as u64 - inline_hdr_size as u64;
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
                if last_block + nodesize != cur {
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
                } else {
                    cluster_size += nodesize;
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

//! # Reader: block device reader with logical-to-physical address resolution
//!
//! Provides `BlockReader` which reads btrfs tree blocks by logical address,
//! resolving them through the chunk tree cache. Also provides `open_filesystem`
//! which bootstraps a complete `BlockReader` from a raw block device or image.

use crate::{
    chunk::{self, ChunkTreeCache},
    raw,
    superblock::{self, Superblock},
    tree::{KeyType, TreeBlock},
    util::read_le_u64,
};
use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, SeekFrom},
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
}

/// Result of opening a btrfs filesystem from a block device or image.
pub struct OpenFs<R> {
    /// Block reader with fully populated chunk cache.
    pub reader: BlockReader<R>,
    /// Parsed superblock.
    pub superblock: Superblock,
    /// Map of tree ID -> root block logical address, from the root tree.
    pub tree_roots: BTreeMap<u64, u64>,
}

/// Open a btrfs filesystem by bootstrapping from the superblock.
///
/// This performs the full bootstrap sequence:
/// 1. Read the superblock (mirror 0)
/// 2. Seed the chunk cache from the sys_chunk_array
/// 3. Read the full chunk tree to complete the cache
/// 4. Read the root tree to collect all tree root pointers
pub fn open_filesystem<R: Read + Seek>(reader: R) -> io::Result<OpenFs<R>> {
    let mut reader = reader;

    // Step 1: read the superblock
    let sb = superblock::read_superblock(&mut reader, 0)?;

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

    Ok(OpenFs {
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
/// Returns a map of tree_id (objectid) -> root block logical address.
fn read_root_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
) -> io::Result<BTreeMap<u64, u64>> {
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
pub fn walk_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_logical: u64,
    traversal: Traversal,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    match traversal {
        Traversal::Bfs => walk_tree_bfs(reader, root_logical, visitor),
        Traversal::Dfs => walk_tree_dfs(reader, root_logical, visitor),
    }
}

fn walk_tree_dfs<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    let block = reader.read_tree_block(logical)?;
    visitor(&block);

    if let TreeBlock::Node { ptrs, .. } = &block {
        for ptr in ptrs {
            walk_tree_dfs(reader, ptr.blockptr, visitor)?;
        }
    }

    Ok(())
}

fn walk_tree_bfs<R: Read + Seek>(
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
pub fn visit_block<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    follow: bool,
    traversal: Traversal,
    visitor: &mut dyn FnMut(&TreeBlock),
) -> io::Result<()> {
    if follow {
        walk_tree(reader, logical, traversal, visitor)
    } else {
        let block = reader.read_tree_block(logical)?;
        visitor(&block);
        Ok(())
    }
}

fn collect_root_items<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    logical: u64,
    tree_roots: &mut BTreeMap<u64, u64>,
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
                let bytenr =
                    read_le_u64(data, item_start + root_item_bytenr_offset);
                if bytenr != 0 {
                    tree_roots.insert(item.key.objectid, bytenr);
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

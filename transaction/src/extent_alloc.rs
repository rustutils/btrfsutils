//! # Extent allocation and freeing
//!
//! Provides proper extent allocation by scanning the extent tree for free
//! space within block groups. Replaces the temporary bump allocator from the
//! transaction module with actual free space tracking.
//!
//! The allocator works by:
//! 1. Loading block group metadata (logical start, length, type, used bytes)
//! 2. Scanning the extent tree within each block group to find gaps
//! 3. Allocating from the first suitable gap
//!
//! Reference counting (extent items with backreferences) is handled by the
//! delayed reference queue, which batches updates and flushes them at commit
//! time.

use crate::fs_info::FsInfo;
use btrfs_disk::{
    items::{BlockGroupFlags, BlockGroupItem},
    tree::{KeyType, TreeBlock},
};
use std::io::{self, Read, Seek, Write};

/// A discovered block group with its logical address range and usage.
#[derive(Debug, Clone)]
pub struct BlockGroup {
    /// Logical start address of this block group.
    pub start: u64,
    /// Length in bytes.
    pub length: u64,
    /// Bytes currently allocated within this group.
    pub used: u64,
    /// Type and RAID profile flags.
    pub flags: BlockGroupFlags,
}

impl BlockGroup {
    /// Free bytes in this block group.
    #[must_use]
    pub fn free(&self) -> u64 {
        self.length.saturating_sub(self.used)
    }

    /// Whether this is a metadata block group.
    #[must_use]
    pub fn is_metadata(&self) -> bool {
        self.flags.contains(BlockGroupFlags::METADATA)
    }

    /// Whether this is a data block group.
    #[must_use]
    pub fn is_data(&self) -> bool {
        self.flags.contains(BlockGroupFlags::DATA)
    }

    /// Whether this is a system block group.
    #[must_use]
    pub fn is_system(&self) -> bool {
        self.flags.contains(BlockGroupFlags::SYSTEM)
    }
}

/// Load all block groups from the filesystem.
///
/// Scans the block group tree (tree 11) if present, otherwise falls back to
/// the extent tree (tree 2) for block group items.
///
/// # Errors
///
/// Returns an error if tree reading fails.
pub fn load_block_groups<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
) -> io::Result<Vec<BlockGroup>> {
    let bg_tree_id = if fs_info.root_bytenr(11).is_some() {
        11u64
    } else {
        2u64
    };

    let root_bytenr = fs_info.root_bytenr(bg_tree_id).ok_or_else(|| {
        io::Error::other(
            "cannot find extent/block-group tree for block group scan",
        )
    })?;

    let mut groups = Vec::new();
    collect_block_groups(fs_info, root_bytenr, &mut groups)?;
    Ok(groups)
}

/// Recursively collect block group items from a tree.
fn collect_block_groups<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
    logical: u64,
    groups: &mut Vec<BlockGroup>,
) -> io::Result<()> {
    let block = fs_info.read_block(logical)?;
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
                    groups.push(BlockGroup {
                        start: item.key.objectid,
                        length: item.key.offset,
                        used: bg.used,
                        flags: bg.flags,
                    });
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                collect_block_groups(fs_info, ptr.blockptr, groups)?;
            }
        }
    }

    Ok(())
}

/// Find free space within a block group by scanning the extent tree.
///
/// Walks the extent tree for extents within `[bg_start, bg_start + bg_length)`
/// and returns gaps (unallocated regions) as `(start, length)` pairs.
///
/// # Errors
///
/// Returns an error if tree reading fails.
pub fn find_free_extents<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
    bg_start: u64,
    bg_length: u64,
    min_size: u64,
) -> io::Result<Vec<(u64, u64)>> {
    let extent_tree_id = 2u64;
    let root_bytenr = fs_info
        .root_bytenr(extent_tree_id)
        .ok_or_else(|| io::Error::other("extent tree not found"))?;

    // Collect all allocated extents within this block group's range
    let mut allocated = Vec::new();
    collect_extents_in_range(
        fs_info,
        root_bytenr,
        bg_start,
        bg_start + bg_length,
        &mut allocated,
    )?;

    // Sort by start address
    allocated.sort_by_key(|&(start, _len)| start);

    // Find gaps between allocated extents
    let mut free = Vec::new();
    let mut cursor = bg_start;

    for &(extent_start, extent_len) in &allocated {
        if extent_start > cursor {
            let gap = extent_start - cursor;
            if gap >= min_size {
                free.push((cursor, gap));
            }
        }
        let extent_end = extent_start + extent_len;
        if extent_end > cursor {
            cursor = extent_end;
        }
    }

    // Check for free space after the last extent
    let bg_end = bg_start + bg_length;
    if cursor < bg_end {
        let gap = bg_end - cursor;
        if gap >= min_size {
            free.push((cursor, gap));
        }
    }

    Ok(free)
}

/// Collect allocated extents within a logical address range from the extent tree.
fn collect_extents_in_range<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
    logical: u64,
    range_start: u64,
    range_end: u64,
    allocated: &mut Vec<(u64, u64)>,
) -> io::Result<()> {
    let block = fs_info.read_block(logical)?;
    let tb = block.as_tree_block();

    match &tb {
        TreeBlock::Leaf { items, .. } => {
            for item in items {
                match item.key.key_type {
                    KeyType::ExtentItem => {
                        // key = (bytenr, EXTENT_ITEM, size)
                        let start = item.key.objectid;
                        let size = item.key.offset;
                        if start < range_end && start + size > range_start {
                            allocated.push((start, size));
                        }
                    }
                    KeyType::MetadataItem => {
                        // key = (bytenr, METADATA_ITEM, level)
                        // size is nodesize
                        let start = item.key.objectid;
                        let size = u64::from(fs_info.nodesize);
                        if start < range_end && start + size > range_start {
                            allocated.push((start, size));
                        }
                    }
                    _ => {}
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                // Only descend into children that could contain keys in range.
                // The key ptr's key is the lowest key in the child subtree.
                // We can skip children whose entire range is before range_start
                // or whose lowest key is past range_end.
                // Conservative: always descend (extent tree items span the
                // full logical space, not just their key's objectid).
                collect_extents_in_range(
                    fs_info,
                    ptr.blockptr,
                    range_start,
                    range_end,
                    allocated,
                )?;
            }
        }
    }

    Ok(())
}

/// Allocate a metadata block from the best available metadata block group.
///
/// Scans block groups for metadata space, finds free extents, and returns the
/// logical address of a free `nodesize`-aligned region.
///
/// # Errors
///
/// Returns an error if no free metadata space is available.
pub fn alloc_metadata_block<R: Read + Write + Seek>(
    fs_info: &mut FsInfo<R>,
) -> io::Result<u64> {
    let nodesize = u64::from(fs_info.nodesize);
    let groups = load_block_groups(fs_info)?;

    // Try metadata block groups first, sorted by most free space
    let mut meta_groups: Vec<&BlockGroup> = groups
        .iter()
        .filter(|bg| bg.is_metadata() && bg.free() >= nodesize)
        .collect();
    meta_groups.sort_by_key(|bg| std::cmp::Reverse(bg.free()));

    for bg in meta_groups {
        let free_extents =
            find_free_extents(fs_info, bg.start, bg.length, nodesize)?;
        for &(start, _len) in &free_extents {
            // Align to nodesize
            let aligned = align_up(start, nodesize);
            if aligned + nodesize <= bg.start + bg.length {
                return Ok(aligned);
            }
        }
    }

    Err(io::Error::other(
        "no free metadata space available in any block group",
    ))
}

/// Align a value up to the given alignment.
const fn align_up(value: u64, align: u64) -> u64 {
    (value + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_group_properties() {
        let bg = BlockGroup {
            start: 0,
            length: 1024 * 1024 * 256,
            used: 1024 * 1024 * 100,
            flags: BlockGroupFlags::METADATA | BlockGroupFlags::DUP,
        };
        assert!(bg.is_metadata());
        assert!(!bg.is_data());
        assert!(!bg.is_system());
        assert_eq!(bg.free(), 1024 * 1024 * 156);
    }

    #[test]
    fn block_group_full() {
        let bg = BlockGroup {
            start: 0,
            length: 1000,
            used: 1000,
            flags: BlockGroupFlags::DATA,
        };
        assert_eq!(bg.free(), 0);
        assert!(bg.is_data());
    }

    #[test]
    fn align_up_cases() {
        assert_eq!(align_up(0, 16384), 0);
        assert_eq!(align_up(1, 16384), 16384);
        assert_eq!(align_up(16384, 16384), 16384);
        assert_eq!(align_up(16385, 16384), 32768);
    }
}

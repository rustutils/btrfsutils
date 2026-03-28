//! # Layout: block address assignment for mkfs tree blocks
//!
//! Assigns logical byte addresses to the initial set of tree blocks within
//! the system chunk. All blocks are laid out sequentially starting at the
//! system group offset (1 MiB).

use btrfs_disk::raw;

/// Byte offset where the system block group starts (1 MiB).
/// From kernel-shared/ctree.h: BTRFS_BLOCK_RESERVED_1M_FOR_SUPER
pub const SYSTEM_GROUP_OFFSET: u64 = 1024 * 1024;

/// Size of the system block group (4 MiB).
/// From mkfs/common.h: BTRFS_MKFS_SYSTEM_GROUP_SIZE
pub const SYSTEM_GROUP_SIZE: u64 = 4 * 1024 * 1024;

/// Identifies a tree block allocated during mkfs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeId {
    Root,
    Extent,
    Chunk,
    Dev,
    Fs,
    Csum,
    FreeSpace,
}

impl TreeId {
    /// The btrfs objectid for this tree.
    pub fn objectid(self) -> u64 {
        match self {
            TreeId::Root => raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            TreeId::Extent => raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
            TreeId::Chunk => raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
            TreeId::Dev => raw::BTRFS_DEV_TREE_OBJECTID as u64,
            TreeId::Fs => raw::BTRFS_FS_TREE_OBJECTID as u64,
            TreeId::Csum => raw::BTRFS_CSUM_TREE_OBJECTID as u64,
            TreeId::FreeSpace => raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64,
        }
    }

    /// All tree blocks in the order they are laid out on disk.
    pub const ALL: [TreeId; 7] = [
        TreeId::Root,
        TreeId::Extent,
        TreeId::Chunk,
        TreeId::Dev,
        TreeId::Fs,
        TreeId::Csum,
        TreeId::FreeSpace,
    ];

    /// Trees that get a ROOT_ITEM in the root tree.
    /// Excludes Root (can't reference itself) and Chunk (handled specially
    /// by the superblock's chunk_root pointer).
    pub const ROOT_ITEM_TREES: [TreeId; 5] = [
        TreeId::Extent,
        TreeId::Dev,
        TreeId::Fs,
        TreeId::Csum,
        TreeId::FreeSpace,
    ];
}

/// Computed block layout for all mkfs tree blocks.
pub struct BlockLayout {
    nodesize: u32,
}

impl BlockLayout {
    pub fn new(nodesize: u32) -> Self {
        Self { nodesize }
    }

    /// Logical byte address of the given tree block.
    pub fn block_addr(&self, tree: TreeId) -> u64 {
        let index = TreeId::ALL.iter().position(|&t| t == tree).unwrap();
        SYSTEM_GROUP_OFFSET + (index as u64) * u64::from(self.nodesize)
    }

    /// Total bytes used by all tree blocks.
    pub fn total_used(&self) -> u64 {
        TreeId::ALL.len() as u64 * u64::from(self.nodesize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_addresses_sequential() {
        let layout = BlockLayout::new(16384);
        assert_eq!(layout.block_addr(TreeId::Root), 0x100000);
        assert_eq!(layout.block_addr(TreeId::Extent), 0x100000 + 16384);
        assert_eq!(layout.block_addr(TreeId::Chunk), 0x100000 + 2 * 16384);
        assert_eq!(layout.block_addr(TreeId::Dev), 0x100000 + 3 * 16384);
        assert_eq!(layout.block_addr(TreeId::Fs), 0x100000 + 4 * 16384);
        assert_eq!(layout.block_addr(TreeId::Csum), 0x100000 + 5 * 16384);
        assert_eq!(layout.block_addr(TreeId::FreeSpace), 0x100000 + 6 * 16384);
    }

    #[test]
    fn total_used() {
        let layout = BlockLayout::new(16384);
        assert_eq!(layout.total_used(), 7 * 16384);
    }
}

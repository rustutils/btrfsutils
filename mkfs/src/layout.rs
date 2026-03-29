//! # Layout: block address assignment for mkfs tree blocks
//!
//! The chunk tree block lives in the system chunk (at SYSTEM_GROUP_OFFSET).
//! All other tree blocks (root, extent, dev, fs, csum, free-space, data-reloc)
//! live in the metadata chunk and are written with DUP (two physical copies).

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
    DataReloc,
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
            TreeId::DataReloc => raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
        }
    }

    /// All tree blocks in the order they are laid out on disk.
    pub const ALL: [TreeId; 8] = [
        TreeId::Root,
        TreeId::Extent,
        TreeId::Chunk,
        TreeId::Dev,
        TreeId::Fs,
        TreeId::Csum,
        TreeId::FreeSpace,
        TreeId::DataReloc,
    ];

    /// Trees that get a ROOT_ITEM in the root tree.
    /// Excludes Root (can't reference itself) and Chunk (handled specially
    /// by the superblock's chunk_root pointer).
    pub const ROOT_ITEM_TREES: [TreeId; 6] = [
        TreeId::Extent,
        TreeId::Dev,
        TreeId::Fs,
        TreeId::Csum,
        TreeId::FreeSpace,
        TreeId::DataReloc,
    ];
}

/// The 7 trees that live in the metadata chunk (everything except Chunk).
pub const NON_CHUNK_TREES: [TreeId; 7] = [
    TreeId::Root,
    TreeId::Extent,
    TreeId::Dev,
    TreeId::Fs,
    TreeId::Csum,
    TreeId::FreeSpace,
    TreeId::DataReloc,
];

/// Computed block layout for all mkfs tree blocks.
///
/// The chunk tree block is placed at SYSTEM_GROUP_OFFSET (in the system
/// chunk). The remaining 7 trees are placed sequentially starting at the
/// metadata chunk's logical address.
pub struct BlockLayout {
    nodesize: u32,
    meta_logical: u64,
}

impl BlockLayout {
    pub fn new(nodesize: u32, meta_logical: u64) -> Self {
        Self {
            nodesize,
            meta_logical,
        }
    }

    /// Logical byte address of the given tree block.
    pub fn block_addr(&self, tree: TreeId) -> u64 {
        if tree == TreeId::Chunk {
            SYSTEM_GROUP_OFFSET
        } else {
            let index =
                NON_CHUNK_TREES.iter().position(|&t| t == tree).unwrap();
            self.meta_logical + (index as u64) * u64::from(self.nodesize)
        }
    }

    /// Bytes used in the system chunk (just the chunk tree block).
    pub fn system_used(&self) -> u64 {
        u64::from(self.nodesize)
    }

    /// Bytes used in the metadata chunk (7 tree blocks).
    pub fn metadata_used(&self) -> u64 {
        NON_CHUNK_TREES.len() as u64 * u64::from(self.nodesize)
    }
}

/// 64 KiB -- default stripe length for btrfs chunks.
/// From kernel-shared/volumes.h: BTRFS_STRIPE_LEN
pub const STRIPE_LEN: u64 = 64 * 1024;

/// Physical and logical offset where non-system chunks start (after system group).
pub const CHUNK_START: u64 = SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;

/// Computed layout for metadata (DUP) and data (SINGLE) block groups.
pub struct ChunkLayout {
    /// Logical address of the metadata chunk.
    pub meta_logical: u64,
    /// Logical size of the metadata chunk (one stripe).
    pub meta_size: u64,
    /// Physical offset of DUP stripe 0.
    pub meta_phys_0: u64,
    /// Physical offset of DUP stripe 1.
    pub meta_phys_1: u64,
    /// Logical address of the data chunk.
    pub data_logical: u64,
    /// Logical and physical size of the data chunk (SINGLE).
    pub data_size: u64,
    /// Physical offset of the data chunk.
    pub data_phys: u64,
}

impl ChunkLayout {
    /// Compute metadata and data chunk placement for the given device size.
    ///
    /// Returns `None` if the device is too small to fit even the minimum
    /// metadata DUP (2 x 32 MiB) plus minimum data SINGLE (64 MiB).
    pub fn new(total_bytes: u64) -> Option<Self> {
        // Meta stripe size: min(256 MiB, total/10), minimum 32 MiB, round down to STRIPE_LEN.
        let meta_size =
            (total_bytes / 10).clamp(32 * 1024 * 1024, 256 * 1024 * 1024);
        let meta_size = meta_size / STRIPE_LEN * STRIPE_LEN;

        // Data size: min(1 GiB, total/10), minimum 64 MiB, round down to STRIPE_LEN.
        let data_size =
            (total_bytes / 10).clamp(64 * 1024 * 1024, 1024 * 1024 * 1024);
        let data_size = data_size / STRIPE_LEN * STRIPE_LEN;

        // DUP: two physical stripes, sequential after system group.
        let meta_phys_0 = CHUNK_START;
        let meta_phys_1 = CHUNK_START + meta_size;

        // Data starts after both DUP stripes.
        let data_phys = CHUNK_START + 2 * meta_size;

        // Validate everything fits.
        if data_phys + data_size > total_bytes {
            return None;
        }

        // Logical addresses: metadata follows system group logically,
        // data follows metadata. These must not overlap with the system
        // group logical range [SYSTEM_GROUP_OFFSET, +SYSTEM_GROUP_SIZE).
        let meta_logical = CHUNK_START;
        let data_logical = CHUNK_START + meta_size;

        Some(ChunkLayout {
            meta_logical,
            meta_size,
            meta_phys_0,
            meta_phys_1,
            data_logical,
            data_size,
            data_phys,
        })
    }

    /// Total physical bytes used by all chunks (system + metadata DUP + data).
    pub fn dev_bytes_used(&self) -> u64 {
        SYSTEM_GROUP_SIZE + 2 * self.meta_size + self.data_size
    }

    /// Map a logical address to its physical write locations.
    ///
    /// System chunk: logical == physical (SINGLE).
    /// Metadata chunk: two physical copies (DUP).
    /// Data chunk: logical maps to one physical location (SINGLE).
    pub fn logical_to_physical(&self, logical: u64) -> Vec<u64> {
        let sys_range =
            SYSTEM_GROUP_OFFSET..SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;
        let meta_range = self.meta_logical..self.meta_logical + self.meta_size;
        let data_range = self.data_logical..self.data_logical + self.data_size;

        if sys_range.contains(&logical) {
            // System chunk: logical == physical
            vec![logical]
        } else if meta_range.contains(&logical) {
            let off = logical - self.meta_logical;
            vec![self.meta_phys_0 + off, self.meta_phys_1 + off]
        } else if data_range.contains(&logical) {
            let off = logical - self.data_logical;
            vec![self.data_phys + off]
        } else {
            panic!("logical address {logical:#x} not in any chunk")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_addresses() {
        // With a 256 MiB device, meta_logical = CHUNK_START = 5 MiB
        let meta_logical = CHUNK_START;
        let layout = BlockLayout::new(16384, meta_logical);

        // Chunk tree is in the system chunk at SYSTEM_GROUP_OFFSET
        assert_eq!(layout.block_addr(TreeId::Chunk), SYSTEM_GROUP_OFFSET);

        // Other 7 trees are sequential in the metadata chunk
        assert_eq!(layout.block_addr(TreeId::Root), meta_logical);
        assert_eq!(layout.block_addr(TreeId::Extent), meta_logical + 16384);
        assert_eq!(layout.block_addr(TreeId::Dev), meta_logical + 2 * 16384);
        assert_eq!(layout.block_addr(TreeId::Fs), meta_logical + 3 * 16384);
        assert_eq!(layout.block_addr(TreeId::Csum), meta_logical + 4 * 16384);
        assert_eq!(
            layout.block_addr(TreeId::FreeSpace),
            meta_logical + 5 * 16384
        );
        assert_eq!(
            layout.block_addr(TreeId::DataReloc),
            meta_logical + 6 * 16384
        );
    }

    #[test]
    fn system_and_metadata_used() {
        let layout = BlockLayout::new(16384, CHUNK_START);
        assert_eq!(layout.system_used(), 16384);
        assert_eq!(layout.metadata_used(), 7 * 16384);
    }

    #[test]
    fn chunk_layout_256m() {
        // 256 MiB device: meta = min(256M, 25.6M) -> 32M (minimum), data = min(1G, 25.6M) -> 64M
        let cl = ChunkLayout::new(256 * 1024 * 1024).unwrap();
        assert_eq!(cl.meta_size, 32 * 1024 * 1024);
        assert_eq!(cl.data_size, 64 * 1024 * 1024);
        assert_eq!(cl.meta_phys_0, CHUNK_START);
        assert_eq!(cl.meta_phys_1, CHUNK_START + 32 * 1024 * 1024);
        assert_eq!(cl.data_phys, CHUNK_START + 64 * 1024 * 1024);
        assert_eq!(cl.meta_logical, CHUNK_START);
        assert_eq!(cl.data_logical, CHUNK_START + 32 * 1024 * 1024);
    }

    #[test]
    fn chunk_layout_1g() {
        // 1 GiB: meta = min(256M, 102.4M) -> 102M (rounded), data = min(1G, 102.4M) -> 102M
        let cl = ChunkLayout::new(1024 * 1024 * 1024).unwrap();
        let expected_stripe =
            (1024 * 1024 * 1024 / 10) / STRIPE_LEN * STRIPE_LEN;
        assert_eq!(cl.meta_size, expected_stripe);
        assert_eq!(cl.data_size, expected_stripe);
    }

    #[test]
    fn chunk_layout_10g() {
        // 10 GiB: meta = min(256M, 1G) -> 256M, data = min(1G, 1G) -> 1G
        let cl = ChunkLayout::new(10 * 1024 * 1024 * 1024).unwrap();
        assert_eq!(cl.meta_size, 256 * 1024 * 1024);
        assert_eq!(cl.data_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn chunk_layout_too_small() {
        // 100 MiB: needs 5M + 2*32M + 64M = 133M, doesn't fit
        assert!(ChunkLayout::new(100 * 1024 * 1024).is_none());
    }

    #[test]
    fn chunk_layout_dev_bytes_used() {
        let cl = ChunkLayout::new(256 * 1024 * 1024).unwrap();
        // system(4M) + 2*meta(32M) + data(64M) = 132M
        assert_eq!(
            cl.dev_bytes_used(),
            SYSTEM_GROUP_SIZE + 2 * 32 * 1024 * 1024 + 64 * 1024 * 1024
        );
    }
}

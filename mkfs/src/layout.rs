//! # Layout: block address assignment for mkfs tree blocks
//!
//! The chunk tree block lives in the system chunk (at SYSTEM_GROUP_OFFSET).
//! All other tree blocks (root, extent, dev, fs, csum, free-space, data-reloc)
//! live in the metadata chunk and are written with DUP (two physical copies).

use crate::args::Profile;
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
    BlockGroup,
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
            TreeId::BlockGroup => raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID as u64,
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
        } else if tree == TreeId::BlockGroup {
            // Block-group tree is the 8th tree in the metadata chunk,
            // after the 7 base trees.
            self.meta_logical
                + (NON_CHUNK_TREES.len() as u64) * u64::from(self.nodesize)
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

    /// Bytes used in the metadata chunk by the base trees (7 tree blocks).
    /// When block-group-tree is enabled, add nodesize for the extra tree.
    pub fn metadata_used(&self, has_block_group_tree: bool) -> u64 {
        let count = if has_block_group_tree {
            NON_CHUNK_TREES.len() as u64 + 1
        } else {
            NON_CHUNK_TREES.len() as u64
        };
        count * u64::from(self.nodesize)
    }
}

/// 64 KiB -- default stripe length for btrfs chunks.
/// From kernel-shared/volumes.h: BTRFS_STRIPE_LEN
pub const STRIPE_LEN: u64 = 64 * 1024;

/// A physical stripe location in a chunk.
pub struct StripeInfo {
    pub devid: u64,
    pub offset: u64,
    pub dev_uuid: uuid::Uuid,
}

/// Physical and logical offset where non-system chunks start (after system group).
pub const CHUNK_START: u64 = SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;

/// Computed layout for metadata and data block groups.
pub struct ChunkLayout {
    /// Logical address of the metadata chunk.
    pub meta_logical: u64,
    /// Logical size of the metadata chunk (one stripe).
    pub meta_size: u64,
    /// Physical stripes for the metadata chunk.
    pub meta_stripes: Vec<StripeInfo>,
    /// Logical address of the data chunk.
    pub data_logical: u64,
    /// Logical and physical size of the data chunk.
    pub data_size: u64,
    /// Physical stripes for the data chunk.
    pub data_stripes: Vec<StripeInfo>,
}

/// Device info needed for chunk layout computation.
/// Avoids a circular dependency on `crate::mkfs::DeviceInfo`.
pub struct ChunkDevice {
    pub devid: u64,
    pub total_bytes: u64,
    pub dev_uuid: uuid::Uuid,
}

impl ChunkLayout {
    /// Compute metadata and data chunk placement for the given devices.
    ///
    /// For DUP metadata (single device): two stripes on device 1.
    /// For RAID1 metadata (multi-device): one stripe on each of the first
    /// two devices.
    /// For SINGLE data: one stripe on device 1.
    ///
    /// Returns `None` if the devices are too small.
    pub fn new(
        devices: &[ChunkDevice],
        metadata_profile: Profile,
        data_profile: Profile,
    ) -> Option<Self> {
        assert!(!devices.is_empty());
        let total_bytes: u64 = devices.iter().map(|d| d.total_bytes).sum();

        // Meta stripe size: clamp(total/10, 32M, 256M), round down to STRIPE_LEN.
        let meta_size =
            (total_bytes / 10).clamp(32 * 1024 * 1024, 256 * 1024 * 1024);
        let meta_size = meta_size / STRIPE_LEN * STRIPE_LEN;

        // Data size: clamp(total/10, 64M, 1G), round down to STRIPE_LEN.
        let data_size =
            (total_bytes / 10).clamp(64 * 1024 * 1024, 1024 * 1024 * 1024);
        let data_size = data_size / STRIPE_LEN * STRIPE_LEN;

        // Build metadata stripes based on profile.
        let meta_stripes = match metadata_profile {
            Profile::Dup => {
                // Two stripes on device 1, sequential after system group.
                vec![
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[0].dev_uuid,
                    },
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: CHUNK_START + meta_size,
                        dev_uuid: devices[0].dev_uuid,
                    },
                ]
            }
            Profile::Raid1 => {
                // One stripe on device 1 at CHUNK_START, one on device 2
                // at CHUNK_START.
                vec![
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[0].dev_uuid,
                    },
                    StripeInfo {
                        devid: devices[1].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[1].dev_uuid,
                    },
                ]
            }
            Profile::Single => {
                vec![StripeInfo {
                    devid: devices[0].devid,
                    offset: CHUNK_START,
                    dev_uuid: devices[0].dev_uuid,
                }]
            }
            _ => {
                // Other profiles not yet supported for metadata.
                return None;
            }
        };

        // Data starts after the last metadata stripe on device 1.
        // Compute the highest physical end on device 1 from meta stripes.
        let dev1_meta_end = meta_stripes
            .iter()
            .filter(|s| s.devid == devices[0].devid)
            .map(|s| s.offset + meta_size)
            .max()
            .unwrap_or(CHUNK_START);

        // Build data stripes based on profile.
        let data_stripes = match data_profile {
            Profile::Single => {
                vec![StripeInfo {
                    devid: devices[0].devid,
                    offset: dev1_meta_end,
                    dev_uuid: devices[0].dev_uuid,
                }]
            }
            Profile::Dup => {
                vec![
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: dev1_meta_end,
                        dev_uuid: devices[0].dev_uuid,
                    },
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: dev1_meta_end + data_size,
                        dev_uuid: devices[0].dev_uuid,
                    },
                ]
            }
            Profile::Raid1 => {
                // Data RAID1: one stripe on each device.
                let dev2_meta_end = meta_stripes
                    .iter()
                    .filter(|s| s.devid == devices[1].devid)
                    .map(|s| s.offset + meta_size)
                    .max()
                    .unwrap_or(CHUNK_START);
                vec![
                    StripeInfo {
                        devid: devices[0].devid,
                        offset: dev1_meta_end,
                        dev_uuid: devices[0].dev_uuid,
                    },
                    StripeInfo {
                        devid: devices[1].devid,
                        offset: dev2_meta_end,
                        dev_uuid: devices[1].dev_uuid,
                    },
                ]
            }
            _ => {
                return None;
            }
        };

        // Validate everything fits on each device.
        for dev in devices {
            let used = Self::compute_dev_physical_end(
                dev.devid,
                &meta_stripes,
                meta_size,
                &data_stripes,
                data_size,
            );
            if used > dev.total_bytes {
                return None;
            }
        }

        // Logical addresses: metadata follows system group logically,
        // data follows metadata.
        let meta_logical = CHUNK_START;
        let data_logical = CHUNK_START + meta_size;

        Some(ChunkLayout {
            meta_logical,
            meta_size,
            meta_stripes,
            data_logical,
            data_size,
            data_stripes,
        })
    }

    /// Compute the highest physical byte used on a device, including the
    /// system group on device 1.
    fn compute_dev_physical_end(
        devid: u64,
        meta_stripes: &[StripeInfo],
        meta_size: u64,
        data_stripes: &[StripeInfo],
        data_size: u64,
    ) -> u64 {
        let mut end = if devid == 1 {
            SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE
        } else {
            0
        };
        for s in meta_stripes {
            if s.devid == devid {
                end = end.max(s.offset + meta_size);
            }
        }
        for s in data_stripes {
            if s.devid == devid {
                end = end.max(s.offset + data_size);
            }
        }
        end
    }

    /// Total physical bytes used on a specific device by all chunks.
    ///
    /// Device 1 always has the system group. Metadata and data stripes
    /// contribute their stripe size for each stripe on this device.
    pub fn dev_bytes_used_for(&self, devid: u64) -> u64 {
        let mut used = if devid == 1 { SYSTEM_GROUP_SIZE } else { 0 };
        for s in &self.meta_stripes {
            if s.devid == devid {
                used += self.meta_size;
            }
        }
        for s in &self.data_stripes {
            if s.devid == devid {
                used += self.data_size;
            }
        }
        used
    }

    /// Total physical bytes used across all devices (sum of all stripes).
    pub fn total_bytes_used(&self) -> u64 {
        SYSTEM_GROUP_SIZE
            + (self.meta_stripes.len() as u64 * self.meta_size)
            + (self.data_stripes.len() as u64 * self.data_size)
    }

    /// Map a logical address to its physical write locations.
    ///
    /// Returns `(devid, physical_offset)` pairs.
    /// System chunk: always device 1, logical == physical.
    /// Metadata chunk: one pair per stripe.
    /// Data chunk: one pair per stripe.
    pub fn logical_to_physical(&self, logical: u64) -> Vec<(u64, u64)> {
        let sys_range =
            SYSTEM_GROUP_OFFSET..SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;
        let meta_range = self.meta_logical..self.meta_logical + self.meta_size;
        let data_range = self.data_logical..self.data_logical + self.data_size;

        if sys_range.contains(&logical) {
            // System chunk: device 1, logical == physical
            vec![(1, logical)]
        } else if meta_range.contains(&logical) {
            let off = logical - self.meta_logical;
            self.meta_stripes
                .iter()
                .map(|s| (s.devid, s.offset + off))
                .collect()
        } else if data_range.contains(&logical) {
            let off = logical - self.data_logical;
            self.data_stripes
                .iter()
                .map(|s| (s.devid, s.offset + off))
                .collect()
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
        assert_eq!(layout.metadata_used(false), 7 * 16384);
        assert_eq!(layout.metadata_used(true), 8 * 16384);
    }

    fn test_uuid() -> uuid::Uuid {
        uuid::Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap()
    }

    fn single_device(size: u64) -> Vec<ChunkDevice> {
        vec![ChunkDevice {
            devid: 1,
            total_bytes: size,
            dev_uuid: test_uuid(),
        }]
    }

    #[test]
    fn chunk_layout_256m() {
        // 256 MiB device: meta = min(256M, 25.6M) -> 32M (minimum), data = min(1G, 25.6M) -> 64M
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        assert_eq!(cl.meta_size, 32 * 1024 * 1024);
        assert_eq!(cl.data_size, 64 * 1024 * 1024);
        assert_eq!(cl.meta_stripes.len(), 2);
        assert_eq!(cl.meta_stripes[0].offset, CHUNK_START);
        assert_eq!(cl.meta_stripes[1].offset, CHUNK_START + 32 * 1024 * 1024);
        assert_eq!(cl.data_stripes.len(), 1);
        assert_eq!(cl.data_stripes[0].offset, CHUNK_START + 64 * 1024 * 1024);
        assert_eq!(cl.meta_logical, CHUNK_START);
        assert_eq!(cl.data_logical, CHUNK_START + 32 * 1024 * 1024);
    }

    #[test]
    fn chunk_layout_1g() {
        // 1 GiB: meta = min(256M, 102.4M) -> 102M (rounded), data = min(1G, 102.4M) -> 102M
        let devs = single_device(1024 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        let expected_stripe =
            (1024 * 1024 * 1024 / 10) / STRIPE_LEN * STRIPE_LEN;
        assert_eq!(cl.meta_size, expected_stripe);
        assert_eq!(cl.data_size, expected_stripe);
    }

    #[test]
    fn chunk_layout_10g() {
        // 10 GiB: meta = min(256M, 1G) -> 256M, data = min(1G, 1G) -> 1G
        let devs = single_device(10 * 1024 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        assert_eq!(cl.meta_size, 256 * 1024 * 1024);
        assert_eq!(cl.data_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn chunk_layout_too_small() {
        // 100 MiB: needs 5M + 2*32M + 64M = 133M, doesn't fit
        let devs = single_device(100 * 1024 * 1024);
        assert!(
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).is_none()
        );
    }

    #[test]
    fn chunk_layout_total_bytes_used() {
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        // system(4M) + 2*meta(32M) + data(64M) = 132M
        assert_eq!(
            cl.total_bytes_used(),
            SYSTEM_GROUP_SIZE + 2 * 32 * 1024 * 1024 + 64 * 1024 * 1024
        );
    }

    #[test]
    fn chunk_layout_dev_bytes_used_single_device() {
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        // All chunks on device 1: system(4M) + 2*meta(32M) + data(64M) = 132M
        assert_eq!(
            cl.dev_bytes_used_for(1),
            SYSTEM_GROUP_SIZE + 2 * 32 * 1024 * 1024 + 64 * 1024 * 1024
        );
    }

    fn two_devices(size: u64) -> Vec<ChunkDevice> {
        let uuid2 =
            uuid::Uuid::parse_str("cafebabe-cafe-babe-cafe-babecafebabe")
                .unwrap();
        vec![
            ChunkDevice {
                devid: 1,
                total_bytes: size,
                dev_uuid: test_uuid(),
            },
            ChunkDevice {
                devid: 2,
                total_bytes: size,
                dev_uuid: uuid2,
            },
        ]
    }

    #[test]
    fn chunk_layout_raid1_stripes() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid1, Profile::Single).unwrap();
        // RAID1 metadata: one stripe on each device at CHUNK_START
        assert_eq!(cl.meta_stripes.len(), 2);
        assert_eq!(cl.meta_stripes[0].devid, 1);
        assert_eq!(cl.meta_stripes[0].offset, CHUNK_START);
        assert_eq!(cl.meta_stripes[1].devid, 2);
        assert_eq!(cl.meta_stripes[1].offset, CHUNK_START);
        // Data SINGLE on device 1 after metadata
        assert_eq!(cl.data_stripes.len(), 1);
        assert_eq!(cl.data_stripes[0].devid, 1);
        assert_eq!(cl.data_stripes[0].offset, CHUNK_START + cl.meta_size);
    }

    #[test]
    fn chunk_layout_raid1_dev_bytes() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid1, Profile::Single).unwrap();
        // total = 512M, so meta_size and data_size are based on 512M/10
        // Device 1: system(4M) + meta + data
        assert_eq!(
            cl.dev_bytes_used_for(1),
            SYSTEM_GROUP_SIZE + cl.meta_size + cl.data_size
        );
        // Device 2: meta only (one RAID1 stripe)
        assert_eq!(cl.dev_bytes_used_for(2), cl.meta_size);
    }

    #[test]
    fn logical_to_physical_returns_devid() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid1, Profile::Single).unwrap();
        // System chunk: device 1 only
        let sys = cl.logical_to_physical(SYSTEM_GROUP_OFFSET);
        assert_eq!(sys, vec![(1, SYSTEM_GROUP_OFFSET)]);
        // Metadata: one on each device
        let meta = cl.logical_to_physical(cl.meta_logical);
        assert_eq!(meta.len(), 2);
        assert_eq!(meta[0].0, 1);
        assert_eq!(meta[1].0, 2);
    }
}

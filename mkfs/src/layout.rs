//! # Layout: block address assignment for mkfs tree blocks
//!
//! The chunk tree block lives in the system chunk (at `SYSTEM_GROUP_OFFSET`).
//! All other tree blocks (root, extent, dev, fs, csum, free-space, data-reloc)
//! live in the metadata chunk and are written with DUP (two physical copies).

use crate::args::Profile;
use btrfs_disk::raw;

/// Byte offset where the system block group starts (1 MiB).
/// From kernel-shared/ctree.h: `BTRFS_BLOCK_RESERVED_1M_FOR_SUPER`
pub const SYSTEM_GROUP_OFFSET: u64 = 1024 * 1024;

/// Size of the system block group (4 MiB).
/// From mkfs/common.h: `BTRFS_MKFS_SYSTEM_GROUP_SIZE`
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
    Quota,
}

impl TreeId {
    /// The btrfs objectid for this tree.
    #[must_use]
    pub fn objectid(self) -> u64 {
        match self {
            TreeId::Root => u64::from(raw::BTRFS_ROOT_TREE_OBJECTID),
            TreeId::Extent => u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
            TreeId::Chunk => u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID),
            TreeId::Dev => u64::from(raw::BTRFS_DEV_TREE_OBJECTID),
            TreeId::Fs => u64::from(raw::BTRFS_FS_TREE_OBJECTID),
            TreeId::Csum => u64::from(raw::BTRFS_CSUM_TREE_OBJECTID),
            TreeId::FreeSpace => u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID),
            #[allow(clippy::cast_sign_loss)]
            // bindgen produces i32, but value is a valid u64
            TreeId::DataReloc => raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64,
            TreeId::BlockGroup => {
                u64::from(raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID)
            }
            TreeId::Quota => u64::from(raw::BTRFS_QUOTA_TREE_OBJECTID),
        }
    }

    /// Always-present tree blocks in the order they are laid out on
    /// disk. Excludes the trees that may be deferred to post-bootstrap
    /// (`Csum`, `DataReloc`) and the user-feature trees (`FreeSpace`,
    /// `BlockGroup`, `Quota`), which are appended after the base trees
    /// when present in mkfs's bootstrap. `Csum` and `DataReloc` are
    /// "optional" only in the sense that mkfs may skip them when
    /// post-bootstrap will run and create them instead — the final
    /// filesystem always has both.
    pub const ALL: [TreeId; 5] = [
        TreeId::Root,
        TreeId::Extent,
        TreeId::Chunk,
        TreeId::Dev,
        TreeId::Fs,
    ];

    /// Always-present trees that get a `ROOT_ITEM` in the root tree.
    /// Excludes Root (can't reference itself), Chunk (handled by the
    /// superblock's `chunk_root` pointer), and the optional trees
    /// (`Csum`, `DataReloc`, `FreeSpace`, `BlockGroup`, `Quota`).
    pub const ROOT_ITEM_TREES: [TreeId; 3] =
        [TreeId::Extent, TreeId::Dev, TreeId::Fs];
}

/// The 4 always-present trees that live in the metadata chunk.
/// Excludes `Chunk` (lives in the system chunk) and the trees that
/// may or may not be present in mkfs's bootstrap (`Csum`, `DataReloc`,
/// `FreeSpace`, `BlockGroup`, `Quota`).
pub const NON_CHUNK_TREES: [TreeId; 4] =
    [TreeId::Root, TreeId::Extent, TreeId::Dev, TreeId::Fs];

/// Computed block layout for all mkfs tree blocks.
///
/// The chunk tree block is placed at `SYSTEM_GROUP_OFFSET` (in the system
/// chunk). The remaining 7 trees are placed sequentially starting at the
/// metadata chunk's logical address.
pub struct BlockLayout {
    nodesize: u32,
    meta_logical: u64,
}

impl BlockLayout {
    /// Create a layout with the given nodesize and metadata chunk logical address.
    #[must_use]
    pub fn new(nodesize: u32, meta_logical: u64) -> Self {
        Self {
            nodesize,
            meta_logical,
        }
    }

    /// Logical byte address of the given tree block.
    ///
    /// Optional trees (`BlockGroup`, `FreeSpace`, `Csum`, `DataReloc`,
    /// `Quota`) are placed after the 4 always-present trees. The slot
    /// ordering convention is `BlockGroup`, `FreeSpace`, `Csum`,
    /// `DataReloc`, `Quota`.
    ///
    /// The `optional_trees_before` parameter specifies how many
    /// optional tree slots precede this one. For base trees and
    /// `Chunk`, it is ignored.
    ///
    /// # Panics
    ///
    /// Panics if `tree` is not in `NON_CHUNK_TREES` and is not `Chunk`,
    /// `BlockGroup`, `FreeSpace`, `Csum`, `DataReloc`, or `Quota`.
    #[must_use]
    pub fn block_addr_with_offset(
        &self,
        tree: TreeId,
        optional_trees_before: u64,
    ) -> u64 {
        if tree == TreeId::Chunk {
            SYSTEM_GROUP_OFFSET
        } else if matches!(
            tree,
            TreeId::BlockGroup
                | TreeId::FreeSpace
                | TreeId::Csum
                | TreeId::DataReloc
                | TreeId::Quota
        ) {
            self.meta_logical
                + (NON_CHUNK_TREES.len() as u64 + optional_trees_before)
                    * u64::from(self.nodesize)
        } else {
            let index =
                NON_CHUNK_TREES.iter().position(|&t| t == tree).unwrap();
            self.meta_logical + (index as u64) * u64::from(self.nodesize)
        }
    }

    /// Convenience wrapper: block address for base trees and `Chunk`.
    /// For optional trees, use `block_addr_with_offset`.
    #[must_use]
    pub fn block_addr(&self, tree: TreeId) -> u64 {
        self.block_addr_with_offset(tree, 0)
    }

    /// Bytes used in the system chunk (just the chunk tree block).
    #[must_use]
    pub fn system_used(&self) -> u64 {
        u64::from(self.nodesize)
    }

    /// Bytes used in the metadata chunk by the base trees (4 tree
    /// blocks) plus any optional trees that are present in mkfs's
    /// bootstrap (block-group-tree, free-space-tree, csum tree,
    /// data-reloc tree, quota tree).
    ///
    /// `has_csum_tree` and `has_data_reloc_tree` reflect whether
    /// mkfs's bootstrap creates these (always true today for
    /// unsupported-by-post-bootstrap profiles like RAID5/RAID6;
    /// false when post-bootstrap will create them instead).
    // Five booleans is more than clippy::fn_params_excessive_bools
    // would like, but each bool has a clear meaning and call sites
    // (in `make_btrfs` and `build_extent_tree`) read naturally.
    #[allow(clippy::fn_params_excessive_bools)]
    #[must_use]
    pub fn metadata_used(
        &self,
        has_block_group_tree: bool,
        has_free_space_tree: bool,
        has_csum_tree: bool,
        has_data_reloc_tree: bool,
        has_quota_tree: bool,
    ) -> u64 {
        let mut count = NON_CHUNK_TREES.len() as u64;
        if has_block_group_tree {
            count += 1;
        }
        if has_free_space_tree {
            count += 1;
        }
        if has_csum_tree {
            count += 1;
        }
        if has_data_reloc_tree {
            count += 1;
        }
        if has_quota_tree {
            count += 1;
        }
        count * u64::from(self.nodesize)
    }
}

/// 64 KiB -- default stripe length for btrfs chunks.
/// From kernel-shared/volumes.h: `BTRFS_STRIPE_LEN`
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
    /// Per-stripe physical size of the metadata chunk.
    pub meta_size: u64,
    /// Physical stripes for the metadata chunk.
    pub meta_stripes: Vec<StripeInfo>,
    /// Logical address of the data chunk.
    pub data_logical: u64,
    /// Per-stripe physical size of the data chunk.
    pub data_size: u64,
    /// Physical stripes for the data chunk.
    pub data_stripes: Vec<StripeInfo>,
    /// Metadata RAID profile.
    metadata_profile: Profile,
    /// Data RAID profile.
    data_profile: Profile,
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
    ///
    /// # Panics
    ///
    /// Panics if `devices` is empty.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::similar_names)]
    pub fn new(
        devices: &[ChunkDevice],
        metadata_profile: Profile,
        data_profile: Profile,
    ) -> Option<Self> {
        assert!(!devices.is_empty());
        let total_bytes: u64 = devices.iter().map(|d| d.total_bytes).sum();

        // Meta stripe size: clamp(total/10, 8M, 256M), round down to STRIPE_LEN.
        // The 8M minimum matches btrfs-progs calc_size for small volumes.
        // The kernel allocates larger chunks at runtime as needed.
        let meta_size =
            (total_bytes / 10).clamp(8 * 1024 * 1024, 256 * 1024 * 1024);
        let meta_size = meta_size / STRIPE_LEN * STRIPE_LEN;

        // Data size: clamp(total/10, 8M, 1G), round down to STRIPE_LEN.
        let data_size =
            (total_bytes / 10).clamp(8 * 1024 * 1024, 1024 * 1024 * 1024);
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
            Profile::Raid1 | Profile::Raid1c3 | Profile::Raid1c4 => {
                // One stripe per device, up to the profile's stripe count.
                let n = metadata_profile.num_stripes(devices.len()) as usize;
                if devices.len() < n {
                    return None;
                }
                (0..n)
                    .map(|i| StripeInfo {
                        devid: devices[i].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[i].dev_uuid,
                    })
                    .collect()
            }
            Profile::Single => {
                vec![StripeInfo {
                    devid: devices[0].devid,
                    offset: CHUNK_START,
                    dev_uuid: devices[0].dev_uuid,
                }]
            }
            Profile::Raid0 | Profile::Raid5 | Profile::Raid6 => {
                // One stripe per device, all starting at CHUNK_START.
                let n = metadata_profile.num_stripes(devices.len()) as usize;
                if devices.len() < metadata_profile.min_devices() {
                    return None;
                }
                (0..n)
                    .map(|i| StripeInfo {
                        devid: devices[i].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[i].dev_uuid,
                    })
                    .collect()
            }
            Profile::Raid10 => {
                // Striped mirrors: num_stripes rounded to even, placed in pairs.
                let n = metadata_profile.num_stripes(devices.len()) as usize;
                if n < 2 || devices.len() < metadata_profile.min_devices() {
                    return None;
                }
                (0..n)
                    .map(|i| StripeInfo {
                        devid: devices[i].devid,
                        offset: CHUNK_START,
                        dev_uuid: devices[i].dev_uuid,
                    })
                    .collect()
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
            Profile::Raid1
            | Profile::Raid1c3
            | Profile::Raid1c4
            | Profile::Raid0
            | Profile::Raid5
            | Profile::Raid6
            | Profile::Raid10 => {
                let n = data_profile.num_stripes(devices.len()) as usize;
                if n < 1 || devices.len() < data_profile.min_devices() {
                    return None;
                }
                (0..n)
                    .map(|i| {
                        let dev_meta_end = meta_stripes
                            .iter()
                            .filter(|s| s.devid == devices[i].devid)
                            .map(|s| s.offset + meta_size)
                            .max()
                            .unwrap_or(CHUNK_START);
                        StripeInfo {
                            devid: devices[i].devid,
                            offset: dev_meta_end,
                            dev_uuid: devices[i].dev_uuid,
                        }
                    })
                    .collect()
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
        // data follows metadata. Logical size depends on profile.
        let meta_logical = CHUNK_START;
        let meta_logical_size =
            meta_size * u64::from(metadata_profile.data_stripes(devices.len()));
        let data_logical = CHUNK_START + meta_logical_size;

        Some(ChunkLayout {
            meta_logical,
            meta_size,
            meta_stripes,
            data_logical,
            data_size,
            data_stripes,
            metadata_profile,
            data_profile,
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
    #[must_use]
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
    #[must_use]
    pub fn total_bytes_used(&self) -> u64 {
        SYSTEM_GROUP_SIZE
            + (self.meta_stripes.len() as u64 * self.meta_size)
            + (self.data_stripes.len() as u64 * self.data_size)
    }

    /// Logical size of the metadata chunk.
    ///
    /// For mirror profiles this equals the per-stripe size. For striped
    /// profiles the logical size is the stripe size multiplied by the
    /// number of data stripes.
    #[must_use]
    pub fn meta_logical_size(&self) -> u64 {
        self.meta_size
            * u64::from(
                self.metadata_profile.data_stripes(self.meta_stripes.len()),
            )
    }

    /// Logical size of the data chunk.
    #[must_use]
    pub fn data_logical_size(&self) -> u64 {
        self.data_size
            * u64::from(self.data_profile.data_stripes(self.data_stripes.len()))
    }

    /// Map a logical address to its physical write locations.
    ///
    /// Returns `(devid, physical_offset)` pairs.
    /// System chunk: always device 1, logical == physical.
    /// Mirror profiles: one pair per stripe (all get identical data).
    /// RAID0/RAID5/RAID6: one pair (the single data stripe owning that offset).
    /// RAID10: two pairs (the mirror pair for that stripe group).
    ///
    /// # Panics
    ///
    /// Panics if the logical address is not in any known chunk.
    #[must_use]
    pub fn logical_to_physical(&self, logical: u64) -> Vec<(u64, u64)> {
        let sys_range =
            SYSTEM_GROUP_OFFSET..SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE;
        let meta_logical_size = self.meta_logical_size();
        let data_logical_size = self.data_logical_size();
        let meta_range =
            self.meta_logical..self.meta_logical + meta_logical_size;
        let data_range =
            self.data_logical..self.data_logical + data_logical_size;

        if sys_range.contains(&logical) {
            // System chunk: device 1, logical == physical
            vec![(1, logical)]
        } else if meta_range.contains(&logical) {
            let off = logical - self.meta_logical;
            Self::map_offset(off, &self.meta_stripes, self.metadata_profile)
        } else if data_range.contains(&logical) {
            let off = logical - self.data_logical;
            Self::map_offset(off, &self.data_stripes, self.data_profile)
        } else {
            panic!("logical address {logical:#x} not in any chunk")
        }
    }

    /// Map a logical offset within a chunk to physical (devid, offset) pairs.
    fn map_offset(
        off: u64,
        stripes: &[StripeInfo],
        profile: Profile,
    ) -> Vec<(u64, u64)> {
        if profile.is_mirror() {
            // Mirror profiles: all stripes get the same data.
            stripes.iter().map(|s| (s.devid, s.offset + off)).collect()
        } else if profile == Profile::Raid10 {
            // Striped mirrors: find the mirror pair for this offset.
            let sub = profile.sub_stripes() as usize;
            let data_groups = stripes.len() / sub;
            let group = ((off / STRIPE_LEN) % data_groups as u64) as usize;
            let phys_off = (off / (STRIPE_LEN * data_groups as u64))
                * STRIPE_LEN
                + (off % STRIPE_LEN);
            (0..sub)
                .map(|s| {
                    let stripe = &stripes[group * sub + s];
                    (stripe.devid, stripe.offset + phys_off)
                })
                .collect()
        } else {
            // RAID0, RAID5, RAID6: data is striped, each offset maps to one stripe.
            let data_count = u64::from(profile.data_stripes(stripes.len()));
            let stripe_idx = ((off / STRIPE_LEN) % data_count) as usize;
            let phys_off = (off / (STRIPE_LEN * data_count)) * STRIPE_LEN
                + (off % STRIPE_LEN);
            let stripe = &stripes[stripe_idx];
            vec![(stripe.devid, stripe.offset + phys_off)]
        }
    }
}

/// Dynamic block address allocator for rootdir mode.
///
/// Unlike `BlockLayout` which assigns a fixed address per `TreeId`,
/// `BlockAllocator` hands out sequential addresses from the system and
/// metadata chunks. This supports trees that need multiple blocks.
pub struct BlockAllocator {
    nodesize: u32,
    system_start: u64,
    next_system: u64,
    system_end: u64,
    meta_start: u64,
    next_meta: u64,
    meta_end: u64,
}

impl BlockAllocator {
    /// Create an allocator for the given chunk layout.
    #[must_use]
    pub fn new(nodesize: u32, meta_logical: u64, meta_size: u64) -> Self {
        Self {
            nodesize,
            system_start: SYSTEM_GROUP_OFFSET,
            next_system: SYSTEM_GROUP_OFFSET,
            system_end: SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE,
            meta_start: meta_logical,
            next_meta: meta_logical,
            meta_end: meta_logical + meta_size,
        }
    }

    /// Allocate a block in the system chunk (for the chunk tree).
    ///
    /// # Errors
    ///
    /// Returns an error if the system chunk is full.
    pub fn alloc_system(&mut self) -> anyhow::Result<u64> {
        let addr = self.next_system;
        if addr + u64::from(self.nodesize) > self.system_end {
            anyhow::bail!(
                "system chunk full: cannot allocate more tree blocks"
            );
        }
        self.next_system += u64::from(self.nodesize);
        Ok(addr)
    }

    /// Allocate a block in the metadata chunk (for all non-chunk trees).
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata chunk is full.
    pub fn alloc_metadata(&mut self) -> anyhow::Result<u64> {
        let addr = self.next_meta;
        if addr + u64::from(self.nodesize) > self.meta_end {
            anyhow::bail!(
                "metadata chunk full: cannot allocate more tree blocks"
            );
        }
        self.next_meta += u64::from(self.nodesize);
        Ok(addr)
    }

    /// Total bytes used in the system chunk.
    #[must_use]
    pub fn system_used(&self) -> u64 {
        self.next_system - self.system_start
    }

    /// Total bytes used in the metadata chunk.
    #[must_use]
    pub fn metadata_used(&self) -> u64 {
        self.next_meta - self.meta_start
    }

    /// Reset the allocator to reuse from the beginning.
    /// Used during the convergence loop when block counts change.
    pub fn reset(&mut self) {
        self.next_system = self.system_start;
        self.next_meta = self.meta_start;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_allocator_basic() {
        let mut alloc =
            BlockAllocator::new(16384, CHUNK_START, 32 * 1024 * 1024);
        let a1 = alloc.alloc_system().unwrap();
        assert_eq!(a1, SYSTEM_GROUP_OFFSET);
        let a2 = alloc.alloc_metadata().unwrap();
        assert_eq!(a2, CHUNK_START);
        let a3 = alloc.alloc_metadata().unwrap();
        assert_eq!(a3, CHUNK_START + 16384);
        assert_eq!(alloc.system_used(), 16384);
        assert_eq!(alloc.metadata_used(), 32768);
    }

    #[test]
    fn block_allocator_reset() {
        let mut alloc =
            BlockAllocator::new(16384, CHUNK_START, 32 * 1024 * 1024);
        alloc.alloc_system().unwrap();
        alloc.alloc_metadata().unwrap();
        alloc.reset();
        assert_eq!(alloc.system_used(), 0);
        assert_eq!(alloc.metadata_used(), 0);
        let a1 = alloc.alloc_system().unwrap();
        assert_eq!(a1, SYSTEM_GROUP_OFFSET);
    }

    #[test]
    fn block_addresses() {
        // With a 256 MiB device, meta_logical = CHUNK_START = 5 MiB
        let meta_logical = CHUNK_START;
        let layout = BlockLayout::new(16384, meta_logical);

        // Chunk tree is in the system chunk at SYSTEM_GROUP_OFFSET
        assert_eq!(layout.block_addr(TreeId::Chunk), SYSTEM_GROUP_OFFSET);

        // The 4 always-present trees are sequential in the metadata
        // chunk. Csum and DataReloc are now optional and live in the
        // optional slot region after them.
        assert_eq!(layout.block_addr(TreeId::Root), meta_logical);
        assert_eq!(layout.block_addr(TreeId::Extent), meta_logical + 16384);
        assert_eq!(layout.block_addr(TreeId::Dev), meta_logical + 2 * 16384);
        assert_eq!(layout.block_addr(TreeId::Fs), meta_logical + 3 * 16384);
    }

    #[test]
    fn optional_block_addresses() {
        // Optional trees (BlockGroup, FreeSpace, Csum, DataReloc,
        // Quota) take slots 4..9 in the order they're enabled. The
        // base trees end at slot 3 (Fs).
        let meta_logical = CHUNK_START;
        let layout = BlockLayout::new(16384, meta_logical);

        // BlockGroup as the first optional slot.
        assert_eq!(
            layout.block_addr_with_offset(TreeId::BlockGroup, 0),
            meta_logical + 4 * 16384
        );
        // FreeSpace as the second optional slot (e.g. when BGT is on).
        assert_eq!(
            layout.block_addr_with_offset(TreeId::FreeSpace, 1),
            meta_logical + 5 * 16384
        );
        // Csum as the third optional slot.
        assert_eq!(
            layout.block_addr_with_offset(TreeId::Csum, 2),
            meta_logical + 6 * 16384
        );
        // DataReloc as the fourth optional slot.
        assert_eq!(
            layout.block_addr_with_offset(TreeId::DataReloc, 3),
            meta_logical + 7 * 16384
        );
        // Quota at the fifth slot.
        assert_eq!(
            layout.block_addr_with_offset(TreeId::Quota, 4),
            meta_logical + 8 * 16384
        );
    }

    #[test]
    fn system_and_metadata_used() {
        let layout = BlockLayout::new(16384, CHUNK_START);
        assert_eq!(layout.system_used(), 16384);
        // Base trees only (4 always-present trees).
        assert_eq!(
            layout.metadata_used(false, false, false, false, false),
            4 * 16384
        );
        // + BlockGroup.
        assert_eq!(
            layout.metadata_used(true, false, false, false, false),
            5 * 16384
        );
        // + FreeSpace.
        assert_eq!(
            layout.metadata_used(false, true, false, false, false),
            5 * 16384
        );
        // + Csum.
        assert_eq!(
            layout.metadata_used(false, false, true, false, false),
            5 * 16384
        );
        // + DataReloc.
        assert_eq!(
            layout.metadata_used(false, false, false, true, false),
            5 * 16384
        );
        // + Quota.
        assert_eq!(
            layout.metadata_used(false, false, false, false, true),
            5 * 16384
        );
        // All five optional trees.
        assert_eq!(
            layout.metadata_used(true, true, true, true, true),
            9 * 16384
        );
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
        // 256 MiB device: 256M/10 = 25.6M, rounded down to STRIPE_LEN
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        let expected_stripe =
            (256 * 1024 * 1024 / 10) / STRIPE_LEN * STRIPE_LEN;
        assert_eq!(cl.meta_size, expected_stripe);
        assert_eq!(cl.data_size, expected_stripe);
        assert_eq!(cl.meta_stripes.len(), 2);
        assert_eq!(cl.meta_stripes[0].offset, CHUNK_START);
        assert_eq!(cl.meta_stripes[1].offset, CHUNK_START + expected_stripe);
        assert_eq!(cl.data_stripes.len(), 1);
        assert_eq!(
            cl.data_stripes[0].offset,
            CHUNK_START + 2 * expected_stripe
        );
        assert_eq!(cl.meta_logical, CHUNK_START);
        assert_eq!(cl.data_logical, CHUNK_START + expected_stripe);
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
        // 20 MiB: needs 5M + 2*8M + 8M = 29M, doesn't fit
        let devs = single_device(20 * 1024 * 1024);
        assert!(
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).is_none()
        );
    }

    #[test]
    fn chunk_layout_small_device() {
        // 64 MiB: fits with 8M chunks (5M + 2*8M + 8M = 29M)
        let devs = single_device(64 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        assert_eq!(cl.meta_size, 8 * 1024 * 1024);
        assert_eq!(cl.data_size, 8 * 1024 * 1024);
    }

    #[test]
    fn chunk_layout_total_bytes_used() {
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        // system(4M) + 2*meta + data
        assert_eq!(
            cl.total_bytes_used(),
            SYSTEM_GROUP_SIZE + 2 * cl.meta_size + cl.data_size
        );
    }

    #[test]
    fn chunk_layout_dev_bytes_used_single_device() {
        let devs = single_device(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Dup, Profile::Single).unwrap();
        // All chunks on device 1: system(4M) + 2*meta + data
        assert_eq!(
            cl.dev_bytes_used_for(1),
            SYSTEM_GROUP_SIZE + 2 * cl.meta_size + cl.data_size
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

    #[test]
    fn logical_to_physical_raid0_maps_to_single_stripe() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid0, Profile::Raid0).unwrap();
        // First STRIPE_LEN of metadata maps to stripe 0 (device 1).
        let r = cl.logical_to_physical(cl.meta_logical);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, 1); // device 1
        // Second STRIPE_LEN maps to stripe 1 (device 2).
        let r2 = cl.logical_to_physical(cl.meta_logical + STRIPE_LEN);
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].0, 2); // device 2
    }

    #[test]
    fn logical_to_physical_raid10_maps_to_mirror_pair() {
        let uuid3 =
            uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111")
                .unwrap();
        let uuid4 =
            uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222")
                .unwrap();
        let devs = vec![
            ChunkDevice {
                devid: 1,
                total_bytes: 256 * 1024 * 1024,
                dev_uuid: test_uuid(),
            },
            ChunkDevice {
                devid: 2,
                total_bytes: 256 * 1024 * 1024,
                dev_uuid: uuid3,
            },
            ChunkDevice {
                devid: 3,
                total_bytes: 256 * 1024 * 1024,
                dev_uuid: uuid4,
            },
            ChunkDevice {
                devid: 4,
                total_bytes: 256 * 1024 * 1024,
                dev_uuid: uuid::Uuid::parse_str(
                    "cafebabe-cafe-babe-cafe-babecafebabe",
                )
                .unwrap(),
            },
        ];
        let cl =
            ChunkLayout::new(&devs, Profile::Raid10, Profile::Raid10).unwrap();
        // RAID10 with 4 stripes: 2 data groups, each mirrored.
        // First STRIPE_LEN maps to group 0 (stripes 0 and 1 = devices 1,2).
        let r = cl.logical_to_physical(cl.meta_logical);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, 1);
        assert_eq!(r[1].0, 2);
        // Second STRIPE_LEN maps to group 1 (stripes 2 and 3 = devices 3,4).
        let r2 = cl.logical_to_physical(cl.meta_logical + STRIPE_LEN);
        assert_eq!(r2.len(), 2);
        assert_eq!(r2[0].0, 3);
        assert_eq!(r2[1].0, 4);
    }

    #[test]
    fn raid0_logical_size_is_stripe_times_devices() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid0, Profile::Raid0).unwrap();
        assert_eq!(cl.meta_logical_size(), cl.meta_size * 2);
        assert_eq!(cl.data_logical_size(), cl.data_size * 2);
    }

    #[test]
    fn mirror_logical_size_equals_stripe_size() {
        let devs = two_devices(256 * 1024 * 1024);
        let cl =
            ChunkLayout::new(&devs, Profile::Raid1, Profile::Single).unwrap();
        assert_eq!(cl.meta_logical_size(), cl.meta_size);
        assert_eq!(cl.data_logical_size(), cl.data_size);
    }
}

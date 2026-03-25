//! # Block group space usage: how much space each chunk type allocates and uses
//!
//! Reports the allocated and used byte counts for each combination of block
//! group type (data, metadata, system) and RAID profile.  This is the data
//! underlying the `btrfs filesystem df` command.

use crate::raw::{
    BTRFS_AVAIL_ALLOC_BIT_SINGLE, BTRFS_BLOCK_GROUP_DATA, BTRFS_BLOCK_GROUP_DUP,
    BTRFS_BLOCK_GROUP_METADATA, BTRFS_BLOCK_GROUP_RAID0, BTRFS_BLOCK_GROUP_RAID1,
    BTRFS_BLOCK_GROUP_RAID1C3, BTRFS_BLOCK_GROUP_RAID1C4, BTRFS_BLOCK_GROUP_RAID5,
    BTRFS_BLOCK_GROUP_RAID6, BTRFS_BLOCK_GROUP_RAID10, BTRFS_BLOCK_GROUP_SYSTEM,
    BTRFS_SPACE_INFO_GLOBAL_RSV, btrfs_ioc_space_info, btrfs_ioctl_space_args,
    btrfs_ioctl_space_info,
};
use bitflags::bitflags;
use std::{
    fmt, mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

bitflags! {
    /// Flags describing the type and RAID profile of a btrfs block group.
    ///
    /// The lower bits encode the chunk type (data, metadata, system) and the
    /// upper bits encode the RAID profile. A `Display` implementation formats
    /// the flags as `"<type>, <profile>"`, matching the output of
    /// `btrfs filesystem df`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct BlockGroupFlags: u64 {
        // --- chunk types ---
        const DATA            = BTRFS_BLOCK_GROUP_DATA as u64;
        const SYSTEM          = BTRFS_BLOCK_GROUP_SYSTEM as u64;
        const METADATA        = BTRFS_BLOCK_GROUP_METADATA as u64;

        // --- RAID profiles ---
        const RAID0           = BTRFS_BLOCK_GROUP_RAID0 as u64;
        const RAID1           = BTRFS_BLOCK_GROUP_RAID1 as u64;
        const DUP             = BTRFS_BLOCK_GROUP_DUP as u64;
        const RAID10          = BTRFS_BLOCK_GROUP_RAID10 as u64;
        const RAID5           = BTRFS_BLOCK_GROUP_RAID5 as u64;
        const RAID6           = BTRFS_BLOCK_GROUP_RAID6 as u64;
        const RAID1C3         = BTRFS_BLOCK_GROUP_RAID1C3 as u64;
        const RAID1C4         = BTRFS_BLOCK_GROUP_RAID1C4 as u64;

        // AVAIL_ALLOC_BIT_SINGLE is the explicit "single" marker (bit 48).
        // When no profile bits are set the allocation is also single.
        const SINGLE          = BTRFS_AVAIL_ALLOC_BIT_SINGLE;

        // Pseudo-type used for the global reservation pool.
        const GLOBAL_RSV      = BTRFS_SPACE_INFO_GLOBAL_RSV;
    }
}

impl BlockGroupFlags {
    /// Returns the human-readable chunk type name.
    pub fn type_name(self) -> &'static str {
        if self.contains(Self::GLOBAL_RSV) {
            return "GlobalReserve";
        }
        let ty = self & (Self::DATA | Self::SYSTEM | Self::METADATA);
        match ty {
            t if t == Self::DATA => "Data",
            t if t == Self::SYSTEM => "System",
            t if t == Self::METADATA => "Metadata",
            t if t == Self::DATA | Self::METADATA => "Data+Metadata",
            _ => "unknown",
        }
    }

    /// Returns the human-readable RAID profile name.
    pub fn profile_name(self) -> &'static str {
        let profile = self
            & (Self::RAID0
                | Self::RAID1
                | Self::DUP
                | Self::RAID10
                | Self::RAID5
                | Self::RAID6
                | Self::RAID1C3
                | Self::RAID1C4
                | Self::SINGLE);
        match profile {
            p if p == Self::RAID0 => "RAID0",
            p if p == Self::RAID1 => "RAID1",
            p if p == Self::DUP => "DUP",
            p if p == Self::RAID10 => "RAID10",
            p if p == Self::RAID5 => "RAID5",
            p if p == Self::RAID6 => "RAID6",
            p if p == Self::RAID1C3 => "RAID1C3",
            p if p == Self::RAID1C4 => "RAID1C4",
            // Both explicit SINGLE and no-profile-bits mean "single".
            _ => "single",
        }
    }
}

impl fmt::Display for BlockGroupFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.type_name(), self.profile_name())
    }
}

/// Space usage information for one block group type/profile combination.
///
/// Returned by [`space_info`]. The `flags` field describes the chunk type and
/// RAID profile; `total_bytes` and `used_bytes` are the allocated and in-use
/// byte counts respectively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceInfo {
    pub flags: BlockGroupFlags,
    pub total_bytes: u64,
    pub used_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- type_name ---

    #[test]
    fn type_name_data() {
        assert_eq!(BlockGroupFlags::DATA.type_name(), "Data");
    }

    #[test]
    fn type_name_metadata() {
        assert_eq!(BlockGroupFlags::METADATA.type_name(), "Metadata");
    }

    #[test]
    fn type_name_system() {
        assert_eq!(BlockGroupFlags::SYSTEM.type_name(), "System");
    }

    #[test]
    fn type_name_data_metadata() {
        let flags = BlockGroupFlags::DATA | BlockGroupFlags::METADATA;
        assert_eq!(flags.type_name(), "Data+Metadata");
    }

    #[test]
    fn type_name_global_rsv() {
        assert_eq!(BlockGroupFlags::GLOBAL_RSV.type_name(), "GlobalReserve");
    }

    #[test]
    fn type_name_global_rsv_takes_precedence() {
        let flags = BlockGroupFlags::GLOBAL_RSV | BlockGroupFlags::METADATA;
        assert_eq!(flags.type_name(), "GlobalReserve");
    }

    // --- profile_name ---

    #[test]
    fn profile_name_single_no_bits() {
        assert_eq!(BlockGroupFlags::DATA.profile_name(), "single");
    }

    #[test]
    fn profile_name_single_explicit() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::SINGLE).profile_name(),
            "single"
        );
    }

    #[test]
    fn profile_name_raid0() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID0).profile_name(),
            "RAID0"
        );
    }

    #[test]
    fn profile_name_raid1() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1).profile_name(),
            "RAID1"
        );
    }

    #[test]
    fn profile_name_dup() {
        assert_eq!(
            (BlockGroupFlags::METADATA | BlockGroupFlags::DUP).profile_name(),
            "DUP"
        );
    }

    #[test]
    fn profile_name_raid10() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID10).profile_name(),
            "RAID10"
        );
    }

    #[test]
    fn profile_name_raid5() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID5).profile_name(),
            "RAID5"
        );
    }

    #[test]
    fn profile_name_raid6() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID6).profile_name(),
            "RAID6"
        );
    }

    #[test]
    fn profile_name_raid1c3() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1C3).profile_name(),
            "RAID1C3"
        );
    }

    #[test]
    fn profile_name_raid1c4() {
        assert_eq!(
            (BlockGroupFlags::DATA | BlockGroupFlags::RAID1C4).profile_name(),
            "RAID1C4"
        );
    }

    // --- Display ---

    #[test]
    fn display_data_single() {
        assert_eq!(format!("{}", BlockGroupFlags::DATA), "Data, single");
    }

    #[test]
    fn display_metadata_dup() {
        let flags = BlockGroupFlags::METADATA | BlockGroupFlags::DUP;
        assert_eq!(format!("{flags}"), "Metadata, DUP");
    }

    #[test]
    fn display_system_raid1() {
        let flags = BlockGroupFlags::SYSTEM | BlockGroupFlags::RAID1;
        assert_eq!(format!("{flags}"), "System, RAID1");
    }

    #[test]
    fn display_global_rsv() {
        let flags = BlockGroupFlags::GLOBAL_RSV | BlockGroupFlags::METADATA;
        assert_eq!(format!("{flags}"), "GlobalReserve, single");
    }

    // --- from_bits_truncate ---

    #[test]
    fn from_bits_preserves_known_flags() {
        let raw = BTRFS_BLOCK_GROUP_DATA as u64 | BTRFS_BLOCK_GROUP_RAID1 as u64;
        let flags = BlockGroupFlags::from_bits_truncate(raw);
        assert!(flags.contains(BlockGroupFlags::DATA));
        assert!(flags.contains(BlockGroupFlags::RAID1));
    }
}

impl From<btrfs_ioctl_space_info> for SpaceInfo {
    fn from(raw: btrfs_ioctl_space_info) -> Self {
        Self {
            flags: BlockGroupFlags::from_bits_truncate(raw.flags),
            total_bytes: raw.total_bytes,
            used_bytes: raw.used_bytes,
        }
    }
}

/// Query space usage by block group type for the filesystem referred to by
/// `fd`.
///
/// Returns one [`SpaceInfo`] entry per block group type/profile combination.
///
/// Uses a two-phase ioctl call: the first call with `space_slots = 0`
/// retrieves the entry count, and the second call retrieves all entries.
pub fn space_info(fd: BorrowedFd) -> nix::Result<Vec<SpaceInfo>> {
    // Phase 1: query with space_slots = 0 to discover the number of entries.
    let mut args: btrfs_ioctl_space_args = unsafe { mem::zeroed() };
    unsafe { btrfs_ioc_space_info(fd.as_raw_fd(), &mut args) }?;
    let count = args.total_spaces as usize;

    if count == 0 {
        return Ok(Vec::new());
    }

    // Phase 2: allocate a buffer large enough to hold the header plus all
    // entries, then call again with space_slots set to the count.
    //
    // We use Vec<u64> rather than Vec<u8> to guarantee 8-byte alignment,
    // matching the alignment requirement of btrfs_ioctl_space_args.
    let base_size = mem::size_of::<btrfs_ioctl_space_args>();
    let info_size = mem::size_of::<btrfs_ioctl_space_info>();
    let total_bytes = base_size + count * info_size;
    let num_u64s = total_bytes.div_ceil(mem::size_of::<u64>());
    let mut buf = vec![0u64; num_u64s];

    // SAFETY: buf is correctly sized and aligned for btrfs_ioctl_space_args.
    // We write space_slots before the ioctl and read spaces[] only after the
    // ioctl has populated them, keeping everything within the allocation.
    unsafe {
        let args_ptr = buf.as_mut_ptr() as *mut btrfs_ioctl_space_args;
        (*args_ptr).space_slots = count as u64;
        btrfs_ioc_space_info(fd.as_raw_fd(), &mut *args_ptr)?;
        Ok((*args_ptr)
            .spaces
            .as_slice(count)
            .iter()
            .copied()
            .map(SpaceInfo::from)
            .collect())
    }
}

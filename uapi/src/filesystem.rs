use crate::raw::{BTRFS_FS_INFO_FLAG_GENERATION, btrfs_ioc_fs_info, btrfs_ioctl_fs_info_args};
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};
use uuid::Uuid;

/// Information about a mounted btrfs filesystem, as returned by
/// `BTRFS_IOC_FS_INFO`.
#[derive(Debug, Clone)]
pub struct FsInfo {
    /// Filesystem UUID.
    pub uuid: Uuid,
    /// Number of devices in the filesystem.
    pub num_devices: u64,
    /// Highest device ID in the filesystem.
    pub max_id: u64,
    /// B-tree node size in bytes.
    pub nodesize: u32,
    /// Sector size in bytes.
    pub sectorsize: u32,
    /// Generation number of the filesystem.
    pub generation: u64,
}

/// Query information about the btrfs filesystem referred to by `fd`.
pub fn fs_info(fd: BorrowedFd) -> nix::Result<FsInfo> {
    let mut raw: btrfs_ioctl_fs_info_args = unsafe { mem::zeroed() };
    raw.flags = BTRFS_FS_INFO_FLAG_GENERATION as u64;
    unsafe { btrfs_ioc_fs_info(fd.as_raw_fd(), &mut raw) }?;

    Ok(FsInfo {
        uuid: Uuid::from_bytes(raw.fsid),
        num_devices: raw.num_devices,
        max_id: raw.max_id,
        nodesize: raw.nodesize,
        sectorsize: raw.sectorsize,
        generation: raw.generation,
    })
}

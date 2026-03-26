//! # Receive support: ioctls for applying a send stream to a filesystem
//!
//! Provides safe wrappers for the kernel interfaces used when receiving a btrfs
//! send stream: marking a subvolume as received (`SET_RECEIVED_SUBVOL`), cloning
//! extents between files (`CLONE_RANGE`), and searching the UUID tree to locate
//! subvolumes by their UUID or received UUID.

use crate::{
    raw::{
        self, btrfs_ioc_clone_range, btrfs_ioc_encoded_write, btrfs_ioc_set_received_subvol,
        btrfs_ioctl_clone_range_args, btrfs_ioctl_encoded_io_args,
        btrfs_ioctl_received_subvol_args,
    },
    tree_search::{SearchKey, tree_search},
};
use nix::libc::c_int;
use std::os::fd::{AsRawFd, BorrowedFd};
use uuid::Uuid;

/// Result of searching the UUID tree for a subvolume.
#[derive(Debug, Clone)]
pub struct SubvolSearchResult {
    /// The root ID (subvolume ID) found in the UUID tree.
    pub root_id: u64,
}

/// Mark a subvolume as received by setting its received UUID and stransid.
///
/// After applying a send stream, this ioctl records the sender's UUID and
/// transaction ID so that future incremental sends can use this subvolume as
/// a reference. Returns the receive transaction ID assigned by the kernel.
pub fn received_subvol_set(fd: BorrowedFd<'_>, uuid: &Uuid, stransid: u64) -> nix::Result<u64> {
    let mut args: btrfs_ioctl_received_subvol_args = unsafe { std::mem::zeroed() };

    let uuid_bytes = uuid.as_bytes();
    // uuid field is [c_char; 16]; copy byte-by-byte.
    for (i, &b) in uuid_bytes.iter().enumerate() {
        args.uuid[i] = b as std::os::raw::c_char;
    }
    args.stransid = stransid;

    // SAFETY: args is fully initialized, fd is a valid borrowed fd to a subvolume.
    unsafe {
        btrfs_ioc_set_received_subvol(fd.as_raw_fd() as c_int, &mut args)?;
    }

    Ok(args.rtransid)
}

/// Clone a range of bytes from one file to another using `BTRFS_IOC_CLONE_RANGE`.
///
/// Both files must be on the same btrfs filesystem. The destination file
/// descriptor `dest_fd` is the ioctl target.
pub fn clone_range(
    dest_fd: BorrowedFd<'_>,
    src_fd: BorrowedFd<'_>,
    src_offset: u64,
    src_length: u64,
    dest_offset: u64,
) -> nix::Result<()> {
    let args = btrfs_ioctl_clone_range_args {
        src_fd: src_fd.as_raw_fd() as i64,
        src_offset,
        src_length,
        dest_offset,
    };

    // SAFETY: args is fully initialized, both fds are valid.
    unsafe {
        btrfs_ioc_clone_range(dest_fd.as_raw_fd() as c_int, &args)?;
    }

    Ok(())
}

/// Write pre-compressed data to a file using `BTRFS_IOC_ENCODED_WRITE`.
///
/// This passes compressed data directly to the filesystem without
/// decompression, which is more efficient than decompressing and writing.
/// The kernel may reject the call with `ENOTTY` (old kernel), `EINVAL`
/// (unsupported parameters), or `ENOSPC`; callers should fall back to
/// manual decompression + pwrite in those cases.
#[allow(clippy::too_many_arguments)]
pub fn encoded_write(
    fd: BorrowedFd<'_>,
    data: &[u8],
    offset: u64,
    unencoded_file_len: u64,
    unencoded_len: u64,
    unencoded_offset: u64,
    compression: u32,
    encryption: u32,
) -> nix::Result<()> {
    let iov = nix::libc::iovec {
        iov_base: data.as_ptr() as *mut _,
        iov_len: data.len(),
    };

    let mut args: btrfs_ioctl_encoded_io_args = unsafe { std::mem::zeroed() };
    args.iov = &iov as *const _ as *mut _;
    args.iovcnt = 1;
    args.offset = offset as i64;
    args.len = unencoded_file_len;
    args.unencoded_len = unencoded_len;
    args.unencoded_offset = unencoded_offset;
    args.compression = compression;
    args.encryption = encryption;

    // SAFETY: args.iov points to a stack-allocated iovec whose iov_base
    // references `data` which outlives this call. The ioctl reads from the
    // iov buffers and writes encoded data to the file.
    unsafe {
        btrfs_ioc_encoded_write(fd.as_raw_fd() as c_int, &args)?;
    }

    Ok(())
}

/// Search the UUID tree for a subvolume by its UUID.
///
/// Returns the root ID of the matching subvolume, or `Errno::ENOENT` if not
/// found.
pub fn subvolume_search_by_uuid(fd: BorrowedFd<'_>, uuid: &Uuid) -> nix::Result<u64> {
    search_uuid_tree(fd, uuid, raw::BTRFS_UUID_KEY_SUBVOL as u32)
}

/// Search the UUID tree for a subvolume by its received UUID.
///
/// Returns the root ID of the matching subvolume, or `Errno::ENOENT` if not
/// found.
pub fn subvolume_search_by_received_uuid(fd: BorrowedFd<'_>, uuid: &Uuid) -> nix::Result<u64> {
    search_uuid_tree(fd, uuid, raw::BTRFS_UUID_KEY_RECEIVED_SUBVOL as u32)
}

/// Internal: search the UUID tree for a given key type.
///
/// The UUID tree encodes UUIDs as a compound key: objectid = LE u64 from
/// bytes [0..8], offset = LE u64 from bytes [8..16]. The item type selects
/// whether we are looking for regular UUIDs or received UUIDs. The data
/// payload is a single LE u64 root ID.
fn search_uuid_tree(fd: BorrowedFd<'_>, uuid: &Uuid, item_type: u32) -> nix::Result<u64> {
    let bytes = uuid.as_bytes();
    let objectid = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

    let mut key = SearchKey::for_type(raw::BTRFS_UUID_TREE_OBJECTID as u64, item_type);
    key.min_objectid = objectid;
    key.max_objectid = objectid;
    key.min_offset = offset;
    key.max_offset = offset;

    let mut result: Option<u64> = None;

    tree_search(fd, key, |_hdr, data| {
        if data.len() >= 8 {
            result = Some(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        }
        Ok(())
    })?;

    result.ok_or(nix::errno::Errno::ENOENT)
}

//! # Send and receive: generating and applying btrfs send streams
//!
//! The send side wraps `BTRFS_IOC_SEND`, which produces a binary stream
//! representing the contents of a read-only subvolume (or the delta between a
//! parent and child snapshot).
//!
//! The receive side wraps the ioctls used when applying a send stream:
//! marking a subvolume as received (`SET_RECEIVED_SUBVOL`), cloning extents
//! between files (`CLONE_RANGE`), writing pre-compressed data
//! (`ENCODED_WRITE`), and searching the UUID tree to locate subvolumes by
//! their UUID or received UUID.

use crate::{
    raw::{
        self, btrfs_ioc_clone_range, btrfs_ioc_encoded_read,
        btrfs_ioc_encoded_write, btrfs_ioc_send, btrfs_ioc_set_received_subvol,
        btrfs_ioctl_clone_range_args, btrfs_ioctl_encoded_io_args,
        btrfs_ioctl_received_subvol_args, btrfs_ioctl_send_args,
    },
    tree_search::{SearchKey, tree_search},
};
use bitflags::bitflags;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use uuid::Uuid;

bitflags! {
    /// Flags for the send ioctl.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendFlags: u64 {
        /// Do not include file data in the stream (metadata only).
        const NO_FILE_DATA = raw::BTRFS_SEND_FLAG_NO_FILE_DATA as u64;
        /// Omit the stream header (for multi-subvolume sends).
        const OMIT_STREAM_HEADER = raw::BTRFS_SEND_FLAG_OMIT_STREAM_HEADER as u64;
        /// Omit the end-cmd marker (for multi-subvolume sends).
        const OMIT_END_CMD = raw::BTRFS_SEND_FLAG_OMIT_END_CMD as u64;
        /// Request a specific protocol version (set the version field).
        const VERSION = raw::BTRFS_SEND_FLAG_VERSION as u64;
        /// Send compressed data directly without decompressing.
        const COMPRESSED = raw::BTRFS_SEND_FLAG_COMPRESSED as u64;
    }
}

/// Invoke `BTRFS_IOC_SEND` on the given subvolume.
///
/// The kernel writes the send stream to `send_fd` (the write end of a pipe).
/// The caller is responsible for reading from the read end of the pipe,
/// typically in a separate thread.
///
/// `clone_sources` is a list of root IDs that the kernel may reference for
/// clone operations in the stream. `parent_root` is the root ID of the parent
/// snapshot for incremental sends, or `0` for a full send.
pub fn send(
    subvol_fd: BorrowedFd<'_>,
    send_fd: RawFd,
    parent_root: u64,
    clone_sources: &mut [u64],
    flags: SendFlags,
    version: u32,
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_send_args = unsafe { std::mem::zeroed() };
    args.send_fd = send_fd as i64;
    args.parent_root = parent_root;
    args.clone_sources_count = clone_sources.len() as u64;
    args.clone_sources = if clone_sources.is_empty() {
        std::ptr::null_mut()
    } else {
        clone_sources.as_mut_ptr()
    };
    args.flags = flags.bits();
    args.version = version;

    // SAFETY: args is fully initialized, clone_sources points to valid memory
    // that outlives the ioctl call, and subvol_fd is a valid borrowed fd.
    unsafe {
        btrfs_ioc_send(subvol_fd.as_raw_fd(), &args)?;
    }

    Ok(())
}

/// Result of searching the UUID tree for a subvolume.
#[derive(Debug, Clone)]
pub struct SubvolumeSearchResult {
    /// The root ID (subvolume ID) found in the UUID tree.
    pub root_id: u64,
}

/// Mark a subvolume as received by setting its received UUID and stransid.
///
/// After applying a send stream, this ioctl records the sender's UUID and
/// transaction ID so that future incremental sends can use this subvolume as
/// a reference. Returns the receive transaction ID assigned by the kernel.
pub fn received_subvol_set(
    fd: BorrowedFd<'_>,
    uuid: &Uuid,
    stransid: u64,
) -> nix::Result<u64> {
    let mut args: btrfs_ioctl_received_subvol_args =
        unsafe { std::mem::zeroed() };

    let uuid_bytes = uuid.as_bytes();
    // uuid field is [c_char; 16]; copy byte-by-byte.
    for (i, &b) in uuid_bytes.iter().enumerate() {
        args.uuid[i] = b as std::os::raw::c_char;
    }
    args.stransid = stransid;

    // SAFETY: args is fully initialized, fd is a valid borrowed fd to a subvolume.
    unsafe {
        btrfs_ioc_set_received_subvol(fd.as_raw_fd(), &mut args)?;
    }

    Ok(args.rtransid)
}

/// Clone a range of bytes from one file to another using `BTRFS_IOC_CLONE_RANGE`.
///
/// Both files must be on the same btrfs filesystem. The destination file
/// descriptor `dest_fd` is the ioctl target.
///
/// Errors: EXDEV if source and destination are on different filesystems.
/// EINVAL if the range is not sector-aligned or extends beyond EOF.
/// ETXTBSY if the destination file is a swap file.
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
        btrfs_ioc_clone_range(dest_fd.as_raw_fd(), &args)?;
    }

    Ok(())
}

/// Write pre-compressed data to a file using `BTRFS_IOC_ENCODED_WRITE`.
///
/// This passes compressed data directly to the filesystem without
/// decompression, which is more efficient than decompressing and writing.
///
/// Errors: ENOTTY on kernels that do not support encoded writes (pre-5.18).
/// EINVAL if the compression type, alignment, or lengths are not accepted.
/// ENOSPC if the filesystem has no room for the encoded extent.  Callers
/// should fall back to manual decompression + pwrite for any of these.
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
        btrfs_ioc_encoded_write(fd.as_raw_fd(), &args)?;
    }

    Ok(())
}

/// Metadata returned by [`encoded_read`] describing how the data is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedReadResult {
    /// File offset of the extent.
    pub offset: u64,
    /// Length of the extent in the file (after decompression).
    pub unencoded_file_len: u64,
    /// Unencoded (decompressed) length of the data.
    pub unencoded_len: u64,
    /// Offset within the unencoded data where the file content starts.
    pub unencoded_offset: u64,
    /// Compression type (0 = none, 1 = zlib, 2 = lzo, 3 = zstd).
    pub compression: u32,
    /// Encryption type (currently always 0).
    pub encryption: u32,
    /// Number of bytes actually read into the buffer.
    pub bytes_read: usize,
}

/// Read compressed (encoded) data from a file using `BTRFS_IOC_ENCODED_READ`.
///
/// This reads the raw compressed extent data without decompressing it,
/// which is the counterpart to [`encoded_write`]. The caller provides a
/// buffer to receive the data; the returned [`EncodedReadResult`] describes
/// the encoding and how many bytes were read.
///
/// Errors: ENOTTY on kernels that do not support encoded reads (pre-5.18).
/// EINVAL if the offset or length are not accepted.
pub fn encoded_read(
    fd: BorrowedFd<'_>,
    buf: &mut [u8],
    offset: u64,
    len: u64,
) -> nix::Result<EncodedReadResult> {
    let iov = nix::libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut _,
        iov_len: buf.len(),
    };

    let mut args: btrfs_ioctl_encoded_io_args = unsafe { std::mem::zeroed() };
    args.iov = &iov as *const _ as *mut _;
    args.iovcnt = 1;
    args.offset = offset as i64;
    args.len = len;

    // SAFETY: args.iov points to a stack-allocated iovec whose iov_base
    // references `buf` which outlives this call. The ioctl writes encoded
    // data into the iov buffers.
    let ret = unsafe { btrfs_ioc_encoded_read(fd.as_raw_fd(), &mut args) }?;

    Ok(EncodedReadResult {
        offset: args.offset as u64,
        unencoded_file_len: args.len,
        unencoded_len: args.unencoded_len,
        unencoded_offset: args.unencoded_offset,
        compression: args.compression,
        encryption: args.encryption,
        bytes_read: ret as usize,
    })
}

/// Search the UUID tree for a subvolume by its UUID.
///
/// Returns the root ID of the matching subvolume, or `Errno::ENOENT` if not
/// found.
pub fn subvolume_search_by_uuid(
    fd: BorrowedFd<'_>,
    uuid: &Uuid,
) -> nix::Result<u64> {
    search_uuid_tree(fd, uuid, raw::BTRFS_UUID_KEY_SUBVOL)
}

/// Search the UUID tree for a subvolume by its received UUID.
///
/// Returns the root ID of the matching subvolume, or `Errno::ENOENT` if not
/// found.
pub fn subvolume_search_by_received_uuid(
    fd: BorrowedFd<'_>,
    uuid: &Uuid,
) -> nix::Result<u64> {
    search_uuid_tree(fd, uuid, raw::BTRFS_UUID_KEY_RECEIVED_SUBVOL)
}

/// Internal: search the UUID tree for a given key type.
///
/// The UUID tree encodes UUIDs as a compound key: objectid = LE u64 from
/// bytes [0..8], offset = LE u64 from bytes [8..16]. The item type selects
/// whether we are looking for regular UUIDs or received UUIDs. The data
/// payload is a single LE u64 root ID.
fn search_uuid_tree(
    fd: BorrowedFd<'_>,
    uuid: &Uuid,
    item_type: u32,
) -> nix::Result<u64> {
    let bytes = uuid.as_bytes();
    let objectid = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

    let mut key =
        SearchKey::for_type(raw::BTRFS_UUID_TREE_OBJECTID as u64, item_type);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_flags_no_file_data() {
        let flags = SendFlags::NO_FILE_DATA;
        assert!(flags.contains(SendFlags::NO_FILE_DATA));
        assert!(!flags.contains(SendFlags::COMPRESSED));
    }

    #[test]
    fn send_flags_combine() {
        let flags = SendFlags::NO_FILE_DATA | SendFlags::COMPRESSED;
        assert!(flags.contains(SendFlags::NO_FILE_DATA));
        assert!(flags.contains(SendFlags::COMPRESSED));
        assert!(!flags.contains(SendFlags::VERSION));
    }

    #[test]
    fn send_flags_empty() {
        let flags = SendFlags::empty();
        assert!(flags.is_empty());
        assert_eq!(flags.bits(), 0);
    }

    #[test]
    fn send_flags_debug() {
        let flags = SendFlags::OMIT_STREAM_HEADER | SendFlags::OMIT_END_CMD;
        let s = format!("{flags:?}");
        assert!(s.contains("OMIT_STREAM_HEADER"), "debug: {s}");
        assert!(s.contains("OMIT_END_CMD"), "debug: {s}");
    }

    #[test]
    fn encoded_read_result_equality() {
        let a = EncodedReadResult {
            offset: 0,
            unencoded_file_len: 4096,
            unencoded_len: 4096,
            unencoded_offset: 0,
            compression: 0,
            encryption: 0,
            bytes_read: 4096,
        };
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn encoded_read_result_debug() {
        let r = EncodedReadResult {
            offset: 0,
            unencoded_file_len: 4096,
            unencoded_len: 8192,
            unencoded_offset: 0,
            compression: 3,
            encryption: 0,
            bytes_read: 1024,
        };
        let s = format!("{r:?}");
        assert!(s.contains("compression: 3"), "debug: {s}");
        assert!(s.contains("bytes_read: 1024"), "debug: {s}");
    }

    #[test]
    fn subvolume_search_result_debug() {
        let r = SubvolumeSearchResult { root_id: 256 };
        let s = format!("{r:?}");
        assert!(s.contains("256"), "debug: {s}");
    }
}

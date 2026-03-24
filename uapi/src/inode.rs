//! Inode and path resolution — mapping between inodes, logical addresses, and paths.
//!
//! Covers looking up the subvolume root ID that contains a given file, resolving
//! an inode number to its filesystem paths, mapping a logical byte address back
//! to the inodes that reference it, and resolving a subvolume ID to its path
//! within the filesystem.

use nix::libc::c_int;
use std::os::fd::{AsRawFd, BorrowedFd};

use crate::raw::{
    BTRFS_FIRST_FREE_OBJECTID, btrfs_ioc_ino_lookup, btrfs_ioc_ino_paths, btrfs_ioc_logical_ino_v2,
};
use crate::tree_search::{SearchKey, tree_search};

/// Look up the tree ID (root ID) of the subvolume containing the given file or directory.
///
/// For a file or directory, returns the containing tree root ID.
/// For a subvolume, returns its own tree ID.
///
/// # Arguments
///
/// * `fd` - File descriptor to a file or directory on the btrfs filesystem
///
/// # Returns
///
/// The tree ID (root ID) of the containing subvolume
///
/// # Errors
///
/// Returns an error if the ioctl fails (e.g., file descriptor is not on a btrfs filesystem)
pub fn lookup_path_rootid(fd: BorrowedFd<'_>) -> nix::Result<u64> {
    let mut args = crate::raw::btrfs_ioctl_ino_lookup_args {
        treeid: 0,
        objectid: BTRFS_FIRST_FREE_OBJECTID as u64,
        ..unsafe { std::mem::zeroed() }
    };

    unsafe {
        btrfs_ioc_ino_lookup(fd.as_raw_fd() as c_int, &mut args)?;
    }

    Ok(args.treeid)
}

/// Get file system paths for the given inode.
///
/// Returns a vector of path strings relative to the filesystem root that correspond
/// to the given inode number.
///
/// # Arguments
///
/// * `fd` - File descriptor to a file or directory on the btrfs filesystem
/// * `inum` - Inode number to look up
///
/// # Returns
///
/// A vector of path strings for the given inode
///
/// # Errors
///
/// Returns an error if the ioctl fails
pub fn ino_paths(fd: BorrowedFd<'_>, inum: u64) -> nix::Result<Vec<String>> {
    const PATH_MAX: usize = 4096;

    // First, allocate a buffer for the response
    // The buffer needs to be large enough to hold btrfs_data_container plus the path data
    let mut buf = vec![0u8; PATH_MAX];

    // Set up the ioctl arguments
    let mut args = crate::raw::btrfs_ioctl_ino_path_args {
        inum,
        size: PATH_MAX as u64,
        reserved: [0; 4],
        fspath: buf.as_mut_ptr() as u64,
    };

    unsafe {
        btrfs_ioc_ino_paths(fd.as_raw_fd() as c_int, &mut args)?;
    }

    // Parse the results from the data container
    // The buffer is laid out as: btrfs_data_container header, followed by val[] array
    let container = unsafe { &*(buf.as_ptr() as *const crate::raw::btrfs_data_container) };

    let mut paths = Vec::new();

    // Each element in val[] is an offset into the data buffer where a path string starts
    for i in 0..container.elem_cnt as usize {
        // Get the offset from val[i]
        let val_offset = unsafe {
            let val_ptr = container.val.as_ptr();
            *val_ptr.add(i) as usize
        };

        // The path string starts at the base of val array plus the offset
        let val_base = container.val.as_ptr() as usize;
        let path_ptr = (val_base + val_offset) as *const i8;

        // Convert C string to Rust String
        let c_str = unsafe { std::ffi::CStr::from_ptr(path_ptr) };
        if let Ok(path_str) = c_str.to_str() {
            paths.push(path_str.to_string());
        }
    }

    Ok(paths)
}

/// Result from logical-ino resolution: (inode, offset, root)
#[derive(Debug, Clone)]
pub struct LogicalInoResult {
    pub inode: u64,
    pub offset: u64,
    pub root: u64,
}

/// Get inode, offset, and root information for a logical address.
///
/// Resolves a logical address on the filesystem to one or more (inode, offset, root) tuples.
/// The offset is the position within the file, and the root is the subvolume ID.
///
/// # Arguments
///
/// * `fd` - File descriptor to a file or directory on the btrfs filesystem
/// * `logical` - Logical address to resolve
/// * `ignore_offset` - If true, ignores offsets when matching references
/// * `bufsize` - Size of buffer to allocate (default 64KB, max 16MB)
///
/// # Returns
///
/// A vector of (inode, offset, root) tuples
///
/// # Errors
///
/// Returns an error if the ioctl fails
pub fn logical_ino(
    fd: BorrowedFd<'_>,
    logical: u64,
    ignore_offset: bool,
    bufsize: Option<u64>,
) -> nix::Result<Vec<LogicalInoResult>> {
    const MAX_BUFSIZE: u64 = 16 * 1024 * 1024; // 16MB
    const DEFAULT_BUFSIZE: u64 = 64 * 1024; // 64KB

    let size = std::cmp::min(bufsize.unwrap_or(DEFAULT_BUFSIZE), MAX_BUFSIZE);
    let mut buf = vec![0u8; size as usize];

    // Set up flags for v2 ioctl
    let mut flags = 0u64;
    if ignore_offset {
        flags |= crate::raw::BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET as u64;
    }

    // Set up the ioctl arguments
    let mut args = crate::raw::btrfs_ioctl_logical_ino_args {
        logical,
        size,
        reserved: [0; 3],
        flags,
        inodes: buf.as_mut_ptr() as u64,
    };

    unsafe {
        btrfs_ioc_logical_ino_v2(fd.as_raw_fd() as c_int, &mut args)?;
    }

    // Parse the results from the data container
    let container = unsafe { &*(buf.as_ptr() as *const crate::raw::btrfs_data_container) };

    let mut results = Vec::new();

    // Each result is 3 consecutive u64 values: inum, offset, root
    for i in (0..container.elem_cnt as usize).step_by(3) {
        if i + 2 < container.elem_cnt as usize {
            let val_offset_inum = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i) as u64
            };

            let val_offset_offset = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i + 1) as u64
            };

            let val_offset_root = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i + 2) as u64
            };

            results.push(LogicalInoResult {
                inode: val_offset_inum,
                offset: val_offset_offset,
                root: val_offset_root,
            });
        }
    }

    Ok(results)
}

/// Resolve a subvolume ID to its full path on the filesystem.
///
/// Recursively resolves the path to a subvolume by walking the root tree and using
/// INO_LOOKUP to get directory names. The path is built from the subvolume's name
/// and the names of all parent directories up to the mount point.
///
/// # Arguments
///
/// * `fd` - File descriptor to a file or directory on the btrfs filesystem
/// * `subvol_id` - The subvolume ID to resolve
///
/// # Returns
///
/// The full path to the subvolume relative to the filesystem root, or an empty string
/// for the filesystem root itself (FS_TREE_OBJECTID).
///
/// # Errors
///
/// Returns an error if:
/// * The ioctl fails (fd is not on a btrfs filesystem)
/// * The subvolume ID does not exist
/// * The path buffer overflows
///
/// # Example
///
/// ```ignore
/// let path = subvolid_resolve(fd, 5)?;
/// println!("Subvolume 5 is at: {}", path);
/// ```
pub fn subvolid_resolve(fd: BorrowedFd<'_>, subvol_id: u64) -> nix::Result<String> {
    let mut path = String::new();
    subvolid_resolve_sub(fd, &mut path, subvol_id)?;
    Ok(path)
}

fn subvolid_resolve_sub(fd: BorrowedFd<'_>, path: &mut String, subvol_id: u64) -> nix::Result<()> {
    use crate::raw::BTRFS_FS_TREE_OBJECTID;

    // If this is the filesystem root, we're done (empty path means root)
    if subvol_id == BTRFS_FS_TREE_OBJECTID as u64 {
        return Ok(());
    }

    // Search the root tree for ROOT_BACKREF_KEY entries for this subvolume.
    // ROOT_BACKREF_KEY (item type 156) has:
    // - objectid: the subvolume ID
    // - offset: the parent subvolume ID
    // - data: btrfs_root_ref struct containing the subvolume name
    let mut found = false;

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            crate::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            crate::raw::BTRFS_ROOT_BACKREF_KEY as u32,
            subvol_id,
            subvol_id,
        ),
        |hdr, data| {
            found = true;

            // The parent subvolume ID is stored in the offset field
            let parent_subvol_id = hdr.offset;

            // Recursively resolve the parent path first
            subvolid_resolve_sub(fd, path, parent_subvol_id)?;

            // data is the btrfs_root_ref struct.
            // Layout: dirid (u64) + transid (u64) + name_len (u32) + name[name_len]
            if data.len() < 20 {
                return Err(nix::errno::Errno::EOVERFLOW);
            }

            let dirid = u64::from_le_bytes(data[0..8].try_into().unwrap());

            // Skip to name_len (offset 16)
            let name_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

            if data.len() < 20 + name_len {
                return Err(nix::errno::Errno::EOVERFLOW);
            }

            // Get the subvolume name
            let name_bytes = &data[20..20 + name_len];

            // If dirid is not the first free objectid, we need to resolve the directory path too
            if dirid != BTRFS_FIRST_FREE_OBJECTID as u64 {
                // Look up the directory in the parent subvolume
                let mut ino_lookup_args = crate::raw::btrfs_ioctl_ino_lookup_args {
                    treeid: parent_subvol_id,
                    objectid: dirid,
                    ..unsafe { std::mem::zeroed() }
                };

                unsafe {
                    btrfs_ioc_ino_lookup(fd.as_raw_fd() as c_int, &mut ino_lookup_args)?;
                }

                // Get the directory name (it's a null-terminated C string)
                let dir_name = unsafe { std::ffi::CStr::from_ptr(ino_lookup_args.name.as_ptr()) }
                    .to_str()
                    .map_err(|_| nix::errno::Errno::EINVAL)?;

                if !dir_name.is_empty() {
                    if !path.is_empty() {
                        path.push('/');
                    }
                    path.push_str(dir_name);
                }
            }

            // Append the subvolume name
            if !path.is_empty() {
                path.push('/');
            }

            // Convert name bytes to string
            let name_str =
                std::str::from_utf8(name_bytes).map_err(|_| nix::errno::Errno::EINVAL)?;
            path.push_str(name_str);

            Ok(())
        },
    )?;

    if !found {
        return Err(nix::errno::Errno::ENOENT);
    }

    Ok(())
}

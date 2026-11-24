//! # Inode and path resolution: mapping between inodes, logical addresses, and paths
//!
//! Covers looking up the subvolume root ID that contains a given file, resolving
//! an inode number to its filesystem paths, mapping a logical byte address back
//! to the inodes that reference it, and resolving a subvolume ID to its path
//! within the filesystem.

use crate::{
    raw::{
        BTRFS_FIRST_FREE_OBJECTID, btrfs_ioc_ino_lookup,
        btrfs_ioc_ino_lookup_user, btrfs_ioc_ino_paths,
        btrfs_ioc_logical_ino_v2, btrfs_ioctl_ino_lookup_user_args,
        btrfs_root_ref,
    },
    tree_search::{SearchFilter, tree_search},
};
use std::{
    ffi::c_char,
    os::fd::{AsRawFd, BorrowedFd},
};

/// Look up the tree ID (root ID) of the subvolume containing `fd`.
///
/// `fd` is open on a file or directory on a btrfs filesystem. For a file or
/// directory, returns the ID of the containing subvolume. For a subvolume,
/// returns its own tree ID.
///
/// # Errors
///
/// Returns an error if the ioctl fails, for example when `fd` is not on a
/// btrfs filesystem.
pub fn lookup_path_rootid(fd: BorrowedFd<'_>) -> nix::Result<u64> {
    let mut args = crate::raw::btrfs_ioctl_ino_lookup_args {
        treeid: 0,
        objectid: u64::from(BTRFS_FIRST_FREE_OBJECTID),
        ..unsafe { std::mem::zeroed() }
    };

    unsafe {
        btrfs_ioc_ino_lookup(fd.as_raw_fd(), &raw mut args)?;
    }

    Ok(args.treeid)
}

/// Get the filesystem paths of inode `inum`.
///
/// `fd` is open on a file or directory on a btrfs filesystem. Returns every
/// path, relative to the filesystem root, that refers to the inode.
///
/// # Errors
///
/// Returns an error if the ioctl fails.
pub fn ino_paths(fd: BorrowedFd<'_>, inum: u64) -> nix::Result<Vec<String>> {
    const PATH_MAX: usize = 4096;

    // The buffer holds the btrfs_data_container header plus the path data.
    let mut buf = vec![0u8; PATH_MAX];

    let mut args = crate::raw::btrfs_ioctl_ino_path_args {
        inum,
        size: PATH_MAX as u64,
        reserved: [0; 4],
        fspath: buf.as_mut_ptr() as u64,
    };

    unsafe {
        btrfs_ioc_ino_paths(fd.as_raw_fd(), &raw mut args)?;
    }

    // The buffer is a btrfs_data_container header followed by the val[] array.
    // SAFETY: buf is u64-aligned from the Vec<u8> allocation, and the ioctl
    // populated it as a btrfs_data_container. The cast is safe because the
    // buffer was allocated with at least PATH_MAX bytes.
    #[allow(clippy::cast_ptr_alignment)]
    let container =
        unsafe { &*buf.as_ptr().cast::<crate::raw::btrfs_data_container>() };

    let mut paths = Vec::new();

    // Each element in val[] is the offset of a path string, relative to the
    // start of val[].
    for i in 0..container.elem_cnt as usize {
        #[allow(clippy::cast_possible_truncation)] // offsets fit in usize
        let val_offset = unsafe {
            let val_ptr = container.val.as_ptr();
            *val_ptr.add(i) as usize
        };

        let val_base = container.val.as_ptr() as usize;
        let path_ptr = (val_base + val_offset) as *const c_char;

        let c_str = unsafe { std::ffi::CStr::from_ptr(path_ptr) };
        if let Ok(path_str) = c_str.to_str() {
            paths.push(path_str.to_string());
        }
    }

    Ok(paths)
}

/// One reference to a logical address: inode, file offset, and root.
#[derive(Debug, Clone)]
pub struct LogicalInoResult {
    /// Inode number that contains data at the queried logical address.
    pub inode: u64,
    /// Byte offset within the file where the logical address maps.
    pub offset: u64,
    /// Subvolume tree ID (root) that owns this inode.
    pub root: u64,
}

/// Get inode, offset, and root information for a logical address.
///
/// Resolves `logical` to every (inode, offset, root) that references it. The
/// offset is the position within the file, and the root is the subvolume ID.
///
/// `fd` is open on a file or directory on a btrfs filesystem. When
/// `ignore_offset` is true, offsets are ignored when matching references.
/// `bufsize` is the result buffer size (default 64 KiB, max 16 MiB).
///
/// # Errors
///
/// Returns an error if the ioctl fails.
pub fn logical_ino(
    fd: BorrowedFd<'_>,
    logical: u64,
    ignore_offset: bool,
    bufsize: Option<u64>,
) -> nix::Result<Vec<LogicalInoResult>> {
    const MAX_BUFSIZE: u64 = 16 * 1024 * 1024; // 16MB
    const DEFAULT_BUFSIZE: u64 = 64 * 1024; // 64KB

    let size = std::cmp::min(bufsize.unwrap_or(DEFAULT_BUFSIZE), MAX_BUFSIZE);
    #[allow(clippy::cast_possible_truncation)] // buffer size fits in usize
    let mut buf = vec![0u8; size as usize];

    let mut flags = 0u64;
    if ignore_offset {
        flags |= u64::from(crate::raw::BTRFS_LOGICAL_INO_ARGS_IGNORE_OFFSET);
    }

    let mut args = crate::raw::btrfs_ioctl_logical_ino_args {
        logical,
        size,
        reserved: [0; 3],
        flags,
        inodes: buf.as_mut_ptr() as u64,
    };

    unsafe {
        btrfs_ioc_logical_ino_v2(fd.as_raw_fd(), &raw mut args)?;
    }

    // SAFETY: buf is correctly sized and was populated by the ioctl.
    #[allow(clippy::cast_ptr_alignment)]
    let container =
        unsafe { &*buf.as_ptr().cast::<crate::raw::btrfs_data_container>() };

    let mut results = Vec::new();

    // Each result is 3 consecutive u64 values: inum, offset, root.
    for i in (0..container.elem_cnt as usize).step_by(3) {
        if i + 2 < container.elem_cnt as usize {
            let val_offset_inum = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i)
            };

            let val_offset_offset = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i + 1)
            };

            let val_offset_root = unsafe {
                let val_ptr = container.val.as_ptr();
                *val_ptr.add(i + 2)
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
/// Walks the `ROOT_BACKREF` chain in the root tree and uses `INO_LOOKUP` to
/// get directory names. `fd` is open on a file or directory on a btrfs
/// filesystem.
///
/// Returns the path relative to the filesystem root, or an empty string for
/// the filesystem root itself (`FS_TREE_OBJECTID`).
///
/// # Errors
///
/// Returns an error if the ioctl fails (for example when `fd` is not on a
/// btrfs filesystem), if the subvolume ID does not exist, or if a root ref
/// item is truncated.
///
/// # Example
///
/// ```ignore
/// let path = subvolid_resolve(fd, 5)?;
/// println!("Subvolume 5 is at: {}", path);
/// ```
pub fn subvolid_resolve(
    fd: BorrowedFd<'_>,
    subvol_id: u64,
) -> nix::Result<String> {
    let mut path = String::new();
    subvolid_resolve_sub(fd, &mut path, subvol_id)?;
    Ok(path)
}

fn subvolid_resolve_sub(
    fd: BorrowedFd<'_>,
    path: &mut String,
    subvol_id: u64,
) -> nix::Result<()> {
    use crate::raw::BTRFS_FS_TREE_OBJECTID;

    // The filesystem root has an empty path.
    if subvol_id == u64::from(BTRFS_FS_TREE_OBJECTID) {
        return Ok(());
    }

    // Search the root tree for this subvolume's ROOT_BACKREF_KEY items
    // (type 156). The objectid is the subvolume ID, the offset is the parent
    // subvolume ID, and the data is a btrfs_root_ref with the subvolume name.
    let mut found = false;

    tree_search(
        fd,
        SearchFilter::for_objectid_range(
            u64::from(crate::raw::BTRFS_ROOT_TREE_OBJECTID),
            crate::raw::BTRFS_ROOT_BACKREF_KEY,
            subvol_id,
            subvol_id,
        ),
        |hdr, data| {
            use std::mem::{offset_of, size_of};

            found = true;

            let parent_subvol_id = hdr.offset;

            // Resolve the parent path first.
            subvolid_resolve_sub(fd, path, parent_subvol_id)?;

            // data is a packed btrfs_root_ref followed by name bytes.

            let header_size = size_of::<btrfs_root_ref>();
            if data.len() < header_size {
                return Err(nix::errno::Errno::EOVERFLOW);
            }

            let dirid = u64::from_le_bytes(
                data[offset_of!(btrfs_root_ref, dirid)..][..8]
                    .try_into()
                    .unwrap(),
            );

            let name_off = offset_of!(btrfs_root_ref, name_len);
            let name_len =
                u16::from_le_bytes([data[name_off], data[name_off + 1]])
                    as usize;

            if data.len() < header_size + name_len {
                return Err(nix::errno::Errno::EOVERFLOW);
            }

            let name_bytes = &data[header_size..header_size + name_len];

            // A subvolume that is not directly in its parent's root directory
            // also needs the path of the directory that contains it.
            if dirid != u64::from(BTRFS_FIRST_FREE_OBJECTID) {
                let mut ino_lookup_args =
                    crate::raw::btrfs_ioctl_ino_lookup_args {
                        treeid: parent_subvol_id,
                        objectid: dirid,
                        ..unsafe { std::mem::zeroed() }
                    };

                unsafe {
                    btrfs_ioc_ino_lookup(
                        fd.as_raw_fd(),
                        &raw mut ino_lookup_args,
                    )?;
                }

                // INO_LOOKUP returns the directory's path within the parent
                // subvolume as a null-terminated string.
                let dir_name = unsafe {
                    std::ffi::CStr::from_ptr(ino_lookup_args.name.as_ptr())
                }
                .to_str()
                .map_err(|_| nix::errno::Errno::EINVAL)?;

                if !dir_name.is_empty() {
                    if !path.is_empty() {
                        path.push('/');
                    }
                    path.push_str(dir_name);
                }
            }

            if !path.is_empty() {
                path.push('/');
            }

            let name_str = std::str::from_utf8(name_bytes)
                .map_err(|_| nix::errno::Errno::EINVAL)?;
            path.push_str(name_str);

            Ok(())
        },
    )?;

    if !found {
        return Err(nix::errno::Errno::ENOENT);
    }

    Ok(())
}

/// Result of an unprivileged inode lookup (`BTRFS_IOC_INO_LOOKUP_USER`).
///
/// Contains the subvolume name and the path from the fd's subvolume root
/// to the directory containing the subvolume.
#[derive(Debug, Clone)]
pub struct InoLookupUserResult {
    /// Name of the subvolume.
    pub name: String,
    /// Path from the fd's subvolume root to the directory containing the
    /// subvolume entry (`dirid`). Empty if the subvolume sits directly
    /// under the subvolume root.
    pub path: String,
}

/// Unprivileged inode lookup: resolve a subvolume's name and parent path.
///
/// Given a subvolume ID (`treeid`) and the inode of the directory that
/// contains it (`dirid`), returns the subvolume name and the path from
/// the fd's subvolume root to that directory.
///
/// Unlike [`subvolid_resolve`], this does not require `CAP_SYS_ADMIN`.
/// However, it only resolves one level. For nested subvolumes the caller must
/// walk up the tree.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn ino_lookup_user(
    fd: BorrowedFd<'_>,
    treeid: u64,
    dirid: u64,
) -> nix::Result<InoLookupUserResult> {
    let mut args = btrfs_ioctl_ino_lookup_user_args {
        dirid,
        treeid,
        ..unsafe { std::mem::zeroed() }
    };

    unsafe {
        btrfs_ioc_ino_lookup_user(fd.as_raw_fd(), &raw mut args)?;
    }

    let name = unsafe { std::ffi::CStr::from_ptr(args.name.as_ptr()) }
        .to_str()
        .map_err(|_| nix::errno::Errno::EINVAL)?
        .to_owned();

    let path = unsafe { std::ffi::CStr::from_ptr(args.path.as_ptr()) }
        .to_str()
        .map_err(|_| nix::errno::Errno::EINVAL)?
        .to_owned();

    Ok(InoLookupUserResult { name, path })
}

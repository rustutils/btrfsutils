//! # Subvolume and snapshot management: creating, deleting, and querying subvolumes
//!
//! Subvolumes are independently snapshotable subtrees within a btrfs filesystem.
//! Snapshots are subvolumes created as copy-on-write clones of an existing
//! subvolume.  This module covers the full lifecycle: creating and deleting
//! subvolumes and snapshots, reading subvolume metadata and flags, listing all
//! subvolumes in a filesystem, and getting or setting the default subvolume
//! that is mounted when no subvolume is explicitly requested.

use crate::{
    raw::{
        BTRFS_DIR_ITEM_KEY, BTRFS_FIRST_FREE_OBJECTID, BTRFS_FS_TREE_OBJECTID,
        BTRFS_LAST_FREE_OBJECTID, BTRFS_ROOT_BACKREF_KEY, BTRFS_ROOT_ITEM_KEY,
        BTRFS_ROOT_TREE_DIR_OBJECTID, BTRFS_ROOT_TREE_OBJECTID,
        BTRFS_SUBVOL_QGROUP_INHERIT, BTRFS_SUBVOL_RDONLY,
        BTRFS_SUBVOL_SPEC_BY_ID, BTRFS_SUBVOL_SYNC_WAIT_FOR_ONE,
        BTRFS_SUBVOL_SYNC_WAIT_FOR_QUEUED, btrfs_ioc_default_subvol,
        btrfs_ioc_get_subvol_info, btrfs_ioc_ino_lookup,
        btrfs_ioc_snap_create_v2, btrfs_ioc_snap_destroy_v2,
        btrfs_ioc_subvol_create_v2, btrfs_ioc_subvol_getflags,
        btrfs_ioc_subvol_setflags, btrfs_ioc_subvol_sync_wait,
        btrfs_ioctl_get_subvol_info_args, btrfs_ioctl_ino_lookup_args,
        btrfs_ioctl_subvol_wait, btrfs_ioctl_vol_args_v2, btrfs_qgroup_inherit,
    },
    tree_search::{SearchKey, tree_search},
};
use bitflags::bitflags;
use nix::libc::c_char;
use std::{
    collections::HashMap,
    ffi::CStr,
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

/// The top-level subvolume (FS tree); objectid 5, always present.
///
/// Returned by [`subvolume_default_get`] when no explicit default has been set.
pub const FS_TREE_OBJECTID: u64 = BTRFS_FS_TREE_OBJECTID as u64;

bitflags! {
    /// Flags on a btrfs subvolume (the `flags` field of the root item /
    /// `BTRFS_IOC_SUBVOL_{GET,SET}FLAGS`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SubvolumeFlags: u64 {
        /// Subvolume is read-only.
        const RDONLY = BTRFS_SUBVOL_RDONLY as u64;
    }
}

impl std::fmt::Display for SubvolumeFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.contains(SubvolumeFlags::RDONLY) {
            write!(f, "readonly")
        } else {
            write!(f, "-")
        }
    }
}

/// Subvolume metadata returned by `BTRFS_IOC_GET_SUBVOL_INFO`.
#[derive(Debug, Clone)]
pub struct SubvolumeInfo {
    /// Root ID (subvolume ID) of this subvolume.
    pub id: u64,
    /// Name of this subvolume within its parent directory.
    pub name: String,
    /// Root ID of the parent subvolume.
    pub parent_id: u64,
    /// Inode number of the directory within the parent that holds this subvolume.
    pub dir_id: u64,
    /// Current generation of the subvolume.
    pub generation: u64,
    /// Subvolume flags (e.g. read-only).
    pub flags: SubvolumeFlags,
    /// UUID of this subvolume.
    pub uuid: Uuid,
    /// UUID of the parent subvolume (non-nil for snapshots).
    pub parent_uuid: Uuid,
    /// UUID of the received subvolume (non-nil for received snapshots).
    pub received_uuid: Uuid,
    /// Transaction ID when the subvolume was last changed.
    pub ctransid: u64,
    /// Transaction ID when the subvolume was created.
    pub otransid: u64,
    /// Transaction ID when a send was last performed.
    pub stransid: u64,
    /// Transaction ID when this subvolume was last received.
    pub rtransid: u64,
    /// Time of last change.
    pub ctime: SystemTime,
    /// Creation time.
    pub otime: SystemTime,
    /// Time of last send.
    pub stime: SystemTime,
    /// Time of last receive.
    pub rtime: SystemTime,
}

/// A single subvolume entry returned by [`subvolume_list`].
#[derive(Debug, Clone)]
pub struct SubvolumeListItem {
    /// Root ID (subvolume ID).
    pub root_id: u64,
    /// Root ID of the parent subvolume (`0` if unknown / not found).
    pub parent_id: u64,
    /// Inode of the directory in the parent that contains this subvolume.
    pub dir_id: u64,
    /// Current generation.
    pub generation: u64,
    /// Subvolume flags.
    pub flags: SubvolumeFlags,
    /// UUID of this subvolume.
    pub uuid: Uuid,
    /// UUID of the parent subvolume.
    pub parent_uuid: Uuid,
    /// UUID of the received subvolume.
    pub received_uuid: Uuid,
    /// Transaction ID when created.
    pub otransid: u64,
    /// Creation time.
    pub otime: SystemTime,
    /// Subvolume name (the leaf name within its parent).
    ///
    /// Full path resolution relative to the filesystem root requires
    /// `BTRFS_IOC_INO_LOOKUP` and is not yet implemented; this field contains
    /// only the leaf name as stored in the root backref item.
    pub name: String,
}

/// Write `name` into the `name` union member of a zeroed
/// `btrfs_ioctl_vol_args_v2`, returning `ENAMETOOLONG` if it does not fit.
#[allow(clippy::cast_possible_wrap)] // ASCII bytes always fit in c_char
fn set_v2_name(
    args: &mut btrfs_ioctl_vol_args_v2,
    name: &CStr,
) -> nix::Result<()> {
    let bytes = name.to_bytes(); // excludes nul terminator
    // SAFETY: name is the active union member; the struct is already zeroed so
    // the implicit nul terminator is already present.
    let name_buf: &mut [c_char] = unsafe { &mut args.__bindgen_anon_2.name };
    if bytes.len() >= name_buf.len() {
        return Err(nix::errno::Errno::ENAMETOOLONG);
    }
    for (i, &b) in bytes.iter().enumerate() {
        name_buf[i] = b as c_char;
    }
    Ok(())
}

/// Build a `btrfs_qgroup_inherit` buffer for the given qgroup IDs.
///
/// The returned `Vec<u64>` is sized to hold the base struct plus the trailing
/// `qgroups[]` array, with 8-byte alignment guaranteed by the `u64` element
/// type.
fn build_qgroup_inherit(qgroups: &[u64]) -> Vec<u64> {
    let base_size = mem::size_of::<btrfs_qgroup_inherit>();
    let total_size = base_size + std::mem::size_of_val(qgroups);
    let num_u64 = total_size.div_ceil(8);
    let mut buf = vec![0u64; num_u64];

    // SAFETY: buf is large enough and zeroed; we write through a properly
    // aligned pointer (btrfs_qgroup_inherit has 8-byte alignment).
    let inherit =
        unsafe { &mut *buf.as_mut_ptr().cast::<btrfs_qgroup_inherit>() };
    inherit.num_qgroups = qgroups.len() as u64;

    // Write the qgroup IDs into the flexible array member.
    if !qgroups.is_empty() {
        let array = unsafe { inherit.qgroups.as_mut_slice(qgroups.len()) };
        array.copy_from_slice(qgroups);
    }

    buf
}

/// Set the `BTRFS_SUBVOL_QGROUP_INHERIT` fields on a `vol_args_v2` struct.
///
/// `buf` must be the buffer returned by `build_qgroup_inherit`.
fn set_qgroup_inherit(
    args: &mut btrfs_ioctl_vol_args_v2,
    buf: &[u64],
    num_qgroups: usize,
) {
    args.flags |= u64::from(BTRFS_SUBVOL_QGROUP_INHERIT);
    let base_size = mem::size_of::<btrfs_qgroup_inherit>();
    let total_size = base_size + num_qgroups * mem::size_of::<u64>();
    args.__bindgen_anon_1.__bindgen_anon_1.size = total_size as u64;
    args.__bindgen_anon_1.__bindgen_anon_1.qgroup_inherit =
        buf.as_ptr() as *mut btrfs_qgroup_inherit;
}

/// Create a new subvolume named `name` inside the directory referred to by
/// `parent_fd`.
///
/// `name` must be a plain leaf name (no slashes).  The caller is responsible
/// for opening the correct parent directory.  If `qgroups` is non-empty, the
/// new subvolume is added to those qgroups.  Requires `CAP_SYS_ADMIN`.
///
/// Errors: ENAMETOOLONG if `name` does not fit in the 4040-byte kernel
/// buffer.  EEXIST if a subvolume or directory with that name already exists.
/// `EPERM` without `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_create(
    parent_fd: BorrowedFd,
    name: &CStr,
    qgroups: &[u64],
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    set_v2_name(&mut args, name)?;

    let inherit_buf;
    if !qgroups.is_empty() {
        inherit_buf = build_qgroup_inherit(qgroups);
        set_qgroup_inherit(&mut args, &inherit_buf, qgroups.len());
    }

    unsafe {
        btrfs_ioc_subvol_create_v2(parent_fd.as_raw_fd(), &raw const args)
    }?;
    Ok(())
}

/// Delete the subvolume or snapshot named `name` from the directory referred
/// to by `parent_fd`.
///
/// `name` must be a plain leaf name (no slashes).  Requires `CAP_SYS_ADMIN`.
///
/// Deletion is asynchronous: the ioctl removes the directory entry
/// immediately, but the kernel cleaner thread reclaims the on-disk data
/// in the background. Until the next transaction commit the deletion is
/// not visible to other operations (e.g. `subvolume_list` still shows
/// the subvolume). Call `sync` to force a commit, or pass
/// `-c`/`--commit-after` at the CLI level. To wait for the cleaner to
/// finish, use [`subvol_sync_wait_one`].
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_delete(parent_fd: BorrowedFd, name: &CStr) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    set_v2_name(&mut args, name)?;
    unsafe {
        btrfs_ioc_snap_destroy_v2(parent_fd.as_raw_fd(), &raw const args)
    }?;
    Ok(())
}

/// Delete a subvolume by its numeric subvolume ID.
///
/// `fd` must be an open file descriptor on the filesystem (typically the mount
/// point).  Unlike `subvolume_delete`, this does not require knowing the
/// subvolume's path.  Requires `CAP_SYS_ADMIN`.
///
/// See [`subvolume_delete`] for details on commit visibility and async
/// cleanup.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_delete_by_id(
    fd: BorrowedFd,
    subvolid: u64,
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    args.flags = u64::from(BTRFS_SUBVOL_SPEC_BY_ID);
    args.__bindgen_anon_2.subvolid = subvolid;
    unsafe { btrfs_ioc_snap_destroy_v2(fd.as_raw_fd(), &raw const args) }?;
    Ok(())
}

/// Create a snapshot of the subvolume referred to by `source_fd`, placing it
/// as `name` inside the directory referred to by `parent_fd`.
///
/// If `readonly` is `true` the new snapshot is created read-only.  If
/// `qgroups` is non-empty, the new snapshot is added to those qgroups.
/// Requires `CAP_SYS_ADMIN`.
///
/// Errors: ENAMETOOLONG if `name` does not fit in the 4040-byte kernel
/// buffer.  EEXIST if a subvolume or directory with that name already exists.
/// EROFS if `parent_fd` refers to a read-only subvolume.  EPERM without
/// `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn snapshot_create(
    parent_fd: BorrowedFd,
    source_fd: BorrowedFd,
    name: &CStr,
    readonly: bool,
    qgroups: &[u64],
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    // The `fd` field carries the source subvolume file descriptor.
    args.fd = i64::from(source_fd.as_raw_fd());
    if readonly {
        args.flags = u64::from(BTRFS_SUBVOL_RDONLY);
    }
    set_v2_name(&mut args, name)?;

    let inherit_buf;
    if !qgroups.is_empty() {
        inherit_buf = build_qgroup_inherit(qgroups);
        set_qgroup_inherit(&mut args, &inherit_buf, qgroups.len());
    }

    unsafe {
        btrfs_ioc_snap_create_v2(parent_fd.as_raw_fd(), &raw const args)
    }?;
    Ok(())
}

/// Query detailed information about the subvolume that `fd` belongs to.
///
/// `fd` can be any file or directory within the target subvolume.
/// Does not require elevated privileges.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_info(fd: BorrowedFd) -> nix::Result<SubvolumeInfo> {
    subvolume_info_by_id(fd, 0)
}

/// Query detailed information about a subvolume by its numeric root ID.
///
/// `fd` can be any open file descriptor on the filesystem.  If `rootid` is 0,
/// the subvolume that `fd` belongs to is queried (equivalent to
/// `subvolume_info`).  Does not require elevated privileges.
///
/// Errors: ENOENT if no subvolume with that `rootid` exists (or has been
/// deleted but not yet cleaned).
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_info_by_id(
    fd: BorrowedFd,
    rootid: u64,
) -> nix::Result<SubvolumeInfo> {
    let mut raw: btrfs_ioctl_get_subvol_info_args = unsafe { mem::zeroed() };
    raw.treeid = rootid;
    unsafe { btrfs_ioc_get_subvol_info(fd.as_raw_fd(), &raw mut raw) }?;

    let name = unsafe { CStr::from_ptr(raw.name.as_ptr()) }
        .to_string_lossy()
        .into_owned();

    Ok(SubvolumeInfo {
        id: raw.treeid,
        name,
        parent_id: raw.parent_id,
        dir_id: raw.dirid,
        generation: raw.generation,
        flags: SubvolumeFlags::from_bits_truncate(raw.flags),
        uuid: Uuid::from_bytes(raw.uuid),
        parent_uuid: Uuid::from_bytes(raw.parent_uuid),
        received_uuid: Uuid::from_bytes(raw.received_uuid),
        ctransid: raw.ctransid,
        otransid: raw.otransid,
        stransid: raw.stransid,
        rtransid: raw.rtransid,
        ctime: timespec_to_system_time(raw.ctime.sec, raw.ctime.nsec),
        otime: timespec_to_system_time(raw.otime.sec, raw.otime.nsec),
        stime: timespec_to_system_time(raw.stime.sec, raw.stime.nsec),
        rtime: timespec_to_system_time(raw.rtime.sec, raw.rtime.nsec),
    })
}

/// Read the flags of the subvolume that `fd` belongs to.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_flags_get(fd: BorrowedFd) -> nix::Result<SubvolumeFlags> {
    let mut flags: u64 = 0;
    unsafe { btrfs_ioc_subvol_getflags(fd.as_raw_fd(), &raw mut flags) }?;
    Ok(SubvolumeFlags::from_bits_truncate(flags))
}

/// Set the flags of the subvolume that `fd` belongs to.
///
/// Requires `CAP_SYS_ADMIN` to make a subvolume read-only; any user may
/// clear the read-only flag from a subvolume they own.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_flags_set(
    fd: BorrowedFd,
    flags: SubvolumeFlags,
) -> nix::Result<()> {
    let raw: u64 = flags.bits();
    unsafe { btrfs_ioc_subvol_setflags(fd.as_raw_fd(), &raw const raw) }?;
    Ok(())
}

/// Query the ID of the default subvolume of the filesystem referred to by
/// `fd`.
///
/// Searches the root tree for the `BTRFS_DIR_ITEM_KEY` entry at objectid
/// `BTRFS_ROOT_TREE_DIR_OBJECTID` that stores the default subvolume ID.
/// Returns [`FS_TREE_OBJECTID`] if no default has been set.
///
/// Requires `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the tree search ioctl fails.
///
/// # Panics
///
/// Panics if the dir item data is malformed (slice conversion fails). This
/// cannot happen with valid btrfs on-disk data.
pub fn subvolume_default_get(fd: BorrowedFd) -> nix::Result<u64> {
    let mut default_id: Option<u64> = None;

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            u64::from(BTRFS_ROOT_TREE_OBJECTID),
            BTRFS_DIR_ITEM_KEY,
            u64::from(BTRFS_ROOT_TREE_DIR_OBJECTID),
            u64::from(BTRFS_ROOT_TREE_DIR_OBJECTID),
        ),
        |_hdr, data| {
            use crate::raw::btrfs_dir_item;
            use std::mem::{offset_of, size_of};

            let header_size = size_of::<btrfs_dir_item>();
            if data.len() < header_size {
                return Ok(());
            }
            let name_off = offset_of!(btrfs_dir_item, name_len);
            let name_len =
                u16::from_le_bytes([data[name_off], data[name_off + 1]])
                    as usize;
            if data.len() < header_size + name_len {
                return Ok(());
            }
            let item_name = &data[header_size..header_size + name_len];
            if item_name == b"default" {
                let loc_off = offset_of!(btrfs_dir_item, location);
                let target_id = u64::from_le_bytes(
                    data[loc_off..loc_off + 8].try_into().unwrap(),
                );
                default_id = Some(target_id);
            }
            Ok(())
        },
    )?;

    Ok(default_id.unwrap_or(u64::from(BTRFS_FS_TREE_OBJECTID)))
}

/// Set the default subvolume of the filesystem referred to by `fd` to
/// `subvolid`.
///
/// Pass [`FS_TREE_OBJECTID`] to restore the default.  Requires `CAP_SYS_ADMIN`.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvolume_default_set(fd: BorrowedFd, subvolid: u64) -> nix::Result<()> {
    unsafe { btrfs_ioc_default_subvol(fd.as_raw_fd(), &raw const subvolid) }?;
    Ok(())
}

/// List all user subvolumes and snapshots in the filesystem referred to by
/// `fd` by walking the root tree.
///
/// Two tree-search passes are made:
/// 1. `ROOT_ITEM_KEY` — reads each subvolume's metadata (generation, flags,
///    UUIDs, creation time).
/// 2. `BTRFS_ROOT_BACKREF_KEY` — reads each subvolume's parent ID and leaf name.
///
/// Subvolumes for which no backref is found are still included; their
/// `parent_id`, `dir_id`, and `name` will be zeroed / empty.
///
/// Requires `CAP_SYS_ADMIN` for the tree search. Without it the kernel
/// returns `EPERM`; the caller should degrade gracefully (e.g. show only the
/// leaf name without full path resolution).
///
/// # Errors
///
/// Returns `Err` if the tree search ioctl fails.
#[allow(clippy::cast_sign_loss)] // BTRFS_LAST_FREE_OBJECTID: i32 → u64 intentional
pub fn subvolume_list(fd: BorrowedFd) -> nix::Result<Vec<SubvolumeListItem>> {
    let mut items: Vec<SubvolumeListItem> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            u64::from(BTRFS_ROOT_TREE_OBJECTID),
            BTRFS_ROOT_ITEM_KEY,
            u64::from(BTRFS_FIRST_FREE_OBJECTID),
            BTRFS_LAST_FREE_OBJECTID as u64,
        ),
        |hdr, data| {
            if let Some(item) = parse_root_item(hdr.objectid, data) {
                items.push(item);
            }
            Ok(())
        },
    )?;

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            u64::from(BTRFS_ROOT_TREE_OBJECTID),
            BTRFS_ROOT_BACKREF_KEY,
            u64::from(BTRFS_FIRST_FREE_OBJECTID),
            BTRFS_LAST_FREE_OBJECTID as u64,
        ),
        |hdr, data| {
            // ROOT_BACKREF_KEY: objectid = subvol root_id, offset = parent root_id
            let root_id = hdr.objectid;
            let parent_id = hdr.offset;

            if let Some(item) = items.iter_mut().find(|i| i.root_id == root_id)
            {
                // Only take the first ROOT_BACKREF for each subvolume.  A
                // subvolume can have multiple ROOT_BACKREF entries when it is
                // referenced from more than one parent (e.g. the subvolume
                // also appears as a snapshot inside another subvolume).
                // Items are returned in offset-ascending order, so the first
                // entry has the smallest parent_id — which is the canonical
                // location btrfs-progs uses for "top level" and path.
                if item.parent_id == 0 {
                    item.parent_id = parent_id;
                    if let Some((dir_id, name)) = parse_root_ref(data) {
                        item.dir_id = dir_id;
                        item.name = name;
                    }
                }
            }
            Ok(())
        },
    )?;

    // Determine which subvolume the fd is open on.  Paths are expressed
    // relative to this subvolume, matching btrfs-progs behaviour.
    let top_id =
        crate::inode::lookup_path_rootid(fd).unwrap_or(FS_TREE_OBJECTID);

    resolve_full_paths(fd, &mut items, top_id);

    Ok(items)
}

/// Call `BTRFS_IOC_INO_LOOKUP` for `dir_id` within `parent_tree` and return
/// the path from that tree's root to the directory.
///
/// The kernel returns the path with a trailing `/` when the directory is not
/// the tree root, and an empty string when `dir_id` is the tree root itself.
/// This prefix can be concatenated directly with the subvolume's leaf name to
/// form the full segment within the parent.
fn ino_lookup_dir_path(
    fd: BorrowedFd,
    parent_tree: u64,
    dir_id: u64,
) -> nix::Result<String> {
    let mut args = btrfs_ioctl_ino_lookup_args {
        treeid: parent_tree,
        objectid: dir_id,
        ..unsafe { mem::zeroed() }
    };
    // SAFETY: args is a valid, zeroed btrfs_ioctl_ino_lookup_args; the ioctl
    // fills in args.name with a null-terminated path string.
    unsafe { btrfs_ioc_ino_lookup(fd.as_raw_fd(), &raw mut args) }?;

    // args.name is [c_char; 4080]; find the null terminator and decode.
    let name_ptr: *const c_char = args.name.as_ptr();
    // SAFETY: the ioctl guarantees null termination within the 4080-byte buffer.
    let cstr = unsafe { CStr::from_ptr(name_ptr) };
    Ok(cstr.to_string_lossy().into_owned())
}

/// Resolve the `name` field of every item in `items` from a bare leaf name to
/// the full path relative to the filesystem root.
///
/// For each subvolume we already have `parent_id`, `dir_id`, and the leaf name
/// from the `ROOT_BACKREF` pass.  A single `BTRFS_IOC_INO_LOOKUP` call per item
/// gives the path from the parent tree's root down to the directory that
/// contains the subvolume (the "dir prefix").  Concatenating that prefix with
/// the leaf name yields the subvolume's segment within its parent.  Walking up
/// the parent chain (using the in-memory items map) and joining segments with
/// `/` gives the final full path.
fn resolve_full_paths(
    fd: BorrowedFd,
    items: &mut [SubvolumeListItem],
    top_id: u64,
) {
    // Map root_id → index for O(1) parent lookups.
    let id_to_idx: HashMap<u64, usize> = items
        .iter()
        .enumerate()
        .map(|(i, item)| (item.root_id, i))
        .collect();

    // Compute the "segment" for each item: the path of this subvolume within
    // its immediate parent (dir prefix from INO_LOOKUP + leaf name).
    // If INO_LOOKUP is not available (e.g. missing CAP_SYS_ADMIN), fall back
    // to the bare leaf name so the list still works.
    let segments: Vec<String> = items
        .iter()
        .map(|item| {
            if item.parent_id == 0 || item.name.is_empty() {
                return item.name.clone();
            }
            match ino_lookup_dir_path(fd, item.parent_id, item.dir_id) {
                Ok(prefix) => format!("{}{}", prefix, item.name),
                Err(_) => item.name.clone(),
            }
        })
        .collect();

    // Walk each item's parent chain, joining segments, and cache results so
    // every ancestor is resolved at most once.
    let mut full_paths: HashMap<u64, String> = HashMap::new();
    let root_ids: Vec<u64> = items.iter().map(|i| i.root_id).collect();
    for root_id in root_ids {
        build_full_path(
            root_id,
            top_id,
            &id_to_idx,
            &segments,
            items,
            &mut full_paths,
        );
    }

    for item in items.iter_mut() {
        if let Some(path) = full_paths.remove(&item.root_id) {
            item.name = path;
        }
    }
}

/// Compute the full path for `root_id`, memoizing into `cache`.
///
/// Walks the ancestor chain iteratively to avoid stack overflow on deep
/// subvolume trees.  Collects segments from the target up to the FS tree
/// root, then joins them in reverse order.
///
/// Cycle detection is included: `ROOT_BACKREF` entries can form mutual parent
/// relationships (e.g. a snapshot stored inside the subvolume it was taken
/// from), which would otherwise loop forever.
fn build_full_path(
    root_id: u64,
    top_id: u64,
    id_to_idx: &HashMap<u64, usize>,
    segments: &[String],
    items: &[SubvolumeListItem],
    cache: &mut HashMap<u64, String>,
) -> String {
    // Collect the chain of root_ids from `root_id` up to the top subvolume
    // (the one the fd is open on) or the FS tree root, stopping early if we
    // hit an already-cached ancestor or a cycle.
    let mut chain: Vec<u64> = Vec::new();
    let mut visited: HashMap<u64, usize> = HashMap::new();
    let mut cur = root_id;
    loop {
        if cache.contains_key(&cur) {
            break;
        }
        if visited.contains_key(&cur) {
            // Cycle detected: truncate the chain back to where the cycle
            // starts so we don't join nonsensical repeated segments.
            let cycle_start = visited[&cur];
            chain.truncate(cycle_start);
            break;
        }
        let Some(&idx) = id_to_idx.get(&cur) else {
            break;
        };
        visited.insert(cur, chain.len());
        chain.push(cur);
        let parent = items[idx].parent_id;
        if parent == 0
            || parent == FS_TREE_OBJECTID
            || parent == top_id
            || !id_to_idx.contains_key(&parent)
        {
            break;
        }
        cur = parent;
    }

    // Resolve each node in the chain from root toward the target, building
    // on any already-cached prefix we stopped at.
    for &id in chain.iter().rev() {
        let Some(&idx) = id_to_idx.get(&id) else {
            cache.insert(id, String::new());
            continue;
        };
        let segment = &segments[idx];
        let parent_id = items[idx].parent_id;

        let full_path = if parent_id == 0
            || parent_id == FS_TREE_OBJECTID
            || parent_id == top_id
            || !id_to_idx.contains_key(&parent_id)
        {
            segment.clone()
        } else if let Some(parent_path) = cache.get(&parent_id) {
            if parent_path.is_empty() {
                segment.clone()
            } else {
                format!("{parent_path}/{segment}")
            }
        } else {
            segment.clone()
        };

        cache.insert(id, full_path);
    }

    cache.get(&root_id).cloned().unwrap_or_default()
}

/// Parse a `ROOT_ITEM` payload into a [`SubvolumeListItem`].
fn parse_root_item(root_id: u64, data: &[u8]) -> Option<SubvolumeListItem> {
    let ri = btrfs_disk::items::RootItem::parse(data)?;
    let flags = SubvolumeFlags::from_bits_truncate(ri.flags.bits());
    let otime = timespec_to_system_time(ri.otime.sec, ri.otime.nsec);

    Some(SubvolumeListItem {
        root_id,
        parent_id: 0,
        dir_id: 0,
        generation: ri.generation,
        flags,
        uuid: ri.uuid,
        parent_uuid: ri.parent_uuid,
        received_uuid: ri.received_uuid,
        otransid: ri.otransid,
        otime,
        name: String::new(),
    })
}

/// Parse a `btrfs_root_ref` payload (packed, LE). The name immediately
/// follows the fixed-size header.
fn parse_root_ref(data: &[u8]) -> Option<(u64, String)> {
    let rr = btrfs_disk::items::RootRef::parse(data)?;
    let name = String::from_utf8_lossy(&rr.name).into_owned();
    Some((rr.dirid, name))
}

/// Convert an on-disk `btrfs_timespec` (LE sec + LE nsec, packed) to
/// [`SystemTime`].  Returns [`UNIX_EPOCH`] if sec == 0.
/// Convert a (sec, nsec) timestamp to [`SystemTime`].
/// Returns [`UNIX_EPOCH`] if sec == 0.
fn timespec_to_system_time(sec: u64, nsec: u32) -> SystemTime {
    if sec == 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::new(sec, nsec)
}

/// A child subvolume reference returned by [`subvol_rootrefs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubvolRootRef {
    /// Root ID (tree ID) of the child subvolume.
    pub treeid: u64,
    /// Directory inode ID where the child is attached in the parent.
    pub dirid: u64,
}

/// List the child subvolumes directly under the subvolume opened by `fd`.
///
/// Returns all subvolumes whose root is referenced from the given
/// subvolume. The kernel returns results in batches of up to 255; this
/// function handles continuation automatically by advancing `min_treeid`.
///
/// Does not require `CAP_SYS_ADMIN`.
///
/// Errors: `ENOTTY` on kernels older than 4.18.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvol_rootrefs(fd: BorrowedFd) -> nix::Result<Vec<SubvolRootRef>> {
    use crate::raw::{
        btrfs_ioc_get_subvol_rootref, btrfs_ioctl_get_subvol_rootref_args,
    };

    let mut results = Vec::new();
    let mut min_treeid: u64 = 0;

    loop {
        let mut args: btrfs_ioctl_get_subvol_rootref_args =
            unsafe { std::mem::zeroed() };
        args.min_treeid = min_treeid;

        let ret = unsafe {
            btrfs_ioc_get_subvol_rootref(fd.as_raw_fd(), &raw mut args)
        };

        // The kernel returns EOVERFLOW when there are more results than
        // fit in one batch. We read what we got and loop with the updated
        // min_treeid.
        let overflow = match ret {
            Ok(_) => false,
            Err(nix::errno::Errno::EOVERFLOW) => true,
            Err(e) => return Err(e),
        };

        let count = args.num_items as usize;
        for i in 0..count {
            let r = &args.rootref[i];
            results.push(SubvolRootRef {
                treeid: r.treeid,
                dirid: r.dirid,
            });
        }

        if !overflow || count == 0 {
            break;
        }

        // Advance past the last returned treeid for the next batch.
        min_treeid = args.rootref[count - 1].treeid + 1;
    }

    Ok(results)
}

/// Wait for a specific deleted subvolume to be fully cleaned by the kernel.
///
/// Blocks until the background cleaner has finished removing the on-disk
/// data for the given subvolume ID. Returns `Ok(())` both when the wait
/// completes and when the subvolume is already gone (`ENOENT`).
/// Useful after `subvolume_delete` when subsequent operations depend on
/// the subvolume being fully gone (e.g. qgroup staleness checks).
///
/// # Errors
///
/// Returns `Err` if the ioctl fails (other than `ENOENT`).
pub fn subvol_sync_wait_one(fd: BorrowedFd, subvolid: u64) -> nix::Result<()> {
    let args = btrfs_ioctl_subvol_wait {
        subvolid,
        mode: BTRFS_SUBVOL_SYNC_WAIT_FOR_ONE,
        count: 0,
    };
    match unsafe { btrfs_ioc_subvol_sync_wait(fd.as_raw_fd(), &raw const args) }
    {
        Ok(_) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Wait for all currently queued subvolume deletions to complete.
///
/// Blocks until every subvolume that was in the deletion queue at the time
/// of the call has been fully cleaned. Does not wait for subvolumes
/// deleted after the call is made.
///
/// # Errors
///
/// Returns `Err` if the ioctl fails.
pub fn subvol_sync_wait_all(fd: BorrowedFd) -> nix::Result<()> {
    let args = btrfs_ioctl_subvol_wait {
        subvolid: 0,
        mode: BTRFS_SUBVOL_SYNC_WAIT_FOR_QUEUED,
        count: 0,
    };
    unsafe { btrfs_ioc_subvol_sync_wait(fd.as_raw_fd(), &raw const args) }?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        time::{Duration, UNIX_EPOCH},
    };
    use uuid::Uuid;

    fn test_item(root_id: u64, parent_id: u64) -> SubvolumeListItem {
        SubvolumeListItem {
            root_id,
            parent_id,
            dir_id: 0,
            generation: 0,
            flags: SubvolumeFlags::empty(),
            uuid: Uuid::nil(),
            parent_uuid: Uuid::nil(),
            received_uuid: Uuid::nil(),
            otransid: 0,
            otime: UNIX_EPOCH,
            name: String::new(),
        }
    }

    #[test]
    fn timespec_zero_returns_epoch() {
        assert_eq!(timespec_to_system_time(0, 0), UNIX_EPOCH);
    }

    #[test]
    fn timespec_zero_sec_with_nonzero_nsec_returns_epoch() {
        // sec==0 triggers the early return regardless of nsec
        assert_eq!(timespec_to_system_time(0, 500_000_000), UNIX_EPOCH);
    }

    #[test]
    fn timespec_nonzero_returns_correct_time() {
        let t = timespec_to_system_time(1000, 500);
        assert_eq!(t, UNIX_EPOCH + Duration::new(1000, 500));
    }

    #[test]
    fn subvolume_flags_display_readonly() {
        let flags = SubvolumeFlags::RDONLY;
        assert_eq!(format!("{}", flags), "readonly");
    }

    #[test]
    fn subvolume_flags_display_empty() {
        let flags = SubvolumeFlags::empty();
        assert_eq!(format!("{}", flags), "-");
    }

    #[test]
    fn parse_root_ref_valid() {
        // btrfs_root_ref: dirid (8 LE) + sequence (8 LE) + name_len (2 LE) + name bytes
        let name = b"mysubvol";
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_le_bytes()); // dirid
        buf.extend_from_slice(&1u64.to_le_bytes()); // sequence
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes()); // name_len
        buf.extend_from_slice(name);

        let result = parse_root_ref(&buf);
        assert!(result.is_some());
        let (dir_id, parsed_name) = result.unwrap();
        assert_eq!(dir_id, 42);
        assert_eq!(parsed_name, "mysubvol");
    }

    #[test]
    fn parse_root_ref_too_short_header() {
        // Less than 18 bytes (sizeof btrfs_root_ref)
        let buf = [0u8; 10];
        assert!(parse_root_ref(&buf).is_none());
    }

    #[test]
    fn parse_root_ref_truncated_name() {
        // Header claims 10-byte name but buffer only has the header.
        // The parser succeeds but returns an empty name (graceful truncation).
        let mut buf = vec![0u8; 18];
        buf[16] = 10; // name_len = 10
        buf[17] = 0;
        let result = parse_root_ref(&buf);
        assert!(result.is_some());
        let (_, name) = result.unwrap();
        assert!(name.is_empty());
    }

    #[test]
    fn parse_root_ref_empty_name() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u64.to_le_bytes()); // dirid
        buf.extend_from_slice(&0u64.to_le_bytes()); // sequence
        buf.extend_from_slice(&0u16.to_le_bytes()); // name_len = 0

        let result = parse_root_ref(&buf);
        assert!(result.is_some());
        let (dir_id, parsed_name) = result.unwrap();
        assert_eq!(dir_id, 100);
        assert_eq!(parsed_name, "");
    }

    #[test]
    fn build_full_path_single_subvol_parent_fs_tree() {
        // Subvolume 256, parent is FS_TREE (5)
        let items = vec![test_item(256, FS_TREE_OBJECTID)];
        let segments = vec!["mysub".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            256,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "mysub");
    }

    #[test]
    fn build_full_path_nested_chain() {
        // A (256) -> B (257) -> C (258), all parented under FS_TREE
        let items = vec![
            test_item(256, FS_TREE_OBJECTID),
            test_item(257, 256),
            test_item(258, 257),
        ];
        let segments = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let id_to_idx: HashMap<u64, usize> =
            [(256, 0), (257, 1), (258, 2)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            258,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "A/B/C");
    }

    #[test]
    fn build_full_path_stops_at_top_id() {
        // A (256) -> B (257) -> C (258), top_id = 257 (B)
        // Paths are relative to top_id, so C's parent (257) == top_id means
        // C's path is just its own segment, not "A/B/C".
        let items = vec![
            test_item(256, FS_TREE_OBJECTID),
            test_item(257, 256),
            test_item(258, 257),
        ];
        let segments = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let id_to_idx: HashMap<u64, usize> =
            [(256, 0), (257, 1), (258, 2)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            258, 257, &id_to_idx, &segments, &items, &mut cache,
        );
        assert_eq!(path, "C");

        // B's path is also just "B" (its parent 256/A is below top_id in the
        // tree, but B's own parent is not top_id — A's parent is FS_TREE).
        // With top_id=257, building B: parent=256, 256 is in id_to_idx but
        // 256's parent is FS_TREE (5) which triggers the stop, so chain = [257, 256],
        // and A gets its segment, B gets "A/B".
        let path_b = build_full_path(
            257, 257, &id_to_idx, &segments, &items, &mut cache,
        );
        // 257 itself: its parent is 256, 256 != top_id (257), so we walk up.
        // 256's parent is FS_TREE (5), which triggers stop. chain = [257, 256].
        // 256 resolves to "A" (parent is FS_TREE), 257 resolves to "A/B".
        assert_eq!(path_b, "A/B");
    }

    #[test]
    fn build_full_path_cycle_detection() {
        // A (256) parent is B (257), B (257) parent is A (256) — mutual cycle
        let items = vec![test_item(256, 257), test_item(257, 256)];
        let segments = vec!["A".to_string(), "B".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0), (257, 1)].into();
        let mut cache = HashMap::new();

        // Must not hang. The result is truncated due to cycle detection.
        let _path = build_full_path(
            256,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        // Just verify it terminates and returns something (exact value depends
        // on cycle truncation heuristic).
    }

    #[test]
    fn build_full_path_cached_ancestor() {
        // A (256) -> B (257) -> C (258)
        // Pre-cache B's path; building C should use it.
        let items = vec![
            test_item(256, FS_TREE_OBJECTID),
            test_item(257, 256),
            test_item(258, 257),
        ];
        let segments = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let id_to_idx: HashMap<u64, usize> =
            [(256, 0), (257, 1), (258, 2)].into();
        let mut cache = HashMap::new();
        cache.insert(257, "A/B".to_string());

        let path = build_full_path(
            258,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "A/B/C");
    }

    #[test]
    fn build_full_path_unknown_parent() {
        // Subvolume 256, parent 999 not in id_to_idx
        let items = vec![test_item(256, 999)];
        let segments = vec!["orphan".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            256,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "orphan");
    }

    #[test]
    fn build_full_path_parent_id_zero() {
        // Subvolume 256, parent_id == 0 (no backref found)
        let items = vec![test_item(256, 0)];
        let segments = vec!["noparent".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            256,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "noparent");
    }

    #[test]
    fn build_full_path_already_cached_target() {
        // If the target itself is already cached, return the cached value.
        let items = vec![test_item(256, FS_TREE_OBJECTID)];
        let segments = vec!["A".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0)].into();
        let mut cache = HashMap::new();
        cache.insert(256, "cached/path".to_string());

        let path = build_full_path(
            256,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "cached/path");
    }

    #[test]
    fn build_full_path_root_id_not_in_items() {
        // root_id not present in id_to_idx at all
        let items = vec![test_item(256, FS_TREE_OBJECTID)];
        let segments = vec!["A".to_string()];
        let id_to_idx: HashMap<u64, usize> = [(256, 0)].into();
        let mut cache = HashMap::new();

        let path = build_full_path(
            999,
            FS_TREE_OBJECTID,
            &id_to_idx,
            &segments,
            &items,
            &mut cache,
        );
        assert_eq!(path, "");
    }
}

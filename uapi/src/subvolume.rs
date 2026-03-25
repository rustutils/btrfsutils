//! # Subvolume and snapshot management: creating, deleting, and querying subvolumes
//!
//! Subvolumes are independently snapshotable subtrees within a btrfs filesystem.
//! Snapshots are subvolumes created as copy-on-write clones of an existing
//! subvolume.  This module covers the full lifecycle: creating and deleting
//! subvolumes and snapshots, reading subvolume metadata and flags, listing all
//! subvolumes in a filesystem, and getting or setting the default subvolume
//! that is mounted when no subvolume is explicitly requested.

use crate::{
    field_size,
    raw::{
        BTRFS_DIR_ITEM_KEY, BTRFS_FIRST_FREE_OBJECTID, BTRFS_FS_TREE_OBJECTID,
        BTRFS_LAST_FREE_OBJECTID, BTRFS_ROOT_BACKREF_KEY, BTRFS_ROOT_ITEM_KEY,
        BTRFS_ROOT_TREE_DIR_OBJECTID, BTRFS_ROOT_TREE_OBJECTID, BTRFS_SUBVOL_RDONLY,
        btrfs_ioc_default_subvol, btrfs_ioc_get_subvol_info, btrfs_ioc_ino_lookup,
        btrfs_ioc_snap_create_v2, btrfs_ioc_snap_destroy_v2, btrfs_ioc_subvol_create_v2,
        btrfs_ioc_subvol_getflags, btrfs_ioc_subvol_setflags, btrfs_ioctl_get_subvol_info_args,
        btrfs_ioctl_ino_lookup_args, btrfs_ioctl_vol_args_v2, btrfs_root_item, btrfs_timespec,
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
        const RDONLY = 1 << 1;
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
fn set_v2_name(args: &mut btrfs_ioctl_vol_args_v2, name: &CStr) -> nix::Result<()> {
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

/// Create a new subvolume named `name` inside the directory referred to by
/// `parent_fd`.
///
/// `name` must be a plain leaf name (no slashes).  The caller is responsible
/// for opening the correct parent directory.  Requires `CAP_SYS_ADMIN`.
pub fn subvolume_create(parent_fd: BorrowedFd, name: &CStr) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    set_v2_name(&mut args, name)?;
    unsafe { btrfs_ioc_subvol_create_v2(parent_fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Delete the subvolume or snapshot named `name` from the directory referred
/// to by `parent_fd`.
///
/// `name` must be a plain leaf name (no slashes).  Requires `CAP_SYS_ADMIN`.
pub fn subvolume_delete(parent_fd: BorrowedFd, name: &CStr) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    set_v2_name(&mut args, name)?;
    unsafe { btrfs_ioc_snap_destroy_v2(parent_fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Create a snapshot of the subvolume referred to by `source_fd`, placing it
/// as `name` inside the directory referred to by `parent_fd`.
///
/// If `readonly` is `true` the new snapshot is created read-only.
/// Requires `CAP_SYS_ADMIN`.
pub fn snapshot_create(
    parent_fd: BorrowedFd,
    source_fd: BorrowedFd,
    name: &CStr,
    readonly: bool,
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_vol_args_v2 = unsafe { mem::zeroed() };
    // The `fd` field carries the source subvolume file descriptor.
    args.fd = source_fd.as_raw_fd() as i64;
    if readonly {
        args.flags = BTRFS_SUBVOL_RDONLY as u64;
    }
    set_v2_name(&mut args, name)?;
    unsafe { btrfs_ioc_snap_create_v2(parent_fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Query detailed information about the subvolume that `fd` belongs to.
///
/// `fd` can be any file or directory within the target subvolume.
/// Does not require elevated privileges.
pub fn subvolume_info(fd: BorrowedFd) -> nix::Result<SubvolumeInfo> {
    let mut raw: btrfs_ioctl_get_subvol_info_args = unsafe { mem::zeroed() };
    unsafe { btrfs_ioc_get_subvol_info(fd.as_raw_fd(), &mut raw) }?;

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
        ctime: ioctl_timespec_to_system_time(raw.ctime.sec, raw.ctime.nsec),
        otime: ioctl_timespec_to_system_time(raw.otime.sec, raw.otime.nsec),
        stime: ioctl_timespec_to_system_time(raw.stime.sec, raw.stime.nsec),
        rtime: ioctl_timespec_to_system_time(raw.rtime.sec, raw.rtime.nsec),
    })
}

/// Read the flags of the subvolume that `fd` belongs to.
pub fn subvolume_flags_get(fd: BorrowedFd) -> nix::Result<SubvolumeFlags> {
    let mut flags: u64 = 0;
    unsafe { btrfs_ioc_subvol_getflags(fd.as_raw_fd(), &mut flags) }?;
    Ok(SubvolumeFlags::from_bits_truncate(flags))
}

/// Set the flags of the subvolume that `fd` belongs to.
///
/// Requires `CAP_SYS_ADMIN` to make a subvolume read-only; any user may
/// clear the read-only flag from a subvolume they own.
pub fn subvolume_flags_set(fd: BorrowedFd, flags: SubvolumeFlags) -> nix::Result<()> {
    let raw: u64 = flags.bits();
    unsafe { btrfs_ioc_subvol_setflags(fd.as_raw_fd(), &raw) }?;
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
pub fn subvolume_default_get(fd: BorrowedFd) -> nix::Result<u64> {
    let mut default_id: Option<u64> = None;

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            BTRFS_ROOT_TREE_OBJECTID as u64,
            BTRFS_DIR_ITEM_KEY as u32,
            BTRFS_ROOT_TREE_DIR_OBJECTID as u64,
            BTRFS_ROOT_TREE_DIR_OBJECTID as u64,
        ),
        |_hdr, data| {
            use crate::raw::btrfs_dir_item;
            use std::mem::{offset_of, size_of};

            let header_size = size_of::<btrfs_dir_item>();
            if data.len() < header_size {
                return Ok(());
            }
            let name_off = offset_of!(btrfs_dir_item, name_len);
            let name_len = u16::from_le_bytes([data[name_off], data[name_off + 1]]) as usize;
            if data.len() < header_size + name_len {
                return Ok(());
            }
            let item_name = &data[header_size..header_size + name_len];
            if item_name == b"default" {
                let loc_off = offset_of!(btrfs_dir_item, location);
                let target_id = u64::from_le_bytes(data[loc_off..loc_off + 8].try_into().unwrap());
                default_id = Some(target_id);
            }
            Ok(())
        },
    )?;

    Ok(default_id.unwrap_or(BTRFS_FS_TREE_OBJECTID as u64))
}

/// Set the default subvolume of the filesystem referred to by `fd` to
/// `subvolid`.
///
/// Pass [`FS_TREE_OBJECTID`] to restore the default.  Requires `CAP_SYS_ADMIN`.
pub fn subvolume_default_set(fd: BorrowedFd, subvolid: u64) -> nix::Result<()> {
    unsafe { btrfs_ioc_default_subvol(fd.as_raw_fd(), &subvolid) }?;
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
/// Requires `CAP_SYS_ADMIN`.
pub fn subvolume_list(fd: BorrowedFd) -> nix::Result<Vec<SubvolumeListItem>> {
    let mut items: Vec<SubvolumeListItem> = Vec::new();

    tree_search(
        fd,
        SearchKey::for_objectid_range(
            BTRFS_ROOT_TREE_OBJECTID as u64,
            BTRFS_ROOT_ITEM_KEY as u32,
            BTRFS_FIRST_FREE_OBJECTID as u64,
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
            BTRFS_ROOT_TREE_OBJECTID as u64,
            BTRFS_ROOT_BACKREF_KEY as u32,
            BTRFS_FIRST_FREE_OBJECTID as u64,
            BTRFS_LAST_FREE_OBJECTID as u64,
        ),
        |hdr, data| {
            // ROOT_BACKREF_KEY: objectid = subvol root_id, offset = parent root_id
            let root_id = hdr.objectid;
            let parent_id = hdr.offset;

            if let Some(item) = items.iter_mut().find(|i| i.root_id == root_id) {
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
    let top_id = crate::inode::lookup_path_rootid(fd).unwrap_or(FS_TREE_OBJECTID);

    resolve_full_paths(fd, &mut items, top_id)?;

    Ok(items)
}

/// Call `BTRFS_IOC_INO_LOOKUP` for `dir_id` within `parent_tree` and return
/// the path from that tree's root to the directory.
///
/// The kernel returns the path with a trailing `/` when the directory is not
/// the tree root, and an empty string when `dir_id` is the tree root itself.
/// This prefix can be concatenated directly with the subvolume's leaf name to
/// form the full segment within the parent.
fn ino_lookup_dir_path(fd: BorrowedFd, parent_tree: u64, dir_id: u64) -> nix::Result<String> {
    let mut args = btrfs_ioctl_ino_lookup_args {
        treeid: parent_tree,
        objectid: dir_id,
        ..unsafe { mem::zeroed() }
    };
    // SAFETY: args is a valid, zeroed btrfs_ioctl_ino_lookup_args; the ioctl
    // fills in args.name with a null-terminated path string.
    unsafe { btrfs_ioc_ino_lookup(fd.as_raw_fd(), &mut args) }?;

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
/// from the ROOT_BACKREF pass.  A single `BTRFS_IOC_INO_LOOKUP` call per item
/// gives the path from the parent tree's root down to the directory that
/// contains the subvolume (the "dir prefix").  Concatenating that prefix with
/// the leaf name yields the subvolume's segment within its parent.  Walking up
/// the parent chain (using the in-memory items map) and joining segments with
/// `/` gives the final full path.
fn resolve_full_paths(
    fd: BorrowedFd,
    items: &mut Vec<SubvolumeListItem>,
    top_id: u64,
) -> nix::Result<()> {
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

    Ok(())
}

/// Compute the full path for `root_id`, memoizing into `cache`.
///
/// Walks the ancestor chain iteratively to avoid stack overflow on deep
/// subvolume trees.  Collects segments from the target up to the FS tree
/// root, then joins them in reverse order.
///
/// Cycle detection is included: ROOT_BACKREF entries can form mutual parent
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
                format!("{}/{}", parent_path, segment)
            }
        } else {
            segment.clone()
        };

        cache.insert(id, full_path);
    }

    cache.get(&root_id).cloned().unwrap_or_default()
}

/// `btrfs_root_item` field offsets (packed, LE).
fn parse_root_item(root_id: u64, data: &[u8]) -> Option<SubvolumeListItem> {
    use std::mem::offset_of;

    // Items shorter than generation_v2 are "legacy" and do not carry
    // UUID / otime / otransid fields.
    let legacy_boundary = offset_of!(btrfs_root_item, generation_v2);
    if data.len() < legacy_boundary {
        return None;
    }

    let generation = rle64(data, offset_of!(btrfs_root_item, generation));
    let flags_raw = rle64(data, offset_of!(btrfs_root_item, flags));
    let flags = SubvolumeFlags::from_bits_truncate(flags_raw);

    // Extended fields exist only in non-legacy items.
    let otime_nsec = offset_of!(btrfs_root_item, otime) + offset_of!(btrfs_timespec, nsec);
    let (uuid, parent_uuid, received_uuid, otransid, otime) =
        if data.len() >= otime_nsec + field_size!(btrfs_timespec, nsec) {
            let off_uuid = offset_of!(btrfs_root_item, uuid);
            let off_parent = offset_of!(btrfs_root_item, parent_uuid);
            let off_received = offset_of!(btrfs_root_item, received_uuid);
            let uuid_size = field_size!(btrfs_root_item, uuid);
            let uuid = Uuid::from_bytes(data[off_uuid..off_uuid + uuid_size].try_into().unwrap());
            let parent_uuid =
                Uuid::from_bytes(data[off_parent..off_parent + uuid_size].try_into().unwrap());
            let received_uuid = Uuid::from_bytes(
                data[off_received..off_received + uuid_size]
                    .try_into()
                    .unwrap(),
            );
            let otransid = rle64(data, offset_of!(btrfs_root_item, otransid));
            let otime_sec = offset_of!(btrfs_root_item, otime);
            let otime = timespec_to_system_time(rle64(data, otime_sec), rle32(data, otime_nsec));
            (uuid, parent_uuid, received_uuid, otransid, otime)
        } else {
            (Uuid::nil(), Uuid::nil(), Uuid::nil(), 0, UNIX_EPOCH)
        };

    Some(SubvolumeListItem {
        root_id,
        parent_id: 0,
        dir_id: 0,
        generation,
        flags,
        uuid,
        parent_uuid,
        received_uuid,
        otransid,
        otime,
        name: String::new(),
    })
}

/// Parse a `btrfs_root_ref` payload (packed, LE). The name immediately
/// follows the fixed-size header.
fn parse_root_ref(data: &[u8]) -> Option<(u64, String)> {
    use crate::raw::btrfs_root_ref;
    use std::mem::{offset_of, size_of};

    let header_size = size_of::<btrfs_root_ref>();
    if data.len() < header_size {
        return None;
    }
    let dir_id = rle64(data, offset_of!(btrfs_root_ref, dirid));
    let name_off = offset_of!(btrfs_root_ref, name_len);
    let name_len = u16::from_le_bytes([data[name_off], data[name_off + 1]]) as usize;
    if data.len() < header_size + name_len {
        return None;
    }
    let name = String::from_utf8_lossy(&data[header_size..header_size + name_len]).into_owned();
    Some((dir_id, name))
}

#[inline]
fn rle64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
fn rle32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

/// Convert an on-disk `btrfs_timespec` (LE sec + LE nsec, packed) to
/// [`SystemTime`].  Returns [`UNIX_EPOCH`] if sec == 0.
fn timespec_to_system_time(sec: u64, nsec: u32) -> SystemTime {
    if sec == 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::new(sec, nsec)
}

/// Convert a `btrfs_ioctl_timespec` (host byte order, with padding) to
/// [`SystemTime`].  Returns [`UNIX_EPOCH`] if sec == 0.
fn ioctl_timespec_to_system_time(sec: u64, nsec: u32) -> SystemTime {
    if sec == 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::new(sec, nsec)
}

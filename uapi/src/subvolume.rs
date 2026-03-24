//! # Subvolume and snapshot management: creating, deleting, and querying subvolumes
//!
//! Subvolumes are independently snapshotable subtrees within a btrfs filesystem.
//! Snapshots are subvolumes created as copy-on-write clones of an existing
//! subvolume.  This module covers the full lifecycle: creating and deleting
//! subvolumes and snapshots, reading subvolume metadata and flags, listing all
//! subvolumes in a filesystem, and getting or setting the default subvolume
//! that is mounted when no subvolume is explicitly requested.

use std::{
    ffi::CStr,
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bitflags::bitflags;
use nix::libc::c_char;
use uuid::Uuid;

use crate::{
    raw::{
        BTRFS_DIR_ITEM_KEY, BTRFS_FIRST_FREE_OBJECTID, BTRFS_FS_TREE_OBJECTID,
        BTRFS_LAST_FREE_OBJECTID, BTRFS_ROOT_BACKREF_KEY, BTRFS_ROOT_ITEM_KEY,
        BTRFS_ROOT_TREE_DIR_OBJECTID, BTRFS_ROOT_TREE_OBJECTID, BTRFS_SUBVOL_RDONLY,
        btrfs_ioc_default_subvol, btrfs_ioc_get_subvol_info, btrfs_ioc_snap_create_v2,
        btrfs_ioc_snap_destroy_v2, btrfs_ioc_subvol_create_v2, btrfs_ioc_subvol_getflags,
        btrfs_ioc_subvol_setflags, btrfs_ioctl_get_subvol_info_args, btrfs_ioctl_vol_args_v2,
    },
    tree_search::{SearchKey, tree_search},
};

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
            // btrfs_dir_item (30 bytes, packed):
            //   [0..8]  location.objectid  LE u64 — target root ID
            //   [8]     location.type_     u8
            //   [9..17] location.offset    LE u64
            //   [17..25] transid           LE u64
            //   [25..27] data_len          LE u16
            //   [27..29] name_len          LE u16
            //   [29]    type_              u8
            //   [30..]  name               (name_len bytes)
            if data.len() < 30 {
                return Ok(());
            }
            let name_len = u16::from_le_bytes([data[27], data[28]]) as usize;
            if data.len() < 30 + name_len {
                return Ok(());
            }
            let item_name = &data[30..30 + name_len];
            if item_name == b"default" {
                let target_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
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
                item.parent_id = parent_id;
                if let Some((dir_id, name)) = parse_root_ref(data) {
                    item.dir_id = dir_id;
                    item.name = name;
                }
            }
            Ok(())
        },
    )?;

    Ok(items)
}

/// `btrfs_root_item` field offsets (packed, LE).
mod root_item_off {
    pub const GENERATION: usize = 160;
    pub const FLAGS: usize = 208;
    /// Byte offset of `generation_v2`; items shorter than this are "legacy"
    /// and do not carry UUID / otime / otransid fields.
    pub const LEGACY_BOUNDARY: usize = 239;
    pub const UUID: usize = 247;
    pub const PARENT_UUID: usize = 263;
    pub const RECEIVED_UUID: usize = 279;
    pub const OTRANSID: usize = 303;
    /// `otime` is a packed `btrfs_timespec`: sec (LE u64) + nsec (LE u32).
    pub const OTIME_SEC: usize = 339;
    pub const OTIME_NSEC: usize = 347;
}

fn parse_root_item(root_id: u64, data: &[u8]) -> Option<SubvolumeListItem> {
    use root_item_off::*;

    if data.len() < LEGACY_BOUNDARY {
        // Too short even for the legacy fields we need.
        return None;
    }

    let generation = rle64(data, GENERATION);
    let flags_raw = rle64(data, FLAGS);
    let flags = SubvolumeFlags::from_bits_truncate(flags_raw);

    // Extended fields exist only in non-legacy items.
    let (uuid, parent_uuid, received_uuid, otransid, otime) = if data.len()
        >= root_item_off::OTIME_NSEC + 4
    {
        let uuid = Uuid::from_bytes(data[UUID..UUID + 16].try_into().unwrap());
        let parent_uuid = Uuid::from_bytes(data[PARENT_UUID..PARENT_UUID + 16].try_into().unwrap());
        let received_uuid =
            Uuid::from_bytes(data[RECEIVED_UUID..RECEIVED_UUID + 16].try_into().unwrap());
        let otransid = rle64(data, OTRANSID);
        let otime = timespec_to_system_time(rle64(data, OTIME_SEC), rle32(data, OTIME_NSEC));
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

/// Parse a `btrfs_root_ref` payload (packed, LE):
///
/// ```text
/// [0..8]   dirid     LE u64
/// [8..16]  sequence  LE u64
/// [16..18] name_len  LE u16
/// [18..]   name      (name_len bytes, UTF-8)
/// ```
fn parse_root_ref(data: &[u8]) -> Option<(u64, String)> {
    if data.len() < 18 {
        return None;
    }
    let dir_id = rle64(data, 0);
    let name_len = u16::from_le_bytes([data[16], data[17]]) as usize;
    if data.len() < 18 + name_len {
        return None;
    }
    let name = String::from_utf8_lossy(&data[18..18 + name_len]).into_owned();
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

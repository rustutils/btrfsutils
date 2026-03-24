//! # Quota group (qgroup) management: hierarchical accounting of disk usage
//!
//! Qgroups track how much disk space a subvolume (or a group of subvolumes)
//! uses.  Every subvolume automatically gets a level-0 qgroup whose ID matches
//! the subvolume ID.  Higher-level qgroups can be created and linked into a
//! parent–child hierarchy so that space usage rolls up through the tree.
//!
//! Quota must be enabled on the filesystem (see [`crate::quota`]) before any
//! qgroup operations will succeed.  Most operations require `CAP_SYS_ADMIN`.

use std::{
    collections::{HashMap, HashSet},
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

use bitflags::bitflags;
use nix::errno::Errno;

use crate::{
    raw::{
        BTRFS_FIRST_FREE_OBJECTID, BTRFS_LAST_FREE_OBJECTID, BTRFS_QGROUP_INFO_KEY,
        BTRFS_QGROUP_LIMIT_EXCL_CMPR, BTRFS_QGROUP_LIMIT_KEY, BTRFS_QGROUP_LIMIT_MAX_EXCL,
        BTRFS_QGROUP_LIMIT_MAX_RFER, BTRFS_QGROUP_LIMIT_RFER_CMPR, BTRFS_QGROUP_RELATION_KEY,
        BTRFS_QGROUP_STATUS_FLAG_INCONSISTENT, BTRFS_QGROUP_STATUS_FLAG_ON,
        BTRFS_QGROUP_STATUS_FLAG_RESCAN, BTRFS_QGROUP_STATUS_FLAG_SIMPLE_MODE,
        BTRFS_QGROUP_STATUS_KEY, BTRFS_QUOTA_TREE_OBJECTID, BTRFS_ROOT_ITEM_KEY,
        BTRFS_ROOT_TREE_OBJECTID, btrfs_ioc_qgroup_assign, btrfs_ioc_qgroup_create,
        btrfs_ioc_qgroup_limit, btrfs_ioctl_qgroup_assign_args, btrfs_ioctl_qgroup_create_args,
        btrfs_ioctl_qgroup_limit_args, btrfs_qgroup_limit,
    },
    tree_search::{SearchKey, tree_search},
};

// ---------------------------------------------------------------------------
// Qgroup ID encoding
// ---------------------------------------------------------------------------

/// Extract the hierarchy level from a packed qgroup ID.
///
/// `qgroupid = (level << 48) | subvolid`.  Level 0 qgroups correspond
/// directly to subvolumes.
#[inline]
pub fn qgroupid_level(qgroupid: u64) -> u16 {
    (qgroupid >> 48) as u16
}

/// Extract the subvolume ID component from a packed qgroup ID.
///
/// Only meaningful for level-0 qgroups.
#[inline]
pub fn qgroupid_subvolid(qgroupid: u64) -> u64 {
    qgroupid & 0x0000_FFFF_FFFF_FFFF
}

// ---------------------------------------------------------------------------
// Public flag types
// ---------------------------------------------------------------------------

bitflags! {
    /// Status flags for the quota tree as a whole (`BTRFS_QGROUP_STATUS_KEY`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QgroupStatusFlags: u64 {
        /// Quota accounting is enabled.
        const ON           = BTRFS_QGROUP_STATUS_FLAG_ON as u64;
        /// A rescan is currently in progress.
        const RESCAN       = BTRFS_QGROUP_STATUS_FLAG_RESCAN as u64;
        /// Accounting is inconsistent and a rescan is needed.
        const INCONSISTENT = BTRFS_QGROUP_STATUS_FLAG_INCONSISTENT as u64;
        /// Simple quota mode (squota) is active.
        const SIMPLE_MODE  = BTRFS_QGROUP_STATUS_FLAG_SIMPLE_MODE as u64;
    }
}

bitflags! {
    /// Which limit fields are actively enforced on a qgroup.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QgroupLimitFlags: u64 {
        /// `max_rfer` (maximum referenced bytes) is enforced.
        const MAX_RFER  = BTRFS_QGROUP_LIMIT_MAX_RFER as u64;
        /// `max_excl` (maximum exclusive bytes) is enforced.
        const MAX_EXCL  = BTRFS_QGROUP_LIMIT_MAX_EXCL as u64;
        /// Referenced bytes are compressed before comparison.
        const RFER_CMPR = BTRFS_QGROUP_LIMIT_RFER_CMPR as u64;
        /// Exclusive bytes are compressed before comparison.
        const EXCL_CMPR = BTRFS_QGROUP_LIMIT_EXCL_CMPR as u64;
    }
}

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// Usage and limit information for a single qgroup.
#[derive(Debug, Clone)]
pub struct QgroupInfo {
    /// Packed qgroup ID: `(level << 48) | subvolid`.
    pub qgroupid: u64,
    /// Total referenced bytes (includes shared data).
    pub rfer: u64,
    /// Referenced bytes after compression.
    pub rfer_cmpr: u64,
    /// Exclusively-owned bytes (not shared with any other subvolume).
    pub excl: u64,
    /// Exclusively-owned bytes after compression.
    pub excl_cmpr: u64,
    /// Limit flags — which of the limit fields below are enforced.
    pub limit_flags: QgroupLimitFlags,
    /// Maximum referenced bytes.  `u64::MAX` when no limit is set.
    pub max_rfer: u64,
    /// Maximum exclusive bytes.  `u64::MAX` when no limit is set.
    pub max_excl: u64,
    /// IDs of qgroups that are parents of this one in the hierarchy.
    pub parents: Vec<u64>,
    /// IDs of qgroups that are children of this one in the hierarchy.
    pub children: Vec<u64>,
    /// Level-0 only: `true` when the corresponding subvolume no longer
    /// exists (this is a "stale" qgroup left behind after deletion).
    pub stale: bool,
}

/// Result of [`qgroup_list`]: overall quota status and per-qgroup details.
#[derive(Debug, Clone)]
pub struct QgroupList {
    /// Flags from the `BTRFS_QGROUP_STATUS_KEY` item.
    pub status_flags: QgroupStatusFlags,
    /// All qgroups found in the quota tree, sorted by `qgroupid`.
    pub qgroups: Vec<QgroupInfo>,
}

// ---------------------------------------------------------------------------
// Internal builder used while scanning the quota tree
// ---------------------------------------------------------------------------

#[derive(Default)]
struct QgroupEntryBuilder {
    // From INFO item
    has_info: bool,
    rfer: u64,
    rfer_cmpr: u64,
    excl: u64,
    excl_cmpr: u64,
    // From LIMIT item
    has_limit: bool,
    limit_flags: u64,
    max_rfer: u64,
    max_excl: u64,
    // From RELATION items
    parents: Vec<u64>,
    children: Vec<u64>,
}

impl QgroupEntryBuilder {
    fn build(self, qgroupid: u64, stale: bool) -> QgroupInfo {
        QgroupInfo {
            qgroupid,
            rfer: self.rfer,
            rfer_cmpr: self.rfer_cmpr,
            excl: self.excl,
            excl_cmpr: self.excl_cmpr,
            limit_flags: QgroupLimitFlags::from_bits_truncate(self.limit_flags),
            max_rfer: if self.limit_flags & BTRFS_QGROUP_LIMIT_MAX_RFER as u64 != 0 {
                self.max_rfer
            } else {
                u64::MAX
            },
            max_excl: if self.limit_flags & BTRFS_QGROUP_LIMIT_MAX_EXCL as u64 != 0 {
                self.max_excl
            } else {
                u64::MAX
            },
            parents: self.parents,
            children: self.children,
            stale,
        }
    }
}

// ---------------------------------------------------------------------------
// On-disk struct parsers (all fields LE)
// ---------------------------------------------------------------------------

/// `btrfs_qgroup_status_item` field offsets.
mod status_off {
    pub const FLAGS: usize = 16;
}

/// `btrfs_qgroup_info_item` field offsets.
mod info_off {
    pub const RFER: usize = 8;
    pub const RFER_CMPR: usize = 16;
    pub const EXCL: usize = 24;
    pub const EXCL_CMPR: usize = 32;
}

/// `btrfs_qgroup_limit_item` field offsets.
mod limit_off {
    pub const FLAGS: usize = 0;
    pub const MAX_RFER: usize = 8;
    pub const MAX_EXCL: usize = 16;
}

#[inline]
fn rle64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn parse_status_flags(data: &[u8]) -> Option<u64> {
    if data.len() < status_off::FLAGS + 8 {
        return None;
    }
    Some(rle64(data, status_off::FLAGS))
}

fn parse_info(builder: &mut QgroupEntryBuilder, data: &[u8]) {
    if data.len() < info_off::EXCL_CMPR + 8 {
        return;
    }
    builder.has_info = true;
    builder.rfer = rle64(data, info_off::RFER);
    builder.rfer_cmpr = rle64(data, info_off::RFER_CMPR);
    builder.excl = rle64(data, info_off::EXCL);
    builder.excl_cmpr = rle64(data, info_off::EXCL_CMPR);
}

fn parse_limit(builder: &mut QgroupEntryBuilder, data: &[u8]) {
    if data.len() < limit_off::MAX_EXCL + 8 {
        return;
    }
    builder.has_limit = true;
    builder.limit_flags = rle64(data, limit_off::FLAGS);
    builder.max_rfer = rle64(data, limit_off::MAX_RFER);
    builder.max_excl = rle64(data, limit_off::MAX_EXCL);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a new qgroup with the given `qgroupid` on the filesystem referred
/// to by `fd`.
///
/// `qgroupid` is the packed form: `(level << 48) | subvolid`.
pub fn qgroup_create(fd: BorrowedFd, qgroupid: u64) -> nix::Result<()> {
    let mut args: btrfs_ioctl_qgroup_create_args = unsafe { mem::zeroed() };
    args.create = 1;
    args.qgroupid = qgroupid;
    // SAFETY: args is fully initialised above and lives for the duration of
    // the ioctl call.
    unsafe { btrfs_ioc_qgroup_create(fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Destroy the qgroup with the given `qgroupid` on the filesystem referred
/// to by `fd`.
pub fn qgroup_destroy(fd: BorrowedFd, qgroupid: u64) -> nix::Result<()> {
    let mut args: btrfs_ioctl_qgroup_create_args = unsafe { mem::zeroed() };
    args.create = 0;
    args.qgroupid = qgroupid;
    // SAFETY: args is fully initialised above and lives for the duration of
    // the ioctl call.
    unsafe { btrfs_ioc_qgroup_create(fd.as_raw_fd(), &args) }?;
    Ok(())
}

/// Assign qgroup `src` as a member of qgroup `dst` (i.e. `src` becomes a
/// child of `dst`).
///
/// Returns `true` if the kernel indicates that a quota rescan is now needed
/// (the ioctl returned a positive value).
pub fn qgroup_assign(fd: BorrowedFd, src: u64, dst: u64) -> nix::Result<bool> {
    let mut args: btrfs_ioctl_qgroup_assign_args = unsafe { mem::zeroed() };
    args.assign = 1;
    args.src = src;
    args.dst = dst;
    // SAFETY: args is fully initialised above and lives for the duration of
    // the ioctl call.
    let ret = unsafe { btrfs_ioc_qgroup_assign(fd.as_raw_fd(), &args) }?;
    Ok(ret > 0)
}

/// Remove the child–parent relationship between qgroups `src` and `dst`.
///
/// Returns `true` if the kernel indicates that a quota rescan is now needed.
pub fn qgroup_remove(fd: BorrowedFd, src: u64, dst: u64) -> nix::Result<bool> {
    let mut args: btrfs_ioctl_qgroup_assign_args = unsafe { mem::zeroed() };
    args.assign = 0;
    args.src = src;
    args.dst = dst;
    // SAFETY: args is fully initialised above and lives for the duration of
    // the ioctl call.
    let ret = unsafe { btrfs_ioc_qgroup_assign(fd.as_raw_fd(), &args) }?;
    Ok(ret > 0)
}

/// Set usage limits on a qgroup.
///
/// Pass `QgroupLimitFlags::MAX_RFER` in `flags` to enforce `max_rfer`, and/or
/// `QgroupLimitFlags::MAX_EXCL` to enforce `max_excl`.  Clear a limit by
/// omitting the corresponding flag.
pub fn qgroup_limit(
    fd: BorrowedFd,
    qgroupid: u64,
    flags: QgroupLimitFlags,
    max_rfer: u64,
    max_excl: u64,
) -> nix::Result<()> {
    let lim = btrfs_qgroup_limit {
        flags: flags.bits(),
        max_referenced: max_rfer,
        max_exclusive: max_excl,
        rsv_referenced: 0,
        rsv_exclusive: 0,
    };
    let mut args: btrfs_ioctl_qgroup_limit_args = unsafe { mem::zeroed() };
    args.qgroupid = qgroupid;
    args.lim = lim;
    // SAFETY: args is fully initialised above and lives for the duration of
    // the ioctl call.  The ioctl number is #43 (_IOR direction in the kernel
    // header), which reads args from userspace.
    unsafe { btrfs_ioc_qgroup_limit(fd.as_raw_fd(), &mut args) }?;
    Ok(())
}

/// List all qgroups and overall quota status for the filesystem referred to
/// by `fd`.
///
/// Returns `Ok(QgroupList { status_flags: empty, qgroups: [] })` when quota
/// accounting is not enabled (`ENOENT` from the kernel).
pub fn qgroup_list(fd: BorrowedFd) -> nix::Result<QgroupList> {
    // Build a map of qgroupid → builder as we walk the quota tree.
    let mut builders: HashMap<u64, QgroupEntryBuilder> = HashMap::new();
    let mut status_flags = QgroupStatusFlags::empty();

    // Scan the quota tree for STATUS / INFO / LIMIT / RELATION items in one pass.
    let quota_key = SearchKey {
        tree_id: BTRFS_QUOTA_TREE_OBJECTID as u64,
        min_objectid: 0,
        max_objectid: u64::MAX,
        min_type: BTRFS_QGROUP_STATUS_KEY as u32,
        max_type: BTRFS_QGROUP_RELATION_KEY as u32,
        min_offset: 0,
        max_offset: u64::MAX,
        min_transid: 0,
        max_transid: u64::MAX,
    };

    let scan_result = tree_search(fd, quota_key, |hdr, data| {
        match hdr.item_type as u32 {
            t if t == BTRFS_QGROUP_STATUS_KEY as u32 => {
                if let Some(raw) = parse_status_flags(data) {
                    status_flags = QgroupStatusFlags::from_bits_truncate(raw);
                }
            }
            t if t == BTRFS_QGROUP_INFO_KEY as u32 => {
                // offset = qgroupid
                let entry = builders.entry(hdr.offset).or_default();
                parse_info(entry, data);
            }
            t if t == BTRFS_QGROUP_LIMIT_KEY as u32 => {
                // offset = qgroupid
                let entry = builders.entry(hdr.offset).or_default();
                parse_limit(entry, data);
            }
            t if t == BTRFS_QGROUP_RELATION_KEY as u32 => {
                // The kernel stores two entries per relation:
                //   (child, RELATION_KEY, parent)
                //   (parent, RELATION_KEY, child)
                // Only process the canonical form where objectid > offset,
                // i.e. parent > child.
                if hdr.objectid > hdr.offset {
                    let parent = hdr.objectid;
                    let child = hdr.offset;
                    builders.entry(child).or_default().parents.push(parent);
                    builders.entry(parent).or_default().children.push(child);
                }
            }
            _ => {}
        }
        Ok(())
    });

    match scan_result {
        Err(Errno::ENOENT) => {
            // Quota tree does not exist — quotas are disabled.
            return Ok(QgroupList {
                status_flags: QgroupStatusFlags::empty(),
                qgroups: Vec::new(),
            });
        }
        Err(e) => return Err(e),
        Ok(()) => {}
    }

    // Collect existing subvolume IDs so we can mark stale level-0 qgroups.
    let existing_subvol_ids = collect_subvol_ids(fd)?;

    // Convert builders to QgroupInfo, computing stale flag for level-0 groups.
    let mut qgroups: Vec<QgroupInfo> = builders
        .into_iter()
        .map(|(qgroupid, builder)| {
            let stale = if qgroupid_level(qgroupid) == 0 {
                !existing_subvol_ids.contains(&qgroupid_subvolid(qgroupid))
            } else {
                false
            };
            builder.build(qgroupid, stale)
        })
        .collect();

    qgroups.sort_by_key(|q| q.qgroupid);

    Ok(QgroupList {
        status_flags,
        qgroups,
    })
}

/// Collect the set of all existing subvolume IDs by scanning
/// `ROOT_ITEM_KEY` entries in the root tree.
fn collect_subvol_ids(fd: BorrowedFd) -> nix::Result<HashSet<u64>> {
    let mut ids: HashSet<u64> = HashSet::new();

    // BTRFS_LAST_FREE_OBJECTID binds as i32 = -256; cast to u64 gives
    // 0xFFFFFFFF_FFFFFF00 as expected.
    let key = SearchKey::for_objectid_range(
        BTRFS_ROOT_TREE_OBJECTID as u64,
        BTRFS_ROOT_ITEM_KEY as u32,
        BTRFS_FIRST_FREE_OBJECTID as u64,
        BTRFS_LAST_FREE_OBJECTID as u64,
    );

    tree_search(fd, key, |hdr, _data| {
        ids.insert(hdr.objectid);
        Ok(())
    })?;

    Ok(ids)
}

/// Destroy all "stale" level-0 qgroups — those whose corresponding subvolume
/// no longer exists.
///
/// In simple-quota mode (`SIMPLE_MODE` flag set), stale qgroups with non-zero
/// `rfer` or `excl` are retained because they hold accounting information for
/// dropped subvolumes.
///
/// Returns the number of qgroups successfully destroyed.
pub fn qgroup_clear_stale(fd: BorrowedFd) -> nix::Result<usize> {
    let list = qgroup_list(fd)?;
    let simple_mode = list.status_flags.contains(QgroupStatusFlags::SIMPLE_MODE);

    let mut count = 0usize;

    for qg in &list.qgroups {
        // Only process level-0 stale qgroups.
        if qgroupid_level(qg.qgroupid) != 0 || !qg.stale {
            continue;
        }

        // In simple-quota mode, keep stale qgroups that still have usage data.
        if simple_mode && (qg.rfer != 0 || qg.excl != 0) {
            continue;
        }

        if qgroup_destroy(fd, qg.qgroupid).is_ok() {
            count += 1;
        }
    }

    Ok(count)
}

//! btrfs ioctl handlers exposed through the FUSE protocol.
//!
//! Each `BTRFS_IOC_*` request that arrives at the FUSE driver is
//! decoded here, dispatched to the corresponding read on
//! [`btrfs_fs::Filesystem`], and the response is serialised back
//! into the on-disk C struct layout that userspace tools (the
//! upstream `btrfs` CLI, our `btrfs-cli`, libbtrfsutil, ...) expect.
//!
//! The kernel ioctl numbers are computed at compile time via
//! `ior` / `iow` / `iowr` const helpers rather than imported from
//! bindgen output — `bindgen` does not expand the `_IOR` macro
//! family, so the numbers don't appear as constants in
//! `btrfs_disk::raw`. This is the only place in the project that
//! re-derives them.
//!
//! Currently implemented:
//! - `BTRFS_IOC_FS_INFO` (F6.1)
//! - `BTRFS_IOC_GET_FEATURES` (F6.1)
//! - `BTRFS_IOC_GET_SUBVOL_INFO` (F6.1)
//! - `BTRFS_IOC_DEV_INFO` (F6.2)
//! - `BTRFS_IOC_INO_LOOKUP` (F6.2)
//! - `BTRFS_IOC_TREE_SEARCH` (F6.3, fixed 4 KiB)
//! - `BTRFS_IOC_TREE_SEARCH_V2` (F6.3, code path uses retry — see
//!   note below; works only from callers that opt into
//!   `FUSE_IOCTL_UNRESTRICTED`)
//! - `BTRFS_IOC_GET_SUBVOL_ROOTREF` (F6.3, fixed 4 KiB)
//!
//! Not implemented over FUSE: `BTRFS_IOC_INO_PATHS` and
//! `BTRFS_IOC_LOGICAL_INO_V2`. Both require `FUSE_IOCTL_RETRY`,
//! which the Linux kernel only honours for ioctls issued with
//! `FUSE_IOCTL_UNRESTRICTED` set. The standard libc `ioctl(2)` path
//! the `btrfs` CLI takes does not set that flag, so a retry response
//! is rejected with `-EIO` before the FUSE driver is re-entered.
//! The same restriction means our `TREE_SEARCH_V2` retry handler is
//! effectively unreachable in practice today; v1 (fits in the
//! cmd-encoded 4 KiB) remains the working path. See
//! `fs/PLAN.md` § F6.3 for next steps (kernel relaxation, CUSE
//! init, or a custom FUSE protocol implementation).

use btrfs_fs::{Filesystem, Inode, RootRef, SearchFilter, SubvolId};
use bytes::{Buf, BufMut};
use fuser::{Errno, IoctlFlags, IoctlIovec};
use std::fs::File;

// ── ioctl number encoding ─────────────────────────────────────────

const IOC_NRSHIFT: u32 = 0;
const IOC_TYPESHIFT: u32 = 8;
const IOC_SIZESHIFT: u32 = 16;
const IOC_DIRSHIFT: u32 = 30;
const IOC_READ: u32 = 2;

const fn ioc(dir: u32, ty: u8, nr: u8, size: u32) -> u32 {
    (dir << IOC_DIRSHIFT)
        | ((ty as u32) << IOC_TYPESHIFT)
        | ((nr as u32) << IOC_NRSHIFT)
        | (size << IOC_SIZESHIFT)
}

const fn ior(ty: u8, nr: u8, size: u32) -> u32 {
    ioc(IOC_READ, ty, nr, size)
}

const fn iowr(ty: u8, nr: u8, size: u32) -> u32 {
    ioc(IOC_READ | 1, ty, nr, size)
}

/// `BTRFS_IOCTL_MAGIC` from `<linux/btrfs.h>`.
const BTRFS_MAGIC: u8 = 0x94;

/// Size of `struct btrfs_ioctl_fs_info_args` (1024 bytes).
const FS_INFO_SIZE: u32 = 1024;
/// Size of `struct btrfs_ioctl_feature_flags` (24 bytes).
const FEATURE_FLAGS_SIZE: u32 = 24;
/// Size of `struct btrfs_ioctl_get_subvol_info_args` (504 bytes).
const SUBVOL_INFO_SIZE: u32 = 504;
/// Size of `struct btrfs_ioctl_dev_info_args`: `devid`(8) + `uuid`(16) +
/// `bytes_used`(8) + `total_bytes`(8) + `unused`[379](3032) + `path`[1024]
/// = 4096 bytes.
const DEV_INFO_SIZE: u32 = 4096;
/// Size of `struct btrfs_ioctl_ino_lookup_args`: `treeid`(8) +
/// `objectid`(8) + `name`[4080] = 4096 bytes.
const INO_LOOKUP_SIZE: u32 = 4096;
/// Size of `struct btrfs_ioctl_search_args` (v1): `key`
/// (`btrfs_ioctl_search_key`, 104) + `buf[3992]` = 4096 bytes.
/// Fixed-size — no retry needed.
const SEARCH_ARGS_V1_SIZE: u32 = 4096;
/// Size of the `buf` area in v1: 4096 - 104 = 3992 bytes.
const SEARCH_ARGS_V1_BUF: usize = 3992;
/// Size of `struct btrfs_ioctl_search_args_v2`: `key`
/// (`btrfs_ioctl_search_key`, 104) + `buf_size`(8) + `buf[0]`(0) =
/// 112 bytes. The trailing `buf[0]` is a flexible array — the
/// userspace caller passes 112 + `buf_size` bytes, which exceeds
/// the 14-bit cap and requires the FUSE retry mechanism.
const SEARCH_ARGS_V2_SIZE: u32 = 112;
/// Size of `struct btrfs_ioctl_search_key` (the prefix of
/// `btrfs_ioctl_search_args_v2`).
const SEARCH_KEY_SIZE: usize = 104;
/// Size of `struct btrfs_ioctl_search_header`: `transid`(8) +
/// `objectid`(8) + `offset`(8) + `type`(4) + `len`(4) = 32 bytes.
/// Written between each item in the response buf area; the items
/// are emitted directly so the constant is documentary, but the
/// `Filesystem::tree_search` caller uses the same value internally
/// when calculating `max_buf_size` budget.
#[allow(dead_code)]
const SEARCH_HEADER_SIZE: usize = 32;
/// Size of `struct btrfs_ioctl_get_subvol_rootref_args`:
/// `min_treeid`(8) + `rootref[255]`(255 * 16 = 4080) + `num_items`(1)
/// + `align[7]`(7) = 4096 bytes. Fixed-size — no retry needed.
const SUBVOL_ROOTREF_SIZE: u32 = 4096;
/// `BTRFS_MAX_ROOTREF_BUFFER_NUM`: kernel cap on entries returned per
/// `GET_SUBVOL_ROOTREF` call. Userspace pages through by setting
/// `min_treeid` to the next id past the last returned one.
const MAX_ROOTREF_BUFFER_NUM: usize = 255;

pub const BTRFS_IOC_FS_INFO: u32 = ior(BTRFS_MAGIC, 31, FS_INFO_SIZE);
pub const BTRFS_IOC_GET_FEATURES: u32 =
    ior(BTRFS_MAGIC, 57, FEATURE_FLAGS_SIZE);
pub const BTRFS_IOC_GET_SUBVOL_INFO: u32 =
    ior(BTRFS_MAGIC, 60, SUBVOL_INFO_SIZE);
pub const BTRFS_IOC_DEV_INFO: u32 = iowr(BTRFS_MAGIC, 30, DEV_INFO_SIZE);
pub const BTRFS_IOC_INO_LOOKUP: u32 = iowr(BTRFS_MAGIC, 18, INO_LOOKUP_SIZE);
pub const BTRFS_IOC_TREE_SEARCH: u32 =
    iowr(BTRFS_MAGIC, 17, SEARCH_ARGS_V1_SIZE);
pub const BTRFS_IOC_TREE_SEARCH_V2: u32 =
    iowr(BTRFS_MAGIC, 17, SEARCH_ARGS_V2_SIZE);
pub const BTRFS_IOC_GET_SUBVOL_ROOTREF: u32 =
    iowr(BTRFS_MAGIC, 61, SUBVOL_ROOTREF_SIZE);

// ── handlers ──────────────────────────────────────────────────────

/// Outcome of an ioctl dispatch: bytes to return to userspace,
/// an [`Errno`] for the FUSE adapter to forward, or a
/// `FUSE_IOCTL_RETRY` request describing the userspace iovecs the
/// kernel should re-send the ioctl with.
pub enum IoctlOutcome {
    Ok(Vec<u8>),
    Err(Errno),
    Retry {
        in_iovs: Vec<IoctlIovec>,
        out_iovs: Vec<IoctlIovec>,
    },
}

/// Decode `cmd` and dispatch to the matching handler. Unknown ioctls
/// produce `ENOTTY`, the standard "no such ioctl" return.
///
/// `arg` is the userspace pointer the ioctl was called with — used
/// by handlers that respond with `FUSE_IOCTL_RETRY` (variable-size
/// buffers). `flags` indicates whether this is the first call or
/// the post-retry pass with `FUSE_IOCTL_UNRESTRICTED` set.
/// `in_data` carries the input portion (`devid` for `DEV_INFO`,
/// `treeid`+`objectid` for `INO_LOOKUP`, etc.).
pub async fn dispatch(
    fs: &Filesystem<File>,
    target: Inode,
    cmd: u32,
    // `arg` and `flags` are part of the patched fuser callback
    // signature for the now-defunct retry path; they're forwarded
    // here for symmetry but no current handler uses them. Both go
    // away in the follow-up commit that switches back to released
    // fuser 0.17 (see fs/PLAN.md § F6.4).
    _arg: u64,
    _flags: IoctlFlags,
    in_data: &[u8],
) -> IoctlOutcome {
    match cmd {
        BTRFS_IOC_FS_INFO => fs_info(fs),
        BTRFS_IOC_GET_FEATURES => get_features(fs),
        BTRFS_IOC_GET_SUBVOL_INFO => get_subvol_info(fs, target.subvol).await,
        BTRFS_IOC_DEV_INFO => dev_info(fs, in_data),
        BTRFS_IOC_INO_LOOKUP => ino_lookup(fs, target.subvol, in_data).await,
        BTRFS_IOC_TREE_SEARCH => {
            tree_search_v1(fs, target.subvol, in_data).await
        }
        BTRFS_IOC_TREE_SEARCH_V2 => tree_search_v2(),
        BTRFS_IOC_GET_SUBVOL_ROOTREF => {
            get_subvol_rootref(fs, target.subvol, in_data).await
        }
        _ => IoctlOutcome::Err(Errno::ENOTTY),
    }
}

/// `BTRFS_IOC_FS_INFO`: filesystem-wide identity and geometry. All
/// fields come straight off the parsed superblock.
fn fs_info(fs: &Filesystem<File>) -> IoctlOutcome {
    let sb = fs.superblock();
    let mut buf: Vec<u8> = Vec::with_capacity(FS_INFO_SIZE as usize);

    // For a single-device image the highest devid is the one in the
    // superblock's embedded `dev_item`. We don't currently expose it
    // on Superblock, but `num_devices` is the right ceiling here too
    // (devids are dense from 1).
    let max_id = sb.num_devices.max(1);

    buf.put_u64_le(max_id);
    buf.put_u64_le(sb.num_devices);
    buf.put_slice(sb.fsid.as_bytes());
    buf.put_u32_le(sb.nodesize);
    buf.put_u32_le(sb.sectorsize);
    buf.put_u32_le(sb.sectorsize); // clone_alignment == sectorsize
    buf.put_u16_le(sb.csum_type.to_raw());
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u16_le(sb.csum_type.size() as u16);
    buf.put_u64_le(0); // flags (in/out — no flags set on read)
    buf.put_u64_le(sb.generation);
    buf.put_slice(sb.metadata_uuid.as_bytes());

    // 944 bytes of reserved padding.
    buf.resize(FS_INFO_SIZE as usize, 0);
    debug_assert_eq!(buf.len(), FS_INFO_SIZE as usize);
    IoctlOutcome::Ok(buf)
}

/// `BTRFS_IOC_GET_FEATURES`: three feature flag words
/// (`compat` / `compat_ro` / `incompat`).
fn get_features(fs: &Filesystem<File>) -> IoctlOutcome {
    let sb = fs.superblock();
    let mut buf: Vec<u8> = Vec::with_capacity(FEATURE_FLAGS_SIZE as usize);
    buf.put_u64_le(sb.compat_flags);
    buf.put_u64_le(sb.compat_ro_flags);
    buf.put_u64_le(sb.incompat_flags);
    debug_assert_eq!(buf.len(), FEATURE_FLAGS_SIZE as usize);
    IoctlOutcome::Ok(buf)
}

/// `BTRFS_IOC_GET_SUBVOL_INFO`: full metadata for the subvolume the
/// ioctl was issued against. The subvolume id comes from the FUSE
/// inode that was used to open the file descriptor; we map FUSE
/// inode → btrfs `Inode` upstream and pass the `subvol` field here.
async fn get_subvol_info(
    fs: &Filesystem<File>,
    subvol: SubvolId,
) -> IoctlOutcome {
    let info = match fs.get_subvol_info(subvol).await {
        Ok(Some(info)) => info,
        Ok(None) => return IoctlOutcome::Err(Errno::ENOENT),
        Err(e) => {
            log::warn!("ioctl GET_SUBVOL_INFO subvol={}: {e}", subvol.0);
            return IoctlOutcome::Err(Errno::EIO);
        }
    };

    let mut buf: Vec<u8> = Vec::with_capacity(SUBVOL_INFO_SIZE as usize);
    buf.put_u64_le(info.id.0);

    // 256-byte name field, NUL-padded. Truncate if longer (BTRFS_VOL_NAME_MAX = 255).
    let mut name_buf = [0u8; 256];
    let n = info.name.len().min(255);
    name_buf[..n].copy_from_slice(&info.name[..n]);
    buf.put_slice(&name_buf);

    buf.put_u64_le(info.parent.map_or(0, |p| p.0));
    buf.put_u64_le(info.dirid);
    buf.put_u64_le(info.generation);
    let flags: u64 = if info.readonly { 1 << 0 } else { 0 }; // BTRFS_ROOT_SUBVOL_RDONLY
    buf.put_u64_le(flags);
    buf.put_slice(info.uuid.as_bytes());
    buf.put_slice(info.parent_uuid.as_bytes());
    buf.put_slice(info.received_uuid.as_bytes());
    buf.put_u64_le(info.ctransid);
    buf.put_u64_le(info.otransid);
    buf.put_u64_le(0); // stransid (send) — not tracked by SubvolInfo yet
    buf.put_u64_le(0); // rtransid (receive)

    // 4 × btrfs_ioctl_timespec — { sec: u64, nsec: u32, _pad: u32 } each.
    write_timespec(&mut buf, info.ctime);
    write_timespec(&mut buf, info.otime);
    write_timespec(&mut buf, std::time::SystemTime::UNIX_EPOCH); // stime
    write_timespec(&mut buf, std::time::SystemTime::UNIX_EPOCH); // rtime

    // 8 × u64 reserved.
    for _ in 0..8 {
        buf.put_u64_le(0);
    }
    debug_assert_eq!(buf.len(), SUBVOL_INFO_SIZE as usize);
    IoctlOutcome::Ok(buf)
}

/// Serialise a `SystemTime` as the kernel's
/// `struct btrfs_ioctl_timespec { __u64 sec; __u32 nsec; }` plus a
/// 4-byte alignment pad.
fn write_timespec(buf: &mut Vec<u8>, t: std::time::SystemTime) {
    let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    buf.put_u64_le(dur.as_secs());
    buf.put_u32_le(dur.subsec_nanos());
    buf.put_u32_le(0); // pad to 16-byte stride
}

/// `BTRFS_IOC_DEV_INFO`: per-device geometry. Userspace passes the
/// `devid` (or all-zero `uuid` to look up by id); the kernel fills
/// in `path`, `bytes_used`, `total_bytes`. We only support
/// single-device images today, so any `devid != 1` (or unmatched
/// uuid) returns `ENODEV`.
fn dev_info(fs: &Filesystem<File>, in_data: &[u8]) -> IoctlOutcome {
    if in_data.len() < DEV_INFO_SIZE as usize {
        return IoctlOutcome::Err(Errno::EINVAL);
    }
    let mut cursor = in_data;
    let req_devid = cursor.get_u64_le();
    let mut req_uuid = [0u8; 16];
    cursor.copy_to_slice(&mut req_uuid);

    // The kernel honours `devid` first, falling back to `uuid` only
    // when `devid == 0`. Mirror that.
    let dev = if req_devid != 0 {
        fs.dev_info(req_devid)
    } else {
        // Lookup by UUID. Single-device only — match against the
        // primary device's uuid.
        let primary = fs.dev_info(1);
        primary.filter(|d| d.uuid.as_bytes() == &req_uuid)
    };
    let Some(dev) = dev else {
        return IoctlOutcome::Err(Errno::ENODEV);
    };

    let mut buf: Vec<u8> = Vec::with_capacity(DEV_INFO_SIZE as usize);
    buf.put_u64_le(dev.devid);
    buf.put_slice(dev.uuid.as_bytes());
    buf.put_u64_le(dev.bytes_used);
    buf.put_u64_le(dev.total_bytes);
    // `unused[379]` reserved padding before `path`.
    buf.resize(buf.len() + 379 * 8, 0);
    // path: 1024 bytes, NUL-padded. We don't have a real device path
    // (the FS sees a backing file, not a /dev node); leave empty.
    buf.resize(DEV_INFO_SIZE as usize, 0);
    debug_assert_eq!(buf.len(), DEV_INFO_SIZE as usize);
    IoctlOutcome::Ok(buf)
}

/// `BTRFS_IOC_INO_LOOKUP`: resolve a `(treeid, objectid)` pair to
/// the path of the inode within its subvolume tree.
///
/// If `treeid == 0`, use the subvolume of the file the ioctl was
/// issued against (passed in via `current_subvol`).
async fn ino_lookup(
    fs: &Filesystem<File>,
    current_subvol: SubvolId,
    in_data: &[u8],
) -> IoctlOutcome {
    if in_data.len() < INO_LOOKUP_SIZE as usize {
        return IoctlOutcome::Err(Errno::EINVAL);
    }
    let mut cursor = in_data;
    let treeid = cursor.get_u64_le();
    let objectid = cursor.get_u64_le();

    let subvol = if treeid == 0 {
        current_subvol
    } else {
        SubvolId(treeid)
    };

    let path = match fs.ino_lookup(subvol, objectid).await {
        Ok(Some(p)) => p,
        Ok(None) => return IoctlOutcome::Err(Errno::ENOENT),
        Err(e) => {
            log::warn!(
                "ioctl INO_LOOKUP subvol={} objectid={objectid}: {e}",
                subvol.0,
            );
            return IoctlOutcome::Err(Errno::EIO);
        }
    };

    let mut buf: Vec<u8> = Vec::with_capacity(INO_LOOKUP_SIZE as usize);
    // The kernel writes back the resolved treeid (in case it was 0)
    // and objectid (unchanged), then the path.
    buf.put_u64_le(subvol.0);
    buf.put_u64_le(objectid);
    // Path field is 4080 bytes, NUL-padded. Append a trailing `/`
    // when the result is non-empty to match kernel `INO_LOOKUP`
    // behaviour. Truncate to fit if longer.
    let mut path_bytes = path.clone();
    if !path_bytes.is_empty() {
        path_bytes.push(b'/');
    }
    let max = 4080 - 1; // leave room for trailing NUL
    let n = path_bytes.len().min(max);
    buf.put_slice(&path_bytes[..n]);
    buf.resize(INO_LOOKUP_SIZE as usize, 0);
    debug_assert_eq!(buf.len(), INO_LOOKUP_SIZE as usize);
    IoctlOutcome::Ok(buf)
}

/// `BTRFS_IOC_TREE_SEARCH` (v1): same semantics as v2 but with a
/// fixed 3992-byte response buffer. No retry needed because the
/// whole 4096-byte struct fits in the cmd-encoded size.
async fn tree_search_v1(
    fs: &Filesystem<File>,
    current_subvol: SubvolId,
    in_data: &[u8],
) -> IoctlOutcome {
    if in_data.len() < SEARCH_ARGS_V1_SIZE as usize {
        return IoctlOutcome::Err(Errno::EINVAL);
    }
    let (filter, raw_key) = match parse_search_key(in_data, current_subvol) {
        Ok(v) => v,
        Err(o) => return o,
    };

    let items = match fs.tree_search(filter, SEARCH_ARGS_V1_BUF).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!("ioctl TREE_SEARCH tree={} failed: {e}", filter.tree_id,);
            return IoctlOutcome::Err(Errno::EIO);
        }
    };

    let mut out: Vec<u8> = Vec::with_capacity(SEARCH_ARGS_V1_SIZE as usize);
    write_search_key(&mut out, filter.tree_id, &raw_key, items.len(), None);
    write_search_items(&mut out, &items);
    out.resize(SEARCH_ARGS_V1_SIZE as usize, 0);
    IoctlOutcome::Ok(out)
}

/// `BTRFS_IOC_TREE_SEARCH_V2`: cannot be served over FUSE.
///
/// v2 needs the kernel's `FUSE_IOCTL_RETRY` round-trip to extend
/// the result buffer past the 14-bit cmd-encoded size, but Linux
/// only honours that retry response when the original request set
/// `FUSE_IOCTL_UNRESTRICTED` — which standard `ioctl(2)` callers
/// never do. Returning `ENOPROTOOPT` is our private signal to
/// `btrfs-uapi`'s `tree_search_auto` wrapper, which catches it
/// and falls back to v1 (fixed 4 KiB buffer, paginated). v1 is
/// fully supported on this driver and matches v2's semantics.
///
/// `ENOPROTOOPT` was picked over the more common `ENOTSUP` /
/// `EOPNOTSUPP` because nothing else in the btrfs ioctl surface
/// returns it, so it acts as a private channel: if uapi sees it
/// here, it's overwhelmingly *us* speaking. See
/// `fs/PLAN.md` § F6.4.
fn tree_search_v2() -> IoctlOutcome {
    IoctlOutcome::Err(Errno::ENOPROTOOPT)
}

/// `BTRFS_IOC_GET_SUBVOL_ROOTREF`: list child subvolumes of the
/// subvolume the ioctl was issued against, paged in chunks of up to
/// 255 entries each.
///
/// Userspace fills in `min_treeid` (8 bytes at the start of the
/// args struct) to begin or resume iteration. We walk `ROOT_REF`
/// entries in the root tree where `objectid == current_subvol` and
/// `offset >= min_treeid`, emit up to 255 `(treeid, dirid)` pairs,
/// and update `min_treeid` to the next id past the last entry —
/// callers that want full enumeration loop until `num_items < 255`.
async fn get_subvol_rootref(
    fs: &Filesystem<File>,
    current_subvol: SubvolId,
    in_data: &[u8],
) -> IoctlOutcome {
    if in_data.len() < SUBVOL_ROOTREF_SIZE as usize {
        return IoctlOutcome::Err(Errno::EINVAL);
    }
    let min_treeid = u64::from_le_bytes(in_data[..8].try_into().unwrap());

    // ROOT_REF_KEY = 156. We pull at most one extra so we know when
    // there are more entries beyond the buffer cap (the kernel signals
    // this via the updated `min_treeid` field on the next iteration).
    let filter = SearchFilter {
        tree_id: 1,
        min_objectid: current_subvol.0,
        max_objectid: current_subvol.0,
        min_type: 156,
        max_type: 156,
        min_offset: min_treeid,
        max_offset: u64::MAX,
        min_transid: 0,
        max_transid: u64::MAX,
        #[allow(clippy::cast_possible_truncation)]
        max_items: (MAX_ROOTREF_BUFFER_NUM as u32).saturating_add(1),
    };
    let items = match fs.tree_search(filter, usize::MAX).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "ioctl GET_SUBVOL_ROOTREF subvol={}: {e}",
                current_subvol.0,
            );
            return IoctlOutcome::Err(Errno::EIO);
        }
    };

    // Compound-key search returns items whose compound (objectid,
    // type, offset) lies in the configured range; with both objectid
    // and type pinned, every returned item is the right shape, but
    // belt-and-braces filter on the type just in case.
    let mut entries: Vec<(u64, u64)> = Vec::new();
    let mut next_min_treeid = min_treeid;
    for item in items
        .iter()
        .filter(|it| it.objectid == current_subvol.0 && it.item_type == 156)
    {
        if entries.len() >= MAX_ROOTREF_BUFFER_NUM {
            // Buffer full: the kernel sets min_treeid to the next id
            // past the last included entry so the next call resumes
            // there.
            next_min_treeid = item.offset;
            break;
        }
        let Some(rr) = RootRef::parse(&item.data) else {
            continue;
        };
        entries.push((item.offset, rr.dirid));
    }

    let mut out: Vec<u8> = Vec::with_capacity(SUBVOL_ROOTREF_SIZE as usize);
    out.put_u64_le(next_min_treeid);
    for (treeid, dirid) in &entries {
        out.put_u64_le(*treeid);
        out.put_u64_le(*dirid);
    }
    // Pad the rest of the rootref array (up to 255 entries × 16 bytes).
    out.resize(8 + MAX_ROOTREF_BUFFER_NUM * 16, 0);
    #[allow(clippy::cast_possible_truncation)]
    out.put_u8(entries.len() as u8);
    // align[7]
    out.resize(SUBVOL_ROOTREF_SIZE as usize, 0);
    debug_assert_eq!(out.len(), SUBVOL_ROOTREF_SIZE as usize);
    IoctlOutcome::Ok(out)
}

/// Snapshot of the raw fields read from the search key, kept around
/// so [`write_search_key`] can echo them back unchanged in the
/// response (only `nr_items` and `buf_size` are mutated).
struct RawSearchKey {
    min_objectid: u64,
    max_objectid: u64,
    min_offset: u64,
    max_offset: u64,
    min_transid: u64,
    max_transid: u64,
    min_type: u32,
    max_type: u32,
}

/// Parse the 104-byte `btrfs_ioctl_search_key` prefix from `in_data`
/// into a [`SearchFilter`] and a [`RawSearchKey`] echo. Returns
/// `Err(IoctlOutcome::Err)` if the buffer is too short.
fn parse_search_key(
    in_data: &[u8],
    current_subvol: SubvolId,
) -> Result<(SearchFilter, RawSearchKey), IoctlOutcome> {
    if in_data.len() < SEARCH_KEY_SIZE {
        return Err(IoctlOutcome::Err(Errno::EINVAL));
    }
    let mut key = &in_data[..SEARCH_KEY_SIZE];
    let tree_id = key.get_u64_le();
    let min_objectid = key.get_u64_le();
    let max_objectid = key.get_u64_le();
    let min_offset = key.get_u64_le();
    let max_offset = key.get_u64_le();
    let min_transid = key.get_u64_le();
    let max_transid = key.get_u64_le();
    let min_type = key.get_u32_le();
    let max_type = key.get_u32_le();
    let nr_items = key.get_u32_le();

    let filter = SearchFilter {
        // tree_id == 0 means "use the file's current subvolume",
        // matching the kernel behaviour for fd-issued searches.
        tree_id: if tree_id == 0 {
            current_subvol.0
        } else {
            tree_id
        },
        min_objectid,
        max_objectid,
        min_type,
        max_type,
        min_offset,
        max_offset,
        min_transid,
        max_transid,
        max_items: nr_items,
    };
    let raw = RawSearchKey {
        min_objectid,
        max_objectid,
        min_offset,
        max_offset,
        min_transid,
        max_transid,
        min_type,
        max_type,
    };
    Ok((filter, raw))
}

/// Write the 104-byte search key back into `out`, with `nr_items`
/// updated to the actual count returned. For v2 the trailing
/// `buf_size` field is appended (pass `Some(buf_size)`); v1 stops
/// at the 104-byte key (pass `None`).
fn write_search_key(
    out: &mut Vec<u8>,
    tree_id: u64,
    raw: &RawSearchKey,
    actual_items: usize,
    buf_size_v2: Option<u64>,
) {
    out.put_u64_le(tree_id);
    out.put_u64_le(raw.min_objectid);
    out.put_u64_le(raw.max_objectid);
    out.put_u64_le(raw.min_offset);
    out.put_u64_le(raw.max_offset);
    out.put_u64_le(raw.min_transid);
    out.put_u64_le(raw.max_transid);
    out.put_u32_le(raw.min_type);
    out.put_u32_le(raw.max_type);
    #[allow(clippy::cast_possible_truncation)]
    out.put_u32_le(actual_items as u32);
    // Reserved fields after nr_items: u32 unused, then 4 × u64
    // unused (see `btrfs_ioctl_search_key` layout).
    out.put_u32_le(0); // unused
    for _ in 0..4 {
        out.put_u64_le(0); // unused1..unused4
    }
    if let Some(buf_size) = buf_size_v2 {
        out.put_u64_le(buf_size);
        debug_assert_eq!(out.len(), SEARCH_ARGS_V2_SIZE as usize);
    } else {
        debug_assert_eq!(out.len(), SEARCH_KEY_SIZE);
    }
}

/// Append items to the response buffer in
/// `btrfs_ioctl_search_header`-prefixed order.
fn write_search_items(out: &mut Vec<u8>, items: &[btrfs_fs::SearchItem]) {
    for item in items {
        out.put_u64_le(item.transid);
        out.put_u64_le(item.objectid);
        out.put_u64_le(item.offset);
        out.put_u32_le(item.item_type);
        #[allow(clippy::cast_possible_truncation)]
        out.put_u32_le(item.data.len() as u32);
        out.put_slice(&item.data);
    }
}

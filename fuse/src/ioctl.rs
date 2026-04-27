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
//! Currently implemented (F6.1):
//! - `BTRFS_IOC_FS_INFO`
//! - `BTRFS_IOC_GET_FEATURES`
//! - `BTRFS_IOC_GET_SUBVOL_INFO`
//!
//! Coming in F6.2: `TREE_SEARCH_V2` (variable-size out buffer with
//! the FUSE retry dance), `INO_LOOKUP*`, `LOGICAL_INO`, `DEV_INFO`,
//! `GET_SUBVOL_ROOTREF`, `INO_PATHS`.

use btrfs_fs::{Filesystem, Inode, SubvolId};
use bytes::BufMut;
use fuser::Errno;
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

/// `BTRFS_IOCTL_MAGIC` from `<linux/btrfs.h>`.
const BTRFS_MAGIC: u8 = 0x94;

/// Size of `struct btrfs_ioctl_fs_info_args` (1024 bytes).
const FS_INFO_SIZE: u32 = 1024;
/// Size of `struct btrfs_ioctl_feature_flags` (24 bytes).
const FEATURE_FLAGS_SIZE: u32 = 24;
/// Size of `struct btrfs_ioctl_get_subvol_info_args`.
///
/// Layout: `treeid`(8) + `name`(256) + `parent_id`(8) + `dirid`(8) +
/// `generation`(8) + `flags`(8) + `uuid`(16) + `parent_uuid`(16) +
/// `received_uuid`(16) + `ctransid`/`otransid`/`stransid`/`rtransid`(32) +
/// 4×`timespec`(64) + `reserved`(64) = 504 bytes.
const SUBVOL_INFO_SIZE: u32 = 504;

pub const BTRFS_IOC_FS_INFO: u32 = ior(BTRFS_MAGIC, 31, FS_INFO_SIZE);
pub const BTRFS_IOC_GET_FEATURES: u32 =
    ior(BTRFS_MAGIC, 57, FEATURE_FLAGS_SIZE);
pub const BTRFS_IOC_GET_SUBVOL_INFO: u32 =
    ior(BTRFS_MAGIC, 60, SUBVOL_INFO_SIZE);

// ── handlers ──────────────────────────────────────────────────────

/// Outcome of an ioctl dispatch: either bytes to return to userspace
/// (success) or an [`Errno`] for the FUSE adapter to forward.
pub enum IoctlOutcome {
    Ok(Vec<u8>),
    Err(Errno),
}

/// Decode `cmd` and dispatch to the matching handler. Unknown ioctls
/// produce `ENOTTY`, the standard "no such ioctl" return.
pub async fn dispatch(
    fs: &Filesystem<File>,
    target: Inode,
    cmd: u32,
) -> IoctlOutcome {
    match cmd {
        BTRFS_IOC_FS_INFO => fs_info(fs),
        BTRFS_IOC_GET_FEATURES => get_features(fs),
        BTRFS_IOC_GET_SUBVOL_INFO => get_subvol_info(fs, target.subvol).await,
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

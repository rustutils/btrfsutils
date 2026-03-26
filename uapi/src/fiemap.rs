//! # Physical extent layout of files via `FS_IOC_FIEMAP`
//!
//! `FS_IOC_FIEMAP` is a standard VFS ioctl (not btrfs-specific) that reports
//! the physical extents backing a file.  It is the mechanism `btrfs filesystem
//! du` uses to determine how much disk space each file occupies and which
//! extents are shared with other files.

use nix::libc;
use std::os::unix::io::BorrowedFd;

// FS_IOC_FIEMAP = _IOWR('f', 11, struct fiemap)
// struct fiemap (without flexible array member) = 32 bytes
// On 64-bit Linux: (3 << 30) | (32 << 16) | (0x66 << 8) | 11 = 0xC020_660B
const FS_IOC_FIEMAP: libc::Ioctl = 0xC020_660Bu32 as libc::Ioctl;

// fiemap header field offsets (all native byte order)
const FM_START: usize = 0; // u64 — logical offset to start from
const FM_LENGTH: usize = 8; // u64 — logical length to map
const FM_FLAGS: usize = 16; // u32 — request flags
const FM_MAPPED: usize = 20; // u32 — out: number of extents returned
const FM_COUNT: usize = 24; // u32 — in:  number of extent slots

// fiemap_extent field offsets within one 56-byte slot
const FE_LOGICAL: usize = 0; // u64
const FE_PHYSICAL: usize = 8; // u64
const FE_LENGTH: usize = 16; // u64
// fe_reserved64[2] at 24..40
const FE_FLAGS: usize = 40; // u32

const FE_SIZE: usize = 56;

const FIEMAP_EXTENT_LAST: u32 = 0x0000_0001;
const FIEMAP_EXTENT_UNKNOWN: u32 = 0x0000_0002;
const FIEMAP_EXTENT_DELALLOC: u32 = 0x0000_0004;
const FIEMAP_EXTENT_DATA_INLINE: u32 = 0x0000_0200;
const FIEMAP_EXTENT_SHARED: u32 = 0x0000_2000;

/// Flags for extents whose bytes we do not count (unknown location,
/// not-yet-written, or stored inline in metadata).
const SKIP_FLAGS: u32 =
    FIEMAP_EXTENT_UNKNOWN | FIEMAP_EXTENT_DELALLOC | FIEMAP_EXTENT_DATA_INLINE;

/// Number of extent slots to request per ioctl call.
const EXTENTS_PER_BATCH: u32 = 256;

/// Summary of the physical extent usage of a single file.
#[derive(Debug, Clone, Default)]
pub struct FileExtentInfo {
    /// Sum of the lengths of all non-inline, non-delalloc extents.
    pub total_bytes: u64,
    /// Bytes covered by extents flagged `FIEMAP_EXTENT_SHARED`.
    pub shared_bytes: u64,
    /// Physical `(start, end_exclusive)` ranges of every shared extent.
    ///
    /// Callers that need to compute a "set shared" total across multiple files
    /// should collect these ranges, sort, and merge overlaps.
    pub shared_extents: Vec<(u64, u64)>,
}

/// Query `FS_IOC_FIEMAP` for every extent of the file referred to by `fd`.
///
/// The returned [`FileExtentInfo`] includes total bytes, shared bytes, and the
/// physical ranges of all shared extents so the caller can compute cross-file
/// deduplication counts.
///
/// `fd` must be open on a regular file.  Symlinks and directories will return
/// an empty result or an error depending on the kernel version.
pub fn file_extents(fd: BorrowedFd) -> nix::Result<FileExtentInfo> {
    use std::os::fd::AsRawFd;

    // We use a Vec<u64> to guarantee 8-byte alignment for the fiemap buffer.
    let slots = EXTENTS_PER_BATCH as usize;
    let buf_bytes = 32 + slots * FE_SIZE;
    let words = buf_bytes.div_ceil(8);
    let mut buf: Vec<u64> = vec![0u64; words];

    let raw_fd = fd.as_raw_fd();
    let mut info = FileExtentInfo::default();
    let mut logical_start: u64 = 0;
    let mut done = false;

    while !done {
        buf.fill(0);
        {
            let b = as_bytes_mut(&mut buf);
            write_u64(b, FM_START, logical_start);
            write_u64(b, FM_LENGTH, u64::MAX.saturating_sub(logical_start));
            write_u32(b, FM_FLAGS, 0);
            write_u32(b, FM_COUNT, EXTENTS_PER_BATCH);
        }

        // SAFETY: buf is aligned and large enough for the fiemap header plus
        // EXTENTS_PER_BATCH extent slots.  The ioctl only writes within that
        // region.  raw_fd is a valid open file descriptor for the duration of
        // this call.
        let ret = unsafe {
            libc::ioctl(
                raw_fd,
                FS_IOC_FIEMAP,
                buf.as_mut_ptr() as *mut libc::c_void,
            )
        };
        if ret < 0 {
            return Err(nix::errno::Errno::last());
        }

        let b = as_bytes(&buf);
        let nr = read_u32(b, FM_MAPPED) as usize;
        if nr == 0 {
            break;
        }

        let mut last_logical: u64 = logical_start;
        let mut last_length: u64 = 0;

        for i in 0..nr {
            let off = 32 + i * FE_SIZE;
            let flags = read_u32(b, off + FE_FLAGS);
            let length = read_u64(b, off + FE_LENGTH);
            let physical = read_u64(b, off + FE_PHYSICAL);

            last_logical = read_u64(b, off + FE_LOGICAL);
            last_length = length;

            if flags & FIEMAP_EXTENT_LAST != 0 {
                done = true;
            }

            if flags & SKIP_FLAGS != 0 || length == 0 {
                continue;
            }

            info.total_bytes += length;

            if flags & FIEMAP_EXTENT_SHARED != 0 {
                info.shared_bytes += length;
                info.shared_extents.push((physical, physical + length));
            }
        }

        // Advance the logical cursor past the last extent seen.
        let next = last_logical.saturating_add(last_length);
        if next <= logical_start {
            break; // guard against zero-length loops
        }
        logical_start = next;
    }

    Ok(info)
}

fn as_bytes(v: &[u64]) -> &[u8] {
    // SAFETY: any &[u64] can be viewed as &[u8]; length scales correctly.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast(), v.len() * 8) }
}

fn as_bytes_mut(v: &mut [u64]) -> &mut [u8] {
    // SAFETY: same as above, with exclusive access.
    unsafe {
        std::slice::from_raw_parts_mut(v.as_mut_ptr().cast(), v.len() * 8)
    }
}

fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

fn write_u64(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_ne_bytes());
}

fn write_u32(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_ne_bytes());
}

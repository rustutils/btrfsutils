//! Safe wrapper for the btrfs defragment ioctl.
//!
//! Provides [`defrag_range`], which defragments a byte range within a file or
//! an entire file on a btrfs filesystem.

use std::mem;
use std::os::fd::AsRawFd;
use std::os::unix::io::BorrowedFd;

use crate::raw::{
    BTRFS_DEFRAG_RANGE_COMPRESS, BTRFS_DEFRAG_RANGE_COMPRESS_LEVEL, BTRFS_DEFRAG_RANGE_NOCOMPRESS,
    BTRFS_DEFRAG_RANGE_START_IO, btrfs_ioc_defrag_range, btrfs_ioctl_defrag_range_args,
};

/// Compression algorithm to use when defragmenting.
///
/// Corresponds to the `BTRFS_COMPRESS_*` values from `compression.h`.
/// The numeric values are part of the on-disk/ioctl ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressType {
    Zlib = 1,
    Lzo = 2,
    Zstd = 3,
}

impl CompressType {
    /// Parse a compress type from a string name, as accepted by the
    /// `btrfs filesystem defrag -c` option.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "zlib" => Some(Self::Zlib),
            "lzo" => Some(Self::Lzo),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Arguments for a defragmentation operation.
///
/// Construct with [`DefragRangeArgs::new`] and use the builder methods to set
/// options. All options are optional; the defaults match the kernel's defaults.
#[derive(Debug, Clone)]
pub struct DefragRangeArgs {
    /// Start offset in bytes. Defaults to `0`.
    pub start: u64,
    /// Number of bytes to defragment. Defaults to `u64::MAX` (the entire file).
    pub len: u64,
    /// Flush dirty pages to disk immediately after defragmenting.
    pub flush: bool,
    /// Extents larger than this threshold are considered already defragmented
    /// and will not be rewritten. `0` uses the kernel default (32 MiB as of
    /// recent kernels). `1` forces every extent to be rewritten.
    pub extent_thresh: u32,
    /// Compress the file while defragmenting. `None` leaves the file's
    /// existing compression attribute unchanged.
    pub compress: Option<CompressSpec>,
    /// Explicitly disable compression during defragmentation (uncompress if
    /// necessary). Mutually exclusive with `compress`.
    pub nocomp: bool,
}

/// Compression specification for [`DefragRangeArgs`].
#[derive(Debug, Clone, Copy)]
pub struct CompressSpec {
    /// Compression algorithm to use.
    pub compress_type: CompressType,
    /// Optional compression level. When `None`, the kernel default for the
    /// chosen algorithm is used. When `Some`, the
    /// `BTRFS_DEFRAG_RANGE_COMPRESS_LEVEL` flag is set and the level is
    /// passed via the `compress.level` union member.
    pub level: Option<i8>,
}

impl DefragRangeArgs {
    /// Create a new `DefragRangeArgs` with all defaults: defragment the
    /// entire file, no compression change, no flush.
    pub fn new() -> Self {
        Self {
            start: 0,
            len: u64::MAX,
            flush: false,
            extent_thresh: 0,
            compress: None,
            nocomp: false,
        }
    }

    /// Set the start offset in bytes.
    pub fn start(mut self, start: u64) -> Self {
        self.start = start;
        self
    }

    /// Set the number of bytes to defragment.
    pub fn len(mut self, len: u64) -> Self {
        self.len = len;
        self
    }

    /// Flush dirty data to disk after defragmenting.
    pub fn flush(mut self) -> Self {
        self.flush = true;
        self
    }

    /// Set the extent size threshold. Extents larger than this will not be
    /// rewritten.
    pub fn extent_thresh(mut self, thresh: u32) -> Self {
        self.extent_thresh = thresh;
        self
    }

    /// Compress the file using the given algorithm while defragmenting.
    pub fn compress(mut self, spec: CompressSpec) -> Self {
        self.compress = Some(spec);
        self.nocomp = false;
        self
    }

    /// Disable compression while defragmenting (decompresses existing data).
    pub fn nocomp(mut self) -> Self {
        self.nocomp = true;
        self.compress = None;
        self
    }
}

impl Default for DefragRangeArgs {
    fn default() -> Self {
        Self::new()
    }
}

/// Defragment a byte range of the file referred to by `fd`.
///
/// `fd` must be an open file descriptor to a regular file on a btrfs
/// filesystem. Pass `&DefragRangeArgs::new()` to defragment the entire file
/// with default settings.
pub fn defrag_range(fd: BorrowedFd, args: &DefragRangeArgs) -> nix::Result<()> {
    let mut raw: btrfs_ioctl_defrag_range_args = unsafe { mem::zeroed() };

    raw.start = args.start;
    raw.len = args.len;
    raw.extent_thresh = args.extent_thresh;

    if args.flush {
        raw.flags |= BTRFS_DEFRAG_RANGE_START_IO as u64;
    }

    if args.nocomp {
        raw.flags |= BTRFS_DEFRAG_RANGE_NOCOMPRESS as u64;
    } else if let Some(spec) = args.compress {
        raw.flags |= BTRFS_DEFRAG_RANGE_COMPRESS as u64;
        match spec.level {
            None => {
                raw.__bindgen_anon_1.compress_type = spec.compress_type as u32;
            }
            Some(level) => {
                raw.flags |= BTRFS_DEFRAG_RANGE_COMPRESS_LEVEL as u64;
                raw.__bindgen_anon_1.compress.type_ = spec.compress_type as u8;
                raw.__bindgen_anon_1.compress.level = level;
            }
        }
    }

    unsafe { btrfs_ioc_defrag_range(fd.as_raw_fd(), &mut raw) }?;
    Ok(())
}

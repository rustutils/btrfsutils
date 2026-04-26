//! # Write helpers for tree blocks and superblocks
//!
//! Re-exports [`btrfs_disk::superblock::ChecksumType`] for the rest of mkfs
//! and provides a thin pwrite helper. Checksum dispatch lives in `btrfs-disk`
//! ([`btrfs_disk::util::csum_tree_block`] for tree blocks and
//! [`btrfs_disk::superblock::csum_superblock`] for superblocks).

use btrfs_disk::raw;
pub use btrfs_disk::superblock::ChecksumType;
use std::{io, mem, os::unix::io::AsRawFd};

/// Write `buf` to `fd` at byte offset `offset` using pwrite.
///
/// # Errors
///
/// Returns an I/O error if the write fails or produces a zero-length write.
#[allow(clippy::cast_possible_truncation)] // offset fits in off_t
#[allow(clippy::cast_possible_wrap)] // offset fits in off_t
#[allow(clippy::cast_sign_loss)] // pwrite returns positive on success
#[allow(clippy::ptr_cast_constness)]
pub fn pwrite_all(
    fd: &impl AsRawFd,
    buf: &[u8],
    offset: u64,
) -> io::Result<()> {
    let raw_fd = fd.as_raw_fd();
    let mut written = 0;
    while written < buf.len() {
        let ret = unsafe {
            libc::pwrite(
                raw_fd,
                buf[written..].as_ptr().cast::<libc::c_void>(),
                buf.len() - written,
                (offset + written as u64) as libc::off_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if ret == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }
        written += ret as usize;
    }
    Ok(())
}

/// Size of the superblock on disk (4096 bytes).
pub const SUPER_INFO_SIZE: usize = mem::size_of::<raw::btrfs_super_block>();

/// Byte offset of the primary superblock on disk (64 KiB).
/// From kernel-shared/ctree.h: `BTRFS_SUPER_INFO_OFFSET`
pub const SUPER_INFO_OFFSET: u64 = 65536;

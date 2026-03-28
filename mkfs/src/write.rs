//! # Write: checksumming and disk I/O for tree blocks and superblocks
//!
//! Provides CRC32C checksum computation (raw, no inversion — matching the
//! btrfs kernel convention) and pwrite helpers for writing blocks to disk.

use btrfs_disk::raw;
use std::{io, mem, os::unix::io::AsRawFd};

/// Size of the checksum field at the start of every tree block and superblock.
const CSUM_SIZE: usize = raw::BTRFS_CSUM_SIZE as usize;

/// Compute a standard CRC32C checksum (init=0xFFFFFFFF, xorout=0xFFFFFFFF).
///
/// This is what btrfs uses for tree block and superblock checksums.
/// The `crc32c` crate's `crc32c()` function computes exactly this.
///
/// Note: the send stream uses a *different* convention (raw CRC32C with
/// init=0, no inversion), but that's handled in the stream crate.
fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute and fill the checksum for a tree block (or superblock).
///
/// Checksums bytes `CSUM_SIZE..len` and writes the result into `buf[0..4]`
/// (the first 4 bytes of the 32-byte csum field; remaining bytes stay zero).
pub fn fill_csum(buf: &mut [u8]) {
    assert!(buf.len() > CSUM_SIZE);
    let crc = crc32c(&buf[CSUM_SIZE..]);
    buf[..4].copy_from_slice(&crc.to_le_bytes());
}

/// Write `buf` to `fd` at byte offset `offset` using pwrite.
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
                buf[written..].as_ptr() as *const libc::c_void,
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
/// From kernel-shared/ctree.h: BTRFS_SUPER_INFO_OFFSET
pub const SUPER_INFO_OFFSET: u64 = 65536;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_value() {
        // Standard CRC32C of "123456789" is 0xE3069283.
        assert_eq!(crc32c(b"123456789"), 0xE3069283);
    }

    #[test]
    fn crc32c_empty() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn fill_csum_writes_first_4_bytes() {
        let mut buf = vec![0u8; 64];
        // Put some data after the csum field
        for (i, b) in buf[CSUM_SIZE..].iter_mut().enumerate() {
            *b = i as u8;
        }
        fill_csum(&mut buf);

        // First 4 bytes should be the LE CRC32C of buf[32..64]
        let expected = crc32c(&buf[CSUM_SIZE..]);
        let stored = u32::from_le_bytes(buf[..4].try_into().unwrap());
        assert_eq!(stored, expected);

        // Bytes 4..32 should still be zero
        assert!(buf[4..CSUM_SIZE].iter().all(|&b| b == 0));
    }
}

//! # Write: checksumming and disk I/O for tree blocks and superblocks
//!
//! Provides checksum computation (CRC32C, xxhash64, SHA256, BLAKE2b) and
//! pwrite helpers for writing blocks to disk.

use btrfs_disk::raw;
use std::{io, mem, os::unix::io::AsRawFd};

/// Size of the checksum field at the start of every tree block and superblock.
const CSUM_SIZE: usize = raw::BTRFS_CSUM_SIZE as usize;

/// Supported checksum algorithms, matching the on-disk `csum_type` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    Crc32c,
    Xxhash64,
    Sha256,
    Blake2b,
}

impl ChecksumType {
    /// The on-disk `csum_type` value for this algorithm.
    pub fn to_raw(self) -> u16 {
        match self {
            ChecksumType::Crc32c => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 as u16
            }
            ChecksumType::Xxhash64 => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH as u16
            }
            ChecksumType::Sha256 => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 as u16
            }
            ChecksumType::Blake2b => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 as u16
            }
        }
    }

    /// Number of bytes in the checksum output.
    pub fn size(self) -> usize {
        match self {
            ChecksumType::Crc32c => 4,
            ChecksumType::Xxhash64 => 8,
            ChecksumType::Sha256 => 32,
            ChecksumType::Blake2b => 32,
        }
    }

    /// Compute the checksum of `data` and return the result bytes.
    pub fn compute(self, data: &[u8]) -> Vec<u8> {
        match self {
            ChecksumType::Crc32c => crc32c::crc32c(data).to_le_bytes().to_vec(),
            ChecksumType::Xxhash64 => {
                xxhash_rust::xxh64::xxh64(data, 0).to_le_bytes().to_vec()
            }
            ChecksumType::Sha256 => {
                use sha2::Digest;
                sha2::Sha256::digest(data).to_vec()
            }
            ChecksumType::Blake2b => {
                use blake2::{Blake2b, Digest, digest::consts::U32};
                <Blake2b<U32>>::digest(data).to_vec()
            }
        }
    }
}

/// Compute and fill the checksum for a tree block (or superblock).
///
/// Checksums bytes `CSUM_SIZE..len` and writes the result into the
/// first `csum_type.size()` bytes of `buf`. Remaining csum field bytes
/// stay zero.
pub fn fill_csum(buf: &mut [u8], csum_type: ChecksumType) {
    assert!(buf.len() > CSUM_SIZE);
    let hash = csum_type.compute(&buf[CSUM_SIZE..]);
    buf[..hash.len()].copy_from_slice(&hash);
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
        let hash = ChecksumType::Crc32c.compute(b"123456789");
        assert_eq!(hash, 0xE3069283u32.to_le_bytes());
    }

    #[test]
    fn crc32c_empty() {
        let hash = ChecksumType::Crc32c.compute(b"");
        assert_eq!(hash, 0u32.to_le_bytes());
    }

    #[test]
    fn xxhash64_deterministic() {
        let hash = ChecksumType::Xxhash64.compute(b"123456789");
        assert_eq!(hash.len(), 8);
        // Verify deterministic: same input → same output.
        assert_eq!(hash, ChecksumType::Xxhash64.compute(b"123456789"));
        // Different input → different output.
        assert_ne!(hash, ChecksumType::Xxhash64.compute(b"12345678"));
    }

    #[test]
    fn sha256_output_size() {
        let hash = ChecksumType::Sha256.compute(b"test");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn blake2b_output_size() {
        let hash = ChecksumType::Blake2b.compute(b"test");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn csum_sizes() {
        assert_eq!(ChecksumType::Crc32c.size(), 4);
        assert_eq!(ChecksumType::Xxhash64.size(), 8);
        assert_eq!(ChecksumType::Sha256.size(), 32);
        assert_eq!(ChecksumType::Blake2b.size(), 32);
    }

    #[test]
    fn fill_csum_writes_correct_bytes() {
        let mut buf = vec![0u8; 64];
        for (i, b) in buf[CSUM_SIZE..].iter_mut().enumerate() {
            *b = i as u8;
        }
        fill_csum(&mut buf, ChecksumType::Crc32c);

        let expected = ChecksumType::Crc32c.compute(&buf[CSUM_SIZE..]);
        assert_eq!(&buf[..4], &expected[..]);
        // Bytes 4..32 should still be zero
        assert!(buf[4..CSUM_SIZE].iter().all(|&b| b == 0));
    }

    #[test]
    fn to_raw_values() {
        assert_eq!(ChecksumType::Crc32c.to_raw(), 0);
        assert_eq!(ChecksumType::Xxhash64.to_raw(), 1);
        assert_eq!(ChecksumType::Sha256.to_raw(), 2);
        assert_eq!(ChecksumType::Blake2b.to_raw(), 3);
    }
}

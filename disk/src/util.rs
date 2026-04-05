//! # Shared helpers for on-disk structures
//!
//! Little-endian writer functions for placing typed values into raw byte
//! buffers at known offsets, and a raw CRC32C matching the kernel's format.

use bytes::Buf;
use uuid::Uuid;

/// Read a UUID (16 bytes) from a `Buf`, advancing the cursor.
///
/// # Panics
///
/// Panics if `buf` has fewer than 16 bytes remaining.
pub fn get_uuid(buf: &mut &[u8]) -> Uuid {
    let bytes: [u8; 16] = buf[..16].try_into().unwrap();
    buf.advance(16);
    Uuid::from_bytes(bytes)
}

/// Write a little-endian u64 into `buf` at byte offset `off`.
pub fn write_le_u64(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

/// Write a little-endian u32 into `buf` at byte offset `off`.
pub fn write_le_u32(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

/// Write a little-endian u16 into `buf` at byte offset `off`.
pub fn write_le_u16(buf: &mut [u8], off: usize, val: u16) {
    buf[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

/// Write a UUID (16 bytes) into `buf` at byte offset `off`.
pub fn write_uuid(buf: &mut [u8], off: usize, uuid: &Uuid) {
    buf[off..off + 16].copy_from_slice(uuid.as_bytes());
}

/// Raw CRC32C matching the kernel's `crc32c_le()` function.
///
/// The seed is passed through directly with no inversion on input or output,
/// unlike the standard ISO 3309 CRC32C which inverts both. This is NOT the
/// function used for on-disk checksums (superblocks, tree blocks, data csums).
/// Use this only for internal hash computations like `extent_data_ref_hash`
/// where the C code calls `crc32c(seed, data, len)` (which maps to
/// `crc32c_le`).
#[must_use]
pub fn raw_crc32c(seed: u32, data: &[u8]) -> u32 {
    // crc32c::crc32c_append(seed) computes: !crc32c_hw(!seed, data)
    // We want: crc32c_hw(seed, data)
    // So: !crc32c::crc32c_append(!seed, data)
    !crc32c::crc32c_append(!seed, data)
}

/// Btrfs name hash for `DIR_ITEM` and `DIR_INDEX` key offsets.
///
/// Raw CRC32C with seed `0xFFFFFFFE` (`~1`) and no finalization XOR.
/// This matches the kernel's `btrfs_name_hash()`.
#[must_use]
pub fn btrfs_name_hash(name: &[u8]) -> u32 {
    raw_crc32c(!1u32, name)
}

/// Standard CRC32C matching the kernel's `hash_crc32c()` / btrfs on-disk
/// checksum format.
///
/// This is the function used for all on-disk checksums: superblocks, tree
/// blocks, and data checksums. The kernel computes these via `hash_crc32c`
/// which calls `crc32c_le(~0, data, len)` and then inverts the result,
/// which is equivalent to standard ISO 3309 CRC32C.
#[must_use]
pub fn btrfs_csum_data(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Recompute the CRC32C checksum of a tree block and write it into the header.
///
/// The checksum covers `buf[32..]` (everything after the csum field).
/// The 4-byte LE result is written to `buf[0..4]` and `buf[4..32]` is zeroed.
/// This is the same algorithm as `superblock::csum_superblock` but for
/// arbitrary-length tree blocks (nodesize bytes).
///
/// # Panics
///
/// Panics if `buf` is 32 bytes or smaller.
pub fn csum_tree_block(buf: &mut [u8]) {
    assert!(buf.len() > 32, "buffer too small for tree block checksum");
    let csum = btrfs_csum_data(&buf[32..]);
    buf[0..4].copy_from_slice(&csum.to_le_bytes());
    buf[4..32].fill(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_le_u64() {
        let mut buf = [0u8; 8];
        write_le_u64(&mut buf, 0, 0x0807060504030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_write_le_u32() {
        let mut buf = [0u8; 4];
        write_le_u32(&mut buf, 0, 0x04030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_write_le_u16() {
        let mut buf = [0u8; 2];
        write_le_u16(&mut buf, 0, 0x0201);
        assert_eq!(buf, [0x01, 0x02]);
    }

    #[test]
    fn test_write_uuid() {
        let uuid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let mut buf = [0u8; 16];
        write_uuid(&mut buf, 0, &uuid);
        assert_eq!(buf, *uuid.as_bytes());
    }

    #[test]
    fn test_roundtrip_u64() {
        let mut buf = [0u8; 16];
        write_le_u64(&mut buf, 4, 0xDEADBEEF_CAFEBABE);
        assert_eq!(
            u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            0xDEADBEEF_CAFEBABE
        );
    }

    #[test]
    fn test_btrfs_name_hash() {
        // Verified against dump-tree output from a real btrfs filesystem
        assert_eq!(btrfs_name_hash(b"hello.txt"), 0x415f_eb59);
        // Different names produce different hashes
        assert_ne!(
            btrfs_name_hash(b"hello.txt"),
            btrfs_name_hash(b"world.txt")
        );
    }

    #[test]
    fn test_roundtrip_uuid() {
        let uuid =
            Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        let mut buf = [0u8; 16];
        write_uuid(&mut buf, 0, &uuid);
        assert_eq!(Uuid::from_bytes(buf), uuid);
    }
}

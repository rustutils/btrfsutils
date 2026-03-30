//! # Shared helpers for on-disk structures
//!
//! Little-endian writer functions for placing typed values into raw byte
//! buffers at known offsets, and a raw CRC32C matching the kernel's format.

use uuid::Uuid;

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

/// Raw CRC32C matching the kernel's `crc32c()` function.
///
/// The seed is passed through directly with no inversion on input or output,
/// unlike the standard ISO 3309 CRC32C which inverts both. Use this when
/// computing btrfs on-disk checksums.
pub fn raw_crc32c(seed: u32, data: &[u8]) -> u32 {
    // crc32c::crc32c_append(seed) computes: !crc32c_hw(!seed, data)
    // We want: crc32c_hw(seed, data)
    // So: !crc32c::crc32c_append(!seed, data)
    !crc32c::crc32c_append(!seed, data)
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
    fn test_roundtrip_uuid() {
        let uuid =
            Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        let mut buf = [0u8; 16];
        write_uuid(&mut buf, 0, &uuid);
        assert_eq!(Uuid::from_bytes(buf), uuid);
    }
}

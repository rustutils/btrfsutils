//! # Utilities: shared parsing helpers for on-disk structures
//!
//! Little-endian reader functions for extracting typed values from raw byte
//! buffers at known offsets. Used throughout the disk crate to parse packed
//! on-disk structures safely without pointer casts.

use uuid::Uuid;

/// Read a little-endian u64 from `buf` at byte offset `off`.
pub fn read_le_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Read a little-endian u32 from `buf` at byte offset `off`.
pub fn read_le_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

/// Read a little-endian u16 from `buf` at byte offset `off`.
pub fn read_le_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

/// Read a UUID (16 bytes, big-endian byte order) from `buf` at byte offset `off`.
pub fn read_uuid(buf: &[u8], off: usize) -> Uuid {
    Uuid::from_bytes(buf[off..off + 16].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_le_u64() {
        let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(read_le_u64(&buf, 0), 0x0807060504030201);
    }

    #[test]
    fn test_read_le_u64_with_offset() {
        let buf = [0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(read_le_u64(&buf, 2), 0x0807060504030201);
    }

    #[test]
    fn test_read_le_u32() {
        let buf = [0x01, 0x02, 0x03, 0x04];
        assert_eq!(read_le_u32(&buf, 0), 0x04030201);
    }

    #[test]
    fn test_read_le_u16() {
        let buf = [0x01, 0x02];
        assert_eq!(read_le_u16(&buf, 0), 0x0201);
    }

    #[test]
    fn test_read_uuid() {
        let bytes = [
            0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe,
            0xef, 0xde, 0xad, 0xbe, 0xef,
        ];
        let uuid = read_uuid(&bytes, 0);
        assert_eq!(uuid.to_string(), "deadbeef-dead-beef-dead-beefdeadbeef");
    }
}

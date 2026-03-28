//! # Shared utilities for the uapi crate

/// Read a little-endian `u64` from `buf` at byte offset `off`.
pub fn read_le_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Read a little-endian `u32` from `buf` at byte offset `off`.
pub fn read_le_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

/// Read a little-endian `u16` from `buf` at byte offset `off`.
pub fn read_le_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

/// Return the size in bytes of a single field within a struct.
///
/// This is the field-level counterpart of `std::mem::size_of::<T>()`.
/// The size is derived from the struct layout produced by bindgen, so it
/// stays in sync with the kernel headers automatically.
///
/// ```ignore
/// use btrfs_uapi::field_size;
/// use btrfs_uapi::raw::btrfs_root_item;
///
/// assert_eq!(field_size!(btrfs_root_item, uuid), 16);
/// ```
#[macro_export]
macro_rules! field_size {
    ($t:ty, $f:ident) => {{
        // Compute the field size via raw pointer arithmetic on a
        // MaybeUninit, avoiding references to potentially misaligned
        // fields in packed structs.
        let uninit = std::mem::MaybeUninit::<$t>::uninit();
        let base = uninit.as_ptr();

        // SAFETY: we never dereference the pointer or read the
        // uninitialised memory; addr_of! only computes an address.
        let field_ptr = unsafe { std::ptr::addr_of!((*base).$f) };

        // The size of the field is the distance from the field pointer
        // to the end of the field, computed as (field_ptr + 1) - field_ptr
        // in bytes (pointer arithmetic on the field's type).
        //
        // SAFETY: field_ptr and field_ptr.add(1) are both within (or one
        // past) the same allocation represented by `uninit`.
        unsafe {
            (field_ptr.add(1) as *const u8).offset_from(field_ptr as *const u8)
                as usize
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::{
        btrfs_dev_extent, btrfs_qgroup_info_item, btrfs_qgroup_limit_item,
        btrfs_qgroup_status_item, btrfs_root_item, btrfs_stripe,
    };

    #[test]
    fn field_size_matches_expected() {
        // UUID fields are [u8; 16].
        assert_eq!(field_size!(btrfs_root_item, uuid), 16);
        assert_eq!(field_size!(btrfs_root_item, parent_uuid), 16);
        assert_eq!(field_size!(btrfs_root_item, received_uuid), 16);

        // Scalar __le64 fields are 8 bytes.
        assert_eq!(field_size!(btrfs_root_item, generation), 8);
        assert_eq!(field_size!(btrfs_root_item, flags), 8);
        assert_eq!(field_size!(btrfs_qgroup_info_item, rfer), 8);
        assert_eq!(field_size!(btrfs_qgroup_limit_item, max_rfer), 8);
        assert_eq!(field_size!(btrfs_qgroup_status_item, flags), 8);
        assert_eq!(field_size!(btrfs_dev_extent, length), 8);

        // Stripe dev_uuid is [u8; 16].
        assert_eq!(field_size!(btrfs_stripe, dev_uuid), 16);
    }

    #[test]
    fn read_le_u64_basic() {
        let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(read_le_u64(&buf, 0), 0x0807060504030201);
    }

    #[test]
    fn read_le_u64_at_offset() {
        let buf = [0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(read_le_u64(&buf, 2), 1);
    }

    #[test]
    fn read_le_u32_basic() {
        let buf = [0x78, 0x56, 0x34, 0x12];
        assert_eq!(read_le_u32(&buf, 0), 0x12345678);
    }

    #[test]
    fn read_le_u16_basic() {
        let buf = [0x02, 0x01];
        assert_eq!(read_le_u16(&buf, 0), 0x0102);
    }
}

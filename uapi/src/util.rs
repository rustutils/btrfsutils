//! # Shared utilities for the uapi crate
//!
//! Provides the `field_size!` macro for computing field sizes from bindgen
//! struct definitions.

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
            (field_ptr.add(1).cast::<u8>()).offset_from(field_ptr.cast::<u8>())
                as usize
        }
    }};
}

#[cfg(test)]
mod tests {
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
}

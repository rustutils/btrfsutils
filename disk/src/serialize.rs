//! # Extent item serialization
//!
//! Serialization functions for extent tree items that don't have a natural
//! owning struct. Item types with structs (`RootItem`, `BlockGroupItem`, etc.)
//! have `to_bytes()` methods directly on the struct in `items.rs`.

use crate::{tree::DiskKey, util::write_disk_key};
use bytes::BufMut;

/// Size of a skinny metadata extent item with one `TREE_BLOCK_REF` inline backref.
///
/// Layout: extent item header (24) + inline ref type (1) + offset (8) = 33 bytes.
pub const METADATA_EXTENT_ITEM_SIZE: usize = 33;

/// Size of a non-skinny metadata extent item with `tree_block_info` and
/// one `TREE_BLOCK_REF` inline backref.
///
/// Layout: `extent_item` (24) + `tree_block_info` (18) + inline ref (9) = 51 bytes.
pub const NON_SKINNY_METADATA_EXTENT_ITEM_SIZE: usize = 51;

/// Serialize a metadata extent item (`METADATA_ITEM`) with a single
/// `TREE_BLOCK_REF` inline backref.
///
/// This is the on-disk format for a tree block extent when the
/// `SKINNY_METADATA` incompat flag is set (modern default). The key is
/// `(bytenr, METADATA_ITEM=169, level)`.
///
/// Data layout (33 bytes):
/// - Extent item header (24 bytes): refs (u64) + generation (u64) + flags (u64)
/// - Inline backref (9 bytes): type (u8, `TREE_BLOCK_REF`=176) + offset (u64, `root_id`)
#[must_use]
pub fn metadata_extent_item_to_bytes(
    refs: u64,
    generation: u64,
    root_id: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(METADATA_EXTENT_ITEM_SIZE);

    // Extent item header
    buf.put_u64_le(refs);
    buf.put_u64_le(generation);
    buf.put_u64_le(crate::items::ExtentFlags::TREE_BLOCK.bits());

    // Inline TREE_BLOCK_REF: type byte + root_id as offset
    buf.put_u8(crate::tree::KeyType::TreeBlockRef.to_raw());
    buf.put_u64_le(root_id);

    debug_assert_eq!(buf.len(), METADATA_EXTENT_ITEM_SIZE);
    buf
}

/// Serialize a non-skinny metadata extent item (`EXTENT_ITEM`) with a
/// `tree_block_info` structure and a `TREE_BLOCK_REF` inline backref.
///
/// This is the on-disk format for old filesystems without the
/// `SKINNY_METADATA` incompat flag. The key is
/// `(bytenr, EXTENT_ITEM=168, nodesize)`.
///
/// Data layout (51 bytes):
/// - Extent item header (24 bytes): refs (u64) + generation (u64) + flags (u64)
/// - `tree_block_info` (18 bytes): first key (17 bytes) + level (u8)
/// - Inline backref (9 bytes): type (u8, `TREE_BLOCK_REF=176`) + offset (u64, `root_id`)
#[must_use]
pub fn non_skinny_metadata_extent_item_to_bytes(
    refs: u64,
    generation: u64,
    root_id: u64,
    first_key: &DiskKey,
    level: u8,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(NON_SKINNY_METADATA_EXTENT_ITEM_SIZE);

    // Extent item header
    buf.put_u64_le(refs);
    buf.put_u64_le(generation);
    buf.put_u64_le(crate::items::ExtentFlags::TREE_BLOCK.bits());

    // tree_block_info: first key + level
    let key_off = buf.len();
    buf.extend_from_slice(&[0u8; 17]);
    write_disk_key(&mut buf[key_off..], 0, first_key);
    buf.put_u8(level);

    // Inline TREE_BLOCK_REF: type byte + root_id as offset
    buf.put_u8(crate::tree::KeyType::TreeBlockRef.to_raw());
    buf.put_u64_le(root_id);

    debug_assert_eq!(buf.len(), NON_SKINNY_METADATA_EXTENT_ITEM_SIZE);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skinny_metadata_extent_size() {
        let bytes = metadata_extent_item_to_bytes(1, 42, 5);
        assert_eq!(bytes.len(), METADATA_EXTENT_ITEM_SIZE);
        assert_eq!(bytes.len(), 33);
    }

    #[test]
    fn non_skinny_metadata_extent_size() {
        let key = DiskKey {
            objectid: 256,
            key_type: crate::tree::KeyType::InodeItem,
            offset: 0,
        };
        let bytes = non_skinny_metadata_extent_item_to_bytes(1, 42, 5, &key, 0);
        assert_eq!(bytes.len(), NON_SKINNY_METADATA_EXTENT_ITEM_SIZE);
        assert_eq!(bytes.len(), 51);
    }

    #[test]
    fn skinny_vs_non_skinny_header_match() {
        let skinny = metadata_extent_item_to_bytes(1, 42, 5);
        let key = DiskKey {
            objectid: 0,
            key_type: crate::tree::KeyType::from_raw(0),
            offset: 0,
        };
        let non_skinny =
            non_skinny_metadata_extent_item_to_bytes(1, 42, 5, &key, 0);
        // First 24 bytes (refs + generation + flags) should be identical
        assert_eq!(&skinny[..24], &non_skinny[..24]);
    }

    #[test]
    fn metadata_extent_flags() {
        let bytes = metadata_extent_item_to_bytes(1, 42, 5);
        let flags = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        assert_eq!(flags, crate::items::ExtentFlags::TREE_BLOCK.bits());
    }
}

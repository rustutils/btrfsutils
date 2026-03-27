//! # Chunk: logical-to-physical address mapping for btrfs filesystems
//!
//! Btrfs maps logical addresses to physical device offsets through chunk items
//! stored in the chunk tree. The superblock embeds a small subset of the chunk
//! tree (the system chunk array) to bootstrap access to the full chunk tree.
//!
//! This module provides a `ChunkTreeCache` that resolves logical addresses to
//! physical offsets, seeded from the sys_chunk_array and then populated from
//! the full chunk tree.

use crate::{
    raw,
    util::{read_le_u16, read_le_u64, read_uuid},
};
use std::{collections::BTreeMap, mem};
use uuid::Uuid;

/// A single stripe in a chunk mapping.
#[derive(Debug, Clone)]
pub struct Stripe {
    pub devid: u64,
    pub offset: u64,
    pub dev_uuid: Uuid,
}

/// A chunk mapping: maps a range of logical addresses to physical device locations.
#[derive(Debug, Clone)]
pub struct ChunkMapping {
    pub logical: u64,
    pub length: u64,
    pub stripe_len: u64,
    pub chunk_type: u64,
    pub num_stripes: u16,
    pub sub_stripes: u16,
    pub stripes: Vec<Stripe>,
}

/// Cache of chunk tree mappings for resolving logical to physical addresses.
///
/// Keyed by logical start address. Uses a BTreeMap for efficient range lookups.
#[derive(Debug, Default)]
pub struct ChunkTreeCache {
    inner: BTreeMap<u64, ChunkMapping>,
}

impl ChunkTreeCache {
    /// Insert a chunk mapping into the cache.
    pub fn insert(&mut self, mapping: ChunkMapping) {
        self.inner.insert(mapping.logical, mapping);
    }

    /// Look up the chunk mapping that contains the given logical address.
    pub fn lookup(&self, logical: u64) -> Option<&ChunkMapping> {
        // Find the entry whose start is <= logical
        self.inner
            .range(..=logical)
            .next_back()
            .map(|(_, mapping)| mapping)
            .filter(|mapping| logical < mapping.logical + mapping.length)
    }

    /// Resolve a logical address to a physical byte offset on the first stripe.
    ///
    /// For read-only access (like dump-tree), the first stripe is sufficient
    /// for single, DUP, and RAID1 profiles. RAID0/5/6/10 would need stripe
    /// index calculation, but for the common case this works.
    pub fn resolve(&self, logical: u64) -> Option<u64> {
        let mapping = self.lookup(logical)?;
        let offset_within_chunk = logical - mapping.logical;
        Some(mapping.stripes[0].offset + offset_within_chunk)
    }

    /// Return the number of cached chunk mappings.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Parse a chunk item (btrfs_chunk + stripes) from a raw byte buffer.
///
/// Returns the chunk mapping and the total number of bytes consumed.
/// `logical` is the logical start address from the key's offset field.
pub fn parse_chunk_item(
    buf: &[u8],
    logical: u64,
) -> Option<(ChunkMapping, usize)> {
    let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();

    if buf.len() < chunk_base_size {
        return None;
    }

    let length = read_le_u64(buf, 0);
    let stripe_len = read_le_u64(buf, 16);
    let chunk_type = read_le_u64(buf, 24);
    let num_stripes = read_le_u16(buf, 44);
    let sub_stripes = read_le_u16(buf, 46);

    let total_size = chunk_base_size + num_stripes as usize * stripe_size;
    if buf.len() < total_size {
        return None;
    }

    let mut stripes = Vec::with_capacity(num_stripes as usize);
    for i in 0..num_stripes as usize {
        let s_off = chunk_base_size + i * stripe_size;
        stripes.push(Stripe {
            devid: read_le_u64(buf, s_off),
            offset: read_le_u64(buf, s_off + 8),
            dev_uuid: read_uuid(buf, s_off + 16),
        });
    }

    let mapping = ChunkMapping {
        logical,
        length,
        stripe_len,
        chunk_type,
        num_stripes,
        sub_stripes,
        stripes,
    };

    Some((mapping, total_size))
}

/// Seed a `ChunkTreeCache` from the superblock's sys_chunk_array.
///
/// The sys_chunk_array contains a subset of chunk items needed to bootstrap
/// access to the full chunk tree (system profile chunks).
pub fn seed_from_sys_chunk_array(array: &[u8], size: u32) -> ChunkTreeCache {
    let array = &array[..size as usize];
    let mut cache = ChunkTreeCache::default();

    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let mut offset = 0usize;

    while offset + disk_key_size <= array.len() {
        let key_offset = read_le_u64(array, offset + 9);
        offset += disk_key_size;

        if let Some((mapping, consumed)) =
            parse_chunk_item(&array[offset..], key_offset)
        {
            cache.insert(mapping);
            offset += consumed;
        } else {
            break;
        }
    }

    cache
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mapping(logical: u64, length: u64, physical: u64) -> ChunkMapping {
        ChunkMapping {
            logical,
            length,
            stripe_len: 65536,
            chunk_type: 0,
            num_stripes: 1,
            sub_stripes: 0,
            stripes: vec![Stripe {
                devid: 1,
                offset: physical,
                dev_uuid: Uuid::nil(),
            }],
        }
    }

    #[test]
    fn empty_cache() {
        let cache = ChunkTreeCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.resolve(0), None);
    }

    #[test]
    fn single_mapping() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(1000, 500, 2000));
        assert_eq!(cache.len(), 1);

        assert_eq!(cache.resolve(1000), Some(2000));
        assert_eq!(cache.resolve(1100), Some(2100));
        assert_eq!(cache.resolve(1499), Some(2499));
        assert_eq!(cache.resolve(1500), None); // past end
        assert_eq!(cache.resolve(999), None); // before start
    }

    #[test]
    fn multiple_mappings() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(0, 1000, 5000));
        cache.insert(make_mapping(1000, 1000, 6000));
        cache.insert(make_mapping(5000, 2000, 10000));

        assert_eq!(cache.resolve(0), Some(5000));
        assert_eq!(cache.resolve(500), Some(5500));
        assert_eq!(cache.resolve(1000), Some(6000));
        assert_eq!(cache.resolve(1999), Some(6999));
        assert_eq!(cache.resolve(2000), None); // gap
        assert_eq!(cache.resolve(5000), Some(10000));
        assert_eq!(cache.resolve(6999), Some(11999));
        assert_eq!(cache.resolve(7000), None);
    }

    #[test]
    fn lookup_returns_mapping() {
        let mut cache = ChunkTreeCache::default();
        cache.insert(make_mapping(1000, 500, 2000));

        let m = cache.lookup(1100).unwrap();
        assert_eq!(m.logical, 1000);
        assert_eq!(m.length, 500);
        assert!(cache.lookup(500).is_none());
    }

    #[test]
    fn seed_from_empty_array() {
        let array = [0u8; 2048];
        let cache = seed_from_sys_chunk_array(&array, 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn parse_chunk_item_basic() {
        let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
        let stripe_size = mem::size_of::<raw::btrfs_stripe>();
        let total = chunk_base_size + stripe_size;
        let mut buf = vec![0u8; total];

        // length
        buf[0..8].copy_from_slice(&1000u64.to_le_bytes());
        // owner
        buf[8..16].copy_from_slice(&2u64.to_le_bytes());
        // stripe_len
        buf[16..24].copy_from_slice(&65536u64.to_le_bytes());
        // type
        buf[24..32].copy_from_slice(&1u64.to_le_bytes());
        // num_stripes
        buf[44..46].copy_from_slice(&1u16.to_le_bytes());

        // stripe: devid=1, offset=5000
        buf[chunk_base_size..chunk_base_size + 8]
            .copy_from_slice(&1u64.to_le_bytes());
        buf[chunk_base_size + 8..chunk_base_size + 16]
            .copy_from_slice(&5000u64.to_le_bytes());

        let (mapping, consumed) = parse_chunk_item(&buf, 0).unwrap();
        assert_eq!(consumed, total);
        assert_eq!(mapping.logical, 0);
        assert_eq!(mapping.length, 1000);
        assert_eq!(mapping.num_stripes, 1);
        assert_eq!(mapping.stripes[0].devid, 1);
        assert_eq!(mapping.stripes[0].offset, 5000);
    }

    #[test]
    fn parse_chunk_item_too_short() {
        let buf = [0u8; 10];
        assert!(parse_chunk_item(&buf, 0).is_none());
    }
}

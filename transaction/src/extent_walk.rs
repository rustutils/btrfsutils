//! # Read-only extent-tree walker for whole-tree conversions
//!
//! Provides a callback-style scanner over allocated extents inside
//! a single block group, plus a pure free-range derivation helper.
//! Used by [`crate::convert::seed_free_space_tree`] and the
//! `convert_to_*` paths to compute per-block-group free ranges
//! from the extent tree.

use crate::{
    filesystem::Filesystem,
    free_space::Range,
    path::BtrfsPath,
    search::{self, SearchIntent, next_leaf},
};
use btrfs_disk::tree::{DiskKey, KeyType};
use std::io::{self, Read, Seek, Write};

/// One allocated extent inside a block group, as seen by the
/// extent-tree walker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedExtent {
    /// Logical byte offset of the first byte of the extent.
    pub start: u64,
    /// Length of the extent in bytes.
    pub length: u64,
}

impl AllocatedExtent {
    /// Half-open end of the extent.
    #[must_use]
    pub fn end(self) -> u64 {
        self.start + self.length
    }
}

/// Tree id for the extent tree.
const EXTENT_TREE_ID: u64 = 2;

/// Iterate every `EXTENT_ITEM` and `METADATA_ITEM` in the extent
/// tree whose start address falls inside
/// `[bg_start, bg_start + bg_length)`, calling `visit` once per item
/// in ascending logical order.
///
/// `EXTENT_ITEM` keys encode the byte length in the key offset;
/// `METADATA_ITEM` keys encode the level in the key offset, so the
/// extent length is taken from `fs_info.nodesize`. Both data and
/// metadata extents are reported (mixed block groups are handled
/// transparently because the walker keys off addresses, not on a
/// data/metadata classification).
///
/// Other extent-tree item types (`TREE_BLOCK_REF`, `EXTENT_DATA_REF`,
/// `BLOCK_GROUP_ITEM`, etc.) whose compound key happens to fall
/// inside the address range are skipped.
///
/// The walker stops at the first key whose objectid is at or past
/// `bg_start + bg_length`. It also stops if the visitor returns an
/// error, surfacing that error to the caller. The compound-key
/// search may return items whose key type is outside the range; the
/// internal filter handles that.
///
/// Read-only: takes no transaction handle and never COWs.
///
/// # Errors
///
/// * Any block read fails.
/// * The visitor returns an error.
/// * Two yielded extents overlap (filesystem inconsistency).
pub fn walk_block_group_extents<R, F>(
    fs_info: &mut Filesystem<R>,
    bg_start: u64,
    bg_length: u64,
    mut visit: F,
) -> io::Result<()>
where
    R: Read + Write + Seek,
    F: FnMut(AllocatedExtent) -> io::Result<()>,
{
    debug_assert!(
        bg_length > 0,
        "walk_block_group_extents: zero-length block group",
    );
    let bg_end = bg_start.checked_add(bg_length).ok_or_else(|| {
        io::Error::other(
            "walk_block_group_extents: bg_start + bg_length overflows u64",
        )
    })?;
    let nodesize = u64::from(fs_info.nodesize);

    // Position the cursor at (bg_start, 0, 0). The compound-key
    // semantics of search_slot put us at-or-before the first item
    // whose objectid is >= bg_start.
    let start_key = DiskKey {
        objectid: bg_start,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs_info,
        EXTENT_TREE_ID,
        &start_key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;

    // Track the previous extent's end so we can detect overlap
    // (extent items in a healthy tree are sorted and disjoint).
    let mut prev_end: Option<u64> = None;

    loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break;
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            if !next_leaf(fs_info, &mut path)? {
                break;
            }
            continue;
        }
        let k = leaf.item_key(slot);
        if k.objectid >= bg_end {
            break;
        }

        let extent = match k.key_type {
            KeyType::ExtentItem if k.objectid >= bg_start => {
                Some(AllocatedExtent {
                    start: k.objectid,
                    length: k.offset,
                })
            }
            KeyType::MetadataItem if k.objectid >= bg_start => {
                Some(AllocatedExtent {
                    start: k.objectid,
                    length: nodesize,
                })
            }
            _ => None,
        };

        if let Some(ext) = extent {
            if ext.length == 0 {
                path.release();
                return Err(io::Error::other(format!(
                    "walk_block_group_extents: zero-length extent at {}",
                    ext.start,
                )));
            }
            if ext.end() > bg_end {
                path.release();
                return Err(io::Error::other(format!(
                    "walk_block_group_extents: extent [{}, {}) crosses block group end {bg_end}",
                    ext.start,
                    ext.end(),
                )));
            }
            if let Some(p) = prev_end
                && ext.start < p
            {
                path.release();
                return Err(io::Error::other(format!(
                    "walk_block_group_extents: overlapping extents (prev end {p} > start {})",
                    ext.start,
                )));
            }
            prev_end = Some(ext.end());
            if let Err(e) = visit(ext) {
                path.release();
                return Err(e);
            }
        }

        path.slots[0] = slot + 1;
    }

    path.release();
    Ok(())
}

/// Pure derivation of the free ranges complementary to a sorted,
/// non-overlapping list of allocated extents inside a block group.
///
/// Yields each maximal gap between adjacent allocated extents
/// (including the leading gap before the first extent and the
/// trailing gap after the last) in ascending order.
///
/// `allocated` must be sorted by `start` and contain no overlapping
/// or out-of-bounds extents; this is the invariant that
/// [`walk_block_group_extents`] enforces.
///
/// # Errors
///
/// * `allocated` is not sorted, contains an overlap, or contains an
///   extent that lies outside `[bg_start, bg_start + bg_length)`.
pub fn derive_free_ranges(
    bg_start: u64,
    bg_length: u64,
    allocated: &[AllocatedExtent],
) -> io::Result<Vec<Range>> {
    let bg_end = bg_start.checked_add(bg_length).ok_or_else(|| {
        io::Error::other(
            "derive_free_ranges: bg_start + bg_length overflows u64",
        )
    })?;
    let mut out: Vec<Range> = Vec::new();
    let mut cursor = bg_start;
    for (i, ext) in allocated.iter().enumerate() {
        if ext.start < bg_start || ext.end() > bg_end {
            return Err(io::Error::other(format!(
                "derive_free_ranges: extent [{}, {}) outside block group [{bg_start}, {bg_end})",
                ext.start,
                ext.end(),
            )));
        }
        if ext.start < cursor {
            return Err(io::Error::other(format!(
                "derive_free_ranges: extents not sorted or overlap at index {i} \
                 (start {} < cursor {cursor})",
                ext.start,
            )));
        }
        if ext.start > cursor {
            out.push(Range::new(cursor, ext.start - cursor));
        }
        cursor = ext.end();
    }
    if cursor < bg_end {
        out.push(Range::new(cursor, bg_end - cursor));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext(start: u64, length: u64) -> AllocatedExtent {
        AllocatedExtent { start, length }
    }

    #[test]
    fn derive_free_ranges_empty_block_group() {
        let r = derive_free_ranges(1000, 4096, &[]).unwrap();
        assert_eq!(r, vec![Range::new(1000, 4096)]);
    }

    #[test]
    fn derive_free_ranges_fully_allocated() {
        let r = derive_free_ranges(1000, 4096, &[ext(1000, 4096)]).unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn derive_free_ranges_leading_gap() {
        let r = derive_free_ranges(1000, 4096, &[ext(2000, 96)]).unwrap();
        assert_eq!(r, vec![Range::new(1000, 1000), Range::new(2096, 3000)]);
    }

    #[test]
    fn derive_free_ranges_no_leading_gap() {
        let r = derive_free_ranges(1000, 4096, &[ext(1000, 1000)]).unwrap();
        assert_eq!(r, vec![Range::new(2000, 3096)]);
    }

    #[test]
    fn derive_free_ranges_no_trailing_gap() {
        let r = derive_free_ranges(1000, 100, &[ext(1050, 50)]).unwrap();
        assert_eq!(r, vec![Range::new(1000, 50)]);
    }

    #[test]
    fn derive_free_ranges_multiple_gaps() {
        let r = derive_free_ranges(
            0,
            10_000,
            &[ext(100, 100), ext(500, 50), ext(1000, 9000)],
        )
        .unwrap();
        assert_eq!(
            r,
            vec![
                Range::new(0, 100),
                Range::new(200, 300),
                Range::new(550, 450),
            ]
        );
    }

    #[test]
    fn derive_free_ranges_adjacent_extents_have_no_gap() {
        let r = derive_free_ranges(0, 300, &[ext(100, 100), ext(200, 100)])
            .unwrap();
        assert_eq!(r, vec![Range::new(0, 100)]);
    }

    #[test]
    fn derive_free_ranges_rejects_overlap() {
        let err = derive_free_ranges(0, 1000, &[ext(0, 200), ext(100, 200)])
            .unwrap_err();
        assert!(err.to_string().contains("not sorted or overlap"));
    }

    #[test]
    fn derive_free_ranges_rejects_extent_before_bg() {
        let err = derive_free_ranges(100, 200, &[ext(50, 10)]).unwrap_err();
        assert!(err.to_string().contains("outside block group"));
    }

    #[test]
    fn derive_free_ranges_rejects_extent_past_bg() {
        let err = derive_free_ranges(100, 200, &[ext(280, 30)]).unwrap_err();
        assert!(err.to_string().contains("outside block group"));
    }

    #[test]
    fn derive_free_ranges_total_length_invariant() {
        // Property: sum(allocated) + sum(free) == bg_length, for any
        // valid input.
        let allocs = [ext(50, 25), ext(100, 200), ext(400, 100)];
        let bg_start = 0u64;
        let bg_length = 1000u64;
        let frees = derive_free_ranges(bg_start, bg_length, &allocs).unwrap();
        let alloc_total: u64 = allocs.iter().map(|e| e.length).sum();
        let free_total: u64 = frees.iter().map(|r| r.length).sum();
        assert_eq!(alloc_total + free_total, bg_length);
    }
}

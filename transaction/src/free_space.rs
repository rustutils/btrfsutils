//! # Free space tree range tracking and apply
//!
//! Helpers for tracking per-block-group byte range deltas during a
//! transaction and applying them to the free space tree (FST).
//!
//! Stage F1 (this module) introduces the data types and the pure
//! coalescing/cancellation helpers used to accumulate ranges that were
//! allocated and freed during a single transaction. Stage F2 will add
//! the pure "apply delta to free-range list" function. Stage F3 wires
//! the result into the on-disk FST update path inside the commit
//! convergence loop.
//!
//! The shape of a "range" in this module is `(start, length)` in bytes,
//! both `u64`. Ranges always live within a single block group and the
//! `start` is the absolute logical address (not block-group relative).

use std::collections::BTreeMap;

/// A half-open byte range `[start, start + length)` within a block group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: u64,
    pub length: u64,
}

impl Range {
    #[must_use]
    pub fn new(start: u64, length: u64) -> Self {
        Self { start, length }
    }

    #[must_use]
    pub fn end(self) -> u64 {
        self.start + self.length
    }

    #[must_use]
    pub fn is_empty(self) -> bool {
        self.length == 0
    }
}

/// A sorted, coalesced list of disjoint byte ranges.
///
/// Invariants maintained after every mutating operation:
///
/// - Ranges are sorted by `start`.
/// - No two ranges overlap or touch (adjacent ranges are merged).
/// - No zero-length ranges.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RangeList {
    ranges: Vec<Range>,
}

impl RangeList {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_sorted_unchecked(ranges: Vec<Range>) -> Self {
        Self { ranges }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[Range] {
        &self.ranges
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<Range> {
        self.ranges
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Insert a range, merging with any existing range it overlaps or
    /// touches.
    pub fn insert(&mut self, range: Range) {
        if range.is_empty() {
            return;
        }
        // Find the first existing range whose end is >= range.start.
        // That is the first range that could possibly merge with ours
        // (touching counts as merge: end == range.start).
        let mut start = range.start;
        let mut end = range.end();

        // Index of the first range that could merge.
        let first = self
            .ranges
            .iter()
            .position(|r| r.end() >= start)
            .unwrap_or(self.ranges.len());

        // Index one past the last range that merges. A range merges if
        // its start <= end.
        let mut last = first;
        while last < self.ranges.len() && self.ranges[last].start <= end {
            start = start.min(self.ranges[last].start);
            end = end.max(self.ranges[last].end());
            last += 1;
        }

        // Replace [first..last) with the merged range.
        let merged = Range::new(start, end - start);
        self.ranges.splice(first..last, std::iter::once(merged));
    }

    /// Subtract a range from the list. The result still satisfies the
    /// invariants.
    ///
    /// If the subtracted range is not fully covered by existing ranges,
    /// the uncovered portions are silently ignored — callers that want
    /// to detect this should use the F2 apply function which performs
    /// extent-tree consistency checks.
    pub fn subtract(&mut self, range: Range) {
        if range.is_empty() {
            return;
        }
        let sub_start = range.start;
        let sub_end = range.end();

        let mut i = 0;
        while i < self.ranges.len() {
            let r = self.ranges[i];
            if r.end() <= sub_start {
                i += 1;
                continue;
            }
            if r.start >= sub_end {
                break;
            }
            // r overlaps [sub_start, sub_end). Compute remainder pieces.
            let left = if r.start < sub_start {
                Some(Range::new(r.start, sub_start - r.start))
            } else {
                None
            };
            let right = if r.end() > sub_end {
                Some(Range::new(sub_end, r.end() - sub_end))
            } else {
                None
            };
            // Replace r with left/right pieces.
            match (left, right) {
                (None, None) => {
                    self.ranges.remove(i);
                }
                (Some(l), None) => {
                    self.ranges[i] = l;
                    i += 1;
                }
                (None, Some(rt)) => {
                    self.ranges[i] = rt;
                    i += 1;
                }
                (Some(l), Some(rt)) => {
                    self.ranges[i] = l;
                    self.ranges.insert(i + 1, rt);
                    i += 2;
                }
            }
        }
    }

    /// Total number of bytes covered by the list.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.length).sum()
    }
}

/// Per-block-group accumulator of allocated and freed ranges produced
/// during a single transaction commit.
///
/// Each block group is keyed by its start address (the same key used
/// for `BLOCK_GROUP_ITEM`). Within each block group, allocated and
/// freed ranges are kept in sorted, coalesced lists. A range that
/// appears in *both* lists is removed from both: it was allocated and
/// freed within the same transaction and the FST should see neither.
#[derive(Debug, Default, Clone)]
pub struct BlockGroupRangeDeltas {
    bgs: BTreeMap<u64, BlockGroupDelta>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockGroupDelta {
    pub allocated: RangeList,
    pub freed: RangeList,
}

impl BlockGroupRangeDeltas {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_allocated(&mut self, bg_start: u64, range: Range) {
        let entry = self.bgs.entry(bg_start).or_default();
        entry.allocated.insert(range);
    }

    pub fn record_freed(&mut self, bg_start: u64, range: Range) {
        let entry = self.bgs.entry(bg_start).or_default();
        entry.freed.insert(range);
    }

    /// Cancel any range that appears in both `allocated` and `freed`
    /// for the same block group. Operates on each block group
    /// independently. Idempotent.
    pub fn cancel_within_transaction(&mut self) {
        for delta in self.bgs.values_mut() {
            // Walk the freed list and subtract any portion that is also
            // in the allocated list, then do the symmetric subtraction.
            // This handles partial overlap correctly: only the
            // overlapping bytes are removed from each side.
            let to_cancel: Vec<Range> = delta.allocated.as_slice().to_vec();
            for r in to_cancel {
                // Compute the intersection between r and the freed
                // list, then subtract that intersection from both.
                let intersected = intersect(&delta.freed, r);
                for piece in intersected {
                    delta.allocated.subtract(piece);
                    delta.freed.subtract(piece);
                }
            }
        }
        // Drop empty block groups.
        self.bgs.retain(|_, d| {
            !(d.allocated.is_empty() && d.freed.is_empty())
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &BlockGroupDelta)> {
        self.bgs.iter()
    }

    #[must_use]
    pub fn get(&self, bg_start: u64) -> Option<&BlockGroupDelta> {
        self.bgs.get(&bg_start)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bgs.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bgs.len()
    }

    pub fn clear(&mut self) {
        self.bgs.clear();
    }
}

/// Intersect a range with a range list, returning the per-piece
/// intersections (sorted, disjoint).
fn intersect(list: &RangeList, range: Range) -> Vec<Range> {
    let mut out = Vec::new();
    if range.is_empty() {
        return out;
    }
    let r_start = range.start;
    let r_end = range.end();
    for piece in list.as_slice() {
        if piece.end() <= r_start {
            continue;
        }
        if piece.start >= r_end {
            break;
        }
        let s = piece.start.max(r_start);
        let e = piece.end().min(r_end);
        if e > s {
            out.push(Range::new(s, e - s));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rl(items: &[(u64, u64)]) -> RangeList {
        let mut l = RangeList::new();
        for &(s, n) in items {
            l.insert(Range::new(s, n));
        }
        l
    }

    fn collect(l: &RangeList) -> Vec<(u64, u64)> {
        l.as_slice().iter().map(|r| (r.start, r.length)).collect()
    }

    #[test]
    fn insert_into_empty() {
        let l = rl(&[(100, 10)]);
        assert_eq!(collect(&l), &[(100, 10)]);
    }

    #[test]
    fn insert_disjoint_sorted() {
        let l = rl(&[(100, 10), (200, 10), (50, 5)]);
        assert_eq!(collect(&l), &[(50, 5), (100, 10), (200, 10)]);
    }

    #[test]
    fn insert_merges_touching() {
        // 100..110 and 110..120 touch -> merge to 100..120
        let l = rl(&[(100, 10), (110, 10)]);
        assert_eq!(collect(&l), &[(100, 20)]);
    }

    #[test]
    fn insert_merges_overlapping() {
        let l = rl(&[(100, 20), (110, 20)]);
        assert_eq!(collect(&l), &[(100, 30)]);
    }

    #[test]
    fn insert_merges_chain() {
        // Insert spanning two existing ranges so they all merge.
        let mut l = rl(&[(100, 10), (200, 10)]);
        l.insert(Range::new(105, 100)); // 105..205
        assert_eq!(collect(&l), &[(100, 110)]); // 100..210
    }

    #[test]
    fn insert_zero_length_noop() {
        let mut l = rl(&[(100, 10)]);
        l.insert(Range::new(150, 0));
        assert_eq!(collect(&l), &[(100, 10)]);
    }

    #[test]
    fn subtract_whole_range() {
        let mut l = rl(&[(100, 10)]);
        l.subtract(Range::new(100, 10));
        assert!(l.is_empty());
    }

    #[test]
    fn subtract_left_edge() {
        let mut l = rl(&[(100, 10)]);
        l.subtract(Range::new(100, 4));
        assert_eq!(collect(&l), &[(104, 6)]);
    }

    #[test]
    fn subtract_right_edge() {
        let mut l = rl(&[(100, 10)]);
        l.subtract(Range::new(106, 4));
        assert_eq!(collect(&l), &[(100, 6)]);
    }

    #[test]
    fn subtract_middle_splits() {
        let mut l = rl(&[(100, 10)]);
        l.subtract(Range::new(103, 4));
        assert_eq!(collect(&l), &[(100, 3), (107, 3)]);
    }

    #[test]
    fn subtract_spanning_multiple_ranges() {
        let mut l = rl(&[(100, 10), (200, 10), (300, 10)]);
        l.subtract(Range::new(105, 200));
        // 100..105 remains; 200..210 fully removed; 300..305 removed,
        // 305..310 remains.
        assert_eq!(collect(&l), &[(100, 5), (305, 5)]);
    }

    #[test]
    fn subtract_outside_noop() {
        let mut l = rl(&[(100, 10)]);
        l.subtract(Range::new(50, 10));
        l.subtract(Range::new(120, 10));
        assert_eq!(collect(&l), &[(100, 10)]);
    }

    #[test]
    fn total_bytes() {
        let l = rl(&[(100, 10), (200, 30)]);
        assert_eq!(l.total_bytes(), 40);
    }

    #[test]
    fn cancel_exact_match_removes_both() {
        let mut d = BlockGroupRangeDeltas::new();
        d.record_allocated(0, Range::new(1000, 16384));
        d.record_freed(0, Range::new(1000, 16384));
        d.cancel_within_transaction();
        assert!(d.is_empty());
    }

    #[test]
    fn cancel_disjoint_keeps_both() {
        let mut d = BlockGroupRangeDeltas::new();
        d.record_allocated(0, Range::new(1000, 16384));
        d.record_freed(0, Range::new(50000, 16384));
        d.cancel_within_transaction();
        let bg = d.get(0).unwrap();
        assert_eq!(bg.allocated.total_bytes(), 16384);
        assert_eq!(bg.freed.total_bytes(), 16384);
    }

    #[test]
    fn cancel_partial_overlap() {
        // Allocated 100..200, freed 150..250. Cancel 150..200 from
        // both. Allocated remains 100..150, freed remains 200..250.
        let mut d = BlockGroupRangeDeltas::new();
        d.record_allocated(0, Range::new(100, 100));
        d.record_freed(0, Range::new(150, 100));
        d.cancel_within_transaction();
        let bg = d.get(0).unwrap();
        assert_eq!(collect(&bg.allocated), &[(100, 50)]);
        assert_eq!(collect(&bg.freed), &[(200, 50)]);
    }

    #[test]
    fn cancel_isolates_per_block_group() {
        let mut d = BlockGroupRangeDeltas::new();
        d.record_allocated(0, Range::new(100, 10));
        d.record_freed(1000, Range::new(100, 10));
        d.cancel_within_transaction();
        // Different block groups; nothing cancels.
        assert_eq!(d.len(), 2);
    }
}

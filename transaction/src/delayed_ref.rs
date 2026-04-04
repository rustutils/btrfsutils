//! # Batched reference count updates
//!
//! Modifying a tree generates many reference count updates (every copy-on-written block
//! creates a new ref and removes an old ref). Processing each one immediately
//! would cause excessive extent tree modifications. Instead, reference updates
//! are queued and batched, then flushed at commit time.

use std::collections::HashMap;

/// A queued reference count change for an extent.
#[derive(Debug, Clone)]
pub struct DelayedRef {
    /// Logical byte address of the extent.
    pub bytenr: u64,
    /// Reference count delta (+1 for new ref, -1 for dropped ref).
    pub delta: i64,
    /// Whether this is a metadata (tree block) or data extent.
    pub is_metadata: bool,
    /// Tree ID that owns the reference.
    pub owner: u64,
    /// For metadata: the tree level. For data: 0.
    pub level: u8,
}

/// Accumulator for delayed reference updates.
///
/// Reference changes are merged by bytenr: a +1 and -1 to the same extent
/// cancel out. At flush time, only net-nonzero changes need to be applied
/// to the extent tree.
#[derive(Debug, Default)]
pub struct DelayedRefQueue {
    /// Net reference count deltas, keyed by extent bytenr.
    refs: HashMap<u64, DelayedRef>,
}

impl DelayedRefQueue {
    /// Create a new empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a reference count increment for an extent.
    pub fn add_ref(
        &mut self,
        bytenr: u64,
        is_metadata: bool,
        owner: u64,
        level: u8,
    ) {
        let entry = self.refs.entry(bytenr).or_insert(DelayedRef {
            bytenr,
            delta: 0,
            is_metadata,
            owner,
            level,
        });
        entry.delta += 1;
    }

    /// Queue a reference count decrement for an extent.
    pub fn drop_ref(
        &mut self,
        bytenr: u64,
        is_metadata: bool,
        owner: u64,
        level: u8,
    ) {
        let entry = self.refs.entry(bytenr).or_insert(DelayedRef {
            bytenr,
            delta: 0,
            is_metadata,
            owner,
            level,
        });
        entry.delta -= 1;
    }

    /// Drain all queued refs with non-zero deltas.
    pub fn drain(&mut self) -> Vec<DelayedRef> {
        let refs: Vec<DelayedRef> = self
            .refs
            .drain()
            .map(|(_, r)| r)
            .filter(|r| r.delta != 0)
            .collect();
        refs
    }

    /// Return true if there are no pending refs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    /// Number of distinct extents with pending changes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.refs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_drop_cancel() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        q.drop_ref(65536, true, 5, 0);
        let refs = q.drain();
        assert!(refs.is_empty());
    }

    #[test]
    fn net_positive() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        let refs = q.drain();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].delta, 1);
    }

    #[test]
    fn net_negative() {
        let mut q = DelayedRefQueue::new();
        q.drop_ref(65536, true, 5, 0);
        let refs = q.drain();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].delta, -1);
    }

    #[test]
    fn multiple_extents() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        q.add_ref(131072, true, 5, 0);
        q.drop_ref(65536, true, 5, 0);
        let refs = q.drain();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].bytenr, 131072);
    }
}

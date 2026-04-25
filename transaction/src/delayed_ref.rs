//! # Batched reference count updates
//!
//! Modifying a tree generates many reference count updates (every copy-on-written block
//! creates a new ref and removes an old ref). Processing each one immediately
//! would cause excessive extent tree modifications. Instead, reference updates
//! are queued and batched, then flushed at commit time.

use std::collections::BTreeMap;

/// Identity of a queued reference change.
///
/// Metadata refs are uniquely identified by `(bytenr, owner_root, level)`:
/// at most one tree-block backref of a given owning root exists per
/// metadata extent. Data refs additionally need the inode and file offset
/// because a single data extent can carry multiple distinct
/// `EXTENT_DATA_REF` backrefs of shape `(root, ino, offset)`, each with
/// its own count.
// `Ord` is required so the queue iterates in deterministic key order
// at flush time. With `HashMap` the flush sequence depended on
// per-process hash randomization, which made successive mkfs runs
// produce byte-different output for the same config (snapshot tests
// flagged this).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DelayedRefKey {
    /// A reference to a metadata (tree block) extent.
    Metadata {
        /// Logical byte address of the tree block.
        bytenr: u64,
        /// Tree objectid that owns the reference.
        owner_root: u64,
        /// Tree level of the referenced block.
        level: u8,
    },
    /// A reference to a data extent.
    Data {
        /// Logical byte address of the data extent.
        bytenr: u64,
        /// Tree objectid (subvolume root) of the referencing inode.
        owner_root: u64,
        /// Inode number that holds the reference.
        owner_ino: u64,
        /// File offset within that inode where the extent is referenced.
        owner_offset: u64,
    },
}

impl DelayedRefKey {
    /// The logical byte address of the referenced extent.
    #[must_use]
    pub fn bytenr(&self) -> u64 {
        match self {
            Self::Metadata { bytenr, .. } | Self::Data { bytenr, .. } => {
                *bytenr
            }
        }
    }

    /// True if this key refers to a metadata (tree block) extent.
    #[must_use]
    pub fn is_metadata(&self) -> bool {
        matches!(self, Self::Metadata { .. })
    }
}

/// A queued reference count change for an extent.
#[derive(Debug, Clone)]
pub struct DelayedRef {
    /// Identity of the backref being changed.
    pub key: DelayedRefKey,
    /// Reference count delta (+1 for new ref, -1 for dropped ref).
    pub delta: i64,
    /// For data refs: the byte length of the extent. Unused for metadata
    /// (the extent length is the filesystem `nodesize` and is recovered
    /// from the filesystem context at flush time).
    pub num_bytes: u64,
}

/// Internal accumulator value: net delta plus the data-extent length.
#[derive(Debug, Clone, Copy, Default)]
struct Entry {
    delta: i64,
    num_bytes: u64,
}

/// Accumulator for delayed reference updates.
///
/// Reference changes are merged by [`DelayedRefKey`]: a +1 and -1 to the
/// same key cancel out. At flush time, only net-nonzero changes need to be
/// applied to the extent tree.
#[derive(Debug, Default)]
pub struct DelayedRefQueue {
    // BTreeMap (not HashMap) so `drain` iterates in deterministic
    // key order — see the comment on `DelayedRefKey`.
    refs: BTreeMap<DelayedRefKey, Entry>,
}

impl DelayedRefQueue {
    /// Create a new empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a reference count increment for a metadata extent.
    ///
    /// `is_metadata` is retained for call-site compatibility and must be
    /// `true`; data refs use [`add_data_ref`](Self::add_data_ref) instead.
    pub fn add_ref(
        &mut self,
        bytenr: u64,
        is_metadata: bool,
        owner_root: u64,
        level: u8,
    ) {
        debug_assert!(
            is_metadata,
            "add_ref is metadata-only; call add_data_ref for data refs"
        );
        let key = DelayedRefKey::Metadata {
            bytenr,
            owner_root,
            level,
        };
        self.refs.entry(key).or_default().delta += 1;
    }

    /// Queue a reference count decrement for a metadata extent.
    pub fn drop_ref(
        &mut self,
        bytenr: u64,
        is_metadata: bool,
        owner_root: u64,
        level: u8,
    ) {
        debug_assert!(
            is_metadata,
            "drop_ref is metadata-only; call drop_data_ref for data refs"
        );
        let key = DelayedRefKey::Metadata {
            bytenr,
            owner_root,
            level,
        };
        self.refs.entry(key).or_default().delta -= 1;
    }

    /// Queue a reference count increment for a data extent backref.
    pub fn add_data_ref(
        &mut self,
        bytenr: u64,
        num_bytes: u64,
        owner_root: u64,
        owner_ino: u64,
        owner_offset: u64,
        refs_to_add: i32,
    ) {
        let key = DelayedRefKey::Data {
            bytenr,
            owner_root,
            owner_ino,
            owner_offset,
        };
        let e = self.refs.entry(key).or_default();
        e.delta += i64::from(refs_to_add);
        // Sanity: a single backref always has a single extent length.
        debug_assert!(e.num_bytes == 0 || e.num_bytes == num_bytes);
        e.num_bytes = num_bytes;
    }

    /// Queue a reference count decrement for a data extent backref.
    pub fn drop_data_ref(
        &mut self,
        bytenr: u64,
        num_bytes: u64,
        owner_root: u64,
        owner_ino: u64,
        owner_offset: u64,
        refs_to_drop: i32,
    ) {
        self.add_data_ref(
            bytenr,
            num_bytes,
            owner_root,
            owner_ino,
            owner_offset,
            -refs_to_drop,
        );
    }

    /// Drain all queued refs with non-zero deltas.
    ///
    /// Iterates in `DelayedRefKey` sort order (deterministic). The
    /// caller observes refs in `(Metadata before Data)` order, then by
    /// `(bytenr, owner_root, level | owner_ino, owner_offset)`.
    pub fn drain(&mut self) -> Vec<DelayedRef> {
        std::mem::take(&mut self.refs)
            .into_iter()
            .filter(|(_, e)| e.delta != 0)
            .map(|(key, e)| DelayedRef {
                key,
                delta: e.delta,
                num_bytes: e.num_bytes,
            })
            .collect()
    }

    /// Return true if there are no pending refs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    /// Number of distinct backrefs with pending changes.
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
    fn distinct_owners_dont_merge() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        q.add_ref(65536, true, 6, 0);
        let refs = q.drain();
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn data_refs_keyed_by_owner_triple() {
        let mut q = DelayedRefQueue::new();
        q.drop_data_ref(0x10000, 4096, 5, 257, 0, 1);
        q.drop_data_ref(0x10000, 4096, 5, 258, 0, 1);
        let refs = q.drain();
        assert_eq!(refs.len(), 2);
        for r in &refs {
            assert_eq!(r.delta, -1);
            assert_eq!(r.num_bytes, 4096);
            assert!(matches!(r.key, DelayedRefKey::Data { .. }));
        }
    }

    #[test]
    fn data_add_drop_cancel() {
        let mut q = DelayedRefQueue::new();
        q.add_data_ref(0x10000, 4096, 5, 257, 0, 1);
        q.drop_data_ref(0x10000, 4096, 5, 257, 0, 1);
        assert!(q.drain().is_empty());
    }

    #[test]
    fn drain_empties_queue() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        assert!(!q.is_empty());
        assert_eq!(q.len(), 1);
        let _ = q.drain();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn double_add_ref() {
        let mut q = DelayedRefQueue::new();
        q.add_ref(65536, true, 5, 0);
        q.add_ref(65536, true, 5, 0);
        let refs = q.drain();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].delta, 2);
    }

    #[test]
    fn empty_queue_drain() {
        let mut q = DelayedRefQueue::new();
        let refs = q.drain();
        assert!(refs.is_empty());
    }
}

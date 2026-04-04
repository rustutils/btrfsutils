//! # B-tree search path
//!
//! `BtrfsPath` holds a stack of `(ExtentBuffer, slot)` pairs from root to
//! leaf, representing the current position in a tree after a search. It is
//! used by `search_slot` to record the descent path and by item operations
//! to know which blocks to modify.

use crate::extent_buffer::{BTRFS_MAX_LEVEL, ExtentBuffer};

/// A search path through a btrfs B-tree.
///
/// After `search_slot` completes, `nodes[level]` holds the extent buffer at
/// that level, and `slots[level]` holds the slot index within that buffer.
/// Level 0 is the leaf; higher levels are internal nodes up to the root.
pub struct BtrfsPath {
    /// Extent buffers at each level of the path.
    pub nodes: [Option<ExtentBuffer>; BTRFS_MAX_LEVEL],
    /// Slot indices at each level.
    pub slots: [usize; BTRFS_MAX_LEVEL],
    /// Lowest level to descend to (0 = leaf, default).
    pub lowest_level: u8,
}

impl BtrfsPath {
    /// Create a new empty path.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: Default::default(),
            slots: [0; BTRFS_MAX_LEVEL],
            lowest_level: 0,
        }
    }

    /// Release all held buffers and reset slots.
    pub fn release(&mut self) {
        for node in &mut self.nodes {
            *node = None;
        }
        self.slots = [0; BTRFS_MAX_LEVEL];
    }

    /// Return a reference to the leaf extent buffer (level 0), if present.
    #[must_use]
    pub fn leaf(&self) -> Option<&ExtentBuffer> {
        self.nodes[0].as_ref()
    }

    /// Return the slot at the leaf level.
    #[must_use]
    pub fn leaf_slot(&self) -> usize {
        self.slots[0]
    }
}

impl Default for BtrfsPath {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_path_is_empty() {
        let path = BtrfsPath::new();
        assert!(path.leaf().is_none());
        assert_eq!(path.leaf_slot(), 0);
        assert_eq!(path.lowest_level, 0);
    }

    #[test]
    fn release_clears_all() {
        let mut path = BtrfsPath::new();
        path.slots[0] = 5;
        path.nodes[0] = Some(ExtentBuffer::new_zeroed(4096, 0));
        path.release();
        assert!(path.leaf().is_none());
        assert_eq!(path.slots[0], 0);
    }
}

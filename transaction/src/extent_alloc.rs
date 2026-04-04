//! # Extent allocation and freeing
//!
//! Proper extent allocation replacing the temporary bump allocator from the
//! transaction module. Handles reference counting for shared extents, block
//! group management, and free space tracking.

// TODO: Phase 7 — alloc_tree_block, free_tree_block, alloc_data_extent,
// free_data_extent, inc_extent_ref, dec_extent_ref

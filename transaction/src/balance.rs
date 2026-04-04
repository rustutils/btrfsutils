//! # Node balancing (push left/right, merge)
//!
//! Before splitting a full leaf or node, try redistributing items to a
//! neighboring sibling. This reduces tree height growth. Balancing is an
//! optimization, not required for correctness.

// TODO: Phase 5 — push_items_left, push_items_right, merge_nodes

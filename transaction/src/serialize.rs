//! # Item type serialization (to_bytes)
//!
//! Serialization functions for tree item payloads. While `btrfs-disk` handles
//! parsing (bytes to typed structs), this module handles the reverse: converting
//! typed data back to on-disk byte representations for insertion into leaves.

// TODO: Phase 4+ — RootItem::to_bytes, BlockGroupItem::to_bytes, etc.

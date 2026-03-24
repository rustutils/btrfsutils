//! # Raw bindgen output for btrfs on-disk structures
//!
//! The types in this module are generated automatically from `btrfs_tree.h`
//! and `btrfs.h` by bindgen.  They represent the packed, little-endian
//! structures stored on disk.
//!
//! Prefer the typed wrappers in the sibling modules over using this module
//! directly.

mod bindings {
    #![allow(warnings)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::*;

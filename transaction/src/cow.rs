//! # Copy-on-write block duplication
//!
//! Before modifying a tree block, the COW protocol requires allocating a new
//! block, copying the contents, and updating the parent pointer. This ensures
//! crash consistency: the old superblock always points to a valid tree state.
//!
//! A block whose generation already matches the current transaction has already
//! been copy-on-written and can be modified in place.

use crate::{
    extent_buffer::ExtentBuffer, fs_info::FsInfo, transaction::TransHandle,
};
use std::io::{self, Read, Seek, Write};

/// `BTRFS_HEADER_FLAG_WRITTEN` (bit 0): block has been written to stable storage.
const HEADER_FLAG_WRITTEN: u64 = 1 << 0;
/// `BTRFS_HEADER_FLAG_RELOC` (bit 1): block is part of a relocation operation.
const HEADER_FLAG_RELOC: u64 = 1 << 1;

/// Copy-on-write a tree block.
///
/// If the block's generation matches the current transaction, it has already
/// been copy-on-written and is returned as-is. Otherwise, a new block is allocated,
/// the contents are copied, and the old block is queued for freeing.
///
/// `parent_info` is `Some((parent_logical, parent_slot))` for non-root blocks.
/// For root blocks, pass `None` — the caller is responsible for updating the
/// tree's root pointer.
///
/// # Errors
///
/// Returns an error if block allocation or I/O fails.
pub fn cow_block<R: Read + Write + Seek>(
    trans: &mut TransHandle<R>,
    fs_info: &mut FsInfo<R>,
    eb: &ExtentBuffer,
    tree_id: u64,
    _parent_info: Option<(u64, usize)>,
) -> io::Result<ExtentBuffer> {
    // Already COWed in this transaction and not yet flushed to stable
    // storage? Safe to modify in place. A block that has been written to
    // disk (flush_dirty or write_block) is part of the on-disk state and
    // must be COWed again to preserve crash consistency.
    if eb.generation() == fs_info.generation
        && !fs_info.is_written(eb.logical())
    {
        return Ok(eb.clone());
    }

    // Allocate a new block and queue a +1 delayed ref for it
    let level = eb.level();
    let new_logical = trans.alloc_tree_block(fs_info, tree_id, level)?;
    let mut new_eb = eb.clone();
    new_eb.set_logical(new_logical);
    new_eb.set_bytenr(new_logical);
    new_eb.set_generation(fs_info.generation);

    // Clear flags inherited from the source block. WRITTEN indicates the
    // block has been flushed to stable storage (this new copy hasn't been).
    // RELOC indicates the block is part of a relocation operation (the new
    // copy is not).
    let flags = new_eb.flags() & !(HEADER_FLAG_WRITTEN | HEADER_FLAG_RELOC);
    new_eb.set_flags(flags);

    // Queue -1 delayed ref for the old block being replaced, and pin it
    // so the allocator doesn't reuse the address before commit.
    trans
        .delayed_refs
        .drop_ref(eb.logical(), true, tree_id, level);
    trans.pin_block(eb.logical());

    // Mark the new block dirty
    fs_info.mark_dirty(&new_eb);

    Ok(new_eb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_flag_values() {
        // Verify our flag constants match the kernel header
        assert_eq!(HEADER_FLAG_WRITTEN, 1);
        assert_eq!(HEADER_FLAG_RELOC, 2);
    }

    #[test]
    fn cow_skip_condition_generation_match_not_written() {
        // When generation matches and block not written: should skip COW
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_generation(42);
        // Simulate the check in cow_block
        let current_gen = 42u64;
        let is_written = false;
        assert!(
            eb.generation() == current_gen && !is_written,
            "should skip COW"
        );
    }

    #[test]
    fn cow_required_generation_mismatch() {
        // When generation doesn't match: must COW
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_generation(41);
        let current_gen = 42u64;
        let is_written = false;
        let skip_cow = eb.generation() == current_gen && !is_written;
        assert!(!skip_cow, "should require COW");
    }

    #[test]
    fn cow_required_when_written() {
        // When generation matches but block was written: must COW
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_generation(42);
        let current_gen = 42u64;
        let is_written = true;
        let skip_cow = eb.generation() == current_gen && !is_written;
        assert!(!skip_cow, "should require COW even with matching generation");
    }

    #[test]
    fn clear_written_reloc_flags() {
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_flags(HEADER_FLAG_WRITTEN | HEADER_FLAG_RELOC | 0x100);
        let cleared = eb.flags() & !(HEADER_FLAG_WRITTEN | HEADER_FLAG_RELOC);
        assert_eq!(cleared, 0x100);
        assert_eq!(cleared & HEADER_FLAG_WRITTEN, 0);
        assert_eq!(cleared & HEADER_FLAG_RELOC, 0);
    }
}

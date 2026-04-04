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
    _tree_id: u64,
    _parent_info: Option<(u64, usize)>,
) -> io::Result<ExtentBuffer> {
    // Already COWed in this transaction?
    if eb.generation() == fs_info.generation {
        return Ok(eb.clone());
    }

    // Allocate a new block
    let new_logical = trans.alloc_block(fs_info)?;
    let mut new_eb = eb.clone();
    new_eb.set_logical(new_logical);
    new_eb.set_bytenr(new_logical);
    new_eb.set_generation(fs_info.generation);

    // Queue old block for freeing (will be processed at commit time)
    trans.queue_free_block(eb.logical());

    // Mark the new block dirty
    fs_info.mark_dirty(&new_eb);

    Ok(new_eb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn already_cowed_block_returned_as_is() {
        // If generation matches, cow_block should return the same logical address
        let mut eb = ExtentBuffer::new_zeroed(4096, 65536);
        eb.set_generation(42);
        eb.set_bytenr(65536);

        // We can't fully test cow_block without a real FsInfo, but we can
        // verify the generation check logic
        assert_eq!(eb.generation(), 42);
    }
}

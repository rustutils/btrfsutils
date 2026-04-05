//! # Leaf item insert, delete, and update operations
//!
//! These functions modify items in leaf nodes. Insert makes room by shifting
//! existing items and their data. Delete reclaims space by shifting items
//! and data back. Update overwrites item data in place (same size only).
//!
//! Items grow forward from the header (as item descriptors), while their data
//! payloads grow backward from the end of the block. The free space is the
//! gap between them.

use crate::buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE};
use btrfs_disk::tree::DiskKey;
use std::io;

/// Compute the free space available in a leaf for a new item.
///
/// A new item requires `ITEM_SIZE` (25) bytes for the descriptor plus
/// `data_size` bytes for the payload.
#[must_use]
pub fn leaf_free_space(eb: &ExtentBuffer) -> u32 {
    eb.leaf_free_space()
}

/// Insert an item with data into a leaf at the given slot.
///
/// The caller must ensure there is enough free space (check with
/// `leaf_free_space`). Items at `slot..nritems` are shifted right to make
/// room, and data belonging to items at `slot..nritems` is shifted left
/// (toward lower offsets) by `data.len()` bytes.
///
/// # Errors
///
/// Returns an error if there is not enough free space.
pub fn insert_item(
    eb: &mut ExtentBuffer,
    slot: usize,
    key: &DiskKey,
    data: &[u8],
) -> io::Result<()> {
    let data_size = data.len() as u32;
    let needed = ITEM_SIZE as u32 + data_size;
    let free = eb.leaf_free_space();
    if free < needed {
        return Err(io::Error::other(format!(
            "leaf full: need {needed} bytes, have {free} free",
        )));
    }

    let nritems = eb.nritems() as usize;

    // Data grows backward from the end of the block. Offsets are relative to
    // HEADER_SIZE and must be in descending order by slot index: item 0 has the
    // highest offset, item N-1 has the lowest.
    //
    // `data_end` is the lowest data offset among current items.
    let data_end = if nritems == 0 {
        eb.nodesize() - HEADER_SIZE as u32
    } else {
        eb.item_offset(nritems - 1)
    };

    if nritems > 0 && slot < nritems {
        // Inserting before existing items. Items at [slot..nritems) have data
        // at offsets LOWER than items at [0..slot). We must shift their data
        // DOWN by data_size to make room, preserving the descending offset
        // invariant.

        // 1. Shift data bytes for items [slot..nritems-1] down by data_size.
        //    Their data spans from data_end to the top of slot's data.
        let abs_data_bottom = HEADER_SIZE + data_end as usize;
        let abs_data_top = HEADER_SIZE
            + eb.item_offset(slot) as usize
            + eb.item_size(slot) as usize;
        if abs_data_bottom < abs_data_top {
            eb.copy_within(
                abs_data_bottom..abs_data_top,
                abs_data_bottom - data_size as usize,
            );
        }

        // 2. Update offsets for items [slot..nritems) (subtract data_size).
        for i in slot..nritems {
            let old_off = eb.item_offset(i);
            eb.set_item_offset(i, old_off - data_size);
        }

        // 3. Shift item descriptors at [slot..nritems) right by one ITEM_SIZE.
        let items_src = HEADER_SIZE + slot * ITEM_SIZE;
        let items_len = (nritems - slot) * ITEM_SIZE;
        let items_dest = items_src + ITEM_SIZE;
        eb.copy_within(items_src..items_src + items_len, items_dest);
    }

    // The new item's data offset: data_end - data_size is the new lowest
    // offset (now occupied by the shifted items). Our item goes at data_end,
    // which is above the shifted data and below items [0..slot-1].
    // Wait — after shifting, the old data_end is now occupied. The new gap
    // is at (data_end - data_size + shifted_data_size)... Actually, the shift
    // moved everything from [data_end..top) down to [data_end-data_size..top-data_size).
    // So the gap is [top-data_size..top). The new item's data goes at
    // the offset that was occupied by the old slot item's data top minus data_size.
    //
    // Simpler: the new item's offset should be: if slot == 0, one below item[1]'s
    // offset (which is the old item[0] offset - data_size). If slot > 0,
    // one below item[slot-1]'s offset.
    //
    // Actually the simplest correct formula: after shifting, the new lowest
    // offset is data_end - data_size (at new item index nritems, which was
    // the old nritems-1 shifted). The new item at slot `slot` should have
    // offset = item[slot+1].offset + item[slot+1].size (right above item
    // slot+1's data). But we can also just compute: the data for the new
    // item should go right above the shifted item at slot+1.
    let new_data_offset = if slot < nritems {
        // Item at slot+1 (which was the old item at slot, now shifted and
        // with updated offset) ends at offset + size. Our data goes right
        // above that.
        eb.item_offset(slot + 1) + eb.item_size(slot + 1)
    } else {
        // Appending at the end: data goes at data_end - data_size
        data_end - data_size
    };

    // Verify the new offset preserves non-ascending order.
    // Zero-size items (like FREE_SPACE_EXTENT) may share the same offset.
    debug_assert!(
        slot == 0 || {
            let prev_off = eb.item_offset(slot - 1);
            new_data_offset <= prev_off
        },
        "insert_item: new offset {new_data_offset} above slot {}'s offset {}",
        slot.saturating_sub(1),
        if slot > 0 {
            eb.item_offset(slot - 1)
        } else {
            0
        }
    );

    // Write the new item descriptor at the insert slot
    eb.set_item_key(slot, key);
    eb.set_item_offset(slot, new_data_offset);
    eb.set_item_size(slot, data_size);

    // Write the data payload
    let abs_data_off = HEADER_SIZE + new_data_offset as usize;
    eb.as_bytes_mut()[abs_data_off..abs_data_off + data.len()]
        .copy_from_slice(data);

    // Increment nritems
    eb.set_nritems(nritems as u32 + 1);

    Ok(())
}

/// Insert an empty item (key + descriptor only, zero-filled data area).
///
/// # Errors
///
/// Returns an error if there is not enough free space.
pub fn insert_empty_item(
    eb: &mut ExtentBuffer,
    slot: usize,
    key: &DiskKey,
    data_size: u32,
) -> io::Result<()> {
    let zeros = vec![0u8; data_size as usize];
    insert_item(eb, slot, key, &zeros)
}

/// Delete `count` items starting at `slot` from a leaf.
///
/// Shifts remaining items left and reclaims data space.
pub fn del_items(eb: &mut ExtentBuffer, slot: usize, count: usize) {
    let nritems = eb.nritems() as usize;
    assert!(
        slot + count <= nritems,
        "del_items: slot {slot} + count {count} > nritems {nritems}"
    );

    if count == 0 {
        return;
    }

    // Calculate total data size being removed
    let mut del_data_size: u32 = 0;
    for i in slot..slot + count {
        del_data_size += eb.item_size(i);
    }

    // The data belonging to items [slot..slot+count] is between:
    // - start: HEADER_SIZE + item_offset(slot+count-1) (lowest data offset in deleted range)
    // - end: HEADER_SIZE + item_offset(slot) + item_size(slot) (highest)
    // But items after the deleted range (slot+count..nritems) have data that
    // is at even lower offsets. We need to shift that data up by del_data_size.
    if slot + count < nritems {
        // Shift data of items [slot+count..nritems-1] up by del_data_size
        let last_item = nritems - 1;
        let move_start = HEADER_SIZE + eb.item_offset(last_item) as usize;
        let move_end = HEADER_SIZE
            + eb.item_offset(slot + count) as usize
            + eb.item_size(slot + count) as usize;
        if move_start < move_end {
            let dest = move_start + del_data_size as usize;
            // Copy from lower to higher (data shifts toward end of block)
            // Use copy_within which handles overlap correctly
            eb.copy_within(move_start..move_end, dest);
        }

        // Update data offsets for items after the deleted range
        for i in slot + count..nritems {
            let old_off = eb.item_offset(i);
            eb.set_item_offset(i, old_off + del_data_size);
        }

        // Shift item descriptors left
        let src = HEADER_SIZE + (slot + count) * ITEM_SIZE;
        let len = (nritems - slot - count) * ITEM_SIZE;
        let dest = HEADER_SIZE + slot * ITEM_SIZE;
        eb.copy_within(src..src + len, dest);
    }

    // Zero out freed item descriptor space
    let new_nritems = nritems - count;
    let zero_start = HEADER_SIZE + new_nritems * ITEM_SIZE;
    let zero_end = HEADER_SIZE + nritems * ITEM_SIZE;
    if zero_start < zero_end {
        eb.zero_range(zero_start, zero_end - zero_start);
    }

    eb.set_nritems(new_nritems as u32);
}

/// Update item data in place. The new data must be the same size as the
/// existing item data.
///
/// # Errors
///
/// Returns an error if the sizes don't match.
pub fn update_item(
    eb: &mut ExtentBuffer,
    slot: usize,
    data: &[u8],
) -> io::Result<()> {
    let size = eb.item_size(slot) as usize;
    if data.len() != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "update_item: data size {} != item size {size}",
                data.len()
            ),
        ));
    }
    eb.item_data_mut(slot).copy_from_slice(data);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::tree::KeyType;

    fn empty_leaf(nodesize: u32) -> ExtentBuffer {
        let mut eb = ExtentBuffer::new_zeroed(nodesize, 65536);
        eb.set_level(0);
        eb.set_nritems(0);
        eb.set_generation(1);
        eb.set_owner(5);
        eb
    }

    fn make_key(oid: u64) -> DiskKey {
        DiskKey {
            objectid: oid,
            key_type: KeyType::InodeItem,
            offset: 0,
        }
    }

    #[test]
    fn insert_single_item() {
        let mut eb = empty_leaf(4096);
        let data = [0xAA; 100];
        insert_item(&mut eb, 0, &make_key(256), &data).unwrap();

        assert_eq!(eb.nritems(), 1);
        assert_eq!(eb.item_key(0).objectid, 256);
        assert_eq!(eb.item_size(0), 100);
        assert_eq!(eb.item_data(0), &data);
    }

    #[test]
    fn insert_multiple_items_in_order() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 50]).unwrap();

        assert_eq!(eb.nritems(), 3);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 2);
        assert_eq!(eb.item_key(2).objectid, 3);
        assert_eq!(eb.item_data(0), &[0x11; 50]);
        assert_eq!(eb.item_data(1), &[0x22; 50]);
        assert_eq!(eb.item_data(2), &[0x33; 50]);
    }

    #[test]
    fn insert_at_beginning() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(5), &[0x55; 30]).unwrap();
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 30]).unwrap();

        assert_eq!(eb.nritems(), 2);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 5);
        assert_eq!(eb.item_data(0), &[0x11; 30]);
        assert_eq!(eb.item_data(1), &[0x55; 30]);
    }

    #[test]
    fn insert_full_leaf_fails() {
        let mut eb = empty_leaf(256); // tiny leaf
        // Try to insert more data than fits
        let big_data = vec![0u8; 200];
        let result = insert_item(&mut eb, 0, &make_key(1), &big_data);
        assert!(result.is_err());
    }

    #[test]
    fn delete_single_item() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 50]).unwrap();

        del_items(&mut eb, 1, 1);

        assert_eq!(eb.nritems(), 2);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 3);
        assert_eq!(eb.item_data(0), &[0x11; 50]);
        assert_eq!(eb.item_data(1), &[0x33; 50]);
    }

    #[test]
    fn delete_first_item() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();

        del_items(&mut eb, 0, 1);

        assert_eq!(eb.nritems(), 1);
        assert_eq!(eb.item_key(0).objectid, 2);
        assert_eq!(eb.item_data(0), &[0x22; 50]);
    }

    #[test]
    fn delete_last_item() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();

        del_items(&mut eb, 1, 1);

        assert_eq!(eb.nritems(), 1);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_data(0), &[0x11; 50]);
    }

    #[test]
    fn delete_all_items() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();

        del_items(&mut eb, 0, 2);

        assert_eq!(eb.nritems(), 0);
    }

    #[test]
    fn delete_multiple_middle() {
        let mut eb = empty_leaf(4096);
        for i in 0..5 {
            insert_item(
                &mut eb,
                i,
                &make_key(i as u64 + 1),
                &[i as u8 + 1; 30],
            )
            .unwrap();
        }

        del_items(&mut eb, 1, 2); // delete items with keys 2 and 3

        assert_eq!(eb.nritems(), 3);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 4);
        assert_eq!(eb.item_key(2).objectid, 5);
    }

    #[test]
    fn update_item_data() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();

        let new_data = [0xFF; 50];
        update_item(&mut eb, 0, &new_data).unwrap();
        assert_eq!(eb.item_data(0), &[0xFF; 50]);
    }

    #[test]
    fn update_item_wrong_size() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();

        let result = update_item(&mut eb, 0, &[0xFF; 30]);
        assert!(result.is_err());
    }

    #[test]
    fn insert_delete_round_trip() {
        let mut eb = empty_leaf(4096);
        let initial_free = eb.leaf_free_space();

        insert_item(&mut eb, 0, &make_key(1), &[0x11; 100]).unwrap();
        let after_insert = eb.leaf_free_space();
        assert!(after_insert < initial_free);

        del_items(&mut eb, 0, 1);
        // After deleting, nritems is 0 so leaf_free_space should be back to max
        assert_eq!(eb.leaf_free_space(), initial_free);
    }

    #[test]
    fn insert_empty_item_zero_size() {
        let mut eb = empty_leaf(4096);
        insert_empty_item(&mut eb, 0, &make_key(1), 0).unwrap();
        assert_eq!(eb.nritems(), 1);
        assert_eq!(eb.item_size(0), 0);
    }

    #[test]
    fn insert_preserves_descending_offsets() {
        let mut eb = empty_leaf(4096);
        // Insert items with varying data sizes
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 200]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 100]).unwrap();

        // Offsets must be in descending order (item 0 highest)
        for i in 0..eb.nritems() as usize - 1 {
            assert!(
                eb.item_offset(i) > eb.item_offset(i + 1),
                "offset[{i}]={} should be > offset[{}]={}",
                eb.item_offset(i),
                i + 1,
                eb.item_offset(i + 1)
            );
        }
    }

    #[test]
    fn first_item_data_ends_at_block_end() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 100]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 75]).unwrap();

        // Item 0's data must end at nodesize - HEADER_SIZE
        let end = eb.item_offset(0) + eb.item_size(0);
        assert_eq!(end, eb.nodesize() - HEADER_SIZE as u32);
    }

    #[test]
    fn insert_in_middle() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(3), &[0x33; 50]).unwrap();
        // Insert between items 0 and 1
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();

        assert_eq!(eb.nritems(), 3);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 2);
        assert_eq!(eb.item_key(2).objectid, 3);
        // Verify all data is correct
        assert_eq!(eb.item_data(0), &[0x11; 50]);
        assert_eq!(eb.item_data(1), &[0x22; 50]);
        assert_eq!(eb.item_data(2), &[0x33; 50]);
    }

    #[test]
    fn insert_variable_sizes() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 10]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 500]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 1]).unwrap();

        assert_eq!(eb.item_data(0), &[0x11; 10]);
        assert_eq!(eb.item_data(1), &[0x22; 500]);
        assert_eq!(eb.item_data(2), &[0x33; 1]);

        // Verify packing: item 0 ends at top
        let end = eb.item_offset(0) + eb.item_size(0);
        assert_eq!(end, eb.nodesize() - HEADER_SIZE as u32);
    }

    #[test]
    fn delete_middle_preserves_data() {
        let mut eb = empty_leaf(4096);
        insert_item(&mut eb, 0, &make_key(1), &[0x11; 50]).unwrap();
        insert_item(&mut eb, 1, &make_key(2), &[0x22; 50]).unwrap();
        insert_item(&mut eb, 2, &make_key(3), &[0x33; 50]).unwrap();

        del_items(&mut eb, 1, 1);
        assert_eq!(eb.nritems(), 2);
        assert_eq!(eb.item_key(0).objectid, 1);
        assert_eq!(eb.item_key(1).objectid, 3);
        assert_eq!(eb.item_data(0), &[0x11; 50]);
        assert_eq!(eb.item_data(1), &[0x33; 50]);
    }
}

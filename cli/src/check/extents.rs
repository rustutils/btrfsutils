use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    items::{ExtentItem, ItemPayload, parse_item_payload},
    reader::{self, BlockReader},
    tree::{KeyType, TreeBlock},
};
use std::io::{Read, Seek};

/// Check extent tree: verify reference counts and detect overlapping extents.
pub fn check_extent_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    extent_root: u64,
    results: &mut CheckResults,
) {
    let mut state = ExtentCheckState::default();

    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                let item_data =
                    &data[item.offset as usize..][..item.size as usize];
                process_extent_item(&item.key, item_data, &mut state, results);
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) = reader::tree_walk_tolerant(
        reader,
        extent_root,
        &mut visitor,
        &mut on_error,
    ) {
        results.report(CheckError::ReadError {
            logical: extent_root,
            detail: format!("extent tree root: {e}"),
        });
        return;
    }

    // Flush the last pending extent.
    flush_pending(&mut state, results);

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    results.total_extent_tree_bytes = state.extent_tree_bytes;
    results.data_bytes_allocated = state.data_bytes_allocated;
    results.data_bytes_referenced = state.data_bytes_referenced;
}

#[derive(Default)]
struct ExtentCheckState {
    /// Currently tracked extent bytenr (0 = none pending).
    pending_bytenr: u64,
    /// Length of the pending extent (from the key offset for EXTENT_ITEM).
    pending_length: u64,
    /// Declared ref count from the ExtentItem.
    pending_refs: u64,
    /// Counted refs (inline + standalone).
    pending_counted: u64,
    /// Whether the pending extent is a data extent.
    pending_is_data: bool,
    /// End of the previous extent (for overlap detection).
    prev_end: u64,
    /// Accumulated stats.
    extent_tree_bytes: u64,
    data_bytes_allocated: u64,
    data_bytes_referenced: u64,
}

fn process_extent_item(
    key: &btrfs_disk::tree::DiskKey,
    data: &[u8],
    state: &mut ExtentCheckState,
    results: &mut CheckResults,
) {
    match key.key_type {
        KeyType::ExtentItem | KeyType::MetadataItem => {
            // New extent: flush the previous one.
            flush_pending(state, results);

            let bytenr = key.objectid;
            let length = if key.key_type == KeyType::ExtentItem {
                key.offset
            } else {
                // MetadataItem: length is nodesize, but we don't have it
                // here. Use 0 to skip overlap checks for metadata items
                // (they use skinny refs where offset = level, not length).
                0
            };

            // Overlap detection.
            if length > 0 && bytenr < state.prev_end && state.prev_end > 0 {
                results.report(CheckError::OverlappingExtent {
                    bytenr,
                    length,
                    prev_end: state.prev_end,
                });
            }

            if length > 0 {
                state.prev_end = bytenr + length;
            }

            // Parse the extent item.
            let payload = parse_item_payload(key, data);
            let (refs, inline_count, is_data) = match &payload {
                ItemPayload::ExtentItem(ei) => {
                    let count = count_inline_refs(ei);
                    (ei.refs, count, ei.is_data())
                }
                _ => (0, 0, false),
            };

            state.pending_bytenr = bytenr;
            state.pending_length = length;
            state.pending_refs = refs;
            state.pending_counted = inline_count;
            state.pending_is_data = is_data;

            if is_data {
                state.data_bytes_allocated += length;
            }
        }

        // Standalone backref items: add to the count of the current extent.
        KeyType::TreeBlockRef
        | KeyType::SharedBlockRef
        | KeyType::ExtentOwnerRef => {
            if key.objectid == state.pending_bytenr {
                state.pending_counted += 1;
            }
        }

        KeyType::ExtentDataRef => {
            if key.objectid == state.pending_bytenr {
                // ExtentDataRef has a count field inside.
                if let ItemPayload::ExtentDataRef(edr) =
                    parse_item_payload(key, data)
                {
                    state.pending_counted += u64::from(edr.count);
                    state.data_bytes_referenced +=
                        state.pending_length * u64::from(edr.count);
                } else {
                    state.pending_counted += 1;
                }
            }
        }

        KeyType::SharedDataRef => {
            if key.objectid == state.pending_bytenr {
                if let ItemPayload::SharedDataRef(sdr) =
                    parse_item_payload(key, data)
                {
                    state.pending_counted += u64::from(sdr.count);
                    state.data_bytes_referenced +=
                        state.pending_length * u64::from(sdr.count);
                } else {
                    state.pending_counted += 1;
                }
            }
        }

        KeyType::BlockGroupItem => {
            // Block group items live in the extent tree (or block group
            // tree). Skip them here — they're checked in chunks.rs.
        }

        _ => {}
    }
}

fn flush_pending(state: &mut ExtentCheckState, results: &mut CheckResults) {
    if state.pending_bytenr == 0 {
        return;
    }

    // For data extents with inline refs only (no standalone ExtentDataRef),
    // account the referenced bytes from the inline ref count.
    if state.pending_is_data && state.data_bytes_referenced == 0 {
        state.data_bytes_referenced +=
            state.pending_length * state.pending_counted;
    }

    if state.pending_refs != state.pending_counted {
        results.report(CheckError::ExtentRefMismatch {
            bytenr: state.pending_bytenr,
            expected: state.pending_refs,
            found: state.pending_counted,
        });
    }

    state.pending_bytenr = 0;
}

/// Count the number of references from inline backrefs in an ExtentItem.
fn count_inline_refs(ei: &ExtentItem) -> u64 {
    let mut count = 0u64;
    for iref in &ei.inline_refs {
        match iref {
            btrfs_disk::items::InlineRef::ExtentDataBackref {
                count: c,
                ..
            } => count += u64::from(*c),
            btrfs_disk::items::InlineRef::SharedDataBackref {
                count: c,
                ..
            } => count += u64::from(*c),
            _ => count += 1,
        }
    }
    count
}

use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    items::{ExtentItem, ItemPayload, parse_item_payload},
    reader::{self, BlockReader},
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::HashSet,
    io::{Read, Seek},
};

/// Header size in a btrfs tree block (bytes before item data area).
const HEADER_SIZE: usize = std::mem::size_of::<btrfs_disk::raw::btrfs_header>();

/// Check extent tree: verify reference counts, detect overlapping extents,
/// and cross-check that every tree block address from tree walks has a
/// corresponding METADATA_ITEM or EXTENT_ITEM entry.
pub fn check_extent_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    extent_root: u64,
    tree_block_addrs: &HashSet<u64>,
    results: &mut CheckResults,
) {
    let mut state = ExtentCheckState::default();

    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                let start = HEADER_SIZE + item.offset as usize;
                let item_data = &data[start..][..item.size as usize];
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

    // Cross-check: every tree block found during tree walks must have a
    // corresponding METADATA_ITEM or EXTENT_ITEM in the extent tree.
    for &addr in tree_block_addrs {
        if !state.extent_item_addrs.contains(&addr) {
            results.report(CheckError::MissingExtentItem { bytenr: addr });
        }
    }

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
    data_bytes_allocated: u64,
    data_bytes_referenced: u64,
    /// All bytenrs that have a METADATA_ITEM or EXTENT_ITEM entry.
    extent_item_addrs: HashSet<u64>,
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
            state.extent_item_addrs.insert(bytenr);
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

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::items::{ExtentFlags, ExtentItem, InlineRef};

    fn make_extent_item(
        refs: u64,
        flags: ExtentFlags,
        inline_refs: Vec<InlineRef>,
    ) -> ExtentItem {
        ExtentItem {
            refs,
            generation: 1,
            flags,
            tree_block_key: None,
            tree_block_level: None,
            skinny_level: None,
            inline_refs,
        }
    }

    #[test]
    fn count_inline_refs_tree_block_backrefs() {
        let ei = make_extent_item(
            2,
            ExtentFlags::TREE_BLOCK,
            vec![
                InlineRef::TreeBlockBackref {
                    ref_offset: 0,
                    root: 1,
                },
                InlineRef::SharedBlockBackref {
                    ref_offset: 0,
                    parent: 4096,
                },
            ],
        );
        assert_eq!(count_inline_refs(&ei), 2);
    }

    #[test]
    fn count_inline_refs_data_backrefs_with_counts() {
        let ei = make_extent_item(
            5,
            ExtentFlags::DATA,
            vec![
                InlineRef::ExtentDataBackref {
                    ref_offset: 0,
                    root: 5,
                    objectid: 256,
                    offset: 0,
                    count: 3,
                },
                InlineRef::SharedDataBackref {
                    ref_offset: 0,
                    parent: 8192,
                    count: 2,
                },
            ],
        );
        assert_eq!(count_inline_refs(&ei), 5);
    }

    #[test]
    fn count_inline_refs_empty() {
        let ei = make_extent_item(0, ExtentFlags::DATA, vec![]);
        assert_eq!(count_inline_refs(&ei), 0);
    }

    #[test]
    fn count_inline_refs_owner_ref() {
        let ei = make_extent_item(
            1,
            ExtentFlags::TREE_BLOCK,
            vec![InlineRef::ExtentOwnerRef {
                ref_offset: 0,
                root: 2,
            }],
        );
        assert_eq!(count_inline_refs(&ei), 1);
    }

    #[test]
    fn flush_pending_no_op_when_empty() {
        let mut state = ExtentCheckState::default();
        let mut results = CheckResults::new(0);
        flush_pending(&mut state, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn flush_pending_matching_refs() {
        let mut state = ExtentCheckState {
            pending_bytenr: 1048576,
            pending_length: 4096,
            pending_refs: 1,
            pending_counted: 1,
            pending_is_data: true,
            ..Default::default()
        };
        let mut results = CheckResults::new(0);
        flush_pending(&mut state, &mut results);
        assert_eq!(results.error_count, 0);
        // Should reset pending_bytenr.
        assert_eq!(state.pending_bytenr, 0);
    }

    #[test]
    fn flush_pending_ref_mismatch_reports_error() {
        let mut state = ExtentCheckState {
            pending_bytenr: 1048576,
            pending_length: 4096,
            pending_refs: 2,
            pending_counted: 1,
            pending_is_data: false,
            ..Default::default()
        };
        let mut results = CheckResults::new(0);
        flush_pending(&mut state, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn flush_pending_accounts_data_referenced_bytes() {
        let mut state = ExtentCheckState {
            pending_bytenr: 1048576,
            pending_length: 4096,
            pending_refs: 1,
            pending_counted: 1,
            pending_is_data: true,
            data_bytes_referenced: 0,
            ..Default::default()
        };
        let mut results = CheckResults::new(0);
        flush_pending(&mut state, &mut results);
        assert_eq!(state.data_bytes_referenced, 4096);
    }
}

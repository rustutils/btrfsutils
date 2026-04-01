use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    items::{BlockGroupItem, DeviceExtent, ItemPayload, parse_item_payload},
    reader::{self, BlockReader},
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::{BTreeMap, HashMap},
    io::{Read, Seek},
};

/// Header size in a btrfs tree block (bytes before item data area).
const HEADER_SIZE: usize = std::mem::size_of::<btrfs_disk::raw::btrfs_header>();

/// Cross-check chunks, block groups, and device extents.
pub fn check_chunks<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    extent_root: u64,
    block_group_tree_root: Option<u64>,
    dev_tree_root: u64,
    results: &mut CheckResults,
) {
    // Collect block groups from the block group tree (if present) or extent tree.
    let bg_root = block_group_tree_root.unwrap_or(extent_root);
    let block_groups = collect_block_groups(reader, bg_root, results);

    // Cross-check: every chunk should have a matching block group.
    for chunk in reader.chunk_cache().iter() {
        if !block_groups.contains_key(&chunk.logical) {
            results.report(CheckError::ChunkMissingBlockGroup {
                logical: chunk.logical,
            });
        }
    }

    // Cross-check: every block group should have a matching chunk.
    for &bg_logical in block_groups.keys() {
        if reader.chunk_cache().lookup(bg_logical).is_none() {
            results.report(CheckError::BlockGroupMissingChunk {
                logical: bg_logical,
            });
        }
    }

    // Collect device extents and check for overlaps.
    check_device_extents(reader, dev_tree_root, results);
}

/// Walk a tree and collect all `BlockGroupItem` entries.
fn collect_block_groups<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root: u64,
    results: &mut CheckResults,
) -> BTreeMap<u64, BlockGroupItem> {
    let mut block_groups = BTreeMap::new();
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                if item.key.key_type != KeyType::BlockGroupItem {
                    continue;
                }
                let start = HEADER_SIZE + item.offset as usize;
                let item_data = &data[start..][..item.size as usize];
                if let ItemPayload::BlockGroupItem(bg) =
                    parse_item_payload(&item.key, item_data)
                {
                    block_groups.insert(item.key.objectid, bg);
                }
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) =
        reader::tree_walk_tolerant(reader, root, &mut visitor, &mut on_error)
    {
        results.report(CheckError::ReadError {
            logical: root,
            detail: format!("block group tree root: {e}"),
        });
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    block_groups
}

/// Walk the device tree and check for overlapping device extents.
fn check_device_extents<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    dev_tree_root: u64,
    results: &mut CheckResults,
) {
    // Collect device extents grouped by device ID.
    let mut dev_extents: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                if item.key.key_type != KeyType::DeviceExtent {
                    continue;
                }
                let start = HEADER_SIZE + item.offset as usize;
                let item_data = &data[start..][..item.size as usize];
                if let Some(de) = DeviceExtent::parse(item_data) {
                    let devid = item.key.objectid;
                    let offset = item.key.offset;
                    dev_extents
                        .entry(devid)
                        .or_default()
                        .push((offset, de.length));
                }
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) = reader::tree_walk_tolerant(
        reader,
        dev_tree_root,
        &mut visitor,
        &mut on_error,
    ) {
        results.report(CheckError::ReadError {
            logical: dev_tree_root,
            detail: format!("device tree root: {e}"),
        });
        return;
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    // Check for overlaps within each device.
    for (devid, extents) in &mut dev_extents {
        extents.sort_by_key(|&(offset, _)| offset);
        for i in 1..extents.len() {
            let prev_end = extents[i - 1].0 + extents[i - 1].1;
            let cur_start = extents[i].0;
            if cur_start < prev_end {
                results.report(CheckError::DeviceExtentOverlap {
                    devid: *devid,
                    offset: cur_start,
                });
            }
        }
    }
}

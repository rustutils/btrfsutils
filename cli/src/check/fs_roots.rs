use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    items::{
        DirItem, FileExtentBody, FileExtentItem, FileExtentType, InodeExtref,
        InodeItem, InodeRef,
    },
    raw,
    reader::{self, BlockReader},
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::{BTreeMap, HashSet},
    io::{Read, Seek},
};

/// Header size in a btrfs tree block (bytes before item data area).
const HEADER_SIZE: usize = std::mem::size_of::<btrfs_disk::raw::btrfs_header>();

/// Check all filesystem trees (subvolumes) for inode consistency.
pub fn check_fs_roots<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    tree_roots: &BTreeMap<u64, (u64, u64)>,
    results: &mut CheckResults,
) {
    for (&tree_id, &(bytenr, _gen)) in tree_roots {
        // FS trees have objectid >= FIRST_FREE_OBJECTID (256) or are the
        // default FS tree (objectid 5).
        let is_fs_tree = tree_id == u64::from(raw::BTRFS_FS_TREE_OBJECTID)
            || tree_id >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);
        if !is_fs_tree {
            continue;
        }

        check_one_fs_tree(reader, tree_id, bytenr, results);
    }
}

/// Mode mask for file type bits.
const S_IFMT: u32 = 0o17_0000;
/// Directory mode flag.
const S_IFDIR: u32 = 0o04_0000;
/// Regular file mode flag.
const S_IFREG: u32 = 0o10_0000;
/// Symlink mode flag.
const S_IFLNK: u32 = 0o12_0000;

#[allow(clippy::too_many_lines)]
fn check_one_fs_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    tree_id: u64,
    root_bytenr: u64,
    results: &mut CheckResults,
) {
    // Collect all items from this FS tree, grouped by inode number.
    let Some(items) = collect_fs_items(reader, root_bytenr, results) else {
        return;
    };

    // Set of all inodes that have an INODE_ITEM.
    let inodes_with_item: HashSet<u64> = items
        .iter()
        .filter(|(_, entries)| {
            entries.iter().any(|(kt, _, _)| *kt == KeyType::InodeItem)
        })
        .map(|(&ino, _)| ino)
        .collect();

    for (&ino, entries) in &items {
        let mut has_inode_item = false;
        let mut inode_nlink: u32 = 0;
        let mut inode_size: u64 = 0;
        let mut inode_nbytes: u64 = 0;
        let mut inode_mode: u32 = 0;
        let mut ref_count: u32 = 0;
        // Track file extent ranges for overlap detection: (file_offset, end).
        let mut extent_ranges: Vec<(u64, u64)> = Vec::new();
        // Sum of name lengths from DIR_INDEX entries (for dir size check).
        let mut dir_index_name_sum: u64 = 0;
        // Computed nbytes from file extents.
        let mut computed_nbytes: u64 = 0;

        for (key_type, key_offset, data) in entries {
            match key_type {
                KeyType::InodeItem => {
                    has_inode_item = true;
                    if let Some(ii) = InodeItem::parse(data) {
                        inode_nlink = ii.nlink;
                        inode_size = ii.size;
                        inode_nbytes = ii.nbytes;
                        inode_mode = ii.mode;
                    }
                }
                KeyType::InodeRef => {
                    for _r in InodeRef::parse_all(data) {
                        ref_count += 1;
                    }
                }
                KeyType::InodeExtref => {
                    for _r in InodeExtref::parse_all(data) {
                        ref_count += 1;
                    }
                }
                KeyType::ExtentData => {
                    if let Some(fe) = FileExtentItem::parse(data) {
                        let len = match &fe.body {
                            FileExtentBody::Regular { num_bytes, .. } => {
                                *num_bytes
                            }
                            FileExtentBody::Inline { inline_size } => {
                                *inline_size as u64
                            }
                        };
                        if len > 0 {
                            extent_ranges
                                .push((*key_offset, *key_offset + len));
                        }

                        // Accumulate nbytes from extents.
                        match &fe.body {
                            FileExtentBody::Inline { inline_size } => {
                                computed_nbytes += *inline_size as u64;
                            }
                            FileExtentBody::Regular {
                                disk_num_bytes, ..
                            } => {
                                // Only count actual disk allocations
                                // (not holes where disk_bytenr == 0),
                                // and not prealloc extents.
                                if fe.extent_type != FileExtentType::Prealloc {
                                    computed_nbytes += disk_num_bytes;
                                }
                            }
                        }
                    }
                }
                KeyType::DirItem => {
                    let dir_items = DirItem::parse_all(data);
                    for di in &dir_items {
                        let child_ino = di.location.objectid;
                        if di.location.key_type == KeyType::InodeItem
                            && child_ino
                                >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID)
                            && !inodes_with_item.contains(&child_ino)
                        {
                            let name =
                                String::from_utf8_lossy(&di.name).into_owned();
                            results.report(CheckError::DirItemOrphan {
                                tree: tree_id,
                                parent_ino: ino,
                                name,
                            });
                        }
                    }
                }
                KeyType::DirIndex => {
                    let dir_items = DirItem::parse_all(data);
                    for di in &dir_items {
                        dir_index_name_sum += di.name.len() as u64 * 2;

                        let child_ino = di.location.objectid;
                        if di.location.key_type == KeyType::InodeItem
                            && child_ino
                                >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID)
                            && !inodes_with_item.contains(&child_ino)
                        {
                            let name =
                                String::from_utf8_lossy(&di.name).into_owned();
                            results.report(CheckError::DirItemOrphan {
                                tree: tree_id,
                                parent_ino: ino,
                                name,
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        // File extent overlap detection. Items arrive sorted by key
        // (objectid, type, offset), so EXTENT_DATA items for one inode are
        // already in file-offset order.
        for i in 1..extent_ranges.len() {
            let prev_end = extent_ranges[i - 1].1;
            let cur_start = extent_ranges[i].0;
            if cur_start < prev_end {
                results.report(CheckError::FileExtentOverlap {
                    tree: tree_id,
                    ino,
                    offset: cur_start,
                });
            }
        }

        // Nlink check: skip the root dir inode (256) which has special nlink
        // handling, and skip inodes without an inode item (already reported).
        if has_inode_item
            && ino >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID)
            && inode_nlink != ref_count
            && ref_count > 0
        {
            results.report(CheckError::NlinkMismatch {
                tree: tree_id,
                ino,
                expected: inode_nlink,
                found: ref_count,
            });
        }

        if !has_inode_item {
            continue;
        }

        let file_type = inode_mode & S_IFMT;

        // Directory inode size check: size should match sum of
        // name_len * 2 from DIR_INDEX entries.
        if file_type == S_IFDIR && inode_size != dir_index_name_sum {
            results.report(CheckError::DirSizeWrong {
                tree: tree_id,
                ino,
                expected: dir_index_name_sum,
                found: inode_size,
            });
        }

        // File nbytes check: nbytes should match computed total from extents.
        if (file_type == S_IFREG || file_type == S_IFLNK)
            && inode_nbytes != computed_nbytes
        {
            results.report(CheckError::NbytesWrong {
                tree: tree_id,
                ino,
                expected: computed_nbytes,
                found: inode_nbytes,
            });
        }
    }
}

/// Collected items for one FS tree, grouped by objectid (inode number).
/// Each entry is (`key_type`, `key_offset`, `raw_data`).
type FsItemMap = BTreeMap<u64, Vec<(KeyType, u64, Vec<u8>)>>;

fn collect_fs_items<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_bytenr: u64,
    results: &mut CheckResults,
) -> Option<FsItemMap> {
    let mut items: FsItemMap = BTreeMap::new();
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf {
            items: leaf_items,
            data,
            ..
        } = block
        {
            for item in leaf_items {
                let start = HEADER_SIZE + item.offset as usize;
                let item_data = data[start..][..item.size as usize].to_vec();
                items.entry(item.key.objectid).or_default().push((
                    item.key.key_type,
                    item.key.offset,
                    item_data,
                ));
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) = reader::tree_walk_tolerant(
        reader,
        root_bytenr,
        &mut visitor,
        &mut on_error,
    ) {
        results.report(CheckError::ReadError {
            logical: root_bytenr,
            detail: format!("fs tree root: {e}"),
        });
        return None;
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    Some(items)
}

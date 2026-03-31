use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    reader::{self, BlockReader},
    superblock::{ChecksumType, Superblock},
    tree::{DiskKey, TreeBlock},
    util::btrfs_csum_data,
};
use std::io::{Read, Seek};
use uuid::Uuid;

/// Check tree block structure for all trees in the filesystem.
///
/// Walks every tree discovered in `tree_roots` plus the root and chunk trees.
/// For each block, verifies checksum, header fields, and key ordering.
pub fn check_all_trees<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    sb: &Superblock,
    tree_roots: &std::collections::BTreeMap<u64, (u64, u64)>,
    results: &mut CheckResults,
) {
    let fsid = effective_fsid(sb);
    let csum_supported = sb.csum_type == ChecksumType::Crc32;

    if !csum_supported {
        eprintln!(
            "warning: checksum type {} is not supported for \
             verification, skipping checksum checks",
            sb.csum_type
        );
    }

    let ctx = TreeCheckCtx {
        fsid,
        super_generation: sb.generation,
        nodesize: sb.nodesize,
        csum_supported,
    };

    // Check root tree.
    check_tree(reader, "root tree", sb.root, &ctx, results);

    // Check chunk tree.
    check_tree(reader, "chunk tree", sb.chunk_root, &ctx, results);

    // Check all trees discovered in the root tree.
    for (&tree_id, &(bytenr, _gen)) in tree_roots {
        let name = tree_name(tree_id);
        check_tree(reader, &name, bytenr, &ctx, results);
    }
}

struct TreeCheckCtx {
    fsid: Uuid,
    super_generation: u64,
    nodesize: u32,
    csum_supported: bool,
}

fn check_tree<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    name: &str,
    root_bytenr: u64,
    ctx: &TreeCheckCtx,
    results: &mut CheckResults,
) {
    let tree: &'static str = leak_name(name);

    // Collect read errors separately to avoid two mutable borrows of results.
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |raw: &[u8], block: &TreeBlock| {
        check_block(raw, block, tree, ctx, results);
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
            detail: format!("{tree} root: {e}"),
        });
        return;
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }
}

fn check_block(
    raw: &[u8],
    block: &TreeBlock,
    tree: &'static str,
    ctx: &TreeCheckCtx,
    results: &mut CheckResults,
) {
    let header = block.header();
    let logical = header.bytenr;

    results.total_tree_bytes += u64::from(ctx.nodesize);

    // Checksum verification (CRC32C only).
    if ctx.csum_supported {
        let computed = btrfs_csum_data(&raw[32..]);
        let stored = u32::from_le_bytes(raw[0..4].try_into().unwrap());
        if computed != stored {
            results.report(CheckError::TreeBlockChecksumMismatch {
                tree,
                logical,
            });
        }
    }

    // Fsid check.
    if header.fsid != ctx.fsid {
        results.report(CheckError::TreeBlockBadFsid { tree, logical });
    }

    // Generation check.
    if header.generation > ctx.super_generation {
        results.report(CheckError::TreeBlockBadGeneration {
            tree,
            logical,
            block_gen: header.generation,
            super_gen: ctx.super_generation,
        });
    }

    match block {
        TreeBlock::Leaf { items, data, .. } => {
            if header.level != 0 {
                results.report(CheckError::TreeBlockBadLevel {
                    tree,
                    logical,
                    detail: format!(
                        "leaf has level {} (expected 0)",
                        header.level
                    ),
                });
            }

            // Key ordering: strictly ascending.
            check_key_order_items(items, tree, logical, results);

            // Space waste calculation.
            let header_size = 101u64; // btrfs_header
            let item_desc_size = 25u64; // btrfs_item (key + offset + size)
            let item_data_total: u64 =
                items.iter().map(|i| u64::from(i.size)).sum();
            let used = header_size
                + (items.len() as u64) * item_desc_size
                + item_data_total;
            let nodesize = u64::from(ctx.nodesize);
            if used < nodesize {
                results.btree_space_waste += nodesize - used;
            }

            let _ = data; // data is available for item-level checks later
        }
        TreeBlock::Node { ptrs, .. } => {
            if header.level == 0 {
                results.report(CheckError::TreeBlockBadLevel {
                    tree,
                    logical,
                    detail: "node has level 0 (expected > 0)".into(),
                });
            }

            // Key ordering: strictly ascending.
            check_key_order_ptrs(ptrs, tree, logical, results);
        }
    }
}

fn check_key_order_items(
    items: &[btrfs_disk::tree::Item],
    tree: &'static str,
    logical: u64,
    results: &mut CheckResults,
) {
    for i in 1..items.len() {
        let prev = &items[i - 1].key;
        let cur = &items[i].key;
        if !key_less(prev, cur) {
            results.report(CheckError::KeyOrderViolation {
                tree,
                logical,
                index: i,
            });
        }
    }
}

fn check_key_order_ptrs(
    ptrs: &[btrfs_disk::tree::KeyPtr],
    tree: &'static str,
    logical: u64,
    results: &mut CheckResults,
) {
    for i in 1..ptrs.len() {
        let prev = &ptrs[i - 1].key;
        let cur = &ptrs[i].key;
        if !key_less(prev, cur) {
            results.report(CheckError::KeyOrderViolation {
                tree,
                logical,
                index: i,
            });
        }
    }
}

/// Strict less-than comparison for disk keys.
fn key_less(a: &DiskKey, b: &DiskKey) -> bool {
    let a_type = a.key_type.to_raw();
    let b_type = b.key_type.to_raw();
    (a.objectid, a_type, a.offset) < (b.objectid, b_type, b.offset)
}

/// Return the fsid used for tree block validation. If the METADATA_UUID
/// incompat flag is set, tree blocks use `metadata_uuid`; otherwise `fsid`.
fn effective_fsid(sb: &Superblock) -> Uuid {
    if sb.has_metadata_uuid() {
        sb.metadata_uuid
    } else {
        sb.fsid
    }
}

fn tree_name(tree_id: u64) -> String {
    use btrfs_disk::tree::ObjectId;
    let oid = ObjectId::from_raw(tree_id);
    format!("{oid}")
}

/// Leak a string so it can be used as `&'static str`.
///
/// Used for tree names in error reporting. The set of tree names is small
/// and bounded, so the leaked memory is negligible.
fn leak_name(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::tree::KeyType;

    fn make_key(objectid: u64, key_type: KeyType, offset: u64) -> DiskKey {
        DiskKey {
            objectid,
            key_type,
            offset,
        }
    }

    #[test]
    fn key_less_by_objectid() {
        let a = make_key(1, KeyType::InodeItem, 0);
        let b = make_key(2, KeyType::InodeItem, 0);
        assert!(key_less(&a, &b));
        assert!(!key_less(&b, &a));
    }

    #[test]
    fn key_less_by_type() {
        let a = make_key(256, KeyType::InodeItem, 0);
        let b = make_key(256, KeyType::InodeRef, 0);
        assert!(key_less(&a, &b));
        assert!(!key_less(&b, &a));
    }

    #[test]
    fn key_less_by_offset() {
        let a = make_key(256, KeyType::ExtentData, 0);
        let b = make_key(256, KeyType::ExtentData, 4096);
        assert!(key_less(&a, &b));
        assert!(!key_less(&b, &a));
    }

    #[test]
    fn key_less_equal_is_false() {
        let a = make_key(256, KeyType::InodeItem, 0);
        assert!(!key_less(&a, &a));
    }

    #[test]
    fn check_key_order_items_valid() {
        let items = vec![
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeItem, 0),
                offset: 0,
                size: 0,
            },
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeRef, 256),
                offset: 0,
                size: 0,
            },
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::ExtentData, 0),
                offset: 0,
                size: 0,
            },
        ];
        let mut results = CheckResults::new(0);
        check_key_order_items(&items, "test", 0, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn check_key_order_items_violation() {
        let items = vec![
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeRef, 256),
                offset: 0,
                size: 0,
            },
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeItem, 0),
                offset: 0,
                size: 0,
            },
        ];
        let mut results = CheckResults::new(0);
        check_key_order_items(&items, "test", 0, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn check_key_order_items_duplicate() {
        let items = vec![
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeItem, 0),
                offset: 0,
                size: 0,
            },
            btrfs_disk::tree::Item {
                key: make_key(256, KeyType::InodeItem, 0),
                offset: 0,
                size: 0,
            },
        ];
        let mut results = CheckResults::new(0);
        check_key_order_items(&items, "test", 0, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn check_key_order_items_single_item_no_error() {
        let items = vec![btrfs_disk::tree::Item {
            key: make_key(256, KeyType::InodeItem, 0),
            offset: 0,
            size: 0,
        }];
        let mut results = CheckResults::new(0);
        check_key_order_items(&items, "test", 0, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn check_key_order_items_empty_no_error() {
        let items: Vec<btrfs_disk::tree::Item> = vec![];
        let mut results = CheckResults::new(0);
        check_key_order_items(&items, "test", 0, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn check_key_order_ptrs_valid() {
        let ptrs = vec![
            btrfs_disk::tree::KeyPtr {
                key: make_key(1, KeyType::RootItem, 0),
                blockptr: 4096,
                generation: 1,
            },
            btrfs_disk::tree::KeyPtr {
                key: make_key(2, KeyType::RootItem, 0),
                blockptr: 8192,
                generation: 1,
            },
        ];
        let mut results = CheckResults::new(0);
        check_key_order_ptrs(&ptrs, "test", 0, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn check_key_order_ptrs_violation() {
        let ptrs = vec![
            btrfs_disk::tree::KeyPtr {
                key: make_key(5, KeyType::RootItem, 0),
                blockptr: 4096,
                generation: 1,
            },
            btrfs_disk::tree::KeyPtr {
                key: make_key(2, KeyType::RootItem, 0),
                blockptr: 8192,
                generation: 1,
            },
        ];
        let mut results = CheckResults::new(0);
        check_key_order_ptrs(&ptrs, "test", 0, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn tree_name_known_objectids() {
        assert_eq!(tree_name(1), "ROOT_TREE");
        assert_eq!(tree_name(2), "EXTENT_TREE");
        assert_eq!(tree_name(3), "CHUNK_TREE");
        assert_eq!(tree_name(4), "DEV_TREE");
        assert_eq!(tree_name(5), "FS_TREE");
    }

    #[test]
    fn tree_name_subvolume() {
        // Subvolume trees have IDs >= 256.
        let name = tree_name(256);
        assert!(name.contains("256"));
    }
}

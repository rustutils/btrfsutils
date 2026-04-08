//! # Whole-tree conversion operations
//!
//! Builds new global trees from existing extent-tree state and flips
//! the matching `compat_ro` superblock bits. Provides
//! [`convert_to_free_space_tree`] and
//! [`convert_to_block_group_tree`].

use crate::{
    allocation,
    buffer::ITEM_SIZE,
    extent_walk::{self, AllocatedExtent},
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent, next_leaf},
    transaction::Transaction,
};
use btrfs_disk::{
    raw,
    tree::{DiskKey, KeyType},
};
use std::io::{self, Read, Seek, Write};

/// Tree id of the extent tree.
const EXTENT_TREE_ID: u64 = 2;
/// Tree id of the free space tree.
const FREE_SPACE_TREE_ID: u64 = 10;
/// Tree id of the block group tree.
const BLOCK_GROUP_TREE_ID: u64 = 11;
/// Tree id of the root tree.
const ROOT_TREE_ID: u64 = 1;

/// Convert a filesystem from no-FST (or v1 space cache) to a v2
/// free space tree, in a single transaction.
///
/// The caller must invoke [`Transaction::commit`] afterwards. If
/// the conversion errors out partway through, the caller is
/// expected to abort the transaction.
///
/// This is the simple-case implementation: it refuses to run if a
/// stale v2 FST root or v1 free-space-cache is present, and leaves
/// it to a future clear-cache helper to wipe those before re-running
/// the conversion.
///
/// # Errors
///
/// * The free space tree `compat_ro` bit is already set.
/// * A stale FST root exists (`root_bytenr(10).is_some()` despite
///   the bit being clear, or vice versa).
/// * Any v1 cache item is present in the root tree
///   (`objectid == BTRFS_FREE_SPACE_OBJECTID`).
/// * Any bitmap-layout block group is encountered.
/// * The metadata allocator runs out of space.
/// * Any tree read/write fails.
pub fn convert_to_free_space_tree<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
) -> io::Result<()> {
    let fst_bit = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE);
    let fst_valid_bit =
        u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID);
    let compat_ro = fs_info.superblock.compat_ro_flags;

    // Precondition: FST not already enabled, and no stale on-disk
    // root for tree id 10. The bit-and-pointer state should agree;
    // any disagreement is a corrupted (or partially-converted)
    // filesystem and is bounced back to the caller.
    if compat_ro & fst_bit != 0 {
        return Err(io::Error::other(
            "convert_to_free_space_tree: free space tree already enabled",
        ));
    }
    if fs_info.root_bytenr(FREE_SPACE_TREE_ID).is_some() {
        return Err(io::Error::other(
            "convert_to_free_space_tree: stale free space tree root present (option 2: clear with rescue first)",
        ));
    }
    if compat_ro & fst_valid_bit != 0 {
        return Err(io::Error::other(
            "convert_to_free_space_tree: FREE_SPACE_TREE_VALID set without FREE_SPACE_TREE; refusing to touch",
        ));
    }

    // Refuse if any v1 free-space-cache items are present in the
    // root tree. The v1 cache lives under inode objectid
    // BTRFS_FREE_SPACE_OBJECTID (= -11) and would need to be wiped
    // before a clean v2 build.
    if root_tree_has_v1_cache(fs_info)? {
        return Err(io::Error::other(
            "convert_to_free_space_tree: v1 free-space-cache present (option 2: clear with rescue first)",
        ));
    }

    // Step 1: create the new FST root.
    trans.create_empty_tree(fs_info, FREE_SPACE_TREE_ID)?;

    // Step 2: per-block-group population. Snapshot the BG list now
    // because we will be modifying the FST tree (allocating
    // metadata blocks for its leaves) which does not affect the
    // block-group set itself.
    let block_groups = allocation::load_block_groups(fs_info)?;
    debug_assert!(
        !block_groups.is_empty(),
        "convert_to_free_space_tree: no block groups found",
    );

    for bg in &block_groups {
        let mut allocated: Vec<AllocatedExtent> = Vec::new();
        extent_walk::walk_block_group_extents(
            fs_info,
            bg.start,
            bg.length,
            |e| {
                allocated.push(e);
                Ok(())
            },
        )?;

        let free_ranges =
            extent_walk::derive_free_ranges(bg.start, bg.length, &allocated)?;

        let extent_count = u32::try_from(free_ranges.len()).map_err(|_| {
            io::Error::other(format!(
                "convert_to_free_space_tree: BG {} has too many free extents to fit in u32",
                bg.start,
            ))
        })?;

        // 2a. Insert FREE_SPACE_INFO with non-bitmap layout. The
        // payload is 8 bytes: u32 extent_count, u32 flags=0.
        let mut info_data = [0u8; 8];
        info_data[0..4].copy_from_slice(&extent_count.to_le_bytes());
        // flags = 0 (extent layout, not USING_BITMAPS)
        let info_key = DiskKey {
            objectid: bg.start,
            key_type: KeyType::FreeSpaceInfo,
            offset: bg.length,
        };
        insert_in_tree(
            trans,
            fs_info,
            FREE_SPACE_TREE_ID,
            &info_key,
            &info_data,
        )?;

        // 2b. Insert one zero-payload FREE_SPACE_EXTENT per derived
        // free range.
        for r in &free_ranges {
            debug_assert!(r.length > 0);
            let key = DiskKey {
                objectid: r.start,
                key_type: KeyType::FreeSpaceExtent,
                offset: r.length,
            };
            insert_in_tree(trans, fs_info, FREE_SPACE_TREE_ID, &key, &[])?;
        }
    }

    // Step 3: flip superblock bits. The in-memory superblock is the
    // single source of truth; the commit path serialises it.
    fs_info.superblock.compat_ro_flags |= fst_bit | fst_valid_bit;
    // Zero cache_generation so the kernel does not try to load a
    // stale v1 cache on the next mount.
    fs_info.superblock.cache_generation = 0;

    Ok(())
}

/// Insert one item into the given tree via the standard search +
/// insert path. Used to populate `FREE_SPACE_INFO` and
/// `FREE_SPACE_EXTENT` entries.
fn insert_in_tree<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
    tree_id: u64,
    key: &DiskKey,
    data: &[u8],
) -> io::Result<()> {
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(&mut *trans),
        fs_info,
        tree_id,
        key,
        &mut path,
        SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
        true,
    )?;
    if found {
        path.release();
        return Err(io::Error::other(format!(
            "insert_in_tree: duplicate key {key:?} in tree {tree_id}",
        )));
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| io::Error::other("insert_in_tree: no leaf in path"))?;
    items::insert_item(leaf, path.slots[0], key, data)?;
    fs_info.mark_dirty(leaf);
    path.release();
    Ok(())
}

/// Scan the root tree for any item whose objectid equals
/// `BTRFS_FREE_SPACE_OBJECTID` (= -11). Used as the v1-space-cache
/// presence probe.
fn root_tree_has_v1_cache<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
) -> io::Result<bool> {
    let target_oid = raw::BTRFS_FREE_SPACE_OBJECTID as u64;
    let key = DiskKey {
        objectid: target_oid,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs_info,
        ROOT_TREE_ID,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;

    let result = loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break false;
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            if !next_leaf(fs_info, &mut path)? {
                break false;
            }
            continue;
        }
        let k = leaf.item_key(slot);
        if k.objectid == target_oid {
            break true;
        }
        if k.objectid > target_oid {
            break false;
        }
        path.slots[0] = slot + 1;
    };
    path.release();
    Ok(result)
}

/// One snapshotted `BLOCK_GROUP_ITEM` from the extent tree.
struct BlockGroupItemSnapshot {
    key: DiskKey,
    /// Raw on-disk payload bytes, copied verbatim from the extent
    /// tree leaf so the BGT round-trips byte-for-byte.
    data: Vec<u8>,
}

/// Convert a filesystem from no-BGT to a v2 block group tree, in
/// a single transaction.
///
/// The caller must invoke [`Transaction::commit`] afterwards. If
/// the conversion errors out partway through, the caller is
/// expected to abort the transaction.
///
/// Like [`convert_to_free_space_tree`], this is the simple-case
/// implementation: it refuses to run if the BGT `compat_ro` bit is
/// already set, if a stale BGT root is registered, or if the FST
/// is missing.
///
/// Multi-leaf BGT is supported: the routing override holds across
/// `split_leaf` calls triggered during BGT population, so any
/// allocator metadata reads continue to consult the extent tree
/// while the BGT is being built.
///
/// # Errors
///
/// * The block group tree `compat_ro` bit is already set.
/// * A root for tree id 11 is already registered.
/// * The free space tree is not enabled (`FREE_SPACE_TREE` bit
///   missing) or not valid (`FREE_SPACE_TREE_VALID` bit missing).
///   The kernel requires both for BGT.
/// * Any tree read/write or allocator operation fails.
pub fn convert_to_block_group_tree<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
) -> io::Result<()> {
    let bgt_bit = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE);
    let fst_bit = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE);
    let fst_valid_bit =
        u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID);
    let compat_ro = fs_info.superblock.compat_ro_flags;

    // Preconditions: refuse-on-weird, no repair attempts.
    if compat_ro & bgt_bit != 0 {
        return Err(io::Error::other(
            "convert_to_block_group_tree: block group tree already enabled",
        ));
    }
    if fs_info.root_bytenr(BLOCK_GROUP_TREE_ID).is_some() {
        return Err(io::Error::other(
            "convert_to_block_group_tree: stale block group tree root present (refusing)",
        ));
    }
    if compat_ro & fst_bit == 0 {
        return Err(io::Error::other(
            "convert_to_block_group_tree: free space tree must be enabled first (kernel requires FST for BGT)",
        ));
    }
    if compat_ro & fst_valid_bit == 0 {
        return Err(io::Error::other(
            "convert_to_block_group_tree: free space tree is not marked VALID (refusing)",
        ));
    }

    // Step 1: snapshot every BLOCK_GROUP_ITEM from the extent
    // tree. This must run BEFORE we pin the override or create
    // any new tree, because load_block_groups currently routes to
    // the extent tree (BGT not registered yet) which is what we
    // want for this read.
    let snapshots = collect_block_group_items(fs_info)?;
    debug_assert!(
        !snapshots.is_empty(),
        "convert_to_block_group_tree: no block group items found in extent tree",
    );

    // Step 2: pin the routing override to the extent tree for the
    // remainder of the conversion. The guard restores the previous
    // value (None) on drop, even on panic or `?`.
    let mut guard = fs_info.pin_block_group_tree(EXTENT_TREE_ID);
    let fs_info = guard.fs_mut();

    // Step 3: create the BGT root. The internal alloc + root-tree
    // ROOT_ITEM insert both call the allocator, which now reads
    // from the extent tree thanks to the override.
    trans.create_empty_tree(fs_info, BLOCK_GROUP_TREE_ID)?;

    // Step 4: insert every snapshotted BG item into BGT, in the
    // order returned by collect_block_group_items (already sorted
    // by extent-tree compound key, so by BG start). Inserts go
    // into the freshly created empty leaf; if there are too many
    // for a single leaf the standard split path runs and the
    // override keeps the allocator reading from the extent tree.
    for snap in &snapshots {
        insert_in_tree(
            trans,
            fs_info,
            BLOCK_GROUP_TREE_ID,
            &snap.key,
            &snap.data,
        )?;
    }

    // Step 5: delete every BG item from the extent tree. These
    // deletions go through the standard search_slot Delete path,
    // which COWs extent tree leaves and may rebalance neighbours.
    // The override holds, so any allocator call during these COWs
    // continues to read BG state from the extent tree (which is
    // semantically still consistent — we are removing
    // BLOCK_GROUP_ITEM records, not changing BG identities).
    for snap in &snapshots {
        delete_one(trans, fs_info, EXTENT_TREE_ID, &snap.key)?;
    }

    // Step 6: flip the compat_ro bit. The in-memory superblock is
    // serialised by the commit path.
    fs_info.superblock.compat_ro_flags |= bgt_bit;

    // Guard drops here, restoring bg_tree_override to None. Past
    // this point, fs_info.block_group_tree_id() returns 11 and
    // any subsequent allocator/update_block_group_used calls
    // (notably from the upcoming commit's flush_delayed_refs)
    // route to the freshly populated BGT.
    Ok(())
}

/// Walk the extent tree and snapshot every `BLOCK_GROUP_ITEM`
/// (key + raw payload bytes) in ascending compound-key order.
fn collect_block_group_items<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
) -> io::Result<Vec<BlockGroupItemSnapshot>> {
    let mut out: Vec<BlockGroupItemSnapshot> = Vec::new();

    // Position the cursor at (0, 0, 0) and walk forward; the
    // compound-key search will land at-or-before the first item.
    // BG items are typically sparse — a few per filesystem — so
    // the linear scan is fine.
    let start_key = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs_info,
        EXTENT_TREE_ID,
        &start_key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;

    loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break;
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            if !next_leaf(fs_info, &mut path)? {
                break;
            }
            continue;
        }
        let k = leaf.item_key(slot);
        if k.key_type == KeyType::BlockGroupItem {
            out.push(BlockGroupItemSnapshot {
                key: k,
                data: leaf.item_data(slot).to_vec(),
            });
        }
        path.slots[0] = slot + 1;
    }
    path.release();

    // The walk yields items in compound-key order, which for
    // BLOCK_GROUP_ITEM (keyed `(start, BLOCK_GROUP_ITEM, length)`)
    // is BG-start order — exactly what BGT expects.
    debug_assert!(
        out.windows(2).all(|w| {
            (w[0].key.objectid, w[0].key.offset)
                < (w[1].key.objectid, w[1].key.offset)
        }),
        "collect_block_group_items: items not strictly sorted by (start, length)",
    );
    Ok(out)
}

/// Delete a single item identified by exact key from the given
/// tree. Errors if the key is not found.
fn delete_one<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
    tree_id: u64,
    key: &DiskKey,
) -> io::Result<()> {
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(&mut *trans),
        fs_info,
        tree_id,
        key,
        &mut path,
        SearchIntent::Delete,
        true,
    )?;
    if !found {
        path.release();
        return Err(io::Error::other(format!(
            "delete_one: key {key:?} not found in tree {tree_id}",
        )));
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| io::Error::other("delete_one: no leaf in path"))?;
    items::del_items(leaf, path.slots[0], 1);
    fs_info.mark_dirty(leaf);
    path.release();
    Ok(())
}

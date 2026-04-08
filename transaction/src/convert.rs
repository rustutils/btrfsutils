//! # Whole-tree conversion operations
//!
//! Builds new global trees from existing extent-tree state and flips
//! the matching `compat_ro` superblock bits. Currently provides
//! [`convert_to_free_space_tree`]; the block-group-tree conversion
//! will land alongside it.

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

/// Tree id of the free space tree.
const FREE_SPACE_TREE_ID: u64 = 10;
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

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

    // Step 2: walk every block group, derive free ranges, insert
    // FREE_SPACE_INFO + FREE_SPACE_EXTENT items.
    seed_free_space_tree(trans, fs_info)?;

    // Step 3: flip superblock bits. The in-memory superblock is the
    // single source of truth; the commit path serialises it.
    fs_info.superblock.compat_ro_flags |= fst_bit | fst_valid_bit;
    // Zero cache_generation so the kernel does not try to load a
    // stale v1 cache on the next mount.
    fs_info.superblock.cache_generation = 0;

    Ok(())
}

/// Walk every block group, derive its free ranges from the extent
/// tree, and insert one `FREE_SPACE_INFO` plus one `FREE_SPACE_EXTENT`
/// per range into the free space tree (objectid 10).
///
/// Idempotent at the per-block-group level: any block group whose
/// `FREE_SPACE_INFO` is already present is skipped, so this can be
/// called against a partially-seeded FST without producing duplicate
/// entries. A pre-existing entry that uses the bitmap layout (the
/// `USING_BITMAPS` flag) is rejected — same restriction as the rest of
/// the v1 FST machinery.
///
/// Preconditions:
/// * The FST root must already be registered
///   (`fs_info.root_bytenr(10).is_some()`). Typically this means the
///   caller has called [`Transaction::create_empty_tree`] for tree id
///   10 in the same transaction (or mkfs's bootstrap put it there).
/// * No `USING_BITMAPS` block groups (only the extent layout is
///   supported in v1).
///
/// Intended for two callers:
/// * [`convert_to_free_space_tree`] uses it as the populate step
///   right after `create_empty_tree(10)`.
/// * mkfs's `post_bootstrap` will use it to seed an FST it just
///   created mid-transaction, so the immediately-following commit's
///   FST update step has entries to apply deltas to.
///
/// # Errors
///
/// Returns an error if the FST root is missing, if any block group
/// uses bitmap layout, if `extent_walk::walk_block_group_extents` or
/// `derive_free_ranges` fails, or if any tree read/write fails.
pub fn seed_free_space_tree<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
) -> io::Result<()> {
    use btrfs_disk::items::FreeSpaceInfoFlags;

    if fs_info.root_bytenr(FREE_SPACE_TREE_ID).is_none() {
        return Err(io::Error::other(
            "seed_free_space_tree: FST root not registered (call create_empty_tree(10) first)",
        ));
    }

    // Snapshot the BG list now: subsequent inserts COW FST leaves
    // (allocating new metadata blocks) but do not change which block
    // groups exist.
    let block_groups = allocation::load_block_groups(fs_info)?;
    debug_assert!(
        !block_groups.is_empty(),
        "seed_free_space_tree: no block groups found",
    );

    for bg in &block_groups {
        // Per-BG idempotency: if a FREE_SPACE_INFO already exists,
        // skip this BG. Reject if it uses bitmap layout (consistent
        // with the FST update path which also can't handle bitmaps).
        if let Some(existing) = trans.read_free_space_info(
            fs_info,
            FREE_SPACE_TREE_ID,
            bg.start,
            bg.length,
        )? {
            if existing.flags.contains(FreeSpaceInfoFlags::USING_BITMAPS) {
                return Err(io::Error::other(format!(
                    "seed_free_space_tree: BG {} uses bitmap layout (unsupported in v1)",
                    bg.start,
                )));
            }
            continue;
        }

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
                "seed_free_space_tree: BG {} has too many free extents to fit in u32",
                bg.start,
            ))
        })?;

        // FREE_SPACE_INFO payload is 8 bytes: u32 extent_count, u32
        // flags. flags = 0 selects the extent (non-bitmap) layout.
        let mut info_data = [0u8; 8];
        info_data[0..4].copy_from_slice(&extent_count.to_le_bytes());
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
    let compat_ro = fs_info.superblock.compat_ro_flags;

    // Preconditions specific to the conversion path: refuse-on-weird,
    // no repair attempts. The shared per-item move + create + flag
    // logic lives in [`create_block_group_tree`], which has weaker
    // preconditions (it tolerates an existing BGT root and an
    // already-set BGT flag for resumable use from mkfs).
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

    create_block_group_tree(trans, fs_info)
}

/// Materialise the block group tree (objectid 11) from the extent
/// tree's `BLOCK_GROUP_ITEM` rows in a single transaction.
///
/// Steps performed:
///
/// 1. Verify the free space tree is enabled and VALID. The kernel
///    requires both before BGT can be turned on.
/// 2. Snapshot every `BLOCK_GROUP_ITEM` from the extent tree
///    (compound-key order). This runs *before* the routing override
///    is pinned, so the read sees the pre-conversion state.
/// 3. Pin the BG-tree-id override to the extent tree for the
///    duration of the function. Allocator + `update_block_group_used`
///    calls during the body keep reading BG state from the extent
///    tree, even though BGT is being built up under them.
/// 4. Create the BGT root if it doesn't already exist.
/// 5. Per snapshot: insert into BGT (skip if the same key is already
///    present there) and delete from the extent tree (skip if it's
///    already gone). This makes the function idempotent at the
///    per-item level — a partial conversion (e.g. interrupted commit)
///    can be resumed by re-calling.
/// 6. Set the `BLOCK_GROUP_TREE` compat_ro flag if it isn't set yet.
///    Callers that already set the flag (mkfs's bootstrap) get
///    a no-op here.
/// 7. Drop the routing override; subsequent allocator /
///    `update_block_group_used` calls (notably the commit's
///    `flush_delayed_refs`) route to the freshly populated BGT.
///
/// Intended for two callers:
/// * [`convert_to_block_group_tree`] uses it as the body after
///   stricter precondition checks (refuses if BGT is already
///   enabled or a stale root is present).
/// * mkfs's `post_bootstrap` will use it directly: bootstrap leaves
///   BG items in the extent tree and sets the BGT flag, then this
///   helper materialises tree 11 and migrates the items in.
///
/// # Errors
///
/// Returns an error if the FST is missing or not VALID, or if any
/// tree read/write or allocator operation fails.
pub fn create_block_group_tree<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
) -> io::Result<()> {
    let bgt_bit = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE);
    let fst_bit = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE);
    let fst_valid_bit =
        u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID);
    let compat_ro = fs_info.superblock.compat_ro_flags;

    if compat_ro & fst_bit == 0 {
        return Err(io::Error::other(
            "create_block_group_tree: free space tree must be enabled first (kernel requires FST for BGT)",
        ));
    }
    if compat_ro & fst_valid_bit == 0 {
        return Err(io::Error::other(
            "create_block_group_tree: free space tree is not marked VALID (refusing)",
        ));
    }

    // Snapshot before pinning so the read sees the extent tree as it
    // was before any conversion-side modifications. May be empty on a
    // re-call where every BG item has already been moved to BGT —
    // that's the per-item-idempotent path and the loop below
    // becomes a no-op.
    let snapshots = collect_block_group_items(fs_info)?;

    let mut guard = fs_info.pin_block_group_tree(EXTENT_TREE_ID);
    let fs_info = guard.fs_mut();

    if fs_info.root_bytenr(BLOCK_GROUP_TREE_ID).is_none() {
        trans.create_empty_tree(fs_info, BLOCK_GROUP_TREE_ID)?;
    }

    // Per-item idempotency: skip the insert when the same key is
    // already in BGT (resume of a partial migration), and skip the
    // delete when the key isn't in the extent tree (already moved).
    for snap in &snapshots {
        if !key_present_in_tree(fs_info, BLOCK_GROUP_TREE_ID, &snap.key)? {
            insert_in_tree(
                trans,
                fs_info,
                BLOCK_GROUP_TREE_ID,
                &snap.key,
                &snap.data,
            )?;
        }
        if key_present_in_tree(fs_info, EXTENT_TREE_ID, &snap.key)? {
            delete_one(trans, fs_info, EXTENT_TREE_ID, &snap.key)?;
        }
    }

    if compat_ro & bgt_bit == 0 {
        fs_info.superblock.compat_ro_flags |= bgt_bit;
    }

    // Guard drops here, restoring bg_tree_override. Past this point,
    // fs_info.block_group_tree_id() returns 11 and any subsequent
    // allocator/update_block_group_used calls (notably from the
    // upcoming commit's flush_delayed_refs) route to the freshly
    // populated BGT.
    Ok(())
}

/// Read-only check: does `tree_id` contain an exact-key match for
/// `key`? Used by [`create_block_group_tree`] for per-item
/// idempotency.
fn key_present_in_tree<R: Read + Write + Seek>(
    fs_info: &mut Filesystem<R>,
    tree_id: u64,
    key: &DiskKey,
) -> io::Result<bool> {
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        fs_info,
        tree_id,
        key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;
    path.release();
    Ok(found)
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

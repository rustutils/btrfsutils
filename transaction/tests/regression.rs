//! Regression tests for bugs found during review and stress testing.
//!
//! Each test targets a specific bug fix and would have caught the bug
//! if it had existed before the fix. These tests use real btrfs images
//! via `mkfs.btrfs` to exercise the full pipeline.

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    balance, cow,
    extent_buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE},
    fs_info::FsInfo,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::TransHandle,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::Command,
};

fn create_test_image() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let img_path = dir.path().join("test.img");
    let file = File::create(&img_path).expect("failed to create image file");
    file.set_len(128 * 1024 * 1024)
        .expect("failed to set image size");
    drop(file);
    let status = Command::new("mkfs.btrfs")
        .args(["-f", "-q"])
        .arg(&img_path)
        .status()
        .expect("mkfs.btrfs not found — install btrfs-progs");
    assert!(status.success(), "mkfs.btrfs failed with {status}");
    (dir, img_path)
}

fn assert_btrfs_check(path: &Path) {
    let output = Command::new("btrfs")
        .args(["check", "--readonly"])
        .arg(path)
        .output()
        .expect("btrfs check not found");
    if output.status.success() {
        return;
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let has_structural_errors = stderr.lines().any(|line| {
        line.contains("ERROR:")
            && !line.contains("free space")
            && !line.contains("cache")
    });
    if has_structural_errors {
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "btrfs check errors:\n--- stderr ---\n{stderr}\n--- stdout ---\n{stdout}"
        );
    }
}

fn open_rw(path: &Path) -> FsInfo<File> {
    let file = File::options().read(true).write(true).open(path).unwrap();
    FsInfo::open(file).expect("open failed")
}

fn make_key(oid: u64) -> DiskKey {
    DiskKey {
        objectid: oid,
        key_type: KeyType::TemporaryItem,
        offset: 0,
    }
}

/// Validate that every leaf in a tree has correct item offset packing:
/// item[0] data ends at nodesize - HEADER_SIZE, offsets are descending.
fn validate_tree_leaves(fs_info: &mut FsInfo<File>, root_bytenr: u64) {
    let eb = fs_info.read_block(root_bytenr).unwrap();
    if eb.level() == 0 {
        validate_leaf(&eb);
    } else {
        for i in 0..eb.nritems() as usize {
            validate_tree_leaves(fs_info, eb.key_ptr_blockptr(i));
        }
    }
}

fn validate_leaf(eb: &btrfs_transaction::extent_buffer::ExtentBuffer) {
    let nritems = eb.nritems() as usize;
    if nritems == 0 {
        return;
    }
    let first_end = eb.item_offset(0) + eb.item_size(0);
    let expected_end = eb.nodesize() - HEADER_SIZE as u32;
    assert_eq!(
        first_end,
        expected_end,
        "leaf at {}: item[0] end={first_end} != {expected_end} (nritems={nritems})",
        eb.logical()
    );
    for i in 0..nritems - 1 {
        assert!(
            eb.item_offset(i) > eb.item_offset(i + 1)
                || (eb.item_size(i) == 0 && eb.item_size(i + 1) == 0),
            "leaf at {}: offset[{i}]={} not > offset[{}]={}",
            eb.logical(),
            eb.item_offset(i),
            i + 1,
            eb.item_offset(i + 1)
        );
    }
}

/// Regression: split_leaf must compact remaining items' data after
/// truncating so item[0] data reaches the end of the block.
/// (Bug: split left a gap in the data area that btrfs check flagged
/// as "unexpected item end".)
#[test]
fn split_leaf_compacts_data() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    let data = [0xAB; 32];
    // Insert enough items to trigger exactly one split
    for i in 0..300 {
        let key = make_key(300_000 + i);
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
        fs.mark_dirty(leaf);
        path.release();
    }

    // Validate leaf offsets BEFORE commit
    let root_bytenr = fs.root_bytenr(1).unwrap();
    validate_tree_leaves(&mut fs, root_bytenr);

    trans.commit(&mut fs).unwrap();
    drop(fs);

    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Regression: push_leaf_right must pack pushed items at the top of the
/// data area (highest offsets) and shift existing data down.
/// (Bug: pushed items were placed below existing data, violating the
/// descending offset invariant.)
#[test]
fn push_leaf_right_preserves_offset_ordering() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    // Insert 500 items to trigger multiple splits and redistributions
    let data = [0xCD; 32];
    for i in 0..500 {
        let key = make_key(400_000 + i);
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
        fs.mark_dirty(leaf);
        path.release();
    }

    // Validate all leaves have correct offset ordering
    let root_bytenr = fs.root_bytenr(1).unwrap();
    validate_tree_leaves(&mut fs, root_bytenr);

    trans.commit(&mut fs).unwrap();
    drop(fs);

    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Regression: write_block must write to all stripe copies (DUP/RAID1).
/// (Bug: only the first stripe was written, causing btrfs check to see
/// stale data on the second copy — "parent transid verify failed".)
#[test]
fn dup_writes_both_copies() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    // Insert enough items to allocate new blocks in the DUP metadata group
    let data = [0xEF; 32];
    for i in 0..400 {
        let key = make_key(500_000 + i);
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
        fs.mark_dirty(leaf);
        path.release();
    }

    trans.commit(&mut fs).unwrap();
    drop(fs);

    // btrfs check reads both DUP copies — if only one was written, it
    // will report "parent transid verify failed"
    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Regression: cow_block must clear WRITTEN and RELOC flags on the new copy.
#[test]
fn cow_block_clears_flags() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    // Read the root tree root and COW it
    let root_bytenr = fs.root_bytenr(1).unwrap();
    let eb = fs.read_block(root_bytenr).unwrap();
    let new_eb = cow::cow_block(&mut trans, &mut fs, &eb, 1, None).unwrap();

    // New block should have no WRITTEN or RELOC flags
    assert_eq!(
        new_eb.flags() & 0x3,
        0,
        "WRITTEN/RELOC flags should be cleared"
    );
    // New block should have current generation
    assert_eq!(new_eb.generation(), fs.generation);
    // New block should be at a different address
    assert_ne!(new_eb.logical(), eb.logical());

    trans.abort(&mut fs);
    drop(dir);
}

/// Regression: alloc_tree_block must queue a delayed ref for the new block.
/// (Bug: split_leaf allocated blocks without extent refs, causing
/// "ref mismatch" errors in btrfs check.)
#[test]
fn alloc_tree_block_creates_extent_ref() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    let logical = trans.alloc_tree_block(&mut fs, 1, 0).unwrap();

    // Should have a pending delayed ref for this block
    let refs = trans.delayed_refs.drain();
    let found = refs.iter().any(|r| r.bytenr == logical && r.delta > 0);
    assert!(found, "alloc_tree_block should queue a +1 delayed ref");

    trans.abort(&mut fs);
    drop(dir);
}

/// Regression: cow_block must pin the old block so the allocator doesn't
/// reuse it before commit.
#[test]
fn cow_block_pins_old_address() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    let root_bytenr = fs.root_bytenr(1).unwrap();
    let eb = fs.read_block(root_bytenr).unwrap();
    let _new_eb = cow::cow_block(&mut trans, &mut fs, &eb, 1, None).unwrap();

    // The old address should be pinned
    assert!(
        trans.is_pinned(root_bytenr),
        "old block at {root_bytenr} should be pinned after COW"
    );

    trans.abort(&mut fs);
    drop(dir);
}

/// Regression: per-block-group used tracking must update each block group
/// individually based on the extents within its address range.
/// (Bug: aggregate delta was applied to a single block group near
/// alloc_cursor, producing incorrect used values.)
#[test]
fn block_group_used_correct_after_commit() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    // Insert items to trigger COW (which creates extent refs)
    let data = [0x99; 16];
    for i in 0..50 {
        let key = make_key(600_000 + i);
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
        fs.mark_dirty(leaf);
        path.release();
    }

    trans.commit(&mut fs).unwrap();
    drop(fs);

    // btrfs check verifies block group used values match extent items
    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Regression: commit convergence loop must stabilize even when COW
/// during delayed ref flush and root item updates generates cascading
/// changes.
#[test]
fn commit_convergence_with_many_trees() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();

    // Modify items in the root tree to trigger root item updates and
    // extent tree COW during commit
    let data = [0x77; 64];
    for i in 0..100 {
        let key = make_key(700_000 + i);
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
        fs.mark_dirty(leaf);
        path.release();
    }

    // commit should not panic with "did not stabilize"
    trans.commit(&mut fs).unwrap();
    drop(fs);

    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Verify all items survive a split + commit + reopen cycle.
#[test]
fn all_items_searchable_after_split() {
    let (dir, img_path) = create_test_image();
    let item_count = 500u64;
    let data = [0xBB; 32];

    {
        let mut fs = open_rw(&img_path);
        let mut trans = TransHandle::start(&mut fs).unwrap();

        for i in 0..item_count {
            let key = make_key(800_000 + i);
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
                true,
            )
            .unwrap();
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
            fs.mark_dirty(leaf);
            path.release();
        }

        trans.commit(&mut fs).unwrap();
    }

    // Reopen and verify every item
    {
        let mut fs = open_rw(&img_path);
        for i in 0..item_count {
            let key = make_key(800_000 + i);
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .unwrap();
            assert!(found, "item {i} not found after reopen");
            let leaf = path.nodes[0].as_ref().unwrap();
            assert_eq!(leaf.item_data(path.slots[0]), &data);
            path.release();
        }
    }

    drop(dir);
}

/// Coverage: push_leaf_left/right COW branch — sibling from a previous
/// generation must be COWed, which changes its logical address and
/// requires updating the parent pointer.
#[test]
fn balance_cows_sibling_from_previous_generation() {
    let (dir, img_path) = create_test_image();

    // Transaction 1: insert items to build a multi-leaf tree
    {
        let mut fs = open_rw(&img_path);
        let mut trans = TransHandle::start(&mut fs).unwrap();
        let data = [0xAA; 32];
        for i in 0..300 {
            let key = make_key(900_000 + i);
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
                true,
            )
            .unwrap();
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
            fs.mark_dirty(leaf);
            path.release();
        }
        trans.commit(&mut fs).unwrap();
    }

    // Transaction 2: insert more items. The existing leaves have the
    // previous generation, so split_leaf's push_leaf_left/right will
    // need to COW the sibling (triggering the left.logical() != left_bytenr
    // and right.logical() != right_bytenr branches).
    {
        let mut fs = open_rw(&img_path);
        let mut trans = TransHandle::start(&mut fs).unwrap();
        let data = [0xBB; 32];
        for i in 0..300 {
            let key = make_key(900_300 + i);
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
                true,
            )
            .unwrap();
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
            fs.mark_dirty(leaf);
            path.release();
        }
        trans.commit(&mut fs).unwrap();
    }

    assert_btrfs_check(&img_path);

    // Verify all 600 items survive
    {
        let mut fs = open_rw(&img_path);
        for i in 0..600 {
            let key = make_key(900_000 + i);
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .unwrap();
            assert!(found, "item {i} not found");
            path.release();
        }
    }

    drop(dir);
}

/// Coverage: balance_node — deletion from a large tree should trigger
/// node merging when child nodes become sparse (<25% occupancy).
/// Also exercises SearchIntent::Delete path in search_slot.
#[test]
fn delete_many_items_triggers_node_rebalance() {
    let (dir, img_path) = create_test_image();

    // Transaction 1: insert enough items to build a multi-level tree
    {
        let mut fs = open_rw(&img_path);
        let mut trans = TransHandle::start(&mut fs).unwrap();
        let data = [0xEE; 32];
        for i in 0..1000 {
            let key = make_key(1_100_000 + i);
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
                true,
            )
            .unwrap();
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data).unwrap();
            fs.mark_dirty(leaf);
            path.release();
        }
        trans.commit(&mut fs).unwrap();
    }

    assert_btrfs_check(&img_path);

    // Transaction 2: delete most items using SearchIntent::Delete
    {
        let mut fs = open_rw(&img_path);
        let mut trans = TransHandle::start(&mut fs).unwrap();
        for i in 0..900 {
            let key = make_key(1_100_000 + i);
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Delete,
                true,
            )
            .unwrap();
            if found {
                let leaf = path.nodes[0].as_mut().unwrap();
                items::del_items(leaf, path.slots[0], 1);
                fs.mark_dirty(leaf);
            }
            path.release();
        }
        trans.commit(&mut fs).unwrap();
    }

    assert_btrfs_check(&img_path);

    // Verify remaining 100 items survive
    {
        let mut fs = open_rw(&img_path);
        for i in 900..1000 {
            let key = make_key(1_100_000 + i);
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .unwrap();
            assert!(found, "item {i} should still exist");
            path.release();
        }
        // Verify deleted items are gone
        for i in 0..900 {
            let key = make_key(1_100_000 + i);
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .unwrap();
            assert!(!found, "item {i} should have been deleted");
            path.release();
        }
    }

    drop(dir);
}

/// Helper: create a level-1 node ExtentBuffer with `n` key pointers.
/// Keys are (start_oid + i*100, RootItem, 0), blockptrs are fake addresses.
fn make_node(
    nodesize: u32,
    logical: u64,
    generation: u64,
    n: usize,
    start_oid: u64,
) -> ExtentBuffer {
    let mut eb = ExtentBuffer::new_zeroed(nodesize, logical);
    eb.set_level(1);
    eb.set_nritems(n as u32);
    eb.set_generation(generation);
    eb.set_owner(1);
    for i in 0..n {
        let key = DiskKey {
            objectid: start_oid + i as u64 * 100,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        eb.set_key_ptr(i, &key, 0x1000_0000 + i as u64 * 0x4000, generation);
    }
    eb
}

/// Coverage: balance_node merge-right path. A sparse child merges with
/// its right sibling, absorbing all key pointers and removing the right
/// sibling's entry from the parent.
#[test]
fn balance_node_merge_right() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;

    // Create a sparse child (2 key pointers) and a right sibling (3 key pointers).
    // Together they fit in one node (5 << max_ptrs).
    let child = make_node(nodesize, 0x2000_0000, generation, 2, 100);
    let right = make_node(nodesize, 0x2000_4000, generation, 3, 300);

    // Put them in the cache so read_block can find them
    fs.mark_dirty(&child);
    fs.mark_dirty(&right);

    // Build parent with 2 children: child at slot 0, right at slot 1
    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x3000_0000);
    parent.set_level(2);
    parent.set_nritems(2);
    parent.set_generation(generation);
    parent.set_owner(1);
    let child_key = DiskKey {
        objectid: 100,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let right_key = DiskKey {
        objectid: 300,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    parent.set_key_ptr(0, &child_key, child.logical(), generation);
    parent.set_key_ptr(1, &right_key, right.logical(), generation);

    // Call balance_node on slot 0 (the sparse child)
    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 0, 1).unwrap();
    assert!(merged, "should have merged");

    // Parent should now have 1 child (the right sibling was absorbed)
    assert_eq!(parent.nritems(), 1);

    // The merged child should have 5 key pointers (2 + 3)
    let merged_bytenr = parent.key_ptr_blockptr(0);
    let merged_eb = fs.read_block(merged_bytenr).unwrap();
    assert_eq!(merged_eb.nritems(), 5);
    assert_eq!(merged_eb.level(), 1);

    // Verify key pointer ordering in merged node
    for i in 0..4 {
        let k1 = merged_eb.key_ptr_key(i);
        let k2 = merged_eb.key_ptr_key(i + 1);
        assert!(
            k1.objectid < k2.objectid,
            "key[{i}].oid={} not < key[{}].oid={}",
            k1.objectid,
            i + 1,
            k2.objectid,
        );
    }

    // Right sibling should be pinned
    assert!(trans.is_pinned(right.logical()));

    trans.abort(&mut fs);
    drop(dir);
}

/// Coverage: balance_node merge-left path. When there's no right sibling
/// (child is at the last slot), the child merges into the left sibling.
#[test]
fn balance_node_merge_left() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;

    // Left sibling (3 key pointers), sparse child at slot 1 (2 key pointers)
    let left = make_node(nodesize, 0x4000_0000, generation, 3, 100);
    let child = make_node(nodesize, 0x4000_4000, generation, 2, 400);

    fs.mark_dirty(&left);
    fs.mark_dirty(&child);

    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x5000_0000);
    parent.set_level(2);
    parent.set_nritems(2);
    parent.set_generation(generation);
    parent.set_owner(1);
    let left_key = DiskKey {
        objectid: 100,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let child_key = DiskKey {
        objectid: 400,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    parent.set_key_ptr(0, &left_key, left.logical(), generation);
    parent.set_key_ptr(1, &child_key, child.logical(), generation);

    // Call balance_node on slot 1 (the sparse child, last slot = no right sibling)
    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 1, 1).unwrap();
    assert!(merged, "should have merged into left");

    // Parent should have 1 child
    assert_eq!(parent.nritems(), 1);

    // The left sibling should now have 5 key pointers (3 + 2)
    let merged_bytenr = parent.key_ptr_blockptr(0);
    let merged_eb = fs.read_block(merged_bytenr).unwrap();
    assert_eq!(merged_eb.nritems(), 5);

    // Child should be pinned (it was absorbed)
    assert!(trans.is_pinned(child.logical()));

    trans.abort(&mut fs);
    drop(dir);
}

/// Coverage: balance_node when child is not sparse (>= 25% full).
/// Should return false without merging.
#[test]
fn balance_node_not_sparse_skips() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;
    let max_ptrs =
        ((nodesize - HEADER_SIZE as u32) / KEY_PTR_SIZE as u32) as usize;

    // Create child with 25% occupancy (at the threshold)
    let threshold = max_ptrs / 4;
    let child = make_node(nodesize, 0x6000_0000, generation, threshold, 100);
    let right = make_node(nodesize, 0x6000_4000, generation, 3, 50000);

    fs.mark_dirty(&child);
    fs.mark_dirty(&right);

    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x7000_0000);
    parent.set_level(2);
    parent.set_nritems(2);
    parent.set_generation(generation);
    parent.set_owner(1);
    parent.set_key_ptr(
        0,
        &DiskKey {
            objectid: 100,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        child.logical(),
        generation,
    );
    parent.set_key_ptr(
        1,
        &DiskKey {
            objectid: 50000,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        right.logical(),
        generation,
    );

    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 0, 1).unwrap();
    assert!(!merged, "should not merge (child is at threshold)");
    assert_eq!(parent.nritems(), 2, "parent unchanged");

    trans.abort(&mut fs);
    drop(dir);
}

/// Coverage: balance_node when combined items exceed max_ptrs.
/// Both siblings exist but neither can absorb the child.
#[test]
fn balance_node_too_full_to_merge() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = TransHandle::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;
    let max_ptrs =
        ((nodesize - HEADER_SIZE as u32) / KEY_PTR_SIZE as u32) as usize;

    // Sparse child (2 ptrs) but left and right siblings are nearly full
    let left = make_node(nodesize, 0x8000_0000, generation, max_ptrs - 1, 100);
    let child = make_node(nodesize, 0x8000_4000, generation, 2, 50000);
    let right =
        make_node(nodesize, 0x8000_8000, generation, max_ptrs - 1, 80000);

    fs.mark_dirty(&left);
    fs.mark_dirty(&child);
    fs.mark_dirty(&right);

    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x9000_0000);
    parent.set_level(2);
    parent.set_nritems(3);
    parent.set_generation(generation);
    parent.set_owner(1);
    parent.set_key_ptr(
        0,
        &DiskKey {
            objectid: 100,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        left.logical(),
        generation,
    );
    parent.set_key_ptr(
        1,
        &DiskKey {
            objectid: 50000,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        child.logical(),
        generation,
    );
    parent.set_key_ptr(
        2,
        &DiskKey {
            objectid: 80000,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        right.logical(),
        generation,
    );

    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 1, 1).unwrap();
    assert!(!merged, "should not merge (siblings too full)");
    assert_eq!(parent.nritems(), 3, "parent unchanged");

    trans.abort(&mut fs);
    drop(dir);
}

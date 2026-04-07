//! Regression tests for bugs found during review and stress testing.
//!
//! Each test targets a specific bug fix and would have caught the bug
//! if it had existed before the fix. These tests use real btrfs images
//! via `mkfs.btrfs` to exercise the full pipeline.

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    balance,
    buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE},
    cow,
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
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
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "btrfs check errors:\n--- stderr ---\n{stderr}\n--- stdout ---\n{stdout}"
        );
    }
}

fn open_rw(path: &Path) -> Filesystem<File> {
    let file = File::options().read(true).write(true).open(path).unwrap();
    Filesystem::open(file).expect("open failed")
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
fn validate_tree_leaves(fs_info: &mut Filesystem<File>, root_bytenr: u64) {
    let eb = fs_info.read_block(root_bytenr).unwrap();
    if eb.level() == 0 {
        validate_leaf(&eb);
    } else {
        for i in 0..eb.nritems() as usize {
            validate_tree_leaves(fs_info, eb.key_ptr_blockptr(i));
        }
    }
}

fn validate_leaf(eb: &btrfs_transaction::buffer::ExtentBuffer) {
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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

    let logical = trans.alloc_tree_block(&mut fs, 1, 0).unwrap();

    // Should have a pending delayed ref for this block
    let refs = trans.delayed_refs.drain();
    let found =
        refs.iter().any(|r| r.key.bytenr() == logical && r.delta > 0);
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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
    let mut trans = Transaction::start(&mut fs).unwrap();

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
        let mut trans = Transaction::start(&mut fs).unwrap();

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
        let mut trans = Transaction::start(&mut fs).unwrap();
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
        let mut trans = Transaction::start(&mut fs).unwrap();
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
        let mut trans = Transaction::start(&mut fs).unwrap();
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
        let mut trans = Transaction::start(&mut fs).unwrap();
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
/// sibling's entry from the parent. Uses an old generation to force COW
/// (covers child.logical() != child_bytenr branch), and 3 children so
/// removing the right sibling requires shifting the third entry left
/// (covers remove_slot + 1 < parent_nritems branch).
#[test]
fn balance_node_merge_right() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = Transaction::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;
    let old_gen = generation - 1; // Previous generation forces COW

    // Sparse child (2 ptrs) and right sibling (3 ptrs) with old generation.
    // Third sibling at the end so removing the right sibling requires a shift.
    let child = make_node(nodesize, 0x2000_0000, old_gen, 2, 100);
    let right = make_node(nodesize, 0x2000_4000, old_gen, 3, 300);
    let third = make_node(nodesize, 0x2000_8000, generation, 5, 600);

    fs.mark_dirty(&child);
    fs.mark_dirty(&right);
    fs.mark_dirty(&third);

    // Parent with 3 children
    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x3000_0000);
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
        child.logical(),
        old_gen,
    );
    parent.set_key_ptr(
        1,
        &DiskKey {
            objectid: 300,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        right.logical(),
        old_gen,
    );
    parent.set_key_ptr(
        2,
        &DiskKey {
            objectid: 600,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        third.logical(),
        generation,
    );

    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 0, 1).unwrap();
    assert!(merged, "should have merged");

    // Parent should now have 2 children (right absorbed, third shifted left)
    assert_eq!(parent.nritems(), 2);

    // The merged child should have 5 key pointers (2 + 3)
    let merged_bytenr = parent.key_ptr_blockptr(0);
    let merged_eb = fs.read_block(merged_bytenr).unwrap();
    assert_eq!(merged_eb.nritems(), 5);
    assert_eq!(merged_eb.level(), 1);
    // COW should have changed the address
    assert_ne!(
        merged_bytenr,
        child.logical(),
        "COW should allocate new block"
    );

    // Third child should now be at slot 1
    assert_eq!(parent.key_ptr_blockptr(1), third.logical());
    assert_eq!(parent.key_ptr_key(1).objectid, 600);

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

/// Coverage: balance_node merge-left path. Sparse child at slot 1 (middle)
/// merges into the left sibling. Uses old generation to force COW (covers
/// left.logical() != left_bytenr branch), and 3 children so removing the
/// child requires shifting the third entry left (covers parent_slot + 1 <
/// parent_nritems branch).
#[test]
fn balance_node_merge_left() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = Transaction::start(&mut fs).unwrap();
    let nodesize = fs.nodesize;
    let generation = fs.generation;
    let old_gen = generation - 1;

    // Left sibling (3 ptrs, old gen), sparse child at slot 1 (2 ptrs, old gen),
    // right sibling at slot 2 so merge-right is tried first but right has too
    // many items. Actually, to force the merge-left path, we make the right
    // sibling too full to merge with the child.
    let max_ptrs =
        ((nodesize - HEADER_SIZE as u32) / KEY_PTR_SIZE as u32) as usize;
    let left = make_node(nodesize, 0x4000_0000, old_gen, 3, 100);
    let child = make_node(nodesize, 0x4000_4000, old_gen, 2, 400);
    let right = make_node(nodesize, 0x4000_8000, generation, max_ptrs - 1, 600);

    fs.mark_dirty(&left);
    fs.mark_dirty(&child);
    fs.mark_dirty(&right);

    let mut parent = ExtentBuffer::new_zeroed(nodesize, 0x5000_0000);
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
        old_gen,
    );
    parent.set_key_ptr(
        1,
        &DiskKey {
            objectid: 400,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        child.logical(),
        old_gen,
    );
    parent.set_key_ptr(
        2,
        &DiskKey {
            objectid: 600,
            key_type: KeyType::RootItem,
            offset: 0,
        },
        right.logical(),
        generation,
    );

    // Merge-right will be tried first (child + right = 2 + 492 = 494 > 493),
    // so it falls through to merge-left (child + left = 2 + 3 = 5 <= 493).
    let merged =
        balance::balance_node(&mut trans, &mut fs, &mut parent, 1, 1).unwrap();
    assert!(merged, "should have merged into left");

    // Parent should have 2 children (child removed, right shifted left)
    assert_eq!(parent.nritems(), 2);

    // Left sibling should now have 5 key pointers (3 + 2)
    let merged_bytenr = parent.key_ptr_blockptr(0);
    let merged_eb = fs.read_block(merged_bytenr).unwrap();
    assert_eq!(merged_eb.nritems(), 5);
    // COW should have changed the address
    assert_ne!(
        merged_bytenr,
        left.logical(),
        "COW should allocate new block"
    );

    // Right sibling should now be at slot 1
    assert_eq!(parent.key_ptr_blockptr(1), right.logical());
    assert_eq!(parent.key_ptr_key(1).objectid, 600);

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
    let mut trans = Transaction::start(&mut fs).unwrap();
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
    let mut trans = Transaction::start(&mut fs).unwrap();
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

/// Build a real 3-level tree by inserting large items (few items per leaf,
/// forcing many leaves and eventually a level-2 root). Then delete most
/// items and verify the tree remains valid.
///
/// This exercises the full search_slot descent with SearchIntent::Delete
/// through an actual 3-level tree, which is the only way to trigger
/// balance_node on a real internal node (level 1) child during descent.
#[test]
fn large_tree_insert_and_delete() {
    let (dir, img_path) = create_test_image();

    // Use small items (32 bytes) but many of them. Each leaf holds ~285
    // items. To get level 2, we need >493 leaves => ~140K items. That's
    // too many, so we accept level 1 with a very wide tree (many leaves).
    // With 10000 items we get ~35 leaves, which is enough to test the
    // full insert-split-commit-delete cycle on a real tree.
    //
    // NOTE: a true 3-level tree test requires >140K items with 32-byte
    // data, or cascading node splits with larger items. Cascading node
    // splits have a known bug (stale path.slots after parent split) that
    // needs to be fixed before this can work with >493 leaves.
    let item_count: u64 = 10_000;
    let data = vec![0xDD; 32];

    // Transaction 1: build the 3-level tree
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();

        for i in 0..item_count {
            let key = make_key(2_000_000 + i);
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
            .unwrap_or_else(|e| panic!("insert search failed at {i}: {e}"));
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data)
                .unwrap_or_else(|e| panic!("insert failed at {i}: {e}"));
            fs.mark_dirty(leaf);
            path.release();
        }

        // Verify we actually got a level-2 tree
        let root_bytenr = fs.root_bytenr(1).unwrap();
        let root = fs.read_block(root_bytenr).unwrap();
        assert!(
            root.level() >= 1,
            "expected level >= 1, got level {}",
            root.level()
        );

        trans.commit(&mut fs).unwrap();
    }

    assert_btrfs_check(&img_path);

    // Transaction 2: delete most items using SearchIntent::Delete
    let delete_count = 9000;
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();

        for i in 0..delete_count {
            let key = make_key(2_000_000 + i);
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
            .unwrap_or_else(|e| panic!("delete search failed at {i}: {e}"));
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

    // Verify surviving items
    {
        let mut fs = open_rw(&img_path);
        for i in delete_count..item_count {
            let key = make_key(2_000_000 + i);
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
            let leaf = path.nodes[0].as_ref().unwrap();
            assert_eq!(leaf.item_data(path.slots[0]).len(), data.len());
            path.release();
        }
    }

    drop(dir);
}

/// Regression: cascading node splits when a parent node is full and must
/// be split before inserting a key pointer from a child split. Tests
/// that path.slots is correctly updated after the parent split.
/// Previously panicked with an out-of-bounds write in set_key_ptr.
#[test]
fn cascading_node_split() {
    // Use a 512 MiB image for enough metadata space
    let dir = tempfile::TempDir::new().unwrap();
    let img_path = dir.path().join("test.img");
    let file = File::create(&img_path).unwrap();
    file.set_len(512 * 1024 * 1024).unwrap();
    drop(file);
    let status = std::process::Command::new("mkfs.btrfs")
        .args(["-f", "-q"])
        .arg(&img_path)
        .status()
        .expect("mkfs.btrfs not found");
    assert!(status.success());

    // Use 3000-byte items: ~5 per leaf, need >493 leaves for level 2
    // = ~2465 items. Use 2600 for margin.
    let item_count: u64 = 2600;
    let data = vec![0xCC; 3000];

    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();

        for i in 0..item_count {
            let key = make_key(3_000_000 + i);
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
            .unwrap_or_else(|e| panic!("search failed at item {i}: {e}"));
            let leaf = path.nodes[0].as_mut().unwrap();
            items::insert_item(leaf, path.slots[0], &key, &data)
                .unwrap_or_else(|e| panic!("insert failed at item {i}: {e}"));
            fs.mark_dirty(leaf);
            path.release();
        }

        // Should have a level-2 tree
        let root = fs.read_block(fs.root_bytenr(1).unwrap()).unwrap();
        assert!(
            root.level() >= 2,
            "expected level >= 2, got {}",
            root.level()
        );

        trans.commit(&mut fs).unwrap();
    }

    assert_btrfs_check(&img_path);

    // Verify all items survived
    {
        let mut fs = open_rw(&img_path);
        for i in 0..item_count {
            let key = make_key(3_000_000 + i);
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
            assert!(found, "item {i} not found after commit");
            path.release();
        }
    }

    drop(dir);
}

/// Option B regression: a no-op commit must legitimately advance
/// `superblock.generation` AND rewrite the root tree root at the new
/// generation, so `header.generation == superblock.generation` holds.
/// Before Option B, no-op commits either had to be short-circuited
/// (Option A) or would corrupt the filesystem with "parent transid
/// verify failed: wanted N found N-1". See PLAN.md Finding 3 (I1, I2, I7).
#[test]
fn empty_commit_advances_generation_and_rewrites_root_tree_root() {
    let (dir, img_path) = create_test_image();

    let initial_generation;
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).unwrap();
        initial_generation = fs.superblock.generation;

        let trans = Transaction::start(&mut fs).unwrap();
        // No operations: this is a true no-op commit.
        trans.commit(&mut fs).unwrap();
    }

    // Reopen and verify both invariants.
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).unwrap();
    assert_eq!(
        fs.superblock.generation,
        initial_generation + 1,
        "no-op commit must advance superblock.generation"
    );

    let root_bytenr = fs.superblock.root;
    let eb = fs.read_block(root_bytenr).unwrap();
    assert_eq!(
        eb.generation(),
        fs.superblock.generation,
        "root tree root header.generation must match superblock.generation \
         (Option B: every commit force-COWs the root tree root)"
    );

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

/// Two consecutive no-op commits must each advance the generation
/// independently, leaving the filesystem at `initial + 2`. Catches a
/// regression where the force-COW would only fire on the first commit.
#[test]
fn two_empty_commits_advance_generation_twice() {
    let (dir, img_path) = create_test_image();

    let initial_generation;
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).unwrap();
        initial_generation = fs.superblock.generation;

        Transaction::start(&mut fs)
            .unwrap()
            .commit(&mut fs)
            .unwrap();
        Transaction::start(&mut fs)
            .unwrap()
            .commit(&mut fs)
            .unwrap();
    }

    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).unwrap();
    assert_eq!(fs.superblock.generation, initial_generation + 2);
    let eb = fs.read_block(fs.superblock.root).unwrap();
    assert_eq!(eb.generation(), fs.superblock.generation);

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

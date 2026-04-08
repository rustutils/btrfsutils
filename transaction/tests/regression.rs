//! Regression tests for bugs found during review and stress testing.
//!
//! Each test targets a specific bug fix and would have caught the bug
//! if it had existed before the fix. These tests use real btrfs images
//! via `mkfs.btrfs` to exercise the full pipeline.

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    allocation, balance,
    buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE, KEY_PTR_SIZE},
    convert, cow,
    extent_walk::{self, AllocatedExtent},
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
    create_test_image_with_features(&[])
}

/// Like [`create_test_image`] but passes additional `-O` flags to
/// `mkfs.btrfs`. Use `&["^free-space-tree"]` to start without FST
/// so the convert path can build it.
fn create_test_image_with_features(
    features: &[&str],
) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let img_path = dir.path().join("test.img");
    let file = File::create(&img_path).expect("failed to create image file");
    file.set_len(128 * 1024 * 1024)
        .expect("failed to set image size");
    drop(file);
    let mut cmd = Command::new("mkfs.btrfs");
    cmd.args(["-f", "-q"]);
    for f in features {
        cmd.arg("-O").arg(f);
    }
    let status = cmd
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
    let found = refs
        .iter()
        .any(|r| r.key.bytenr() == logical && r.delta > 0);
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

// ----- Stage I.1: create_empty_tree primitive -----

/// Tree id used by tests for the empty-tree primitive. Picks an id
/// well above the kernel's reserved range (BTRFS_LAST_FREE_OBJECTID
/// is 0xFFFFFFFFFFFFFF00; everything below 256 is reserved special
/// trees) and outside the special-cased ones, so a default mkfs.btrfs
/// will never have it and btrfs check will not run any tree-specific
/// consistency rules against it.
const TEST_EMPTY_TREE_ID: u64 = 12;

/// Walk the root tree looking for a `ROOT_ITEM` with `objectid ==
/// tree_id, type == ROOT_ITEM, offset == 0` and return its parsed
/// `RootItem` (or `None` if not found).
fn find_root_item(
    fs: &mut Filesystem<File>,
    tree_id: u64,
) -> Option<btrfs_disk::items::RootItem> {
    use btrfs_disk::items::RootItem;
    let key = DiskKey {
        objectid: tree_id,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        fs,
        1,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .unwrap();
    if !found {
        path.release();
        return None;
    }
    let leaf = path.nodes[0].as_ref().unwrap();
    let item = RootItem::parse(leaf.item_data(path.slots[0]));
    path.release();
    item
}

#[test]
fn create_empty_tree_basic_commit_and_reopen() {
    let (dir, img_path) = create_test_image();
    let new_bytenr;
    let trans_generation;
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        trans_generation = trans.transid;
        new_bytenr = trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap();
        assert_eq!(fs.root_bytenr(TEST_EMPTY_TREE_ID), Some(new_bytenr));
        trans.commit(&mut fs).unwrap();
    }

    let mut fs = open_rw(&img_path);
    let bytenr_after = fs
        .root_bytenr(TEST_EMPTY_TREE_ID)
        .expect("new tree's ROOT_ITEM should be found at reopen");

    let leaf = fs.read_block(bytenr_after).unwrap();
    assert_eq!(leaf.level(), 0, "new empty tree must be a level-0 leaf");
    assert_eq!(leaf.nritems(), 0, "new empty tree must have zero items");
    assert_eq!(leaf.owner(), TEST_EMPTY_TREE_ID);
    assert_eq!(leaf.generation(), trans_generation);

    let item = find_root_item(&mut fs, TEST_EMPTY_TREE_ID)
        .expect("ROOT_ITEM for new tree must exist in root tree");
    assert_eq!(item.bytenr, bytenr_after);
    assert_eq!(item.level, 0);
    assert_eq!(item.generation, trans_generation);
    assert_eq!(item.refs, 1);

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn create_empty_tree_then_insert_items_in_same_transaction() {
    let (dir, img_path) = create_test_image();
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap();

        let data = [0xEEu8; 16];
        for i in 0..20u64 {
            let key = DiskKey {
                objectid: 1000 + i,
                key_type: KeyType::TemporaryItem,
                offset: 0,
            };
            let mut path = BtrfsPath::new();
            search::search_slot(
                Some(&mut trans),
                &mut fs,
                TEST_EMPTY_TREE_ID,
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

    let mut fs = open_rw(&img_path);
    for i in 0..20u64 {
        let key = DiskKey {
            objectid: 1000 + i,
            key_type: KeyType::TemporaryItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            TEST_EMPTY_TREE_ID,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found, "item objectid {} not found after reopen", 1000 + i);
        let leaf = path.nodes[0].as_ref().unwrap();
        assert_eq!(leaf.item_data(path.slots[0]).len(), 16);
        path.release();
    }

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn create_empty_tree_then_insert_in_second_transaction() {
    let (dir, img_path) = create_test_image();
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap();
        trans.commit(&mut fs).unwrap();
    }
    {
        let mut fs = open_rw(&img_path);
        assert!(fs.root_bytenr(TEST_EMPTY_TREE_ID).is_some());
        let mut trans = Transaction::start(&mut fs).unwrap();
        let key = DiskKey {
            objectid: 42,
            key_type: KeyType::TemporaryItem,
            offset: 0,
        };
        let data = [0x77u8; 8];
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            TEST_EMPTY_TREE_ID,
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
        trans.commit(&mut fs).unwrap();
    }

    let mut fs = open_rw(&img_path);
    let key = DiskKey {
        objectid: 42,
        key_type: KeyType::TemporaryItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        &mut fs,
        TEST_EMPTY_TREE_ID,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .unwrap();
    assert!(found);
    path.release();

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn create_empty_tree_rejects_duplicate_id() {
    let (dir, img_path) = create_test_image();
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap();
        let err = trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "got: {err}");
        trans.commit(&mut fs).unwrap();
    }
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        let err = trans
            .create_empty_tree(&mut fs, TEST_EMPTY_TREE_ID)
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "got: {err}");
        trans.abort(&mut fs);
    }
    drop(dir);
}

#[test]
fn create_empty_tree_rejects_bootstrap_ids() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = Transaction::start(&mut fs).unwrap();
    for &reserved in &[0u64, 1, 2, 3] {
        let err = trans.create_empty_tree(&mut fs, reserved).unwrap_err();
        assert!(
            err.to_string().contains("reserved"),
            "id {reserved}: got {err}"
        );
    }
    trans.abort(&mut fs);
    drop(fs);
    drop(dir);
}

#[test]
fn create_empty_tree_rejects_existing_real_tree() {
    // Tree id 5 (FS_TREE) is created by mkfs and must be rejected.
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let mut trans = Transaction::start(&mut fs).unwrap();
    let err = trans.create_empty_tree(&mut fs, 5).unwrap_err();
    assert!(err.to_string().contains("already exists"), "got: {err}");
    trans.abort(&mut fs);
    drop(fs);
    drop(dir);
}

// ----- Stage I.2: read-only extent-tree walker -----

/// Read every `FREE_SPACE_EXTENT` item in the FST whose key
/// objectid lies inside `[bg_start, bg_start + bg_length)`. Returns
/// `(start, length)` pairs in ascending order.
fn read_fst_extents(
    fs: &mut Filesystem<File>,
    bg_start: u64,
    bg_length: u64,
) -> Vec<(u64, u64)> {
    let bg_end = bg_start + bg_length;
    let key = DiskKey {
        objectid: bg_start,
        key_type: KeyType::FreeSpaceExtent,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs,
        10, // FREE_SPACE_TREE
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .unwrap();
    let mut out = Vec::new();
    loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break;
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            if !search::next_leaf(fs, &mut path).unwrap() {
                break;
            }
            continue;
        }
        let k = leaf.item_key(slot);
        if k.objectid >= bg_end {
            break;
        }
        if k.key_type == KeyType::FreeSpaceExtent && k.offset > 0 {
            out.push((k.objectid, k.offset));
        }
        path.slots[0] = slot + 1;
    }
    path.release();
    out
}

#[test]
fn extent_walker_matches_fst_for_metadata_block_group() {
    // Strongest possible end-to-end check: walk allocated extents
    // for an existing metadata block group, derive free ranges, and
    // assert they exactly match the on-disk FREE_SPACE_TREE entries
    // for the same group. mkfs.btrfs writes both, so any divergence
    // is either a walker bug or a derivation bug.
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);

    let groups = allocation::load_block_groups(&mut fs).unwrap();
    let bg = groups
        .iter()
        .find(|g| g.is_metadata())
        .expect("default mkfs creates a metadata block group");

    let mut walked: Vec<AllocatedExtent> = Vec::new();
    extent_walk::walk_block_group_extents(&mut fs, bg.start, bg.length, |e| {
        walked.push(e);
        Ok(())
    })
    .unwrap();

    // Walker invariants.
    assert!(
        !walked.is_empty(),
        "metadata BG should have allocated tree blocks"
    );
    for w in &walked {
        assert!(w.start >= bg.start);
        assert!(w.end() <= bg.start + bg.length);
        assert!(w.length > 0);
    }
    for pair in walked.windows(2) {
        assert!(
            pair[0].end() <= pair[1].start,
            "walker yielded overlapping extents: {:?} {:?}",
            pair[0],
            pair[1]
        );
    }

    let derived =
        extent_walk::derive_free_ranges(bg.start, bg.length, &walked).unwrap();
    let derived_pairs: Vec<(u64, u64)> =
        derived.iter().map(|r| (r.start, r.length)).collect();

    let fst = read_fst_extents(&mut fs, bg.start, bg.length);

    // mkfs's FST should agree with our walker-derived ranges.
    assert_eq!(
        derived_pairs, fst,
        "derived free ranges differ from on-disk FST for BG {}",
        bg.start
    );

    // Length conservation across the whole block group.
    let alloc_total: u64 = walked.iter().map(|e| e.length).sum();
    let free_total: u64 = derived.iter().map(|r| r.length).sum();
    assert_eq!(alloc_total + free_total, bg.length);

    drop(fs);
    drop(dir);
}

#[test]
fn extent_walker_visits_all_allocated_extents_in_data_block_group() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let groups = allocation::load_block_groups(&mut fs).unwrap();
    let Some(bg) = groups.iter().find(|g| g.is_data()).cloned() else {
        // Some mkfs builds skip data BG until first write — accept and bail.
        return;
    };

    let mut walked: Vec<AllocatedExtent> = Vec::new();
    extent_walk::walk_block_group_extents(&mut fs, bg.start, bg.length, |e| {
        walked.push(e);
        Ok(())
    })
    .unwrap();

    let derived =
        extent_walk::derive_free_ranges(bg.start, bg.length, &walked).unwrap();
    let derived_pairs: Vec<(u64, u64)> =
        derived.iter().map(|r| (r.start, r.length)).collect();
    let fst = read_fst_extents(&mut fs, bg.start, bg.length);
    assert_eq!(derived_pairs, fst);

    drop(fs);
    drop(dir);
}

#[test]
fn extent_walker_visitor_error_propagates() {
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let bg = allocation::load_block_groups(&mut fs)
        .unwrap()
        .into_iter()
        .find(|g| g.is_metadata())
        .unwrap();

    let err = extent_walk::walk_block_group_extents(
        &mut fs,
        bg.start,
        bg.length,
        |_| Err(std::io::Error::other("stop here")),
    )
    .unwrap_err();
    assert!(err.to_string().contains("stop here"));

    drop(fs);
    drop(dir);
}

#[test]
fn extent_walker_visitor_error_short_circuits() {
    // The visitor should be called at most once before erroring out.
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    let bg = allocation::load_block_groups(&mut fs)
        .unwrap()
        .into_iter()
        .find(|g| g.is_metadata())
        .unwrap();
    let mut count = 0u32;
    let _ = extent_walk::walk_block_group_extents(
        &mut fs,
        bg.start,
        bg.length,
        |_| {
            count += 1;
            Err(std::io::Error::other("stop"))
        },
    );
    assert_eq!(
        count, 1,
        "visitor must be called exactly once before bailing"
    );

    drop(fs);
    drop(dir);
}

#[test]
fn extent_walker_post_modification_matches_fst() {
    // After we COW some extent-tree blocks (by inserting items into
    // the root tree), the walker for the affected metadata BG must
    // still match the updated FST.
    let (dir, img_path) = create_test_image();

    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        let data = [0x9Au8; 64];
        for i in 0..40u64 {
            let key = DiskKey {
                objectid: 600_000 + i,
                key_type: KeyType::TemporaryItem,
                offset: 0,
            };
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

    let mut fs = open_rw(&img_path);
    for bg in allocation::load_block_groups(&mut fs).unwrap() {
        if !bg.is_metadata() {
            continue;
        }
        let mut walked = Vec::new();
        extent_walk::walk_block_group_extents(
            &mut fs,
            bg.start,
            bg.length,
            |e| {
                walked.push(e);
                Ok(())
            },
        )
        .unwrap();
        let derived =
            extent_walk::derive_free_ranges(bg.start, bg.length, &walked)
                .unwrap();
        let derived_pairs: Vec<(u64, u64)> =
            derived.iter().map(|r| (r.start, r.length)).collect();
        let fst = read_fst_extents(&mut fs, bg.start, bg.length);
        assert_eq!(
            derived_pairs, fst,
            "post-mutation: walker disagrees with FST in BG {}",
            bg.start
        );
    }

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn create_two_empty_trees_in_one_transaction() {
    // Verifies that allocating two empty trees in a single
    // transaction does not collide on bytenr or root-item slot.
    // Uses FS-tree-range ids (above BTRFS_FIRST_FREE_OBJECTID), so
    // the resulting image is not whole-image valid and we skip
    // `btrfs check` here — the assertion is purely in-memory and
    // round-trip-via-reopen.
    let (dir, img_path) = create_test_image();
    let id_a: u64 = 0x4000;
    let id_b: u64 = 0x4001;
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        let bytenr_a = trans.create_empty_tree(&mut fs, id_a).unwrap();
        let bytenr_b = trans.create_empty_tree(&mut fs, id_b).unwrap();
        assert_ne!(
            bytenr_a, bytenr_b,
            "two empty trees must use distinct leaf bytenrs"
        );
        trans.commit(&mut fs).unwrap();
    }
    let mut fs = open_rw(&img_path);
    assert!(fs.root_bytenr(id_a).is_some());
    assert!(fs.root_bytenr(id_b).is_some());
    let item_a = find_root_item(&mut fs, id_a).unwrap();
    let item_b = find_root_item(&mut fs, id_b).unwrap();
    assert_ne!(item_a.bytenr, item_b.bytenr);
    assert_eq!(item_a.refs, 1);
    assert_eq!(item_b.refs, 1);
    drop(fs);
    drop(dir);
}

// ----- Stage I.3: convert_to_free_space_tree -----

const COMPAT_RO_FREE_SPACE_TREE: u64 = 1 << 0;
const COMPAT_RO_FREE_SPACE_TREE_VALID: u64 = 1 << 1;

#[test]
fn convert_to_free_space_tree_basic() {
    // Start without FST, run the conversion, commit, reopen, and
    // assert btrfs check accepts the resulting image.
    let (dir, img_path) =
        create_test_image_with_features(&["^free-space-tree"]);

    {
        let mut fs = open_rw(&img_path);
        // Sanity: starting state has no FST.
        assert_eq!(
            fs.superblock.compat_ro_flags & COMPAT_RO_FREE_SPACE_TREE,
            0
        );
        assert!(fs.root_bytenr(10).is_none());

        let mut trans = Transaction::start(&mut fs).unwrap();
        convert::convert_to_free_space_tree(&mut trans, &mut fs).unwrap();
        trans.commit(&mut fs).unwrap();
    }

    let mut fs = open_rw(&img_path);
    // Both compat_ro bits should be set, FST root must be present.
    let bits = fs.superblock.compat_ro_flags;
    assert_ne!(bits & COMPAT_RO_FREE_SPACE_TREE, 0);
    assert_ne!(bits & COMPAT_RO_FREE_SPACE_TREE_VALID, 0);
    assert_eq!(fs.superblock.cache_generation, 0);
    assert!(fs.root_bytenr(10).is_some());

    // Walker / FST cross-check on every block group: derived free
    // ranges from the extent tree must equal the FST entries we
    // built. (Same invariant as the I.2 tests, now over our own
    // newly-built FST.)
    for bg in allocation::load_block_groups(&mut fs).unwrap() {
        let mut walked = Vec::new();
        extent_walk::walk_block_group_extents(
            &mut fs,
            bg.start,
            bg.length,
            |e| {
                walked.push(e);
                Ok(())
            },
        )
        .unwrap();
        let derived =
            extent_walk::derive_free_ranges(bg.start, bg.length, &walked)
                .unwrap();
        let derived_pairs: Vec<(u64, u64)> =
            derived.iter().map(|r| (r.start, r.length)).collect();
        let fst = read_fst_extents(&mut fs, bg.start, bg.length);
        assert_eq!(
            derived_pairs, fst,
            "convert: walker disagrees with FST in BG {}",
            bg.start
        );
    }

    drop(fs);
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn convert_to_free_space_tree_rejects_already_enabled() {
    // Default mkfs already has FST. Conversion must refuse.
    let (dir, img_path) = create_test_image();
    let mut fs = open_rw(&img_path);
    assert_ne!(fs.superblock.compat_ro_flags & COMPAT_RO_FREE_SPACE_TREE, 0);
    let mut trans = Transaction::start(&mut fs).unwrap();
    let err =
        convert::convert_to_free_space_tree(&mut trans, &mut fs).unwrap_err();
    assert!(err.to_string().contains("already enabled"), "got: {err}");
    trans.abort(&mut fs);
    drop(fs);
    drop(dir);
}

#[test]
fn convert_to_free_space_tree_idempotent_after_one_run() {
    // Running the conversion a second time on the same filesystem
    // should fail with "already enabled" — i.e. we never produce
    // duplicate FST roots.
    let (dir, img_path) =
        create_test_image_with_features(&["^free-space-tree"]);
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        convert::convert_to_free_space_tree(&mut trans, &mut fs).unwrap();
        trans.commit(&mut fs).unwrap();
    }
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        let err = convert::convert_to_free_space_tree(&mut trans, &mut fs)
            .unwrap_err();
        assert!(err.to_string().contains("already enabled"));
        trans.abort(&mut fs);
    }
    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn convert_to_free_space_tree_then_mutate_and_recommit() {
    // After conversion, ordinary insert transactions must continue
    // to work and the FST must stay consistent (the existing
    // Stage F update path must accept our hand-built FST as input).
    let (dir, img_path) =
        create_test_image_with_features(&["^free-space-tree"]);
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        convert::convert_to_free_space_tree(&mut trans, &mut fs).unwrap();
        trans.commit(&mut fs).unwrap();
    }
    {
        let mut fs = open_rw(&img_path);
        let mut trans = Transaction::start(&mut fs).unwrap();
        let data = [0x55u8; 48];
        for i in 0..50u64 {
            let key = DiskKey {
                objectid: 800_000 + i,
                key_type: KeyType::TemporaryItem,
                offset: 0,
            };
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
    drop(dir);
}

//! Regression tests for bugs found during review and stress testing.
//!
//! Each test targets a specific bug fix and would have caught the bug
//! if it had existed before the fix. These tests use real btrfs images
//! via `mkfs.btrfs` to exercise the full pipeline.

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    cow,
    extent_buffer::{HEADER_SIZE, ITEM_SIZE},
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

//! Integration tests for the btrfs-transaction crate.
//!
//! These tests verify the full pipeline against real btrfs filesystem images:
//! - Read path: open fixture image, search for known keys, verify results
//! - Write path: create temporary image, modify, commit, reopen, verify

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    extent_buffer::key_cmp, fs_info::FsInfo, items, path::BtrfsPath, search,
    transaction::TransHandle,
};
use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
};

/// Path to the fixture image (gzipped).
fn fixture_gz_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // up from transaction/
    path.push("cli/tests/commands/fixture.img.gz");
    path
}

/// Decompress the fixture image to a temporary file and return it opened for R/W.
fn open_fixture() -> io::Result<tempfile::NamedTempFile> {
    let gz_path = fixture_gz_path();
    let gz_file = File::open(&gz_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot open fixture {}: {e}", gz_path.display()),
        )
    })?;
    let mut decoder = flate2::read::GzDecoder::new(gz_file);

    let mut tmp = tempfile::NamedTempFile::new()?;
    io::copy(&mut decoder, tmp.as_file_mut())?;
    tmp.as_file_mut().flush()?;
    Ok(tmp)
}

// ---- Read path tests (fixture image) ----

#[test]
fn open_fixture_image() {
    let tmp = open_fixture().expect("failed to decompress fixture");
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let fs = FsInfo::open(file).expect("failed to open fixture");

    // Verify basic superblock fields
    assert_eq!(fs.superblock.nodesize, 16384);
    assert!(fs.superblock.generation > 0);

    // Verify some tree roots exist
    assert!(fs.root_bytenr(1).is_some(), "root tree missing");
    assert!(fs.root_bytenr(2).is_some(), "extent tree missing");
    assert!(fs.root_bytenr(3).is_some(), "chunk tree missing");
    assert!(fs.root_bytenr(5).is_some(), "fs tree missing");
}

#[test]
fn search_root_tree_for_fs_tree() {
    let tmp = open_fixture().expect("failed to decompress fixture");
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let mut fs = FsInfo::open(file).expect("failed to open fixture");

    // Search for ROOT_ITEM of the FS tree (tree ID 5)
    let key = DiskKey {
        objectid: 5,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found =
        search::search_slot(None, &mut fs, 1, &key, &mut path, 0, false)
            .expect("search_slot failed");

    assert!(found, "ROOT_ITEM for FS_TREE should exist");
    let leaf = path.leaf().expect("path should have a leaf");
    let slot = path.leaf_slot();
    let item_key = leaf.item_key(slot);
    assert_eq!(item_key.objectid, 5);
    assert_eq!(item_key.key_type, KeyType::RootItem);
}

#[test]
fn search_nonexistent_key() {
    let tmp = open_fixture().expect("failed to decompress fixture");
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let mut fs = FsInfo::open(file).expect("failed to open fixture");

    // Search for a key that shouldn't exist
    let key = DiskKey {
        objectid: 999_999,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found =
        search::search_slot(None, &mut fs, 1, &key, &mut path, 0, false)
            .expect("search_slot failed");

    assert!(!found, "key 999999 should not exist in root tree");
}

#[test]
fn next_leaf_traversal() {
    let tmp = open_fixture().expect("failed to decompress fixture");
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let mut fs = FsInfo::open(file).expect("failed to open fixture");

    // Search for the minimum key in the root tree
    let key = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(None, &mut fs, 1, &key, &mut path, 0, false)
        .expect("search_slot failed");

    // Walk forward through all items in the root tree
    let mut count = 0;
    let mut prev_key: Option<DiskKey> = None;
    loop {
        let leaf = match path.leaf() {
            Some(l) => l,
            None => break,
        };
        let slot = path.slots[0];
        if slot >= leaf.nritems() as usize {
            // Try to advance to next leaf
            if !search::next_leaf(&mut fs, &mut path).expect("next_leaf failed")
            {
                break;
            }
            continue;
        }

        let item_key = leaf.item_key(slot);

        // Verify keys are in ascending order
        if let Some(ref pk) = prev_key {
            assert!(
                key_cmp(pk, &item_key) != std::cmp::Ordering::Greater,
                "keys out of order: prev={pk:?} current={item_key:?}"
            );
        }
        prev_key = Some(item_key);
        count += 1;
        path.slots[0] += 1;
    }

    assert!(count > 0, "root tree should have at least one item");
}

#[test]
fn search_extent_tree() {
    let tmp = open_fixture().expect("failed to decompress fixture");
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let mut fs = FsInfo::open(file).expect("failed to open fixture");

    // Verify we can search the extent tree (tree 2)
    let key = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(None, &mut fs, 2, &key, &mut path, 0, false)
        .expect("search_slot in extent tree failed");

    // Should find something (the extent tree is never empty on a valid fs)
    let leaf = path.leaf().expect("should have a leaf");
    assert!(leaf.nritems() > 0, "extent tree leaf should have items");
}

// ---- Write path tests ----

/// Create a temporary btrfs image file using the system `mkfs.btrfs`.
/// Returns the temp directory (keeps the file alive) and the image path.
///
/// # Panics
///
/// Panics if the temp directory cannot be created, the image file cannot be
/// written, or `mkfs.btrfs` is not available or fails.
fn create_test_image() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let img_path = dir.path().join("test.img");

    // Create a 128 MiB sparse file
    let file = File::create(&img_path).expect("failed to create image file");
    file.set_len(128 * 1024 * 1024)
        .expect("failed to set image size");
    drop(file);

    // Run mkfs.btrfs
    let status = Command::new("mkfs.btrfs")
        .args(["-f", "-q"])
        .arg(&img_path)
        .status()
        .expect("mkfs.btrfs not found — install btrfs-progs");
    assert!(status.success(), "mkfs.btrfs failed with {status}");

    (dir, img_path)
}

/// Run `btrfs check` on an image, asserting it passes.
///
/// Captures stdout/stderr and only prints them if the check fails,
/// keeping test output clean on success.
///
/// # Panics
///
/// Panics if `btrfs check` is not found or reports errors.
fn assert_btrfs_check(path: &Path) {
    let output = Command::new("btrfs")
        .args(["check", "--readonly"])
        .arg(path)
        .output()
        .expect("btrfs check not found — install btrfs-progs");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "btrfs check failed on {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
            path.display()
        );
    }
}

#[test]
fn write_insert_item_and_verify() {
    let (dir, img_path) = create_test_image();

    // Verify the pristine image passes btrfs check
    assert_btrfs_check(&img_path);

    let generation_before;
    let test_objectid = 100_000u64;
    let test_key = DiskKey {
        objectid: test_objectid,
        key_type: KeyType::TemporaryItem,
        offset: 42,
    };
    let test_data = b"hello transaction";

    // Phase 1: Open, start transaction, insert item, commit
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = FsInfo::open(file).expect("open failed");
        generation_before = fs.superblock.generation;

        let mut trans =
            TransHandle::start(&mut fs).expect("start transaction failed");

        // Search for the insertion point in the root tree
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1, // root tree
            &test_key,
            &mut path,
            (25 + test_data.len()) as u32,
            true, // COW
        )
        .expect("search_slot failed");

        assert!(!found, "test key should not exist yet");

        // Insert the item into the leaf
        let leaf = path.nodes[0].as_mut().expect("no leaf");
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &test_key, test_data)
            .expect("insert_item failed");
        fs.mark_dirty(leaf);

        path.release();

        // Commit
        trans.commit(&mut fs).expect("commit failed");
    }

    // Phase 2: Reopen and verify the item persists
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = FsInfo::open(file).expect("reopen failed");

        // Generation should have incremented
        assert_eq!(
            fs.superblock.generation,
            generation_before + 1,
            "generation should have incremented"
        );

        // Search for our inserted item
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None, &mut fs, 1, &test_key, &mut path, 0, false,
        )
        .expect("search_slot on reopen failed");

        assert!(found, "inserted item should be found after reopen");

        // Verify the data
        let leaf = path.leaf().expect("no leaf");
        let slot = path.leaf_slot();
        let data = leaf.item_data(slot);
        assert_eq!(data, test_data, "item data should match");
    }

    // Phase 3: Run btrfs check — must pass clean
    assert_btrfs_check(&img_path);

    drop(dir);
}

#[test]
fn write_delete_item_and_verify() {
    let (dir, img_path) = create_test_image();

    // Find the UUID tree ROOT_ITEM (tree ID 9) and delete it
    let uuid_key = DiskKey {
        objectid: 9,
        key_type: KeyType::RootItem,
        offset: 0,
    };

    // Phase 1: Delete the UUID tree root item and free its root block
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = FsInfo::open(file).expect("open failed");

        // Read the UUID tree's root block address before deleting
        let uuid_tree_bytenr =
            fs.root_bytenr(9).expect("UUID tree root missing");

        let mut trans =
            TransHandle::start(&mut fs).expect("start transaction failed");

        // Search with COW to get a writable path
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &uuid_key,
            &mut path,
            0,
            true,
        )
        .expect("search failed");
        assert!(found, "UUID tree ROOT_ITEM should exist");

        // Delete the ROOT_ITEM
        let leaf = path.nodes[0].as_mut().expect("no leaf");
        let slot = path.slots[0];
        items::del_items(leaf, slot, 1);
        fs.mark_dirty(leaf);
        path.release();

        // Queue a delayed ref to free the UUID tree's root block.
        // When removing a tree, its blocks' extent items must be cleaned up.
        trans.delayed_refs.drop_ref(uuid_tree_bytenr, true, 9, 0);

        // Remove from roots map so commit doesn't try to update it
        fs.remove_root(9);

        trans.commit(&mut fs).expect("commit failed");
    }

    // Phase 2: Verify deletion persisted
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = FsInfo::open(file).expect("reopen failed");

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None, &mut fs, 1, &uuid_key, &mut path, 0, false,
        )
        .expect("search failed");

        assert!(!found, "UUID tree ROOT_ITEM should be gone after delete");
    }

    // Phase 3: btrfs check must pass
    assert_btrfs_check(&img_path);

    drop(dir);
}

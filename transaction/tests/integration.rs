//! Integration tests for the btrfs-transaction crate.
//!
//! These tests verify the full pipeline against real btrfs filesystem images:
//! - Read path: open fixture image, search for known keys, verify results
//! - Write path: create temporary image, modify, commit, reopen, verify

use btrfs_disk::tree::{DiskKey, KeyType};
use btrfs_transaction::{
    extent_buffer::key_cmp, fs_info::FsInfo, path::BtrfsPath, search,
};
use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
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

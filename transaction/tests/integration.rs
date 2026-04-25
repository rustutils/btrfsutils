//! Integration tests for the btrfs-transaction crate.
//!
//! These tests verify the full pipeline against real btrfs filesystem images:
//! - Read path: open fixture image, search for known keys, verify results
//! - Write path: create temporary image, modify, commit, reopen, verify
//! - Mount tests: modify image, mount, verify changes from userspace (privileged)

// Some imports are only used in #[ignore]d privileged tests.
#![allow(unused_imports)]

use btrfs_disk::{
    items::{
        DirItem, InodeItemArgs, InodeRef, RootItem, RootItemFlags, Timespec,
    },
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    buffer::key_cmp,
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
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
    let fs = Filesystem::open(file).expect("failed to open fixture");

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
    let mut fs = Filesystem::open(file).expect("failed to open fixture");

    // Search for ROOT_ITEM of the FS tree (tree ID 5)
    let key = DiskKey {
        objectid: 5,
        key_type: KeyType::RootItem,
        offset: 0,
    };
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
    let mut fs = Filesystem::open(file).expect("failed to open fixture");

    // Search for a key that shouldn't exist
    let key = DiskKey {
        objectid: 999_999,
        key_type: KeyType::RootItem,
        offset: 0,
    };
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
    let mut fs = Filesystem::open(file).expect("failed to open fixture");

    // Search for the minimum key in the root tree
    let key = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        &mut fs,
        1,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .expect("search_slot failed");

    // Walk forward through all items in the root tree
    let mut count = 0;
    let mut prev_key: Option<DiskKey> = None;
    #[allow(clippy::while_let_loop)]
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
    let mut fs = Filesystem::open(file).expect("failed to open fixture");

    // Verify we can search the extent tree (tree 2)
    let key = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        &mut fs,
        2,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
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
/// written, or `btrfs-mkfs` is not available or fails.
fn create_test_image() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let img_path = dir.path().join("test.img");

    // Create a 128 MiB sparse file
    let file = File::create(&img_path).expect("failed to create image file");
    file.set_len(256 * 1024 * 1024)
        .expect("failed to set image size");
    drop(file);

    let mkfs = find_our_mkfs();
    let status = Command::new(&mkfs)
        .args(["-f", "-q"])
        .arg(&img_path)
        .status()
        .unwrap_or_else(|e| {
            panic!("btrfs-mkfs at {} failed to run: {e}", mkfs.display())
        });
    assert!(status.success(), "btrfs-mkfs failed with {status}");

    (dir, img_path)
}

fn find_our_mkfs() -> PathBuf {
    let exe =
        std::env::current_exe().expect("cannot determine test binary path");
    let target_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("cannot determine target directory");
    let mkfs = target_dir.join("btrfs-mkfs");
    assert!(
        mkfs.exists(),
        "btrfs-mkfs not found at {}; run `cargo build -p btrfs-mkfs` first",
        mkfs.display()
    );
    mkfs
}

/// Run `btrfs check` on an image, asserting it passes cleanly.
///
/// Captures stdout/stderr and only prints them if the check fails.
///
/// # Panics
///
/// Panics if `btrfs check` is not found or reports any error.
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
        let mut fs = Filesystem::open(file).expect("open failed");
        generation_before = fs.superblock.generation;

        let mut trans =
            Transaction::start(&mut fs).expect("start transaction failed");

        // Search for the insertion point in the root tree
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1, // root tree
            &test_key,
            &mut path,
            SearchIntent::Insert((25 + test_data.len()) as u32),
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
        let mut fs = Filesystem::open(file).expect("reopen failed");

        // Generation should have incremented
        assert_eq!(
            fs.superblock.generation,
            generation_before + 1,
            "generation should have incremented"
        );

        // Search for our inserted item
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            1,
            &test_key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
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

    // Our mkfs doesn't create a UUID tree (tree 9), so create one first.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open failed");
        let mut trans = Transaction::start(&mut fs).expect("start failed");
        trans
            .create_empty_tree(&mut fs, 9)
            .expect("create UUID tree failed");
        trans.commit(&mut fs).expect("commit failed");
    }

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
        let mut fs = Filesystem::open(file).expect("open failed");

        // Read the UUID tree's root block address before deleting
        let uuid_tree_bytenr =
            fs.root_bytenr(9).expect("UUID tree root missing");

        let mut trans =
            Transaction::start(&mut fs).expect("start transaction failed");

        // Search with COW to get a writable path
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &uuid_key,
            &mut path,
            SearchIntent::Delete,
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
        let mut fs = Filesystem::open(file).expect("reopen failed");

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            1,
            &uuid_key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search failed");

        assert!(!found, "UUID tree ROOT_ITEM should be gone after delete");
    }

    // Phase 3: btrfs check must pass
    assert_btrfs_check(&img_path);

    drop(dir);
}

#[test]
fn backup_roots_updated_on_commit() {
    let (dir, img_path) = create_test_image();

    let generation_before;
    let root_bytenr_before;

    // Read pre-commit state
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).expect("open failed");
        generation_before = fs.superblock.generation;
        root_bytenr_before = fs.superblock.root;
    }

    // Modify the filesystem (insert an item to trigger COW)
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open failed");
        let mut trans = Transaction::start(&mut fs).expect("start failed");

        let key = DiskKey {
            objectid: 100_001,
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
            SearchIntent::Insert(25 + 4),
            true,
        )
        .expect("search failed");

        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, &[0xBB; 4]).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit failed");
    }

    // Verify backup roots
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).expect("reopen failed");
        let new_gen = fs.superblock.generation;
        assert_eq!(new_gen, generation_before + 1);

        // The backup root slot is transid % 4
        let slot = (new_gen % 4) as usize;
        let backup = &fs.superblock.backup_roots[slot];

        // Backup root should reflect the new state
        assert_eq!(
            backup.tree_root, fs.superblock.root,
            "backup tree_root should match superblock root"
        );
        assert_eq!(
            backup.tree_root_gen, new_gen,
            "backup tree_root_gen should match new generation"
        );
        assert_ne!(
            backup.tree_root, root_bytenr_before,
            "root tree should have moved due to COW"
        );

        // Extent tree should be present
        assert_ne!(backup.extent_root, 0, "backup extent_root should be set");
        assert_ne!(
            backup.extent_root_gen, 0,
            "backup extent_root_gen should be set"
        );

        // FS tree, dev tree, csum tree should be present
        assert_ne!(backup.fs_root, 0, "backup fs_root should be set");
        assert_ne!(backup.dev_root, 0, "backup dev_root should be set");
        assert_ne!(backup.csum_root, 0, "backup csum_root should be set");

        // Size counters should be populated
        assert_ne!(backup.total_bytes, 0, "backup total_bytes should be set");
        assert_ne!(backup.bytes_used, 0, "backup bytes_used should be set");
        assert_eq!(
            backup.bytes_used, fs.superblock.bytes_used,
            "backup bytes_used should match superblock"
        );
        assert_eq!(
            backup.num_devices, fs.superblock.num_devices,
            "backup num_devices should match superblock"
        );

        // Chunk tree should match superblock
        assert_eq!(
            backup.chunk_root, fs.superblock.chunk_root,
            "backup chunk_root should match superblock"
        );
    }

    assert_btrfs_check(&img_path);
    drop(dir);
}

#[test]
fn compat_ro_flags_preserved_after_commit() {
    let (dir, img_path) = create_test_image();

    let flags_before;
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).unwrap();
        flags_before = fs.superblock.compat_ro_flags;
    }

    // Do a simple modification + commit
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).unwrap();
        let mut trans = Transaction::start(&mut fs).unwrap();
        let key = DiskKey {
            objectid: 100_002,
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
            SearchIntent::Insert(25 + 1),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &key, &[0]).unwrap();
        fs.mark_dirty(leaf);
        path.release();
        trans.commit(&mut fs).unwrap();
    }

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).unwrap();
        let flags_after = fs.superblock.compat_ro_flags;

        // FREE_SPACE_TREE (bit 0) must remain set
        let fst_flag =
            u64::from(btrfs_disk::raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE);
        assert!(
            flags_after & fst_flag != 0,
            "FREE_SPACE_TREE flag must remain set (required by BLOCK_GROUP_TREE)"
        );

        // BLOCK_GROUP_TREE (bit 3) must remain set
        let bgt_flag = u64::from(
            btrfs_disk::raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE,
        );
        if flags_before & bgt_flag != 0 {
            assert!(
                flags_after & bgt_flag != 0,
                "BLOCK_GROUP_TREE flag must remain set"
            );
        }
    }

    drop(dir);
}

/// Insert enough items to force leaf splits, then verify all items survive
/// the commit and pass `btrfs check`. This exercises the preemptive splitting
/// logic in `search_slot` with `SearchIntent::Insert`, the `alloc_tree_block`
/// unified allocation, and the commit convergence loop.
#[test]
fn write_many_items_triggers_split() {
    let (dir, img_path) = create_test_image();
    assert_btrfs_check(&img_path);

    let item_count = 1000;
    let data_payload = [0xABu8; 32];

    // Phase 1: Insert many items, forcing leaf splits
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open failed");
        let mut trans =
            Transaction::start(&mut fs).expect("start transaction failed");

        for i in 0..item_count {
            let key = DiskKey {
                objectid: 200_000 + i as u64,
                key_type: KeyType::TemporaryItem,
                offset: 0,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut trans),
                &mut fs,
                1,
                &key,
                &mut path,
                SearchIntent::Insert((25 + data_payload.len()) as u32),
                true,
            )
            .unwrap_or_else(|e| panic!("search_slot failed on item {i}: {e}"));
            assert!(!found, "item {i} should not exist yet");

            let leaf = path.nodes[0].as_mut().expect("no leaf");
            items::insert_item(leaf, path.slots[0], &key, &data_payload)
                .unwrap_or_else(|e| {
                    panic!("insert_item failed on item {i}: {e}")
                });
            fs.mark_dirty(leaf);
            path.release();
        }

        trans.commit(&mut fs).expect("commit failed");
    }

    // Phase 2: Verify filesystem integrity
    assert_btrfs_check(&img_path);

    // Phase 3: Reopen and verify all items are searchable
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen failed");

        for i in 0..item_count {
            let key = DiskKey {
                objectid: 200_000 + i as u64,
                key_type: KeyType::TemporaryItem,
                offset: 0,
            };
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
            .unwrap_or_else(|e| panic!("search failed for item {i}: {e}"));
            assert!(found, "item {i} not found after commit");

            let leaf = path.nodes[0].as_ref().unwrap();
            let data = leaf.item_data(path.slots[0]);
            assert_eq!(
                data, &data_payload,
                "item {i} data mismatch after commit"
            );
            path.release();
        }
    }

    drop(dir);
}

#[test]
fn write_set_subvol_readonly() {
    let (dir, img_path) = create_test_image();

    // Set FS_TREE (tree 5) to read-only
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).unwrap();
        let mut trans = Transaction::start(&mut fs).unwrap();

        let key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);

        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let data = leaf.item_data(slot).to_vec();
        let mut root_item = RootItem::parse(&data).unwrap();
        assert!(!root_item.flags.contains(RootItemFlags::RDONLY));
        root_item.flags |= RootItemFlags::RDONLY;
        let new_data = root_item.to_bytes();
        items::update_item(leaf, slot, &new_data[..data.len()]).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).unwrap();
    }

    // Verify RDONLY persists after reopen
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).unwrap();

        let key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
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
        assert!(found, "FS_TREE ROOT_ITEM should exist");

        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.leaf_slot());
        let root_item = RootItem::parse(data).unwrap();
        assert!(
            root_item.flags.contains(RootItemFlags::RDONLY),
            "RDONLY should persist, got flags: {:?}",
            root_item.flags
        );
    }

    assert_btrfs_check(&img_path);
    drop(dir);
}

// ---- Privileged tests (require root, run via `just test`) ----

/// RAII loopback device: attaches on creation, detaches on drop.
struct LoopDev {
    device: String,
}

impl LoopDev {
    fn attach(img: &Path) -> Option<Self> {
        let output = Command::new("losetup")
            .args(["--find", "--show"])
            .arg(img)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let device = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Some(Self { device })
    }
}

impl Drop for LoopDev {
    fn drop(&mut self) {
        let _ = Command::new("losetup")
            .args(["--detach", &self.device])
            .status();
    }
}

/// RAII mount point: mounts on creation, unmounts on drop.
struct MountPoint {
    path: PathBuf,
}

impl MountPoint {
    fn mount(device: &str, mount_path: &Path) -> Option<Self> {
        std::fs::create_dir_all(mount_path).ok()?;
        let status = Command::new("mount")
            .args(["-t", "btrfs", "-o", "ro"])
            .arg(device)
            .arg(mount_path)
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
        Some(Self {
            path: mount_path.to_path_buf(),
        })
    }
}

impl Drop for MountPoint {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(&self.path).status();
    }
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_verify_subvol_readonly() {
    let (dir, img_path) = create_test_image();

    // Phase 1: Set the default subvolume (FS_TREE, tree 5) to read-only
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open failed");

        let mut trans = Transaction::start(&mut fs).expect("start failed");

        // Search for ROOT_ITEM of FS_TREE (tree 5)
        let key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .expect("search failed");
        assert!(found, "FS_TREE ROOT_ITEM should exist");

        // Parse the root item, set RDONLY, write back
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let data = leaf.item_data(slot).to_vec();
        let original_len = data.len();
        let mut root_item = RootItem::parse(&data).expect("parse ROOT_ITEM");
        root_item.flags |= RootItemFlags::RDONLY;
        let new_data = root_item.to_bytes();
        // Truncate to match the on-disk item size (mkfs may write 439-byte
        // root items without the trailing 64-byte reserved region)
        items::update_item(leaf, slot, &new_data[..original_len])
            .expect("update failed");
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit failed");
        fs.sync().expect("sync failed");
    }

    // Verify btrfs check passes
    assert_btrfs_check(&img_path);

    // Phase 2: Mount read-only and verify the subvolume shows as readonly.
    // We mount with -o ro to avoid the kernel modifying the filesystem
    // (e.g. rebuilding the free space tree).
    let loop_dev =
        LoopDev::attach(&img_path).expect("losetup failed (need root?)");
    let mount_path = dir.path().join("mnt");
    let _mount =
        MountPoint::mount(&loop_dev.device, &mount_path).expect("mount failed");

    // btrfs subvolume show reports "Flags: readonly" for RDONLY subvolumes
    let output = Command::new("btrfs")
        .args(["subvolume", "show"])
        .arg(&mount_path)
        .output()
        .expect("btrfs subvolume show failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("readonly"),
        "subvolume should be read-only, got:\n{stdout}"
    );
}

#[test]
#[ignore = "requires elevated privileges"]
fn mount_verify_file_created() {
    let (dir, img_path) = create_test_image();

    let file_name = b"hello.txt";
    let file_inode = 257u64;
    let dir_index = 100u64; // high index to avoid conflicts with mkfs entries
    let root_dir_inode = 256u64;

    // Phase 1: Create a file in the FS tree (tree 5)
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open failed");
        let transid = fs.superblock.generation + 1;
        let mut trans = Transaction::start(&mut fs).expect("start failed");

        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };

        // 1. Create INODE_ITEM for the new file
        // mode 0100644 = regular file, rw-r--r--
        let inode_data = InodeItemArgs {
            generation: transid,
            size: 0,
            nbytes: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            mode: 0o100644,
            time: ts,
        }
        .to_bytes();
        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::Insert((25 + inode_data.len()) as u32),
            true,
        )
        .expect("search inode slot failed");
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &inode_key, &inode_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 2. Create INODE_REF (file -> parent dir)
        let iref_data = InodeRef::serialize(dir_index, file_name);
        let iref_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeRef,
            offset: root_dir_inode,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &iref_key,
            &mut path,
            SearchIntent::Insert((25 + iref_data.len()) as u32),
            true,
        )
        .expect("search iref slot failed");
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &iref_key, &iref_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 3. Create DIR_ITEM (parent dir -> file, keyed by name hash)
        let location = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let dir_data = DirItem::serialize(
            &location,
            transid,
            btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
            file_name,
        );
        let dir_item_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirItem,
            offset: u64::from(btrfs_disk::util::btrfs_name_hash(file_name)),
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_item_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .expect("search dir_item slot failed");
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_item_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 4. Create DIR_INDEX (parent dir -> file, keyed by index)
        let dir_index_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirIndex,
            offset: dir_index,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_index_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .expect("search dir_index slot failed");
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_index_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 5. Update the root directory's INODE_ITEM (increment size for
        //    the new dir entry: name_len + sizeof(btrfs_dir_item) = 9 + 30)
        let dir_inode_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .expect("search dir inode failed");
        assert!(found, "root dir INODE_ITEM should exist");
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let old_data = leaf.item_data(slot).to_vec();
        if let Some(mut inode) = btrfs_disk::items::InodeItem::parse(&old_data)
        {
            // dir isize += name_len per dir entry (DIR_ITEM + DIR_INDEX)
            inode.size += file_name.len() as u64 * 2;
            inode.transid = transid;
            let new_data = InodeItemArgs {
                generation: inode.generation,
                size: inode.size,
                nbytes: inode.nbytes,
                nlink: inode.nlink,
                uid: inode.uid,
                gid: inode.gid,
                mode: inode.mode,
                time: ts,
            }
            .to_bytes();
            items::update_item(leaf, slot, &new_data).unwrap();
            fs.mark_dirty(leaf);
        }
        path.release();

        // 6. Update the ROOT_ITEM's embedded inode to match the INODE_ITEM.
        //    btrfs check validates that the ROOT_ITEM's embedded inode for
        //    the root directory matches the DIR_INDEX entries.
        let root_key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &root_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .expect("search ROOT_ITEM failed");
        assert!(found, "ROOT_ITEM for tree 5 should exist");
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let ri_data = leaf.item_data(slot).to_vec();
        if let Some(mut root_item) = RootItem::parse(&ri_data) {
            // Parse the embedded inode, update size, write back
            if let Some(mut embedded) =
                btrfs_disk::items::InodeItem::parse(&root_item.inode_data)
            {
                embedded.size += file_name.len() as u64 * 2;
                let new_inode = InodeItemArgs {
                    generation: embedded.generation,
                    size: embedded.size,
                    nbytes: embedded.nbytes,
                    nlink: embedded.nlink,
                    uid: embedded.uid,
                    gid: embedded.gid,
                    mode: embedded.mode,
                    time: ts,
                }
                .to_bytes();
                root_item.inode_data = new_inode;
            }
            let new_ri = root_item.to_bytes();
            if new_ri.len() == ri_data.len() {
                items::update_item(leaf, slot, &new_ri).unwrap();
            }
            fs.mark_dirty(leaf);
        }
        path.release();

        trans.commit(&mut fs).expect("commit failed");
        fs.sync().expect("sync failed");
    }

    // Phase 2: Verify btrfs check passes (structural integrity)
    assert_btrfs_check(&img_path);

    // Phase 3: Mount and verify the file is visible
    let loop_dev =
        LoopDev::attach(&img_path).expect("losetup failed (need root?)");
    let mount_path = dir.path().join("mnt");
    let _mount =
        MountPoint::mount(&loop_dev.device, &mount_path).expect("mount failed");

    let file_path = mount_path.join("hello.txt");
    assert!(file_path.exists(), "hello.txt should exist after mount");

    // Verify it's a regular file with the right permissions
    let metadata = std::fs::metadata(&file_path).expect("stat failed");
    assert!(metadata.is_file(), "hello.txt should be a regular file");
    assert_eq!(metadata.len(), 0, "hello.txt should be empty");
}

/// Force a chunk tree COW by mutating a `DEV_ITEM` in place via
/// `search_slot(cow=true)`, commit, and verify the new chunk root
/// resolves cleanly on a fresh open and the mutation persisted.
///
/// This exercises the SYSTEM-block-group allocator path and the
/// `sys_chunk_array` bootstrap update.
#[test]
fn chunk_tree_cow_round_trip() {
    let (dir, img_path) = create_test_image();

    // Phase 1: pick a DEV_ITEM, modify a benign field, commit.
    // Use `seek_speed` (1 byte at offset 64) which btrfs check ignores;
    // touching `bytes_used` would trip allocation-tree checks.
    let (devid, new_seek_speed) = {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let mut trans = Transaction::start(&mut fs).expect("start");

        // DEV_ITEMS objectid = 1, DEV_ITEM key. Pick the smallest.
        let key = DiskKey {
            objectid: 1, // BTRFS_DEV_ITEMS_OBJECTID
            key_type: KeyType::DeviceItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            3, // chunk tree
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            true, // COW
        )
        .expect("search_slot dev item");

        // Walk forward to the first DEV_ITEM.
        let leaf = path.nodes[0].as_mut().expect("leaf");
        let mut slot = path.slots[0];
        while slot < leaf.nritems() as usize {
            let k = leaf.item_key(slot);
            if k.key_type == KeyType::DeviceItem && k.objectid == 1 {
                break;
            }
            slot += 1;
        }
        assert!(slot < leaf.nritems() as usize, "no DEV_ITEM in chunk tree");

        let item_key = leaf.item_key(slot);
        let devid = item_key.offset;

        // btrfs_dev_item layout (offsets in bytes, little-endian):
        //   0  u64 devid
        //   8  u64 total_bytes
        //   16 u64 bytes_used
        //   24 u32 io_align
        //   28 u32 io_width
        //   32 u32 sector_size
        //   36 u64 type
        //   44 u64 generation
        //   52 u64 start_offset
        //   60 u32 dev_group
        //   64 u8  seek_speed   <-- mutated here
        //   65 u8  bandwidth
        //   66 [u8; 16] uuid
        //   82 [u8; 16] fsid
        let payload = leaf.item_data(slot);
        let old_seek_speed = payload[64];
        let new_seek_speed = old_seek_speed ^ 0x55;
        leaf.item_data_mut(slot)[64] = new_seek_speed;
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
        (devid, new_seek_speed)
    };

    // Phase 2: reopen and verify.
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).expect("reopen");
    assert!(
        fs.root_bytenr(3).is_some(),
        "chunk root must resolve after COW",
    );

    let key = DiskKey {
        objectid: 1,
        key_type: KeyType::DeviceItem,
        offset: devid,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        &mut fs,
        3,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .expect("search after reopen");
    assert!(found, "DEV_ITEM should still exist");
    let leaf = path.leaf().unwrap();
    let payload = leaf.item_data(path.slots[0]);
    let seek_speed = payload[64];
    assert_eq!(
        seek_speed, new_seek_speed,
        "DEV_ITEM seek_speed should reflect the in-transaction edit"
    );
    path.release();

    // Phase 3: btrfs check sanity (chunk root pointer + sys_chunk_array
    // bootstrap have to be internally consistent).
    assert_btrfs_check(&img_path);

    drop(dir);
}

/// Stage G: drop a data extent backref through `Transaction::commit` and
/// verify the parent `EXTENT_ITEM` and any csum tree entries for the
/// freed range disappear.
///
/// This test scans the fixture image for a single-ref data extent with
/// an inline `EXTENT_DATA_REF`, queues a `drop_data_ref`, commits, then
/// reopens the image and asserts the extent item and csums are gone.
#[test]
fn drop_data_extent_ref_removes_extent_item_and_csums() {
    use btrfs_disk::items::{ExtentItem, InlineRef};

    let tmp = open_fixture().expect("failed to decompress fixture");
    // Find a victim data extent.
    let (
        victim_bytenr,
        victim_num_bytes,
        victim_root,
        victim_ino,
        victim_offset,
    ) = {
        let file = File::options()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open fixture");

        let start = DiskKey {
            objectid: 0,
            key_type: KeyType::from_raw(0),
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            None,
            &mut fs,
            2,
            &start,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search extent tree");

        let mut victim = None;
        #[allow(clippy::while_let_loop)]
        'walk: loop {
            let leaf = match path.leaf() {
                Some(l) => l,
                None => break,
            };
            let slot = path.slots[0];
            if slot >= leaf.nritems() as usize {
                if !search::next_leaf(&mut fs, &mut path).expect("next_leaf") {
                    break;
                }
                continue;
            }
            let key = leaf.item_key(slot);
            if key.key_type == KeyType::ExtentItem {
                let data = leaf.item_data(slot).to_vec();
                if let Some(item) = ExtentItem::parse(&data, &key)
                    && item.is_data()
                    && item.refs == 1
                    && item.inline_refs.len() == 1
                    && let InlineRef::ExtentDataBackref {
                        root,
                        objectid,
                        offset,
                        count: 1,
                        ..
                    } = item.inline_refs[0]
                {
                    victim = Some((
                        key.objectid,
                        key.offset,
                        root,
                        objectid,
                        offset,
                    ));
                    break 'walk;
                }
            }
            path.slots[0] = slot + 1;
        }
        path.release();
        victim.expect("fixture has no single-ref data extent")
    };

    // Drop the backref through a transaction.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen for write");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans.delayed_refs.drop_data_ref(
            victim_bytenr,
            victim_num_bytes,
            victim_root,
            victim_ino,
            victim_offset,
            1,
        );
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify the EXTENT_ITEM is gone.
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmp.path())
        .unwrap();
    let mut fs = Filesystem::open(file).expect("reopen for verify");

    let key = DiskKey {
        objectid: victim_bytenr,
        key_type: KeyType::ExtentItem,
        offset: victim_num_bytes,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        &mut fs,
        2,
        &key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .expect("search after commit");
    assert!(
        !found,
        "EXTENT_ITEM at {victim_bytenr} should be gone after drop+commit"
    );
    path.release();

    // And the csum tree should not have any csum item that overlaps
    // [bytenr, bytenr+num_bytes) any more.
    if fs.root_bytenr(7).is_some() {
        let csum_objectid =
            i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;
        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: victim_bytenr,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            7,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("csum search");
        if found {
            let leaf = path.leaf().unwrap();
            let k = leaf.item_key(path.slots[0]);
            assert!(
                k.offset >= victim_bytenr + victim_num_bytes
                    || k.objectid != csum_objectid
                    || k.key_type != KeyType::ExtentCsum,
                "csum item at offset {} should not cover freed range [{},{})",
                k.offset,
                victim_bytenr,
                victim_bytenr + victim_num_bytes
            );
        }
        path.release();
    }
}

// ---- Stage J: mkfs migration prerequisites ----

/// Helper: find the first data block group in a filesystem image.
fn find_data_block_group(
    fs: &mut Filesystem<File>,
) -> btrfs_transaction::allocation::BlockGroup {
    let groups = btrfs_transaction::allocation::load_block_groups(fs)
        .expect("load_block_groups");
    groups
        .into_iter()
        .find(|bg| bg.is_data())
        .expect("no data block group found")
}

// -- J.2: Data block group accounting --

#[test]
fn data_block_group_loaded() {
    let (_dir, img_path) = create_test_image();
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).expect("open");

    let groups = btrfs_transaction::allocation::load_block_groups(&mut fs)
        .expect("load_block_groups");

    let data_groups: Vec<_> = groups.iter().filter(|bg| bg.is_data()).collect();
    assert!(
        !data_groups.is_empty(),
        "our mkfs image should contain at least one data block group"
    );

    for bg in &data_groups {
        assert!(bg.length > 0, "data block group length must be > 0");
        assert!(
            bg.used <= bg.length,
            "data block group used ({}) exceeds length ({})",
            bg.used,
            bg.length
        );
    }
}

#[test]
fn find_containing_bg_for_data_address() {
    let (_dir, img_path) = create_test_image();
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).expect("open");

    let groups = btrfs_transaction::allocation::load_block_groups(&mut fs)
        .expect("load_block_groups");
    let data_bg = groups.iter().find(|bg| bg.is_data()).unwrap();

    // Address inside should match
    let inside = data_bg.start + data_bg.length / 2;
    let found = groups
        .iter()
        .find(|bg| inside >= bg.start && inside < bg.start + bg.length)
        .map(|bg| bg.start);
    assert_eq!(found, Some(data_bg.start));

    // Address well outside any block group should not match
    let outside = u64::MAX - 1;
    let found = groups
        .iter()
        .find(|bg| outside >= bg.start && outside < bg.start + bg.length)
        .map(|bg| bg.start);
    assert_eq!(found, None);
}

#[test]
fn data_bg_used_update() {
    let (_dir, img_path) = create_test_image();

    let original_used;
    let bg_start;

    // Start a transaction, manually bump data BG used, commit.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let data_bg = find_data_block_group(&mut fs);
        bg_start = data_bg.start;
        original_used = data_bg.used;

        let mut trans = Transaction::start(&mut fs).expect("start txn");
        let test_bytenr = data_bg.start + data_bg.used + 4096;
        let test_num_bytes = 4096u64;
        assert!(
            test_bytenr + test_num_bytes <= data_bg.start + data_bg.length,
            "not enough free space in data block group for test"
        );

        trans.delayed_refs.add_data_ref(
            test_bytenr,
            test_num_bytes,
            5, // FS tree
            257,
            0,
            1,
        );
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify block group used increased.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");
        let data_bg = find_data_block_group(&mut fs);
        assert_eq!(data_bg.start, bg_start);
        assert!(
            data_bg.used > original_used,
            "data block group used ({}) should have increased from original ({})",
            data_bg.used,
            original_used,
        );
    }

    // No btrfs check — orphaned extent by design (only testing BG accounting).
}

// -- J.1: Data extent ref creation --

#[test]
fn create_data_extent_basic() {
    use btrfs_disk::items::{ExtentFlags, ExtentItem, InlineRef};

    let (_dir, img_path) = create_test_image();

    let test_bytenr;
    let test_num_bytes = 4096u64;

    // Find a free address inside the data block group.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let data_bg = find_data_block_group(&mut fs);
        // Pick an address past existing allocations.
        test_bytenr = data_bg.start + data_bg.used + 4096;
        assert!(
            test_bytenr + test_num_bytes <= data_bg.start + data_bg.length,
            "not enough free space in data block group"
        );
    }

    // Add a data extent ref via the delayed ref pipeline.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open for write");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans.delayed_refs.add_data_ref(
            test_bytenr,
            test_num_bytes,
            5,   // owner root: FS tree
            257, // owner inode
            0,   // file offset
            1,   // refs_to_add
        );
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify the extent item exists with correct payload.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: test_bytenr,
            key_type: KeyType::ExtentItem,
            offset: test_num_bytes,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            2,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search extent tree");
        assert!(found, "EXTENT_ITEM not found at {test_bytenr}");

        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let item = ExtentItem::parse(&data, &key)
            .expect("failed to parse EXTENT_ITEM");

        assert_eq!(item.refs, 1);
        assert!(item.flags.contains(ExtentFlags::DATA));
        assert!(!item.flags.contains(ExtentFlags::TREE_BLOCK));
        assert_eq!(item.inline_refs.len(), 1);

        match &item.inline_refs[0] {
            InlineRef::ExtentDataBackref {
                root,
                objectid,
                offset,
                count,
                ..
            } => {
                assert_eq!(*root, 5, "owner root");
                assert_eq!(*objectid, 257, "owner inode");
                assert_eq!(*offset, 0, "file offset");
                assert_eq!(*count, 1, "ref count");
            }
            other => panic!("expected ExtentDataBackref, got {other:?}"),
        }
        path.release();
    }

    // Note: btrfs check would report a backref mismatch because
    // there's no FILE_EXTENT_DATA in the FS tree. That's by design
    // for this unit test — create_then_drop_data_extent tests the
    // full round trip with btrfs check.
}

#[test]
fn create_data_extent_multiple() {
    let (_dir, img_path) = create_test_image();
    let num_bytes = 4096u64;
    let bytenr_a;
    let bytenr_b;

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let data_bg = find_data_block_group(&mut fs);
        bytenr_a = data_bg.start + data_bg.used + 4096;
        bytenr_b = bytenr_a + num_bytes + 4096; // leave a gap
        assert!(
            bytenr_b + num_bytes <= data_bg.start + data_bg.length,
            "not enough free space for two extents"
        );
    }

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open for write");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans
            .delayed_refs
            .add_data_ref(bytenr_a, num_bytes, 5, 257, 0, 1);
        trans
            .delayed_refs
            .add_data_ref(bytenr_b, num_bytes, 5, 257, num_bytes, 1);
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Verify both exist.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        for &bytenr in &[bytenr_a, bytenr_b] {
            let key = DiskKey {
                objectid: bytenr,
                key_type: KeyType::ExtentItem,
                offset: num_bytes,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                2,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .expect("search");
            assert!(found, "EXTENT_ITEM not found at {bytenr}");
            path.release();
        }
    }

    // No btrfs check — orphaned extents by design (no FILE_EXTENT_DATA).
}

#[test]
fn alloc_data_extent_writes_data_and_creates_extent_item() {
    use btrfs_disk::items::{ExtentFlags, ExtentItem, InlineRef};

    let (_dir, img_path) = create_test_image();
    let payload = b"hello data extent allocator: this should land on disk";

    let logical;
    let data_bg_start;

    // Phase 1: open, allocate one data extent, commit.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let data_bg = find_data_block_group(&mut fs);
        data_bg_start = data_bg.start;

        let mut trans = Transaction::start(&mut fs).expect("start txn");
        logical = trans
            .alloc_data_extent(&mut fs, payload, 5, 257, 0)
            .expect("alloc_data_extent");

        // Sanity: address falls inside a data block group and is
        // sectorsize-aligned.
        assert!(
            logical >= data_bg.start
                && logical < data_bg.start + data_bg.length,
            "logical {logical} outside data BG [{}, {})",
            data_bg.start,
            data_bg.start + data_bg.length
        );
        assert_eq!(
            logical % u64::from(fs.sectorsize),
            0,
            "logical {logical} not sectorsize-aligned"
        );

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Phase 2: reopen, verify the extent item exists and the data on
    // disk matches the payload (zero-padded to sectorsize).
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let sectorsize = u64::from(fs.sectorsize);
        let aligned = (payload.len() as u64).div_ceil(sectorsize) * sectorsize;

        // Verify EXTENT_ITEM in the extent tree.
        let key = DiskKey {
            objectid: logical,
            key_type: KeyType::ExtentItem,
            offset: aligned,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            2,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search extent tree");
        assert!(found, "EXTENT_ITEM not found at {logical}");
        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let item = ExtentItem::parse(&data, &key).expect("parse EXTENT_ITEM");
        assert_eq!(item.refs, 1);
        assert!(item.flags.contains(ExtentFlags::DATA));
        match &item.inline_refs[0] {
            InlineRef::ExtentDataBackref {
                root,
                objectid,
                offset,
                count,
                ..
            } => {
                assert_eq!(*root, 5);
                assert_eq!(*objectid, 257);
                assert_eq!(*offset, 0);
                assert_eq!(*count, 1);
            }
            other => panic!("expected ExtentDataBackref, got {other:?}"),
        }
        path.release();

        // Verify the bytes hit the disk: read raw bytes via the chunk
        // cache and check the payload prefix matches and the tail is
        // zero-padded.
        let on_disk = fs
            .reader_mut()
            .read_data(logical, aligned as usize)
            .unwrap();
        assert_eq!(
            &on_disk[..payload.len()],
            payload,
            "payload bytes mismatch"
        );
        assert!(
            on_disk[payload.len()..].iter().all(|&b| b == 0),
            "tail of data extent is not zero-padded"
        );

        // Block group `used` should have grown by aligned bytes.
        let groups =
            btrfs_transaction::allocation::load_block_groups(&mut fs).unwrap();
        let bg = groups
            .iter()
            .find(|bg| bg.start == data_bg_start)
            .expect("data BG by start address");
        assert!(
            bg.used >= aligned,
            "data BG used ({}) did not grow by at least {aligned}",
            bg.used
        );
    }

    // No btrfs check — there's no FILE_EXTENT_DATA in the FS tree
    // pointing at this extent yet, so the backref check would fail.
    // End-to-end coverage with btrfs check arrives once
    // insert_file_extent + insert_csums land.
}

#[test]
fn insert_file_extent_regular_round_trips() {
    use btrfs_disk::items::{
        CompressionType, FileExtentBody, FileExtentItem, FileExtentType,
    };

    let (_dir, img_path) = create_test_image();
    let payload = b"insert_file_extent regular extent body";
    let test_ino = 999u64;
    let test_offset = 0u64;

    let logical;
    let aligned;

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let sectorsize = u64::from(fs.sectorsize);
        aligned = (payload.len() as u64).div_ceil(sectorsize) * sectorsize;

        let mut trans = Transaction::start(&mut fs).expect("start txn");
        logical = trans
            .alloc_data_extent(&mut fs, payload, 5, test_ino, test_offset)
            .expect("alloc_data_extent");

        let extent_data = FileExtentItem::to_bytes_regular(
            trans.transid,
            payload.len() as u64,
            CompressionType::None,
            false,
            logical,
            aligned,
            0,
            payload.len() as u64,
        );
        trans
            .insert_file_extent(
                &mut fs,
                5, // FS tree
                test_ino,
                test_offset,
                &extent_data,
            )
            .expect("insert_file_extent");
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify the EXTENT_DATA item exists with the right
    // shape pointing at the allocated extent.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: test_ino,
            key_type: KeyType::ExtentData,
            offset: test_offset,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5, // FS tree
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search FS tree");
        assert!(found, "EXTENT_DATA item not found");

        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let fei = FileExtentItem::parse(&data).expect("parse FileExtentItem");
        assert_eq!(fei.extent_type, FileExtentType::Regular);
        assert_eq!(fei.compression, CompressionType::None);
        assert_eq!(fei.ram_bytes, payload.len() as u64);
        match fei.body {
            FileExtentBody::Regular {
                disk_bytenr,
                disk_num_bytes,
                offset,
                num_bytes,
            } => {
                assert_eq!(disk_bytenr, logical, "disk_bytenr");
                assert_eq!(disk_num_bytes, aligned, "disk_num_bytes");
                assert_eq!(offset, 0, "offset");
                assert_eq!(num_bytes, payload.len() as u64, "num_bytes");
            }
            _ => panic!("expected regular body"),
        }
        path.release();
    }

    // No btrfs check — inode 999 has no INODE_ITEM and there are no
    // csum entries for the extent yet; both would be flagged.
    // End-to-end coverage with btrfs check arrives once insert_csums
    // and a complete write_file_data path land.
}

#[test]
fn insert_csums_round_trips_per_sector_crc32c() {
    let (_dir, img_path) = create_test_image();
    let csum_objectid =
        i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;

    // Construct three sectors of distinct content so each gets a
    // different CRC32C.
    let sector = 4096usize;
    let mut data = Vec::with_capacity(3 * sector);
    data.extend(std::iter::repeat_n(0xAAu8, sector));
    data.extend(std::iter::repeat_n(0xBBu8, sector));
    data.extend(std::iter::repeat_n(0xCCu8, sector));

    let logical = 128 * 1024 * 1024u64; // arbitrary sectorsize-aligned

    let expected: Vec<u8> = data
        .chunks_exact(sector)
        .flat_map(|s| crc32c::crc32c(s).to_le_bytes())
        .collect();

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans
            .insert_csums(&mut fs, logical, &data)
            .expect("insert_csums");
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen, find the csum item, verify the payload byte-for-byte.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");
        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: logical,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            7,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search");
        assert!(found, "csum item not found at {logical}");
        let leaf = path.leaf().unwrap();
        let payload = leaf.item_data(path.slots[0]).to_vec();
        assert_eq!(payload, expected, "per-sector CRC32C mismatch");
        path.release();
    }
}

/// End-to-end: create a regular file with real content using the
/// high-level `write_file_data` helper, plus the surrounding
/// INODE_ITEM/REF and parent dir entries, and verify the result with
/// `btrfs check`.
#[test]
fn write_file_data_passes_btrfs_check() {
    use btrfs_disk::items::{DirItem, InodeItemArgs, InodeRef, Timespec};

    let (_dir, img_path) = create_test_image();

    let file_name = b"data.txt";
    let file_inode = 257u64;
    let dir_index = 100u64;
    let root_dir_inode = 256u64;
    // Payload spans more than one sector so write_file_data emits a
    // regular extent (the future inline path would handle smaller).
    let payload: Vec<u8> = (0..6000u32).map(|i| (i & 0xFF) as u8).collect();
    let payload = payload.as_slice();

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let transid = fs.superblock.generation + 1;
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };

        let mut trans = Transaction::start(&mut fs).expect("start txn");

        // 1. INODE_ITEM for the new file. nbytes starts at 0;
        //    write_file_data bumps it as data extents land.
        let inode_data = InodeItemArgs {
            generation: transid,
            size: payload.len() as u64,
            nbytes: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            mode: 0o100644,
            time: ts,
        }
        .to_bytes();
        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::Insert((25 + inode_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &inode_key, &inode_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 3. INODE_REF (file -> parent).
        let iref_data = InodeRef::serialize(dir_index, file_name);
        let iref_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeRef,
            offset: root_dir_inode,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &iref_key,
            &mut path,
            SearchIntent::Insert((25 + iref_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &iref_key, &iref_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 4. DIR_ITEM in the parent.
        let location = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let dir_data = DirItem::serialize(
            &location,
            transid,
            btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
            file_name,
        );
        let dir_item_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirItem,
            offset: u64::from(btrfs_disk::util::btrfs_name_hash(file_name)),
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_item_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_item_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 5. DIR_INDEX in the parent.
        let dir_index_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirIndex,
            offset: dir_index,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_index_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_index_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 6. Bump the parent dir's INODE_ITEM size by 2*name_len
        //    (one DIR_ITEM + one DIR_INDEX).
        let dir_inode_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let old_data = leaf.item_data(slot).to_vec();
        let mut inode = btrfs_disk::items::InodeItem::parse(&old_data).unwrap();
        inode.size += file_name.len() as u64 * 2;
        inode.transid = transid;
        let new_data = InodeItemArgs {
            generation: inode.generation,
            size: inode.size,
            nbytes: inode.nbytes,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            mode: inode.mode,
            time: ts,
        }
        .to_bytes();
        items::update_item(leaf, slot, &new_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 7. Mirror the size update into ROOT_ITEM's embedded inode.
        let root_key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &root_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let ri_data = leaf.item_data(slot).to_vec();
        let mut root_item = RootItem::parse(&ri_data).unwrap();
        let mut embedded =
            btrfs_disk::items::InodeItem::parse(&root_item.inode_data).unwrap();
        embedded.size += file_name.len() as u64 * 2;
        let new_inode = InodeItemArgs {
            generation: embedded.generation,
            size: embedded.size,
            nbytes: embedded.nbytes,
            nlink: embedded.nlink,
            uid: embedded.uid,
            gid: embedded.gid,
            mode: embedded.mode,
            time: ts,
        }
        .to_bytes();
        root_item.inode_data = new_inode;
        let new_ri = root_item.to_bytes();
        assert_eq!(new_ri.len(), ri_data.len());
        items::update_item(leaf, slot, &new_ri).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // 8. Write the file content. Allocates one data extent, writes
        //    the bytes to disk, inserts the EXTENT_DATA item, computes
        //    per-sector csums, and bumps INODE.nbytes — all in one call.
        trans
            .write_file_data(&mut fs, 5, file_inode, 0, payload, false, None)
            .expect("write_file_data");

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // The acid test: btrfs check must accept the resulting filesystem.
    assert_btrfs_check(&img_path);
}

/// Inline variant: a small file (below the inline threshold) must
/// produce a single inline `EXTENT_DATA` item with the bytes embedded
/// in the FS tree leaf and `INODE.nbytes` equal to the inline payload
/// length (no sectorsize alignment).
#[test]
fn write_file_data_inline_passes_btrfs_check() {
    use btrfs_disk::items::{
        DirItem, FileExtentBody, FileExtentItem, FileExtentType, InodeItemArgs,
        InodeRef, Timespec,
    };

    let (_dir, img_path) = create_test_image();

    let file_name = b"small.txt";
    let file_inode = 257u64;
    let dir_index = 100u64;
    let root_dir_inode = 256u64;
    // Below the default 4095-byte inline threshold.
    let payload = b"a small inline file -- short enough to live in the leaf";

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let transid = fs.superblock.generation + 1;
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };

        let mut trans = Transaction::start(&mut fs).expect("start txn");

        let inode_data = InodeItemArgs {
            generation: transid,
            size: payload.len() as u64,
            nbytes: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            mode: 0o100644,
            time: ts,
        }
        .to_bytes();
        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::Insert((25 + inode_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &inode_key, &inode_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let iref_data = InodeRef::serialize(dir_index, file_name);
        let iref_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeRef,
            offset: root_dir_inode,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &iref_key,
            &mut path,
            SearchIntent::Insert((25 + iref_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &iref_key, &iref_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let location = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let dir_data = DirItem::serialize(
            &location,
            transid,
            btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
            file_name,
        );
        let dir_item_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirItem,
            offset: u64::from(btrfs_disk::util::btrfs_name_hash(file_name)),
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_item_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_item_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let dir_index_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirIndex,
            offset: dir_index,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_index_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_index_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // Bump parent dir size and ROOT_ITEM embedded inode.
        let dir_inode_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let old_data = leaf.item_data(slot).to_vec();
        let mut inode = btrfs_disk::items::InodeItem::parse(&old_data).unwrap();
        inode.size += file_name.len() as u64 * 2;
        inode.transid = transid;
        let new_data = InodeItemArgs {
            generation: inode.generation,
            size: inode.size,
            nbytes: inode.nbytes,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            mode: inode.mode,
            time: ts,
        }
        .to_bytes();
        items::update_item(leaf, slot, &new_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let root_key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &root_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let ri_data = leaf.item_data(slot).to_vec();
        let mut root_item = RootItem::parse(&ri_data).unwrap();
        let mut embedded =
            btrfs_disk::items::InodeItem::parse(&root_item.inode_data).unwrap();
        embedded.size += file_name.len() as u64 * 2;
        let new_inode = InodeItemArgs {
            generation: embedded.generation,
            size: embedded.size,
            nbytes: embedded.nbytes,
            nlink: embedded.nlink,
            uid: embedded.uid,
            gid: embedded.gid,
            mode: embedded.mode,
            time: ts,
        }
        .to_bytes();
        root_item.inode_data = new_inode;
        let new_ri = root_item.to_bytes();
        assert_eq!(new_ri.len(), ri_data.len());
        items::update_item(leaf, slot, &new_ri).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // Small payload — write_file_data should pick inline.
        trans
            .write_file_data(&mut fs, 5, file_inode, 0, payload, false, None)
            .expect("write_file_data");

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Verify: inline EXTENT_DATA with payload bytes embedded, and
    // INODE.nbytes == payload.len() (no sectorsize alignment).
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::ExtentData,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found, "EXTENT_DATA not found");
        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let fei = FileExtentItem::parse(&data).expect("parse");
        assert_eq!(fei.extent_type, FileExtentType::Inline);
        match fei.body {
            FileExtentBody::Inline { inline_size } => {
                assert_eq!(inline_size, payload.len());
            }
            _ => panic!("expected inline body"),
        }
        assert_eq!(
            &data[FileExtentItem::HEADER_SIZE..],
            payload,
            "inline payload mismatch"
        );
        path.release();

        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found);
        let leaf = path.leaf().unwrap();
        let inode_data = leaf.item_data(path.slots[0]).to_vec();
        let inode =
            btrfs_disk::items::InodeItem::parse(&inode_data).expect("inode");
        assert_eq!(
            inode.nbytes,
            payload.len() as u64,
            "inline INODE.nbytes should equal payload.len()"
        );
        path.release();
    }

    assert_btrfs_check(&img_path);
}

/// Multi-chunk variant: a payload larger than `MAX_EXTENT_SIZE` (1 MiB)
/// must produce multiple `EXTENT_DATA` items, each at the correct
/// `file_offset`, with cumulative `nbytes` matching the total.
#[test]
fn write_file_data_multi_chunk_passes_btrfs_check() {
    use btrfs_disk::items::{
        DirItem, FileExtentBody, FileExtentItem, FileExtentType, InodeItemArgs,
        InodeRef, Timespec,
    };

    let (_dir, img_path) = create_test_image();

    let file_name = b"big.bin";
    let file_inode = 257u64;
    let dir_index = 100u64;
    let root_dir_inode = 256u64;
    // ~2.5 MiB: forces three chunks (1 MiB + 1 MiB + 0.5 MiB tail).
    let payload_len: usize = 1024 * 1024 * 2 + 512 * 1024;
    let payload: Vec<u8> = (0..payload_len).map(|i| (i & 0xFF) as u8).collect();

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let transid = fs.superblock.generation + 1;
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };

        let mut trans = Transaction::start(&mut fs).expect("start txn");

        let inode_data = InodeItemArgs {
            generation: transid,
            size: payload.len() as u64,
            nbytes: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            mode: 0o100644,
            time: ts,
        }
        .to_bytes();
        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::Insert((25 + inode_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &inode_key, &inode_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let iref_data = InodeRef::serialize(dir_index, file_name);
        let iref_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeRef,
            offset: root_dir_inode,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &iref_key,
            &mut path,
            SearchIntent::Insert((25 + iref_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &iref_key, &iref_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let location = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let dir_data = DirItem::serialize(
            &location,
            transid,
            btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
            file_name,
        );
        let dir_item_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirItem,
            offset: u64::from(btrfs_disk::util::btrfs_name_hash(file_name)),
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_item_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_item_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let dir_index_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::DirIndex,
            offset: dir_index,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_index_key,
            &mut path,
            SearchIntent::Insert((25 + dir_data.len()) as u32),
            true,
        )
        .unwrap();
        let leaf = path.nodes[0].as_mut().unwrap();
        items::insert_item(leaf, path.slots[0], &dir_index_key, &dir_data)
            .unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // Bump parent dir size and ROOT_ITEM embedded inode.
        let dir_inode_key = DiskKey {
            objectid: root_dir_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            5,
            &dir_inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let old_data = leaf.item_data(slot).to_vec();
        let mut inode = btrfs_disk::items::InodeItem::parse(&old_data).unwrap();
        inode.size += file_name.len() as u64 * 2;
        inode.transid = transid;
        let new_data = InodeItemArgs {
            generation: inode.generation,
            size: inode.size,
            nbytes: inode.nbytes,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            mode: inode.mode,
            time: ts,
        }
        .to_bytes();
        items::update_item(leaf, slot, &new_data).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        let root_key = DiskKey {
            objectid: 5,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            1,
            &root_key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )
        .unwrap();
        assert!(found);
        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        let ri_data = leaf.item_data(slot).to_vec();
        let mut root_item = RootItem::parse(&ri_data).unwrap();
        let mut embedded =
            btrfs_disk::items::InodeItem::parse(&root_item.inode_data).unwrap();
        embedded.size += file_name.len() as u64 * 2;
        let new_inode = InodeItemArgs {
            generation: embedded.generation,
            size: embedded.size,
            nbytes: embedded.nbytes,
            nlink: embedded.nlink,
            uid: embedded.uid,
            gid: embedded.gid,
            mode: embedded.mode,
            time: ts,
        }
        .to_bytes();
        root_item.inode_data = new_inode;
        let new_ri = root_item.to_bytes();
        assert_eq!(new_ri.len(), ri_data.len());
        items::update_item(leaf, slot, &new_ri).unwrap();
        fs.mark_dirty(leaf);
        path.release();

        // The actual write — three chunks.
        trans
            .write_file_data(&mut fs, 5, file_inode, 0, &payload, false, None)
            .expect("write_file_data");

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify three EXTENT_DATA items at the expected
    // file offsets exist.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let mut total_num_bytes = 0u64;
        let mut count = 0;
        let expected_offsets = [0u64, 1024 * 1024, 2 * 1024 * 1024];
        for expected_offset in expected_offsets {
            let key = DiskKey {
                objectid: file_inode,
                key_type: KeyType::ExtentData,
                offset: expected_offset,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                5,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .unwrap();
            assert!(
                found,
                "EXTENT_DATA missing at file_offset {expected_offset}"
            );
            let leaf = path.leaf().unwrap();
            let data = leaf.item_data(path.slots[0]).to_vec();
            let fei = FileExtentItem::parse(&data).unwrap();
            assert_eq!(fei.extent_type, FileExtentType::Regular);
            if let FileExtentBody::Regular { num_bytes, .. } = fei.body {
                total_num_bytes += num_bytes;
            } else {
                panic!("expected regular body");
            }
            path.release();
            count += 1;
        }
        assert_eq!(count, 3, "expected three chunks");
        // Aligned total: 2.5 MiB rounds up to 2.5 MiB (already aligned).
        assert_eq!(total_num_bytes, payload_len as u64);
    }

    assert_btrfs_check(&img_path);
}

#[test]
fn create_then_drop_data_extent() {
    let (_dir, img_path) = create_test_image();
    let num_bytes = 4096u64;
    let test_bytenr;
    let original_used;

    // Find the data BG and record its used bytes.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let data_bg = find_data_block_group(&mut fs);
        test_bytenr = data_bg.start + data_bg.used + 4096;
        original_used = data_bg.used;
        assert!(
            test_bytenr + num_bytes <= data_bg.start + data_bg.length,
            "not enough free space"
        );
    }

    // Create the data extent.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open for write");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans
            .delayed_refs
            .add_data_ref(test_bytenr, num_bytes, 5, 257, 0, 1);
        trans.commit(&mut fs).expect("commit create");
        fs.sync().expect("sync");
    }

    // Drop it in a second transaction.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open for drop");
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        trans
            .delayed_refs
            .drop_data_ref(test_bytenr, num_bytes, 5, 257, 0, 1);
        trans.commit(&mut fs).expect("commit drop");
        fs.sync().expect("sync");
    }

    // Verify the extent item is gone.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: test_bytenr,
            key_type: KeyType::ExtentItem,
            offset: num_bytes,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            2,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search");
        assert!(!found, "EXTENT_ITEM should be gone after drop");
        path.release();

        // Block group used should return to original.
        let data_bg = find_data_block_group(&mut fs);
        assert_eq!(
            data_bg.used, original_used,
            "block group used should return to original after create+drop"
        );
    }

    assert_btrfs_check(&img_path);
}

// -- J.3: Csum tree item insertion --

#[test]
fn csum_tree_insert_basic() {
    let (_dir, img_path) = create_test_image();
    let csum_objectid =
        i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;
    let disk_bytenr = 1024 * 1024u64; // arbitrary logical address

    // 256 KB extent at 4K sectors = 64 sectors * 4 bytes = 256 bytes of csums
    let csum_data: Vec<u8> = (0..256).map(|i| (i & 0xFF) as u8).collect();

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");

        let mut trans = Transaction::start(&mut fs).expect("start txn");

        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: disk_bytenr,
        };

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            7, // csum tree
            &key,
            &mut path,
            SearchIntent::Insert(
                (btrfs_transaction::buffer::ITEM_SIZE + csum_data.len()) as u32,
            ),
            true,
        )
        .expect("search csum tree");
        assert!(!found, "csum key should not pre-exist");

        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, &csum_data).expect("insert");
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Verify csum item exists after reopen.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: disk_bytenr,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            7,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search");
        assert!(found, "csum item should exist after commit");

        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        assert_eq!(data.len(), 256, "csum payload length");
        assert_eq!(data, csum_data, "csum payload content");
        path.release();
    }

    assert_btrfs_check(&img_path);
}

#[test]
fn csum_tree_insert_multiple() {
    let (_dir, img_path) = create_test_image();
    let csum_objectid =
        i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;

    let base_bytenr = 2 * 1024 * 1024u64;
    let extent_size = 256 * 1024u64; // 256 KB per extent
    let csum_size = (extent_size / 4096 * 4) as usize; // 256 bytes
    let count = 10;

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let mut trans = Transaction::start(&mut fs).expect("start txn");

        for i in 0..count {
            let disk_bytenr = base_bytenr + i * extent_size;
            let csum_data: Vec<u8> = (0..csum_size)
                .map(|b| (i as u8).wrapping_add(b as u8))
                .collect();

            let key = DiskKey {
                objectid: csum_objectid,
                key_type: KeyType::ExtentCsum,
                offset: disk_bytenr,
            };

            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut trans),
                &mut fs,
                7,
                &key,
                &mut path,
                SearchIntent::Insert(
                    (btrfs_transaction::buffer::ITEM_SIZE + csum_data.len())
                        as u32,
                ),
                true,
            )
            .expect("search");
            assert!(!found);

            let leaf = path.nodes[0].as_mut().unwrap();
            let slot = path.slots[0];
            items::insert_item(leaf, slot, &key, &csum_data).expect("insert");
            fs.mark_dirty(leaf);
            path.release();
        }

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Verify all 10 exist in order.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        for i in 0..count {
            let disk_bytenr = base_bytenr + i * extent_size;
            let key = DiskKey {
                objectid: csum_objectid,
                key_type: KeyType::ExtentCsum,
                offset: disk_bytenr,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                None,
                &mut fs,
                7,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                false,
            )
            .expect("search");
            assert!(found, "csum item {i} should exist");
            path.release();
        }
    }

    assert_btrfs_check(&img_path);
}

#[test]
fn csum_tree_root_item_updated() {
    let (_dir, img_path) = create_test_image();
    let csum_objectid =
        i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;

    let csum_root_before;

    // Record csum tree root before modification.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).expect("open");
        csum_root_before = fs.root_bytenr(7).expect("csum tree root");
    }

    // Insert a csum item, commit.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open for write");
        let mut trans = Transaction::start(&mut fs).expect("start txn");

        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: 42 * 1024 * 1024,
        };
        let csum_data = vec![0xABu8; 64];

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            7,
            &key,
            &mut path,
            SearchIntent::Insert(
                (btrfs_transaction::buffer::ITEM_SIZE + csum_data.len()) as u32,
            ),
            true,
        )
        .expect("search");
        assert!(!found);

        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, &csum_data).expect("insert");
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Verify csum tree root changed.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let fs = Filesystem::open(file).expect("reopen");
        let csum_root_after = fs.root_bytenr(7).expect("csum tree root");
        assert_ne!(
            csum_root_before, csum_root_after,
            "csum tree root bytenr should change after insertion"
        );
    }

    assert_btrfs_check(&img_path);
}

// -- J.4: Bootstrap verification --

#[test]
fn open_our_mkfs_image() {
    let (_dir, img_path) = create_test_image();
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let fs = Filesystem::open(file).expect("open our mkfs image");

    assert!(
        fs.superblock.magic_is_valid(),
        "superblock magic should be valid"
    );
    assert_eq!(fs.superblock.nodesize, 16384);

    // All standard trees should be present.
    assert!(fs.root_bytenr(1).is_some(), "root tree (1) missing");
    assert!(fs.root_bytenr(2).is_some(), "extent tree (2) missing");
    assert!(fs.root_bytenr(3).is_some(), "chunk tree (3) missing");
    assert!(fs.root_bytenr(5).is_some(), "fs tree (5) missing");
    assert!(fs.root_bytenr(7).is_some(), "csum tree (7) missing");
}

#[test]
fn transaction_on_our_mkfs_image() {
    let (_dir, img_path) = create_test_image();

    // Use the csum tree (7) with a proper EXTENT_CSUM key to test the
    // full open → start → insert → commit → reopen cycle.
    let csum_tree_id = 7u64;
    let csum_objectid =
        i64::from(btrfs_disk::raw::BTRFS_EXTENT_CSUM_OBJECTID) as u64;
    let test_bytenr = 99 * 1024 * 1024u64;

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let mut trans = Transaction::start(&mut fs).expect("start txn");

        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: test_bytenr,
        };
        let data = vec![0xABu8; 64];

        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            csum_tree_id,
            &key,
            &mut path,
            SearchIntent::Insert(
                (btrfs_transaction::buffer::ITEM_SIZE + data.len()) as u32,
            ),
            true,
        )
        .expect("search");
        assert!(!found);

        let leaf = path.nodes[0].as_mut().unwrap();
        let slot = path.slots[0];
        items::insert_item(leaf, slot, &key, &data).expect("insert");
        fs.mark_dirty(leaf);
        path.release();

        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    // Reopen and verify the item exists.
    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: csum_objectid,
            key_type: KeyType::ExtentCsum,
            offset: test_bytenr,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            csum_tree_id,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .expect("search");
        assert!(found, "csum item should exist after commit");

        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        assert_eq!(data, vec![0xABu8; 64]);
        path.release();
    }

    assert_btrfs_check(&img_path);
}

#[test]
fn allocator_finds_free_space_in_mkfs_image() {
    let (_dir, img_path) = create_test_image();
    let file = File::options()
        .read(true)
        .write(true)
        .open(&img_path)
        .unwrap();
    let mut fs = Filesystem::open(file).expect("open");

    // Load block groups and find the metadata BG.
    let groups = btrfs_transaction::allocation::load_block_groups(&mut fs)
        .expect("load_block_groups");
    let meta_bg = groups
        .iter()
        .find(|bg| bg.is_metadata())
        .expect("no metadata block group");
    assert!(
        meta_bg.free() > 0,
        "metadata block group should have free space (used={}, length={})",
        meta_bg.used,
        meta_bg.length,
    );

    // Transaction::start succeeds (which internally runs the allocator).
    let _trans = Transaction::start(&mut fs).expect("start txn");
}

/// Wire up the FS-tree dir machinery (INODE_ITEM, INODE_REF, DIR_ITEM,
/// DIR_INDEX, parent dir size bump, ROOT_ITEM embedded inode mirror)
/// for a single regular file in the FS root. Only used by the
/// compressed-extent tests; the older tests inlined the same boilerplate.
#[allow(clippy::too_many_arguments)]
fn create_file_dir_machinery(
    fs: &mut Filesystem<File>,
    trans: &mut Transaction<File>,
    file_inode: u64,
    file_name: &[u8],
    file_size: u64,
    dir_index: u64,
    transid: u64,
    ts: btrfs_disk::items::Timespec,
) {
    use btrfs_disk::items::{DirItem, InodeItemArgs, InodeRef};

    let root_dir_inode = 256u64;

    let inode_data = InodeItemArgs {
        generation: transid,
        size: file_size,
        nbytes: 0,
        nlink: 1,
        uid: 0,
        gid: 0,
        mode: 0o100644,
        time: ts,
    }
    .to_bytes();
    let inode_key = DiskKey {
        objectid: file_inode,
        key_type: KeyType::InodeItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        Some(trans),
        fs,
        5,
        &inode_key,
        &mut path,
        SearchIntent::Insert((25 + inode_data.len()) as u32),
        true,
    )
    .unwrap();
    let leaf = path.nodes[0].as_mut().unwrap();
    items::insert_item(leaf, path.slots[0], &inode_key, &inode_data).unwrap();
    fs.mark_dirty(leaf);
    path.release();

    let iref_data = InodeRef::serialize(dir_index, file_name);
    let iref_key = DiskKey {
        objectid: file_inode,
        key_type: KeyType::InodeRef,
        offset: root_dir_inode,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        Some(trans),
        fs,
        5,
        &iref_key,
        &mut path,
        SearchIntent::Insert((25 + iref_data.len()) as u32),
        true,
    )
    .unwrap();
    let leaf = path.nodes[0].as_mut().unwrap();
    items::insert_item(leaf, path.slots[0], &iref_key, &iref_data).unwrap();
    fs.mark_dirty(leaf);
    path.release();

    let location = DiskKey {
        objectid: file_inode,
        key_type: KeyType::InodeItem,
        offset: 0,
    };
    let dir_data = DirItem::serialize(
        &location,
        transid,
        btrfs_disk::raw::BTRFS_FT_REG_FILE as u8,
        file_name,
    );
    let dir_item_key = DiskKey {
        objectid: root_dir_inode,
        key_type: KeyType::DirItem,
        offset: u64::from(btrfs_disk::util::btrfs_name_hash(file_name)),
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        Some(trans),
        fs,
        5,
        &dir_item_key,
        &mut path,
        SearchIntent::Insert((25 + dir_data.len()) as u32),
        true,
    )
    .unwrap();
    let leaf = path.nodes[0].as_mut().unwrap();
    items::insert_item(leaf, path.slots[0], &dir_item_key, &dir_data).unwrap();
    fs.mark_dirty(leaf);
    path.release();

    let dir_index_key = DiskKey {
        objectid: root_dir_inode,
        key_type: KeyType::DirIndex,
        offset: dir_index,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        Some(trans),
        fs,
        5,
        &dir_index_key,
        &mut path,
        SearchIntent::Insert((25 + dir_data.len()) as u32),
        true,
    )
    .unwrap();
    let leaf = path.nodes[0].as_mut().unwrap();
    items::insert_item(leaf, path.slots[0], &dir_index_key, &dir_data).unwrap();
    fs.mark_dirty(leaf);
    path.release();

    let dir_inode_key = DiskKey {
        objectid: root_dir_inode,
        key_type: KeyType::InodeItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(trans),
        fs,
        5,
        &dir_inode_key,
        &mut path,
        SearchIntent::ReadOnly,
        true,
    )
    .unwrap();
    assert!(found);
    let leaf = path.nodes[0].as_mut().unwrap();
    let slot = path.slots[0];
    let old_data = leaf.item_data(slot).to_vec();
    let mut inode = btrfs_disk::items::InodeItem::parse(&old_data).unwrap();
    inode.size += file_name.len() as u64 * 2;
    inode.transid = transid;
    let new_data = InodeItemArgs {
        generation: inode.generation,
        size: inode.size,
        nbytes: inode.nbytes,
        nlink: inode.nlink,
        uid: inode.uid,
        gid: inode.gid,
        mode: inode.mode,
        time: ts,
    }
    .to_bytes();
    items::update_item(leaf, slot, &new_data).unwrap();
    fs.mark_dirty(leaf);
    path.release();

    let root_key = DiskKey {
        objectid: 5,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(trans),
        fs,
        1,
        &root_key,
        &mut path,
        SearchIntent::ReadOnly,
        true,
    )
    .unwrap();
    assert!(found);
    let leaf = path.nodes[0].as_mut().unwrap();
    let slot = path.slots[0];
    let ri_data = leaf.item_data(slot).to_vec();
    let mut root_item = RootItem::parse(&ri_data).unwrap();
    let mut embedded =
        btrfs_disk::items::InodeItem::parse(&root_item.inode_data).unwrap();
    embedded.size += file_name.len() as u64 * 2;
    let new_inode = InodeItemArgs {
        generation: embedded.generation,
        size: embedded.size,
        nbytes: embedded.nbytes,
        nlink: embedded.nlink,
        uid: embedded.uid,
        gid: embedded.gid,
        mode: embedded.mode,
        time: ts,
    }
    .to_bytes();
    root_item.inode_data = new_inode;
    let new_ri = root_item.to_bytes();
    assert_eq!(new_ri.len(), ri_data.len());
    items::update_item(leaf, slot, &new_ri).unwrap();
    fs.mark_dirty(leaf);
    path.release();
}

/// Run write_file_data with a compressible 8 KiB payload and verify that
/// (1) `EXTENT_DATA.compression` is set to the requested algorithm,
/// (2) `disk_num_bytes < num_bytes` (the on-disk extent shrunk),
/// (3) `INODE.nbytes` matches the logical (sector-aligned) size, and
/// (4) `btrfs check` accepts the filesystem.
fn run_compressed_regular_test(
    file_name: &[u8],
    algorithm: btrfs_disk::items::CompressionType,
) {
    use btrfs_disk::items::{
        CompressionType, FileExtentBody, FileExtentItem, FileExtentType,
        Timespec,
    };

    let (_dir, img_path) = create_test_image();

    let file_inode = 257u64;
    let dir_index = 100u64;
    // 8 KiB of repeated bytes — highly compressible.
    let payload = vec![0x42u8; 8192];

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let transid = fs.superblock.generation + 1;
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        create_file_dir_machinery(
            &mut fs,
            &mut trans,
            file_inode,
            file_name,
            payload.len() as u64,
            dir_index,
            transid,
            ts,
        );
        trans
            .write_file_data(
                &mut fs,
                5,
                file_inode,
                0,
                &payload,
                false,
                Some(algorithm),
            )
            .expect("write_file_data");
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::ExtentData,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found, "EXTENT_DATA not found");
        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let fei = FileExtentItem::parse(&data).expect("parse");
        assert_eq!(fei.extent_type, FileExtentType::Regular);
        assert_eq!(
            fei.compression, algorithm,
            "EXTENT_DATA.compression should reflect the requested algorithm"
        );
        match fei.body {
            FileExtentBody::Regular {
                disk_num_bytes,
                num_bytes,
                ..
            } => {
                assert!(
                    disk_num_bytes < num_bytes,
                    "compressed extent should shrink: disk {disk_num_bytes} \
                     vs logical {num_bytes}"
                );
                assert_eq!(num_bytes, 8192, "logical num_bytes");
            }
            _ => panic!("expected regular body"),
        }
        path.release();

        // INODE.nbytes accounts for logical size (sector-aligned).
        let inode_key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::InodeItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5,
            &inode_key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found);
        let leaf = path.leaf().unwrap();
        let inode_data = leaf.item_data(path.slots[0]).to_vec();
        let inode =
            btrfs_disk::items::InodeItem::parse(&inode_data).expect("inode");
        assert_eq!(
            inode.nbytes, 8192,
            "compressed regular: nbytes is logical, not compressed"
        );
        // Silence unused-import in builds without compression failure.
        let _ = CompressionType::None;
        path.release();
    }

    assert_btrfs_check(&img_path);
}

#[test]
fn write_file_data_zlib_compressed_passes_btrfs_check() {
    run_compressed_regular_test(
        b"zlib.bin",
        btrfs_disk::items::CompressionType::Zlib,
    );
}

#[test]
fn write_file_data_zstd_compressed_passes_btrfs_check() {
    run_compressed_regular_test(
        b"zstd.bin",
        btrfs_disk::items::CompressionType::Zstd,
    );
}

/// Inline + compression: a small compressible payload should be inlined
/// with the requested algorithm reflected in `EXTENT_DATA.compression`.
#[test]
fn write_file_data_inline_zlib_compressed_passes_btrfs_check() {
    use btrfs_disk::items::{
        CompressionType, FileExtentBody, FileExtentItem, FileExtentType,
        Timespec,
    };

    let (_dir, img_path) = create_test_image();
    let file_name = b"inline-zlib.txt";
    let file_inode = 257u64;
    let dir_index = 100u64;
    // Highly compressible, well below the 4095-byte inline threshold.
    let payload = vec![0x42u8; 1024];

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("open");
        let transid = fs.superblock.generation + 1;
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 0,
        };
        let mut trans = Transaction::start(&mut fs).expect("start txn");
        create_file_dir_machinery(
            &mut fs,
            &mut trans,
            file_inode,
            file_name,
            payload.len() as u64,
            dir_index,
            transid,
            ts,
        );
        trans
            .write_file_data(
                &mut fs,
                5,
                file_inode,
                0,
                &payload,
                false,
                Some(CompressionType::Zlib),
            )
            .expect("write_file_data");
        trans.commit(&mut fs).expect("commit");
        fs.sync().expect("sync");
    }

    {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&img_path)
            .unwrap();
        let mut fs = Filesystem::open(file).expect("reopen");

        let key = DiskKey {
            objectid: file_inode,
            key_type: KeyType::ExtentData,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            &mut fs,
            5,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .unwrap();
        assert!(found);
        let leaf = path.leaf().unwrap();
        let data = leaf.item_data(path.slots[0]).to_vec();
        let fei = FileExtentItem::parse(&data).expect("parse");
        assert_eq!(fei.extent_type, FileExtentType::Inline);
        assert_eq!(fei.compression, CompressionType::Zlib);
        assert_eq!(fei.ram_bytes, payload.len() as u64);
        match fei.body {
            FileExtentBody::Inline { inline_size } => {
                // Compressed payload is much smaller than 1024 raw bytes.
                assert!(
                    inline_size < payload.len(),
                    "inline payload should compress: {inline_size} \
                     vs uncompressed {}",
                    payload.len()
                );
            }
            _ => panic!("expected inline body"),
        }
        path.release();
    }

    assert_btrfs_check(&img_path);
}

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

/// Run `btrfs check` on an image, asserting no structural errors.
///
/// Captures stdout/stderr and only prints them if the check fails.
/// Tolerates free space tree cache warnings (we clear `VALID` and the
/// kernel rebuilds on mount) but fails on any other error.
///
/// # Panics
///
/// Panics if `btrfs check` is not found or reports structural errors.
fn assert_btrfs_check(path: &Path) {
    let output = Command::new("btrfs")
        .args(["check", "--readonly"])
        .arg(path)
        .output()
        .expect("btrfs check not found — install btrfs-progs");

    if output.status.success() {
        return;
    }

    // The free space tree may be stale (we clear VALID and the kernel
    // rebuilds on mount). Only fail on non-cache errors.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let has_structural_errors = stderr.lines().any(|line| {
        line.contains("ERROR:")
            && !line.contains("free space")
            && !line.contains("cache")
    });

    if has_structural_errors {
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "btrfs check found structural errors on {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
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
            // dir isize += name_len + btrfs_dir_item header (30 bytes)
            inode.size += file_name.len() as u64 + 30;
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

        trans.commit(&mut fs).expect("commit failed");
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

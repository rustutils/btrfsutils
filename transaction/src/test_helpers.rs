#![allow(dead_code)]
//! Shared test helpers for creating in-memory filesystem state.
//!
//! These helpers create real btrfs filesystem images via `mkfs.btrfs`,
//! open them as `Filesystem`, and start transactions. This enables unit tests
//! that exercise the full COW/split/balance pipeline with real on-disk
//! structures, without requiring elevated privileges.

use crate::{
    extent_buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE},
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
};
use btrfs_disk::tree::{DiskKey, KeyType};
use std::{fs::File, io, path::PathBuf, process::Command};

/// A test fixture that owns a temp directory and provides access to the
/// filesystem image within it.
pub struct TestFixture {
    _dir: tempfile::TempDir,
    pub path: PathBuf,
}

impl TestFixture {
    /// Create a new 128 MiB btrfs filesystem image via `mkfs.btrfs`.
    ///
    /// # Panics
    ///
    /// Panics if mkfs.btrfs is not available or fails.
    pub fn new() -> Self {
        let dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let img_path = dir.path().join("test.img");

        let file =
            File::create(&img_path).expect("failed to create image file");
        file.set_len(128 * 1024 * 1024)
            .expect("failed to set image size");
        drop(file);

        let status = Command::new("mkfs.btrfs")
            .args(["-f", "-q"])
            .arg(&img_path)
            .status()
            .expect("mkfs.btrfs not found — install btrfs-progs");
        assert!(status.success(), "mkfs.btrfs failed with {status}");

        Self {
            _dir: dir,
            path: img_path,
        }
    }

    /// Open the image for read-write access as `Filesystem`.
    pub fn open(&self) -> io::Result<Filesystem<File>> {
        let file = File::options().read(true).write(true).open(&self.path)?;
        Filesystem::open(file)
    }

    /// Run `btrfs check --readonly` and panic if structural errors are found.
    ///
    /// Tolerates free space tree cache warnings (we clear VALID).
    pub fn assert_check(&self) {
        let output = Command::new("btrfs")
            .args(["check", "--readonly"])
            .arg(&self.path)
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
                "btrfs check found structural errors:\n--- stderr ---\n{stderr}\n--- stdout ---\n{stdout}"
            );
        }
    }
}

/// Insert `count` items with `data_size`-byte payloads into `tree_id`,
/// using keys `(start_oid + i, TemporaryItem, 0)`.
///
/// Returns the number of items actually inserted.
pub fn insert_test_items<R: io::Read + io::Write + io::Seek>(
    trans: &mut Transaction<R>,
    fs_info: &mut Filesystem<R>,
    tree_id: u64,
    start_oid: u64,
    count: usize,
    data_size: usize,
) -> io::Result<usize> {
    let data = vec![0xAB; data_size];
    for i in 0..count {
        let key = DiskKey {
            objectid: start_oid + i as u64,
            key_type: KeyType::TemporaryItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        search::search_slot(
            Some(&mut *trans),
            fs_info,
            tree_id,
            &key,
            &mut path,
            SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
            true,
        )?;
        let leaf = path.nodes[0]
            .as_mut()
            .ok_or_else(|| io::Error::other("no leaf"))?;
        items::insert_item(leaf, path.slots[0], &key, &data)?;
        fs_info.mark_dirty(leaf);
        path.release();
    }
    Ok(count)
}

/// Validate that every leaf reachable from a tree root has correct item
/// offset ordering: item[0] data ends at `nodesize - HEADER_SIZE`, and
/// offsets are strictly descending.
pub fn validate_leaf_offsets<R: io::Read + io::Write + io::Seek>(
    fs_info: &mut Filesystem<R>,
    root_bytenr: u64,
) -> io::Result<()> {
    let eb = fs_info.read_block(root_bytenr)?;
    if eb.level() == 0 {
        validate_single_leaf(&eb)?;
    } else {
        for i in 0..eb.nritems() as usize {
            let child_bytenr = eb.key_ptr_blockptr(i);
            validate_leaf_offsets(fs_info, child_bytenr)?;
        }
    }
    Ok(())
}

/// Validate a single leaf's item offset invariants.
fn validate_single_leaf(eb: &ExtentBuffer) -> io::Result<()> {
    let nritems = eb.nritems() as usize;
    if nritems == 0 {
        return Ok(());
    }

    // Item 0's data must end at nodesize - HEADER_SIZE
    let first_end = eb.item_offset(0) + eb.item_size(0);
    let expected_end = eb.nodesize() - HEADER_SIZE as u32;
    if first_end != expected_end {
        return Err(io::Error::other(format!(
            "leaf at {}: item[0] data end={first_end} != expected={expected_end}",
            eb.logical()
        )));
    }

    // Offsets must be strictly descending (or equal for zero-size items)
    for i in 0..nritems - 1 {
        if eb.item_offset(i) < eb.item_offset(i + 1) {
            return Err(io::Error::other(format!(
                "leaf at {}: offset[{i}]={} < offset[{}]={}",
                eb.logical(),
                eb.item_offset(i),
                i + 1,
                eb.item_offset(i + 1)
            )));
        }
    }

    // Keys must be in ascending order
    for i in 0..nritems - 1 {
        let k1 = eb.item_key(i);
        let k2 = eb.item_key(i + 1);
        if crate::extent_buffer::key_cmp(&k1, &k2) != std::cmp::Ordering::Less {
            return Err(io::Error::other(format!(
                "leaf at {}: key[{i}]={:?} not < key[{}]={:?}",
                eb.logical(),
                k1,
                i + 1,
                k2
            )));
        }
    }

    Ok(())
}

#![allow(dead_code)]
//! Shared test helpers for creating in-memory filesystem state.
//!
//! These helpers create real btrfs filesystem images via `mkfs.btrfs`,
//! open them as `Filesystem`, and start transactions. This enables unit tests
//! that exercise the full COW/split/balance pipeline with real on-disk
//! structures, without requiring elevated privileges.

use crate::{
    buffer::{ExtentBuffer, HEADER_SIZE, ITEM_SIZE},
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
};
use btrfs_disk::tree::{DiskKey, KeyType};
use std::{
    collections::BTreeMap, fs::File, io, path::PathBuf, process::Command,
    sync::mpsc, thread, time::Duration,
};

/// A test fixture that owns a temp directory and provides access to the
/// filesystem image within it.
pub struct TestFixture {
    _dir: tempfile::TempDir,
    pub path: PathBuf,
}

impl Default for TestFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl TestFixture {
    /// Create a new 128 MiB btrfs filesystem image via `mkfs.btrfs`.
    ///
    /// # Panics
    ///
    /// Panics if mkfs.btrfs is not available or fails.
    #[must_use]
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
/// offset ordering: `item[0]` data ends at `nodesize - HEADER_SIZE`, and
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
        if crate::buffer::key_cmp(&k1, &k2) != std::cmp::Ordering::Less {
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

// ─── Property-test harness ───────────────────────────────────────────────
//
// Drives the transaction crate from a randomized stream of operations
// against a "playground" tree. The model (a `BTreeMap`) is the oracle:
// after every step, the playground tree must contain exactly the keys
// the model contains, with matching values.
//
// All operations are restricted to one tree id and one key type
// (`TemporaryItem`) so the harness cannot collide with real on-disk
// items, and so proptest's shrinker has a tiny state space to work in.

/// Tree id used as the playground for KV operations. We use the uuid
/// tree (id 9) because (a) it exists on a fresh fs, (b) it normally
/// holds only `TemporaryItem` records the harness can freely mix with,
/// and (c) it matches the failure mode of `rescue clear-uuid-tree`,
/// which is the bug class this harness is built to surface.
pub const PLAYGROUND_TREE: u64 = 9;

/// Compact key generated by the harness. The key type is fixed to
/// `TemporaryItem` so we never collide with real on-disk items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TestKey {
    pub objectid: u8,
    pub offset: u8,
}

impl TestKey {
    #[must_use]
    pub fn to_disk(self) -> DiskKey {
        DiskKey {
            objectid: u64::from(self.objectid),
            key_type: KeyType::TemporaryItem,
            offset: u64::from(self.offset),
        }
    }
}

/// One step in a randomly generated test sequence.
///
/// The variants are deliberately small in number: each one targets a
/// specific code path we want stress-tested, and adding more variants
/// dilutes proptest's ability to find minimal failing inputs. The
/// `DropOwnedBlock` + `AllocBlock` pair is sufficient to express bulk
/// drops without a dedicated variant.
#[derive(Debug, Clone)]
pub enum Op {
    /// Insert a key-value pair into the playground tree. If the key
    /// already exists, this is a no-op for both the model and the tree
    /// (mirroring `search_slot` returning "found").
    Insert { key: TestKey, value: Vec<u8> },
    /// Update an existing key. If the key does not exist, this is a
    /// no-op (so the harness doesn't need to gate on prior state).
    Update { key: TestKey, value: Vec<u8> },
    /// Delete a key from the playground tree. No-op if not present.
    Delete { key: TestKey },

    /// Allocate a metadata block owned by `PLAYGROUND_TREE`. The
    /// resulting bytenr is appended to the model's owned-blocks list.
    AllocBlock,
    /// `drop_ref` + `pin_block` one of the harness-allocated blocks,
    /// indexed modulo the list length. No-op if the list is empty.
    /// This is the operation that reproduces the clear-uuid-tree hang.
    DropOwnedBlock { idx: usize },
    /// Remove `PLAYGROUND_TREE` from `fs.roots`. Mirrors the second
    /// half of the clear-uuid-tree sequence. After this op, all KV
    /// operations against the playground tree become no-ops (the
    /// model still tracks them, but the tree is gone).
    RemovePlaygroundRoot,

    /// Commit the current transaction and start a fresh one.
    Commit,
    /// Abort the current transaction and start a fresh one. The model
    /// is rolled back to its state at the previous commit.
    Abort,
    /// Drop the `Filesystem`, reopen from disk, start a new transaction.
    /// Owned-block bookkeeping is reset because cross-reopen bytenrs
    /// can become stale after intervening COW.
    Reopen,
}

/// Oracle state for the property-test harness.
#[derive(Debug, Default, Clone)]
pub struct Model {
    pub kv: BTreeMap<TestKey, Vec<u8>>,
    /// `(bytenr, level)` of harness-allocated blocks not yet dropped.
    pub owned_blocks: Vec<(u64, u8)>,
    pub playground_removed: bool,
    /// Snapshot of `kv` at the last successful commit, used by `Abort`.
    pub committed_kv: BTreeMap<TestKey, Vec<u8>>,
    pub committed_playground_removed: bool,
}

/// Reasons a sequence run can fail.
#[derive(Debug)]
pub enum Failure {
    OpFailed { step: usize, op: Op, err: io::Error },
    ModelMismatch { step: usize, detail: String },
    Hang { timeout: Duration },
    CheckFailed { stderr: String },
    Other(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpFailed { step, op, err } => {
                write!(f, "op {step} {op:?} failed: {err}")
            }
            Self::ModelMismatch { step, detail } => {
                write!(f, "model mismatch after step {step}: {detail}")
            }
            Self::Hang { timeout } => {
                write!(f, "sequence hung (timeout {timeout:?})")
            }
            Self::CheckFailed { stderr } => {
                write!(f, "btrfs check failed: {stderr}")
            }
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Failure {}

/// Run `f` on a worker thread; return `Err(())` on timeout.
///
/// We cannot safely cancel the worker on timeout (it may hold locks on
/// the image file or be mid-write), so we leak it and let the test
/// process tear down. Proptest gets the failing input either way and
/// can shrink it; the leaked thread is acceptable in test contexts.
///
/// The error type is `()` because there is exactly one failure mode
/// (timeout); callers turn it into a richer `Failure::Hang` value.
#[allow(clippy::result_unit_err)]
pub fn with_watchdog<F, T>(timeout: Duration, f: F) -> Result<T, ()>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).map_err(|_| ())
}

/// Default per-sequence watchdog timeout. Generous because cascading
/// COWs on a fresh fs can legitimately do a few seconds of I/O.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Apply a single op to the live filesystem and the model in lockstep.
///
/// Returns `Ok(true)` if the caller should restart the transaction
/// (after Commit, Abort, or Reopen — these consume `trans`), `Ok(false)`
/// otherwise. When restart is requested, the caller must drop `trans`
/// before calling `Transaction::start` again.
///
/// This function is intentionally split into per-op helpers below
/// rather than inlined as one big match, so that each operation's
/// failure path is small enough to read without scrolling.
fn apply_op<R: io::Read + io::Write + io::Seek>(
    op: &Op,
    trans: &mut Transaction<R>,
    fs: &mut Filesystem<R>,
    model: &mut Model,
) -> io::Result<bool> {
    match op {
        Op::Insert { key, value } => {
            apply_insert(*key, value, trans, fs, model)?;
            Ok(false)
        }
        Op::Update { key, value } => {
            apply_update(*key, value, trans, fs, model)?;
            Ok(false)
        }
        Op::Delete { key } => {
            apply_delete(*key, trans, fs, model)?;
            Ok(false)
        }
        Op::AllocBlock => {
            if model.playground_removed {
                return Ok(false);
            }
            let bytenr = trans.alloc_tree_block(fs, PLAYGROUND_TREE, 0)?;
            model.owned_blocks.push((bytenr, 0));
            Ok(false)
        }
        Op::DropOwnedBlock { idx } => {
            if model.owned_blocks.is_empty() {
                return Ok(false);
            }
            let real_idx = *idx % model.owned_blocks.len();
            let (bytenr, level) = model.owned_blocks.swap_remove(real_idx);
            trans
                .delayed_refs
                .drop_ref(bytenr, true, PLAYGROUND_TREE, level);
            trans.pin_block(bytenr);
            Ok(false)
        }
        Op::RemovePlaygroundRoot => {
            // Real callers (rescue clear-uuid-tree) only call
            // `fs.remove_root` once the tree is in a "clean" state:
            // ROOT_ITEM is about to be deleted from the root tree, all
            // owned blocks are queued for drop+pin, and there are no
            // dirty in-memory blocks belonging to the tree. The harness
            // does not model the ROOT_ITEM delete or the drop sequence
            // explicitly, so we approximate "clean" as "model state
            // matches the last commit". Otherwise the in-memory state
            // includes a COWed-but-unflushed tree root that
            // `update_root_items` will refuse to update (because the
            // tree is no longer in `fs.roots`), leaving a dangling
            // EXTENT_ITEM and an orphan tree block.
            let dirty_uncommitted = model.kv != model.committed_kv
                || !model.owned_blocks.is_empty();
            if !model.playground_removed && !dirty_uncommitted {
                fs.remove_root(PLAYGROUND_TREE);
                model.playground_removed = true;
            }
            Ok(false)
        }
        Op::Commit | Op::Abort | Op::Reopen => {
            // Caller handles these — we can't consume `trans` from
            // behind a `&mut`. Signal restart and let the loop
            // re-dispatch with the right ownership.
            Ok(true)
        }
    }
}

/// Drop and pin every block currently in `model.owned_blocks`,
/// clearing the list. See the call site comment for the rationale.
fn drain_owned_blocks<R: io::Read + io::Write + io::Seek>(
    trans: &mut Transaction<R>,
    model: &mut Model,
) {
    for (bytenr, level) in model.owned_blocks.drain(..) {
        trans
            .delayed_refs
            .drop_ref(bytenr, true, PLAYGROUND_TREE, level);
        trans.pin_block(bytenr);
    }
}

fn apply_insert<R: io::Read + io::Write + io::Seek>(
    key: TestKey,
    value: &[u8],
    trans: &mut Transaction<R>,
    fs: &mut Filesystem<R>,
    model: &mut Model,
) -> io::Result<()> {
    if model.playground_removed || model.kv.contains_key(&key) {
        return Ok(());
    }
    let dk = key.to_disk();
    let mut path = BtrfsPath::new();
    let needed = (ITEM_SIZE + value.len()) as u32;
    let found = search::search_slot(
        Some(&mut *trans),
        fs,
        PLAYGROUND_TREE,
        &dk,
        &mut path,
        SearchIntent::Insert(needed),
        true,
    )?;
    if found {
        // Race with our own model? Treat as no-op for safety.
        path.release();
        return Ok(());
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| io::Error::other("no leaf after search"))?;
    items::insert_item(leaf, path.slots[0], &dk, value)?;
    fs.mark_dirty(leaf);
    path.release();
    model.kv.insert(key, value.to_vec());
    Ok(())
}

fn apply_update<R: io::Read + io::Write + io::Seek>(
    key: TestKey,
    value: &[u8],
    trans: &mut Transaction<R>,
    fs: &mut Filesystem<R>,
    model: &mut Model,
) -> io::Result<()> {
    if model.playground_removed || !model.kv.contains_key(&key) {
        return Ok(());
    }
    // For v1, "update" is delete + insert: `update_item` requires the
    // new payload to be the same size as the old, which our random
    // value generator does not satisfy.
    apply_delete(key, trans, fs, model)?;
    apply_insert(key, value, trans, fs, model)?;
    Ok(())
}

fn apply_delete<R: io::Read + io::Write + io::Seek>(
    key: TestKey,
    trans: &mut Transaction<R>,
    fs: &mut Filesystem<R>,
    model: &mut Model,
) -> io::Result<()> {
    if model.playground_removed || !model.kv.contains_key(&key) {
        return Ok(());
    }
    let dk = key.to_disk();
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(&mut *trans),
        fs,
        PLAYGROUND_TREE,
        &dk,
        &mut path,
        SearchIntent::Delete,
        true,
    )?;
    if !found {
        path.release();
        return Err(io::Error::other(
            "model says key exists but tree disagrees",
        ));
    }
    let slot = path.slots[0];
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| io::Error::other("no leaf after search"))?;
    items::del_items(leaf, slot, 1);
    fs.mark_dirty(leaf);
    path.release();
    model.kv.remove(&key);
    Ok(())
}

/// Verify that the playground tree contains exactly the keys/values in
/// the model. Walks every key in the model and asserts the tree returns
/// it; does not currently walk the tree to assert there are no extras
/// (that requires a leaf iterator and is left for v2).
fn check_model_matches<R: io::Read + io::Write + io::Seek>(
    fs: &mut Filesystem<R>,
    model: &Model,
) -> Result<(), String> {
    if model.playground_removed {
        // Tree is gone — nothing to check on disk.
        return Ok(());
    }
    for (key, want) in &model.kv {
        let dk = key.to_disk();
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            None,
            fs,
            PLAYGROUND_TREE,
            &dk,
            &mut path,
            SearchIntent::ReadOnly,
            false,
        )
        .map_err(|e| format!("search_slot({key:?}) failed: {e}"))?;
        if !found {
            path.release();
            return Err(format!("key {key:?} missing from playground tree"));
        }
        let leaf = path.nodes[0].as_ref().expect("leaf after found");
        let got = leaf.item_data(path.slots[0]).to_vec();
        path.release();
        if got != *want {
            return Err(format!(
                "key {key:?}: tree value {got:?} != model value {want:?}"
            ));
        }
    }
    Ok(())
}

/// Run a generated sequence of ops against a fresh filesystem.
///
/// Creates a new image, opens it, runs the ops, then commits a final
/// time, reopens, re-checks the model, and runs `btrfs check --readonly`
/// (free-space-tree warnings tolerated, structural errors fatal).
///
/// This is the entry point both the hand-crafted reproducer test and
/// the proptest harness call into.
pub fn run_sequence(ops: &[Op]) -> Result<(), Failure> {
    let fixture = TestFixture::new();
    let mut fs = fixture
        .open()
        .map_err(|e| Failure::Other(format!("open: {e}")))?;
    let mut trans = Transaction::start(&mut fs)
        .map_err(|e| Failure::Other(format!("start: {e}")))?;
    let mut model = Model::default();

    for (step, op) in ops.iter().enumerate() {
        let restart =
            apply_op(op, &mut trans, &mut fs, &mut model).map_err(|err| {
                Failure::OpFailed {
                    step,
                    op: op.clone(),
                    err,
                }
            })?;

        if restart {
            // Drain any harness-allocated blocks that were never paired
            // with an explicit DropOwnedBlock. The harness's AllocBlock
            // primitive *only* reserves an address; unlike a real
            // `cow_block` caller, it never writes a tree block at that
            // address, so leaving the +1 delayed ref in place would
            // create an EXTENT_ITEM pointing at unwritten garbage and
            // fail `btrfs check` ("tree extent root N has no tree
            // block found"). Mirror what proptest *would* generate if
            // it had picked a DropOwnedBlock for each: drop_ref + pin
            // each owned block before the lifecycle op consumes the
            // transaction.
            if matches!(op, Op::Commit | Op::Abort | Op::Reopen) {
                drain_owned_blocks(&mut trans, &mut model);
            }
            match op {
                Op::Commit => {
                    trans.commit(&mut fs).map_err(|err| Failure::OpFailed {
                        step,
                        op: op.clone(),
                        err,
                    })?;
                    model.committed_kv = model.kv.clone();
                    model.committed_playground_removed =
                        model.playground_removed;
                    // Owned blocks were freed by the commit's delayed-ref
                    // flush; harness no longer tracks them.
                    model.owned_blocks.clear();
                    trans = Transaction::start(&mut fs)
                        .map_err(|e| Failure::Other(format!("restart: {e}")))?;
                }
                Op::Abort => {
                    trans.abort(&mut fs);
                    model.kv = model.committed_kv.clone();
                    model.playground_removed =
                        model.committed_playground_removed;
                    model.owned_blocks.clear();
                    trans = Transaction::start(&mut fs)
                        .map_err(|e| Failure::Other(format!("restart: {e}")))?;
                }
                Op::Reopen => {
                    trans.abort(&mut fs);
                    drop(fs);
                    fs = fixture
                        .open()
                        .map_err(|e| Failure::Other(format!("reopen: {e}")))?;
                    // Model rolls back to last commit (uncommitted ops
                    // are discarded along with the in-memory state).
                    model.kv = model.committed_kv.clone();
                    model.playground_removed =
                        model.committed_playground_removed;
                    model.owned_blocks.clear();
                    trans = Transaction::start(&mut fs)
                        .map_err(|e| Failure::Other(format!("restart: {e}")))?;
                }
                _ => unreachable!("only lifecycle ops set restart=true"),
            }
        }

        check_model_matches(&mut fs, &model)
            .map_err(|detail| Failure::ModelMismatch { step, detail })?;
    }

    // Final commit + reopen + btrfs check.
    drain_owned_blocks(&mut trans, &mut model);
    trans
        .commit(&mut fs)
        .map_err(|e| Failure::Other(format!("final commit: {e}")))?;
    drop(fs);
    let mut fs2 = fixture
        .open()
        .map_err(|e| Failure::Other(format!("final reopen: {e}")))?;
    check_model_matches(&mut fs2, &model).map_err(|detail| {
        Failure::ModelMismatch {
            step: ops.len(),
            detail,
        }
    })?;
    drop(fs2);

    // `assert_check` panics on structural errors; we want to surface
    // them as a `Failure` so proptest can shrink. Re-implement the
    // tolerant check inline.
    let output = Command::new("btrfs")
        .args(["check", "--readonly"])
        .arg(&fixture.path)
        .output()
        .map_err(|e| Failure::Other(format!("btrfs check spawn: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let structural = stderr.lines().any(|line| {
            line.contains("ERROR:")
                && !line.contains("free space")
                && !line.contains("cache")
        });
        if structural {
            return Err(Failure::CheckFailed { stderr });
        }
    }

    Ok(())
}

/// Watchdog wrapper around `run_sequence` for use from proptest tests.
///
/// `run_sequence` itself is not `Send` because of internal raw pointer
/// state in the buffer cache, so we move `ops` (which *is* `Send`) into
/// the worker thread and reconstruct everything inside it.
pub fn run_sequence_watchdogged(
    ops: Vec<Op>,
    timeout: Duration,
) -> Result<(), Failure> {
    match with_watchdog(timeout, move || run_sequence(&ops)) {
        Ok(result) => result,
        Err(()) => Err(Failure::Hang { timeout }),
    }
}

//! # Post-bootstrap transactions on a freshly-built mkfs image
//!
//! After [`mkfs::make_btrfs`](crate::mkfs::make_btrfs) (or
//! `make_btrfs_with_rootdir`) finishes writing its bootstrap layout to
//! disk, this module applies a single transaction-crate transaction to
//! the resulting image to fill in pieces that mkfs's hand-built layout
//! doesn't yet produce.
//!
//! Right now that is just the empty UUID tree (objectid 9), which
//! btrfs-progs creates by default but our mkfs doesn't (PLAN B.3). The
//! kernel populates UUID-tree entries lazily on snapshot/send, so an
//! empty tree is the correct initial state.
//!
//! ## Why a separate module
//!
//! mkfs's current write path (`tree.rs` + `treebuilder.rs` + raw
//! `pwrite`) is GPL-2.0 territory and was written looking at
//! btrfs-progs. This module is the bridge to the MIT/Apache-licensed
//! transaction crate: it opens the freshly-written image, runs a
//! transaction against it, and closes. Keeping the bridge in its own
//! file makes the licensing boundary obvious and gives us one place to
//! grow as more migration work lands.

use crate::{args::Profile, mkfs::MkfsConfig};
use anyhow::{Context, Result};
use btrfs_disk::raw::{BTRFS_CSUM_TREE_OBJECTID, BTRFS_UUID_TREE_OBJECTID};
use btrfs_transaction::{Filesystem, Transaction};
use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

/// Profiles for which we have verified the post-bootstrap transaction
/// works end-to-end (transaction crate opens the image cleanly, the
/// commit lands, and `btrfs check` passes).
///
/// RAID5 / RAID6 are still excluded: the transaction crate doesn't yet
/// route writes through their parity-aware placement (no `plan_write`
/// implementation for RAID5/RAID6 — needs its own clean-room plan).
/// All other profiles (SINGLE / DUP / RAID0 / RAID1* / RAID10) go
/// through the stripe-aware `BlockReader::write_block`, which routes
/// per the chunk's profile via `ChunkTreeCache::plan_write`.
fn profile_supported(profile: Profile) -> bool {
    matches!(
        profile,
        Profile::Single
            | Profile::Dup
            | Profile::Raid0
            | Profile::Raid1
            | Profile::Raid1c3
            | Profile::Raid1c4
            | Profile::Raid10
    )
}

/// Decide whether to run the post-bootstrap transaction for `cfg`.
///
/// Skips for any profile we haven't verified (see [`profile_supported`]).
///
/// `^free-space-tree` (PLAN B.2) used to be on this skip list too, but
/// the transaction crate's `update_free_space_tree` now respects the
/// `FREE_SPACE_TREE` `compat_ro` flag and treats a cleared flag as "no
/// FST", so it's safe to run post-bootstrap on those images. mkfs
/// still leaves a stale FST tree leaf around in that case (a separate
/// fix); the kernel ignores it.
///
/// `pub(crate)` so `mkfs.rs` can decide which trees to create itself
/// vs. leave for post-bootstrap to create.
pub(crate) fn should_run(cfg: &MkfsConfig) -> bool {
    if !profile_supported(cfg.metadata_profile) {
        return false;
    }
    if !profile_supported(cfg.data_profile) {
        return false;
    }
    true
}

/// Run the post-bootstrap transaction against a freshly-written mkfs
/// image.
///
/// Opens the device(s), starts a transaction, applies the post-bootstrap
/// additions, commits, and syncs. Returns `Ok(())` without doing anything
/// for unsupported profile/feature combinations.
///
/// # Errors
///
/// Returns an error if any device cannot be opened, the bootstrap image
/// cannot be opened by `Filesystem::open*`, the transaction fails, or
/// the underlying I/O fails.
pub fn run(cfg: &MkfsConfig) -> Result<()> {
    debug_assert!(
        !cfg.devices.is_empty(),
        "post_bootstrap::run: cfg has zero devices"
    );

    if !should_run(cfg) {
        return Ok(());
    }

    let paths: Vec<&Path> =
        cfg.devices.iter().map(|d| d.path.as_path()).collect();

    if paths.len() == 1 {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(paths[0])
            .with_context(|| {
                format!("post_bootstrap: open {}", paths[0].display())
            })?;
        let mut fs = Filesystem::open(file)
            .context("post_bootstrap: Filesystem::open")?;
        run_transaction(&mut fs)?;
        fs.sync().context("post_bootstrap: fsync")?;
    } else {
        // Multi-device: build {devid -> handle} keyed by each device's
        // own superblock dev_item.devid (so a caller passing devices in
        // a different order than mkfs assigned them still opens cleanly).
        let mut handles: BTreeMap<u64, std::fs::File> = BTreeMap::new();
        for path in &paths {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .with_context(|| {
                    format!("post_bootstrap: open {}", path.display())
                })?;
            let sb = btrfs_disk::superblock::read_superblock(&mut file, 0)
                .with_context(|| {
                    format!(
                        "post_bootstrap: read superblock from {}",
                        path.display()
                    )
                })?;
            let prev = handles.insert(sb.dev_item.devid, file);
            debug_assert!(
                prev.is_none(),
                "post_bootstrap: duplicate devid {} across input devices",
                sb.dev_item.devid,
            );
        }
        let mut fs = Filesystem::open_multi(handles)
            .context("post_bootstrap: Filesystem::open_multi")?;
        run_transaction(&mut fs)?;
        fs.sync().context("post_bootstrap: fsync")?;
    }
    Ok(())
}

/// Run the transaction body. Separate so callers (e.g. tests) can drive
/// `Filesystem` themselves.
///
/// # Errors
///
/// Returns an error if `Transaction::start`, the per-step additions, or
/// `commit` fails.
pub fn run_transaction(fs: &mut Filesystem<std::fs::File>) -> Result<()> {
    let mut trans =
        Transaction::start(fs).context("post_bootstrap: Transaction::start")?;
    apply_in_transaction(fs, &mut trans)?;
    trans
        .commit(fs)
        .context("post_bootstrap: Transaction::commit")?;
    Ok(())
}

/// Apply every post-bootstrap step in the supplied transaction.
///
/// Each step is idempotent (creates a tree only if it isn't already
/// present in the root tree), so the same `apply_in_transaction` works
/// for both:
///
/// - the no-rootdir `make_btrfs` path, where mkfs's bootstrap omits
///   trees that post-bootstrap will create (currently: UUID tree,
///   csum tree)
/// - the `make_btrfs_with_rootdir` path, where mkfs creates the csum
///   tree itself (because rootdir needs to insert csum items into it)
///   so the create-if-missing call is a no-op
fn apply_in_transaction<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
) -> Result<()> {
    // btrfs-progs creates the csum tree as an empty leaf at mkfs
    // time. mkfs's no-rootdir path now defers that to here so the
    // tree allocation, ROOT_ITEM, and METADATA_ITEM go through the
    // transaction crate. The rootdir path keeps creating it directly
    // because it has csum items to insert at bootstrap time.
    ensure_empty_tree(fs, trans, u64::from(BTRFS_CSUM_TREE_OBJECTID), "csum")?;

    // btrfs-progs creates a UUID tree by default; mkfs's hand-built
    // bootstrap doesn't. The kernel populates entries lazily on
    // snapshot/send.
    ensure_empty_tree(fs, trans, u64::from(BTRFS_UUID_TREE_OBJECTID), "UUID")?;

    Ok(())
}

/// Idempotently create an empty tree at `tree_id` if the root tree
/// does not already reference one. `name` is used in error messages.
fn ensure_empty_tree<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    name: &str,
) -> Result<()> {
    if fs.root_bytenr(tree_id).is_some() {
        return Ok(());
    }
    trans.create_empty_tree(fs, tree_id).with_context(|| {
        format!("post_bootstrap: create_empty_tree({name} tree)")
    })?;
    debug_assert!(
        fs.root_bytenr(tree_id).is_some(),
        "post_bootstrap: {name} tree root not set after create_empty_tree",
    );
    Ok(())
}

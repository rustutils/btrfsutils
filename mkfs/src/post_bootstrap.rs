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
use btrfs_disk::raw::BTRFS_UUID_TREE_OBJECTID;
use btrfs_transaction::{Filesystem, Transaction};
use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

/// Profiles for which we have verified the post-bootstrap transaction
/// works end-to-end (transaction crate opens the image cleanly, the
/// commit lands, and `btrfs check` passes).
///
/// RAID0 / RAID5 / RAID6 metadata images produced by our mkfs today
/// fail `btrfs check` (latent striping bugs that pre-date this
/// integration). RAID10 hits the same striping limitation as RAID0
/// for tree blocks larger than `stripe_len` and the transaction
/// crate's `resolve_all` doesn't yet pick the correct sub-stripe pair
/// for tree-block writes. These cases need their own clean-room plan
/// and are skipped here; the resulting filesystem is unchanged from
/// what mkfs's bootstrap produces (no UUID tree, missing other future
/// post-bootstrap additions) but at least we don't make it worse.
fn profile_supported(profile: Profile) -> bool {
    matches!(
        profile,
        Profile::Single
            | Profile::Dup
            | Profile::Raid1
            | Profile::Raid1c3
            | Profile::Raid1c4
    )
}

/// Decide whether to run the post-bootstrap transaction for `cfg`.
///
/// Skips for any profile we haven't verified (see [`profile_supported`]).
///
/// `^free-space-tree` (PLAN B.2) used to be on this skip list too, but
/// the transaction crate's `update_free_space_tree` now respects the
/// `FREE_SPACE_TREE` compat_ro flag and treats a cleared flag as "no
/// FST", so it's safe to run post-bootstrap on those images. mkfs
/// still leaves a stale FST tree leaf around in that case (a separate
/// fix); the kernel ignores it.
fn should_run(cfg: &MkfsConfig) -> bool {
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
/// additions ([`apply_in_transaction`]), commits, and syncs. Returns
/// `Ok(())` without doing anything for unsupported profile/feature
/// combinations (see [`should_run`]).
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
/// Currently:
///
/// 1. Create the empty UUID tree (objectid 9) if it doesn't already
///    exist. btrfs-progs creates this by default; our mkfs's bootstrap
///    doesn't, so we add it here. Kernel populates entries lazily.
fn apply_in_transaction<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
) -> Result<()> {
    let uuid_tree_id = u64::from(BTRFS_UUID_TREE_OBJECTID);

    if fs.root_bytenr(uuid_tree_id).is_none() {
        trans
            .create_empty_tree(fs, uuid_tree_id)
            .context("post_bootstrap: create_empty_tree(UUID tree)")?;
        debug_assert!(
            fs.root_bytenr(uuid_tree_id).is_some(),
            "post_bootstrap: UUID tree root not set after create_empty_tree",
        );
    }

    Ok(())
}

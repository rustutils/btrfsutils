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
use btrfs_disk::{
    items::{InodeRef, RootItem},
    raw::{BTRFS_CSUM_TREE_OBJECTID, BTRFS_UUID_TREE_OBJECTID},
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    Filesystem, Transaction,
    buffer::ITEM_SIZE,
    inode::InodeArgs,
    items::{insert_item, update_item},
    path::BtrfsPath,
    search::{SearchIntent, search_slot},
};
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
///   csum tree, data-reloc tree)
/// - the `make_btrfs_with_rootdir` path, where mkfs creates the csum
///   and data-reloc trees itself (because rootdir needs to insert
///   csum items into the csum tree at bootstrap time, and uses its
///   own builders for data-reloc), so the create-if-missing calls
///   are no-ops
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

    // The data-reloc tree (objectid -9, equivalent unsigned
    // 0xFFFFFFFF_FFFFFFF7) holds inode 256's INODE_ITEM and a
    // ".." INODE_REF self-reference, like an empty subvolume. mkfs's
    // hand-built bootstrap built this tree itself; the no-rootdir path
    // now defers it to here.
    ensure_data_reloc_tree(fs, trans)?;

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

/// Idempotently create the data-reloc tree (objectid -9) with the
/// minimum on-disk shape `btrfs check` requires: an `INODE_ITEM` for
/// the root directory (inode 256) and a self-pointing `INODE_REF`
/// keyed `(256, INODE_REF, 256)` with `index=0` and `name=".."`.
///
/// This mirrors what mkfs's `build_root_dir_tree` produces for the
/// data-reloc tree so a post-bootstrap-built tree is structurally
/// equivalent to the legacy mkfs-built one.
fn ensure_data_reloc_tree<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
) -> Result<()> {
    // BTRFS_DATA_RELOC_TREE_OBJECTID = -9, which as u64 is
    // 0xFFFFFFFF_FFFFFFF7.
    #[allow(clippy::cast_sign_loss)]
    let dr_id = btrfs_disk::raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64;
    let root_ino = u64::from(btrfs_disk::raw::BTRFS_FIRST_FREE_OBJECTID);

    if fs.root_bytenr(dr_id).is_some() {
        return Ok(());
    }

    // 1. Create the empty tree (allocates leaf, inserts ROOT_ITEM).
    trans.create_empty_tree(fs, dr_id).with_context(|| {
        "post_bootstrap: create_empty_tree(data-reloc tree)".to_string()
    })?;

    // 2. Create the root directory inode (objectid 256). Uses the
    //    transaction crate's create_inode helper, which inserts
    //    INODE_ITEM at (256, INODE_ITEM, 0).
    //
    //    nbytes = nodesize matches mkfs's directory inode convention:
    //    a directory's "data" lives in the tree leaf, which is one
    //    nodesize block. Mode = S_IFDIR | 0755. Timestamps start at
    //    zero (`InodeArgs::new` default) — there's no meaningful
    //    creation time for the data-reloc tree's special root.
    let mut inode_args = InodeArgs::new(trans.transid, 0o040_755);
    inode_args.nbytes = u64::from(fs.nodesize);
    trans
        .create_inode(fs, dr_id, root_ino, &inode_args)
        .context("post_bootstrap: create_inode(data-reloc root dir)")?;

    // 3. Insert the ".." INODE_REF self-reference: (256, INODE_REF,
    //    256). The data-reloc tree's "directory" has no real entries,
    //    so dir_index is 0.
    insert_inode_ref(fs, trans, dr_id, root_ino, root_ino, 0, b"..")?;

    // 4. Patch the ROOT_ITEM in the root tree to mark this as a
    //    subvolume-shaped tree: root_dirid = 256 and the embedded
    //    inode_data mirrors the standalone INODE_ITEM. btrfs check
    //    walks ROOT_ITEM.root_dirid to find the root dir inode, and
    //    cross-checks ROOT_ITEM.inode_data against the standalone
    //    INODE_ITEM (matching nlink, mode, size).
    patch_root_item_subvol_dir(fs, trans, dr_id, &inode_args)?;

    Ok(())
}

/// After `create_empty_tree(tree_id)` and `create_inode(tree_id, 256,
/// args)`, patch the corresponding `ROOT_ITEM` in the root tree so it
/// points at inode 256 as the subvolume root (`root_dirid = 256`) and
/// its embedded `inode_data` matches the standalone `INODE_ITEM`.
/// This is the consistency check `btrfs check` performs across
/// `ROOT_ITEM` and `INODE_ITEM` for subvolume-shaped trees.
fn patch_root_item_subvol_dir<R>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    inode_args: &InodeArgs,
) -> Result<()>
where
    R: std::io::Read + std::io::Write + std::io::Seek,
{
    let root_key = DiskKey {
        objectid: tree_id,
        key_type: KeyType::RootItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let found = search_slot(
        Some(trans),
        fs,
        1, // root tree
        &root_key,
        &mut path,
        SearchIntent::ReadOnly,
        true,
    )
    .context("post_bootstrap: search_slot(ROOT_ITEM)")?;
    if !found {
        path.release();
        return Err(anyhow::anyhow!(
            "post_bootstrap: ROOT_ITEM for tree {tree_id} not found"
        ));
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("post_bootstrap: no leaf in path"))?;
    let slot = path.slots[0];
    let ri_data = leaf.item_data(slot).to_vec();
    let mut root_item = RootItem::parse(&ri_data).ok_or_else(|| {
        anyhow::anyhow!("post_bootstrap: malformed ROOT_ITEM")
    })?;
    root_item.root_dirid =
        u64::from(btrfs_disk::raw::BTRFS_FIRST_FREE_OBJECTID);
    // Mirror the standalone INODE_ITEM into the embedded inode_data
    // (160 bytes). RootItem::to_bytes will splice this back in.
    root_item.inode_data = inode_args.to_bytes();
    let new_ri = root_item.to_bytes();
    update_item(leaf, slot, &new_ri)
        .map_err(|e| anyhow::anyhow!("post_bootstrap: update_item: {e}"))?;
    fs.mark_dirty(leaf);
    path.release();
    Ok(())
}

/// Insert a single `INODE_REF` item at
/// `(child_ino, INODE_REF, parent_ino)` with the given index and name.
/// Used directly (rather than via `link_dir_entry`) when the parent
/// has no real `DIR_ITEM`/`DIR_INDEX` entries to add — e.g. the
/// self-pointing ".." ref at the root of a subvolume-shaped tree.
fn insert_inode_ref<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    child_ino: u64,
    parent_ino: u64,
    index: u64,
    name: &[u8],
) -> Result<()> {
    let key = DiskKey {
        objectid: child_ino,
        key_type: KeyType::InodeRef,
        offset: parent_ino,
    };
    let data = InodeRef::serialize(index, name);
    let item_room = u32::try_from(ITEM_SIZE + data.len())
        .expect("INODE_REF item room fits in u32 (small fixed-size record)");
    let mut path = BtrfsPath::new();
    let found = search_slot(
        Some(trans),
        fs,
        tree_id,
        &key,
        &mut path,
        SearchIntent::Insert(item_room),
        true,
    )
    .context("post_bootstrap: search_slot(INODE_REF)")?;
    if found {
        path.release();
        return Err(anyhow::anyhow!(
            "post_bootstrap: INODE_REF for inode {child_ino} \
             already exists in tree {tree_id}"
        ));
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("post_bootstrap: no leaf in path"))?;
    insert_item(leaf, path.slots[0], &key, &data)
        .map_err(|e| anyhow::anyhow!("post_bootstrap: insert_item: {e}"))?;
    fs.mark_dirty(leaf);
    path.release();
    Ok(())
}

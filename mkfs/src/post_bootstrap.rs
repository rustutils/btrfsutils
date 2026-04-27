//! # Post-bootstrap transactions on a freshly-built mkfs image
//!
//! After [`mkfs::make_btrfs`](crate::mkfs::make_btrfs) finishes
//! writing its bootstrap (Root / Extent / Chunk / Dev + superblock),
//! this module reopens the image with the transaction crate, starts
//! a single transaction, and creates every always-present empty
//! tree the bootstrap omits. The resulting filesystem is
//! mkfs-equivalent to what btrfs-progs produces.
//!
//! Steps applied per transaction (each idempotent so the same code
//! works whether the bootstrap or a previous post-bootstrap run
//! created the tree):
//!
//! 1. Block-group tree (objectid 11) — when the
//!    `BLOCK_GROUP_TREE` `compat_ro` flag is set. Migrates every
//!    `BLOCK_GROUP_ITEM` from the extent tree to BGT via
//!    [`btrfs_transaction::convert::create_block_group_tree`]. Must
//!    come first so subsequent allocations route correctly.
//! 2. FS tree (objectid 5) — empty subvolume shape: inode 256
//!    with `INODE_ROOT_ITEM_INIT`, ".." `INODE_REF`, `ROOT_ITEM`
//!    patched to mirror the inode plus a fsid-derived UUID and
//!    ctime/otime.
//! 3. Csum tree (objectid 7) — empty leaf.
//! 4. Data-reloc tree (objectid -9) — same subvolume shape as the
//!    FS tree, but without `INODE_ROOT_ITEM_INIT` or a UUID
//!    (matches mkfs convention).
//! 5. Quota tree (objectid 8) — when `-O quota` or `-O squota` is
//!    set. STATUS + INFO + LIMIT items for the FS tree's qgroupid
//!    (0/5).
//! 6. Free-space tree (objectid 10) — when the
//!    `FREE_SPACE_TREE` `compat_ro` flag is set. Created empty,
//!    then seeded with `FREE_SPACE_INFO` + `FREE_SPACE_EXTENT`
//!    items derived from the current extent-tree state via
//!    [`btrfs_transaction::convert::seed_free_space_tree`].
//! 7. UUID tree (objectid 9) — empty leaf. Kernel populates
//!    entries lazily on snapshot/send.
//!
//! Also exposes a `create_subvolume_shape` helper (used by the FS
//! tree step here and by the rootdir walker for user `--subvol`
//! trees) so the same allocate-leaf + populate-256 + ".." +
//! `ROOT_ITEM` pattern doesn't get reimplemented.
//!
//! ## Why a separate module
//!
//! mkfs's bootstrap (`tree.rs` + `treebuilder.rs` + raw `pwrite`)
//! is GPL-2.0 territory and was written looking at btrfs-progs.
//! This module is the bridge to the MIT/Apache-licensed transaction
//! crate: it opens the bootstrap image, runs a transaction against
//! it, commits, and closes. Keeping the bridge in its own file
//! makes the licensing boundary obvious.

use crate::{args::Profile, mkfs::MkfsConfig};
use anyhow::{Context, Result};
use btrfs_disk::{
    items::{InodeRef, RootItem},
    raw::{
        BTRFS_CSUM_TREE_OBJECTID, BTRFS_FS_TREE_OBJECTID,
        BTRFS_INODE_ROOT_ITEM_INIT, BTRFS_UUID_TREE_OBJECTID,
    },
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
use uuid::Uuid;

/// Profiles for which we have verified the post-bootstrap transaction
/// works end-to-end (transaction crate opens the image cleanly, the
/// commit lands, and `btrfs check` passes).
///
/// All seven RAID profiles are now supported. `BlockReader::write_block`
/// routes per the chunk's profile via `ChunkTreeCache::plan_write`,
/// which covers SINGLE / DUP / RAID0 / RAID1* / RAID10 directly and
/// RAID5 / RAID6 via the parity-aware executor (`compute_p` /
/// `compute_p_q` in the `disk` crate's `raid56` module).
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
            | Profile::Raid5
            | Profile::Raid6
    )
}

/// Decide whether to run the post-bootstrap transaction for `cfg`.
///
/// Returns `false` only if a profile lands outside the support matrix
/// of [`profile_supported`] — currently impossible since every defined
/// `Profile` variant is supported. Kept as a hook so future profiles
/// (e.g. `raid-stripe-tree`) can be gated here without changing
/// callers.
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
        run_transaction(&mut fs, cfg)?;
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
        run_transaction(&mut fs, cfg)?;
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
pub fn run_transaction(
    fs: &mut Filesystem<std::fs::File>,
    cfg: &MkfsConfig,
) -> Result<()> {
    let mut trans =
        Transaction::start(fs).context("post_bootstrap: Transaction::start")?;
    apply_in_transaction(fs, &mut trans, cfg)?;
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
///   trees that post-bootstrap will create (currently: UUID tree, FS
///   tree, csum tree, data-reloc tree)
/// - the `make_btrfs_with_rootdir` path, where mkfs creates the FS,
///   csum, and data-reloc trees itself (because rootdir populates
///   them with directory/extent/csum items at bootstrap time), so
///   the create-if-missing calls are no-ops
fn apply_in_transaction<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    cfg: &MkfsConfig,
) -> Result<()> {
    let now = cfg.now_secs();

    // The block-group tree (objectid 11) MUST come first. mkfs's
    // bootstrap (when post_bootstrap will run) leaves all
    // BLOCK_GROUP_ITEM rows in the extent tree. Materialising BGT
    // and migrating the items must happen before any other
    // allocation in this transaction so subsequent BG-update calls
    // route to BGT, not the extent tree.
    if cfg.has_block_group_tree() {
        ensure_block_group_tree(fs, trans)?;
    }

    // The FS tree (objectid 5) is the main user-visible tree:
    // subvolume root, root dir inode 256, dir entries, file inodes.
    // For the no-rootdir path it's just the empty subvolume shape
    // (root dir + ".." INODE_REF).
    ensure_fs_tree(fs, trans, now)?;

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

    // The quota tree (objectid 8) is created when `-O quota` or
    // `-O squota` is set. Holds STATUS + INFO + LIMIT items for the
    // FS tree's qgroupid (0/5).
    if cfg.has_quota_tree() {
        ensure_quota_tree(fs, trans, cfg)?;
    }

    // The free-space tree (objectid 10) is created when the
    // `free-space-tree` feature is enabled (default). Created
    // empty, then seeded with FREE_SPACE_INFO + FREE_SPACE_EXTENT
    // items derived from the current extent tree state. Subsequent
    // commit's update_free_space_tree pass will apply any deltas
    // accumulated during this transaction.
    if cfg.has_free_space_tree() {
        ensure_free_space_tree(fs, trans)?;
    }

    // btrfs-progs creates a UUID tree by default; mkfs's hand-built
    // bootstrap doesn't. The kernel populates entries lazily on
    // snapshot/send.
    ensure_empty_tree(fs, trans, u64::from(BTRFS_UUID_TREE_OBJECTID), "UUID")?;

    Ok(())
}

/// Idempotently materialise the block-group tree (objectid 11) by
/// migrating every `BLOCK_GROUP_ITEM` from the extent tree to BGT.
/// Wraps [`btrfs_transaction::convert::create_block_group_tree`],
/// which handles the BG-tree-id routing override during the
/// migration so the helper itself can allocate without splitting BG
/// state across both trees.
fn ensure_block_group_tree<
    R: std::io::Read + std::io::Write + std::io::Seek,
>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
) -> Result<()> {
    let bgt_id = u64::from(btrfs_disk::raw::BTRFS_BLOCK_GROUP_TREE_OBJECTID);
    if fs.root_bytenr(bgt_id).is_some() {
        return Ok(());
    }
    btrfs_transaction::convert::create_block_group_tree(trans, fs)
        .context("post_bootstrap: create_block_group_tree")?;
    Ok(())
}

/// Idempotently create the free-space tree (objectid 10) and seed
/// it with `FREE_SPACE_INFO` + `FREE_SPACE_EXTENT` items for every
/// block group. Wraps
/// [`btrfs_transaction::convert::seed_free_space_tree`], which is
/// per-BG idempotent (so this can run after a partial seed without
/// duplicating items).
fn ensure_free_space_tree<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
) -> Result<()> {
    let fst_id = u64::from(btrfs_disk::raw::BTRFS_FREE_SPACE_TREE_OBJECTID);
    if fs.root_bytenr(fst_id).is_none() {
        trans
            .create_empty_tree(fs, fst_id)
            .context("post_bootstrap: create_empty_tree(free space tree)")?;
    }
    btrfs_transaction::convert::seed_free_space_tree(trans, fs)
        .context("post_bootstrap: seed_free_space_tree")?;
    Ok(())
}

/// Idempotently create the FS tree (objectid 5) as an empty
/// subvolume. Wraps [`create_subvolume_shape`] with the FS-tree-
/// specific UUID derivation: bit-flipped fsid (kept stable so
/// repeat mkfs runs with the same fsid produce the same subvol
/// UUID).
fn ensure_fs_tree<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    now: u64,
) -> Result<()> {
    let fs_id = u64::from(BTRFS_FS_TREE_OBJECTID);
    if fs.root_bytenr(fs_id).is_some() {
        return Ok(());
    }
    let mut uuid_bytes = *fs.superblock.fsid.as_bytes();
    for b in &mut uuid_bytes {
        *b ^= 0xFF;
    }
    let subvol_uuid = Uuid::from_bytes(uuid_bytes);
    create_subvolume_shape(fs, trans, fs_id, now, subvol_uuid)
}

/// Materialise the on-disk shape of a subvolume tree (`INODE_ITEM`
/// for inode 256, ".." `INODE_REF` self-ref, `ROOT_ITEM` patched so
/// `root_dirid = 256` and the embedded `inode_data` mirrors the
/// standalone inode).
///
/// Used by both [`ensure_fs_tree`] (for the canonical FS tree, id 5)
/// and the rootdir subvolume path (for user-created `--subvol`
/// trees, ids 256+).
///
/// Allocates the tree leaf (and its `ROOT_ITEM` in the root tree)
/// via [`Transaction::create_empty_tree`], then populates the
/// subvolume-shape items. The `BTRFS_INODE_ROOT_ITEM_INIT` flag on
/// the inode tells the kernel this is a subvolume root rather than
/// a regular directory, and the `ROOT_ITEM` patch keeps `btrfs check`
/// happy by mirroring the inode metadata into the root item.
///
/// `now` is used as ctime/mtime/atime/otime for the inode and as
/// ctime/otime in the `ROOT_ITEM`. `uuid` lands in `ROOT_ITEM.uuid`.
///
/// # Errors
///
/// Returns an error if any underlying transaction call fails.
pub(crate) fn create_subvolume_shape<R>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    now: u64,
    uuid: Uuid,
) -> Result<()>
where
    R: std::io::Read + std::io::Write + std::io::Seek,
{
    use btrfs_disk::items::{InodeFlags, Timespec};

    let root_ino = u64::from(btrfs_disk::raw::BTRFS_FIRST_FREE_OBJECTID);

    trans.create_empty_tree(fs, tree_id).with_context(|| {
        format!("create_subvolume_shape: create_empty_tree({tree_id})")
    })?;

    let now_ts = Timespec { sec: now, nsec: 0 };
    let mut inode_args =
        InodeArgs::new(trans.transid, 0o040_755).with_uniform_time(now_ts);
    inode_args.nbytes = u64::from(fs.nodesize);
    inode_args.flags =
        InodeFlags::from_bits_truncate(u64::from(BTRFS_INODE_ROOT_ITEM_INIT));
    trans
        .create_inode(fs, tree_id, root_ino, &inode_args)
        .with_context(|| {
            format!("create_subvolume_shape: create_inode({tree_id}, 256)")
        })?;

    insert_inode_ref(fs, trans, tree_id, root_ino, root_ino, 0, b"..")?;

    patch_root_item_subvol_dir(
        fs,
        trans,
        tree_id,
        &inode_args,
        Some(uuid),
        Some(now_ts),
    )?;

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
    //    INODE_ITEM (matching nlink, mode, size). The data-reloc
    //    tree's ROOT_ITEM keeps `uuid = nil` (mkfs's convention).
    patch_root_item_subvol_dir(fs, trans, dr_id, &inode_args, None, None)?;

    Ok(())
}

/// After `create_empty_tree(tree_id)` and `create_inode(tree_id, 256,
/// args)`, patch the corresponding `ROOT_ITEM` in the root tree so it
/// points at inode 256 as the subvolume root (`root_dirid = 256`) and
/// its embedded `inode_data` matches the standalone `INODE_ITEM`.
/// This is the consistency check `btrfs check` performs across
/// `ROOT_ITEM` and `INODE_ITEM` for subvolume-shaped trees.
///
/// `uuid` overrides the `ROOT_ITEM`'s `uuid` field when `Some`. The FS
/// tree uses a UUID derived from the filesystem fsid; the data-reloc
/// tree leaves it nil.
///
/// `time` overrides the `ROOT_ITEM`'s `ctime` and `otime` fields when
/// `Some`. The FS tree sets these to `cfg.now_secs()` to match
/// btrfs-progs's convention; the data-reloc tree leaves them zero.
#[allow(clippy::too_many_arguments)]
fn patch_root_item_subvol_dir<R>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    inode_args: &InodeArgs,
    uuid: Option<Uuid>,
    time: Option<btrfs_disk::items::Timespec>,
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
    if let Some(u) = uuid {
        root_item.uuid = u;
    }
    if let Some(ts) = time {
        root_item.ctime = ts;
        root_item.otime = ts;
    }
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
    insert_raw_item(fs, trans, tree_id, &key, &data, "INODE_REF")
}

/// Insert a raw byte-slice item at `key` in `tree_id`. Errors on
/// duplicate. Shared bottom-half of all the `ensure_*` helpers that
/// need to write items the higher-level transaction APIs don't cover
/// directly (`INODE_REF`, qgroup items, etc.).
fn insert_raw_item<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    tree_id: u64,
    key: &DiskKey,
    data: &[u8],
    what: &str,
) -> Result<()> {
    let item_room = u32::try_from(ITEM_SIZE + data.len()).map_err(|_| {
        anyhow::anyhow!("post_bootstrap: {what} item too large for u32")
    })?;
    let mut path = BtrfsPath::new();
    let found = search_slot(
        Some(trans),
        fs,
        tree_id,
        key,
        &mut path,
        SearchIntent::Insert(item_room),
        true,
    )
    .with_context(|| format!("post_bootstrap: search_slot({what})"))?;
    if found {
        path.release();
        return Err(anyhow::anyhow!(
            "post_bootstrap: {what} already exists in tree {tree_id}"
        ));
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("post_bootstrap: no leaf in path"))?;
    insert_item(leaf, path.slots[0], key, data)
        .map_err(|e| anyhow::anyhow!("post_bootstrap: insert_item: {e}"))?;
    fs.mark_dirty(leaf);
    path.release();
    Ok(())
}

/// Idempotently create the quota tree (objectid 8) with the items
/// that mkfs's `build_quota_tree` would produce: a `QGROUP_STATUS`
/// item plus `QGROUP_INFO` and `QGROUP_LIMIT` for the FS tree's
/// qgroupid (0/5).
///
/// Distinguishes between regular quota (`-O quota`, INCONSISTENT
/// flag, zeroed info — kernel will rescan) and simple quota
/// (`-O squota`, `SIMPLE_MODE` flag, info pre-populated with the FS
/// tree's nodesize usage and `enable_gen` set).
fn ensure_quota_tree<R: std::io::Read + std::io::Write + std::io::Seek>(
    fs: &mut Filesystem<R>,
    trans: &mut Transaction<R>,
    cfg: &MkfsConfig,
) -> Result<()> {
    let quota_id = u64::from(btrfs_disk::raw::BTRFS_QUOTA_TREE_OBJECTID);

    if fs.root_bytenr(quota_id).is_some() {
        return Ok(());
    }

    trans
        .create_empty_tree(fs, quota_id)
        .context("post_bootstrap: create_empty_tree(quota tree)")?;

    let generation = trans.transid;

    let flags = if cfg.squota {
        u64::from(btrfs_disk::raw::BTRFS_QGROUP_STATUS_FLAG_ON)
            | u64::from(btrfs_disk::raw::BTRFS_QGROUP_STATUS_FLAG_SIMPLE_MODE)
    } else {
        u64::from(btrfs_disk::raw::BTRFS_QGROUP_STATUS_FLAG_ON)
            | u64::from(btrfs_disk::raw::BTRFS_QGROUP_STATUS_FLAG_INCONSISTENT)
    };
    let enable_gen = if cfg.squota { Some(generation) } else { None };

    // QGROUP_STATUS at (0, STATUS, 0)
    let status_data =
        crate::items::qgroup_status(1, generation, flags, enable_gen);
    let status_key = DiskKey {
        objectid: 0,
        key_type: KeyType::QgroupStatus,
        offset: 0,
    };
    insert_raw_item(
        fs,
        trans,
        quota_id,
        &status_key,
        &status_data,
        "QGROUP_STATUS",
    )?;

    // QGROUP_INFO at (0, INFO, 5) — for the FS tree.
    let fs_tree_qgroupid = u64::from(btrfs_disk::raw::BTRFS_FS_TREE_OBJECTID);
    let info_data = if cfg.squota {
        crate::items::qgroup_info(
            generation,
            u64::from(cfg.nodesize),
            u64::from(cfg.nodesize),
        )
    } else {
        crate::items::qgroup_info_zeroed()
    };
    let info_key = DiskKey {
        objectid: 0,
        key_type: KeyType::QgroupInfo,
        offset: fs_tree_qgroupid,
    };
    insert_raw_item(fs, trans, quota_id, &info_key, &info_data, "QGROUP_INFO")?;

    // QGROUP_LIMIT at (0, LIMIT, 5)
    let limit_data = crate::items::qgroup_limit_zeroed();
    let limit_key = DiskKey {
        objectid: 0,
        key_type: KeyType::QgroupLimit,
        offset: fs_tree_qgroupid,
    };
    insert_raw_item(
        fs,
        trans,
        quota_id,
        &limit_key,
        &limit_data,
        "QGROUP_LIMIT",
    )?;

    Ok(())
}

use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    items::{FileExtentBody, FileExtentItem},
    raw,
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    filesystem::Filesystem,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
};
use clap::Parser;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, Write},
    path::PathBuf,
};

const ROOT_TREE_OBJECTID: u64 = 1;
const FS_TREE_OBJECTID: u64 = raw::BTRFS_FS_TREE_OBJECTID as u64;
const FIRST_FREE_OBJECTID: u64 = raw::BTRFS_FIRST_FREE_OBJECTID as u64;
const LAST_FREE_OBJECTID: u64 =
    (raw::BTRFS_LAST_FREE_OBJECTID as i64).cast_unsigned();
const FREE_INO_OBJECTID: u64 =
    (raw::BTRFS_FREE_INO_OBJECTID as i64).cast_unsigned();
const FREE_SPACE_OBJECTID: u64 =
    (raw::BTRFS_FREE_SPACE_OBJECTID as i64).cast_unsigned();

/// True if `objectid` names a regular filesystem tree (the default
/// subvolume or a user-created subvolume), as opposed to a
/// system/internal tree.
fn is_fs_tree(objectid: u64) -> bool {
    objectid == FS_TREE_OBJECTID
        || (FIRST_FREE_OBJECTID..=LAST_FREE_OBJECTID).contains(&objectid)
}

/// Remove leftover items pertaining to the deprecated inode cache feature
///
/// For every fs tree (the default subvolume and every user
/// subvolume), walks the items keyed under
/// `BTRFS_FREE_INO_OBJECTID` (and historically also
/// `BTRFS_FREE_SPACE_OBJECTID`, which old kernels used for the
/// per-inode cache bitmap), drops every referenced data extent via
/// the delayed-ref queue, and deletes the items themselves. The
/// transaction crate's data-ref drop path also trims the csum tree
/// for any fully-freed extent.
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueClearInoCacheCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

/// One item collected during the read pass.
#[derive(Debug)]
struct InoCacheItem {
    key: DiskKey,
    /// `Some` for `EXTENT_DATA` items that reference a regular extent
    /// (and so need a data-ref drop in the apply pass). `None` for
    /// every other item kind, which only needs deletion.
    extent: Option<ExtentRef>,
}

#[derive(Debug)]
struct ExtentRef {
    disk_bytenr: u64,
    disk_num_bytes: u64,
}

impl Runnable for RescueClearInoCacheCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)
            .with_context(|| {
                format!("failed to open '{}'", self.device.display())
            })?;

        let mut fs = Filesystem::open(file).with_context(|| {
            format!("failed to open filesystem on '{}'", self.device.display())
        })?;

        // Pass 1: enumerate every fs tree and ensure each one is
        // loaded into the in-memory roots map.
        let fs_tree_ids = collect_fs_tree_ids(&mut fs)?;

        // Pass 2: per fs tree, scan + apply within a single
        // transaction so the data-ref drops and item deletions
        // commit atomically.
        let mut total_subvols = 0usize;
        let mut total_items = 0usize;
        let mut total_extents = 0usize;

        for tree_id in fs_tree_ids {
            // Skip subvolumes whose root is not loadable (could be
            // an orphan ROOT_ITEM left by a half-deleted snapshot).
            if fs.root_bytenr(tree_id).is_none() {
                continue;
            }

            let items_to_clear = collect_ino_cache_items(&mut fs, tree_id)?;
            if items_to_clear.is_empty() {
                continue;
            }

            let mut trans = Transaction::start(&mut fs)
                .context("failed to start transaction")?;

            for item in &items_to_clear {
                if let Some(ext) = &item.extent
                    && ext.disk_bytenr != 0
                {
                    trans.delayed_refs.drop_data_ref(
                        ext.disk_bytenr,
                        ext.disk_num_bytes,
                        tree_id,
                        FREE_INO_OBJECTID,
                        0,
                        1,
                    );
                    total_extents += 1;
                }

                delete_one_item(&mut trans, &mut fs, tree_id, &item.key)?;
            }

            trans
                .commit(&mut fs)
                .context("failed to commit transaction")?;
            fs.sync().context("failed to sync to disk")?;

            total_subvols += 1;
            total_items += items_to_clear.len();
        }

        if total_subvols == 0 {
            println!("no inode cache items found on {}", self.device.display());
        } else {
            println!(
                "cleared inode cache on {} ({} subvolume(s), {} item(s), {} data extent(s) freed)",
                self.device.display(),
                total_subvols,
                total_items,
                total_extents,
            );
        }
        Ok(())
    }
}

/// Walk the root tree and return every fs tree objectid.
fn collect_fs_tree_ids<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
) -> Result<Vec<u64>> {
    let start = DiskKey {
        objectid: 0,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs,
        ROOT_TREE_OBJECTID,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .context("failed to walk root tree for ROOT_ITEMs")?;

    let mut ids: Vec<u64> = Vec::new();
    loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break;
        };
        let nritems = leaf.nritems() as usize;
        if path.slots[0] >= nritems {
            if !search::next_leaf(fs, &mut path).context("next_leaf failed")? {
                break;
            }
            continue;
        }
        let key = leaf.item_key(path.slots[0]);
        if key.key_type == KeyType::RootItem && is_fs_tree(key.objectid) {
            // The same fs tree may have multiple ROOT_ITEMs at
            // different offsets (snapshots). Only the canonical one
            // gets registered in the roots map; uniquify here.
            if ids.last().copied() != Some(key.objectid) {
                ids.push(key.objectid);
            }
        }
        path.slots[0] += 1;
    }
    path.release();
    Ok(ids)
}

/// Collect every cache item to delete in `tree_id`. Returns items in
/// the order they appear in the tree.
fn collect_ino_cache_items<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    tree_id: u64,
) -> Result<Vec<InoCacheItem>> {
    let mut out: Vec<InoCacheItem> = Vec::new();
    for objectid in [FREE_INO_OBJECTID, FREE_SPACE_OBJECTID] {
        collect_for_objectid(fs, tree_id, objectid, &mut out)?;
    }
    Ok(out)
}

fn collect_for_objectid<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    tree_id: u64,
    objectid: u64,
    out: &mut Vec<InoCacheItem>,
) -> Result<()> {
    let start = DiskKey {
        objectid,
        key_type: KeyType::from_raw(0),
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    search::search_slot(
        None,
        fs,
        tree_id,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .with_context(|| {
        format!("failed to search tree {tree_id} for objectid {objectid:#x}")
    })?;

    loop {
        let Some(leaf) = path.nodes[0].as_ref() else {
            break;
        };
        let nritems = leaf.nritems() as usize;
        if path.slots[0] >= nritems {
            if !search::next_leaf(fs, &mut path).context("next_leaf failed")? {
                break;
            }
            continue;
        }
        let key = leaf.item_key(path.slots[0]);
        if key.objectid != objectid {
            break;
        }

        let extent = if key.key_type == KeyType::ExtentData {
            let data = leaf.item_data(path.slots[0]);
            let fei = FileExtentItem::parse(data).with_context(|| {
                format!(
                    "failed to parse FILE_EXTENT for ino cache at offset {}",
                    key.offset
                )
            })?;
            match fei.body {
                FileExtentBody::Regular {
                    disk_bytenr,
                    disk_num_bytes,
                    ..
                } => Some(ExtentRef {
                    disk_bytenr,
                    disk_num_bytes,
                }),
                FileExtentBody::Inline { .. } => None,
            }
        } else {
            None
        };

        out.push(InoCacheItem { key, extent });
        path.slots[0] += 1;
    }
    path.release();
    Ok(())
}

/// Delete a single item identified by an exact key. Returns `false`
/// (without erroring) if the item is missing.
fn delete_one_item<R: Read + Write + Seek>(
    trans: &mut Transaction<R>,
    fs: &mut Filesystem<R>,
    tree_id: u64,
    key: &DiskKey,
) -> Result<bool> {
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        Some(trans),
        fs,
        tree_id,
        key,
        &mut path,
        SearchIntent::Delete,
        true,
    )
    .with_context(|| {
        format!("failed to search for {key:?} in tree {tree_id}")
    })?;
    if !found {
        path.release();
        return Ok(false);
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("delete_one_item: no leaf in path"))?;
    items::del_items(leaf, path.slots[0], 1);
    fs.mark_dirty(leaf);
    path.release();
    Ok(true)
}

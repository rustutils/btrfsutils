use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    items::{FileExtentBody, FileExtentItem},
    raw,
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    allocation,
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

/// Free space tree object ID (tree 10).
const FREE_SPACE_TREE_OBJECTID: u64 =
    raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64;

/// Root tree ID.
const ROOT_TREE_OBJECTID: u64 = 1;

/// Special objectid that holds v1 free space cache headers
/// (`BTRFS_FREE_SPACE_OBJECTID` == -11 sign-extended).
const FREE_SPACE_OBJECTID: u64 =
    (raw::BTRFS_FREE_SPACE_OBJECTID as i64).cast_unsigned();

/// The v1 free space header item is stored under key type 0 (no
/// dedicated `KeyType` variant; this matches the kernel and
/// btrfs-progs).
const FREE_SPACE_HEADER_KEY_TYPE: u8 = 0;

/// Free space cache version to clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SpaceCacheVersion {
    V1,
    V2,
}

/// Completely remove the v1 or v2 free space cache
///
/// For v2, drops the FREE_SPACE_TREE root and clears the
/// FREE_SPACE_TREE and FREE_SPACE_TREE_VALID compat_ro flags so that
/// the kernel rebuilds the tree on the next mount with `space_cache=v2`.
///
/// For v1, see Stage G in transaction/PLAN.md: clearing v1 requires
/// freeing data extents owned by the hidden free-space-cache inodes,
/// which the transaction crate does not yet support.
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueClearSpaceCacheCommand {
    /// Free space cache version to remove
    version: SpaceCacheVersion,

    /// Path to the btrfs device
    device: PathBuf,
}

/// Recursively walk a tree starting at `bytenr`, collecting every block
/// address (root, internal nodes, leaves).
fn collect_tree_blocks<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    bytenr: u64,
    out: &mut Vec<(u64, u8)>,
) -> Result<()> {
    let eb = fs
        .read_block(bytenr)
        .with_context(|| format!("failed to read tree block at {bytenr}"))?;
    let level = eb.level();
    out.push((bytenr, level));

    if eb.is_node() {
        let nritems = eb.nritems() as usize;
        for slot in 0..nritems {
            let child = eb.key_ptr_blockptr(slot);
            collect_tree_blocks(fs, child, out)?;
        }
    }
    Ok(())
}

/// Look up the `FREE_SPACE_HEADER` for one block group and, if
/// present, walk the cache inode's `EXTENT_DATA` items to collect a
/// [`V1CacheEntry`].
fn read_v1_cache_entry<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    bg_start: u64,
) -> Result<Option<V1CacheEntry>> {
    // Step 1: find the FREE_SPACE_HEADER and parse the embedded
    // location disk_key to get the inode number.
    let header_key = DiskKey {
        objectid: FREE_SPACE_OBJECTID,
        key_type: KeyType::from_raw(FREE_SPACE_HEADER_KEY_TYPE),
        offset: bg_start,
    };
    let mut path = BtrfsPath::new();
    let found = search::search_slot(
        None,
        fs,
        ROOT_TREE_OBJECTID,
        &header_key,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .context("failed to search root tree for v1 cache header")?;
    if !found {
        path.release();
        return Ok(None);
    }
    let leaf = path.nodes[0]
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no leaf in v1 header path"))?;
    let payload = leaf.item_data(path.slots[0]);
    if payload.len() < 17 {
        path.release();
        bail!("FREE_SPACE_HEADER for bg {bg_start} truncated");
    }
    // The header begins with a btrfs_disk_key (17 bytes):
    //   u64 objectid | u8 type | u64 offset
    let ino = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    path.release();

    // Step 2: walk the cache inode's EXTENT_DATA items in the root
    // tree, recording (file_offset, disk_bytenr, disk_num_bytes) for
    // each non-inline regular extent.
    let mut extents: Vec<V1Extent> = Vec::new();

    let start = DiskKey {
        objectid: ino,
        key_type: KeyType::ExtentData,
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
    .context("failed to search root tree for v1 cache extents")?;

    'walk: while let Some(leaf) = path.nodes[0].as_ref() {
        let nritems = leaf.nritems() as usize;
        if path.slots[0] >= nritems {
            if !search::next_leaf(fs, &mut path).context("next_leaf failed")? {
                break;
            }
            continue;
        }
        let key = leaf.item_key(path.slots[0]);
        if key.objectid != ino {
            break 'walk;
        }
        if key.key_type == KeyType::ExtentData {
            let data = leaf.item_data(path.slots[0]);
            let fei = FileExtentItem::parse(data).ok_or_else(|| {
                anyhow::anyhow!(
                    "malformed FILE_EXTENT for v1 cache inode {ino} offset {}",
                    key.offset
                )
            })?;
            match fei.body {
                FileExtentBody::Regular {
                    disk_bytenr,
                    disk_num_bytes,
                    ..
                } => {
                    extents.push(V1Extent {
                        file_offset: key.offset,
                        disk_bytenr,
                        disk_num_bytes,
                    });
                }
                FileExtentBody::Inline { .. } => {
                    // Inline extents have no separate data extent;
                    // record with disk_bytenr=0 so the apply pass
                    // still deletes the EXTENT_DATA item but skips
                    // the data ref drop.
                    extents.push(V1Extent {
                        file_offset: key.offset,
                        disk_bytenr: 0,
                        disk_num_bytes: 0,
                    });
                }
            }
        }
        path.slots[0] += 1;
    }
    path.release();

    Ok(Some(V1CacheEntry { ino, extents }))
}

/// Delete a single item identified by an exact key. Returns `false`
/// (without erroring) if the item is missing, matching the C
/// reference's tolerant behaviour for the cache inode item.
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

impl Runnable for RescueClearSpaceCacheCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        match self.version {
            SpaceCacheVersion::V1 => self.clear_v1(),
            SpaceCacheVersion::V2 => self.clear_v2(),
        }
    }
}

/// One free space cache file referenced from a block group's
/// `FREE_SPACE_HEADER`. Collected during the read pass and consumed
/// (via fresh COW searches) during the apply pass.
struct V1CacheEntry {
    /// Inode number that holds the cache file in the root tree.
    ino: u64,
    /// File-extent records found under that inode.
    extents: Vec<V1Extent>,
}

struct V1Extent {
    /// File offset key for this `EXTENT_DATA` item.
    file_offset: u64,
    /// Disk bytenr of the referenced extent (0 = hole/inline, skip).
    disk_bytenr: u64,
    /// On-disk byte length of the referenced extent.
    disk_num_bytes: u64,
}

impl RescueClearSpaceCacheCommand {
    /// Clear the v1 free space cache: for every block group, find its
    /// `FREE_SPACE_HEADER` in the root tree, free the data extents
    /// owned by the cache inode, and delete the cache items
    /// (`FREE_SPACE_HEADER`, `EXTENT_DATA`s, `INODE_ITEM`).
    ///
    /// Mirrors the algorithm in `btrfs_clear_free_space_cache` from
    /// btrfs-progs `kernel-shared/free-space-cache.c`, except that
    /// the entire run happens in a single transaction (the C code
    /// commits in clusters of 16 block groups for very large
    /// filesystems).
    fn clear_v1(&self) -> Result<()> {
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

        // Pass 1: scan every block group for a v1 cache header and
        // collect the work list before mutating anything.
        let block_groups = allocation::load_block_groups(&mut fs)
            .context("failed to load block groups")?;

        let mut entries: Vec<(u64, V1CacheEntry)> = Vec::new();
        for bg in &block_groups {
            if let Some(entry) = read_v1_cache_entry(&mut fs, bg.start)? {
                entries.push((bg.start, entry));
            }
        }

        if entries.is_empty() {
            // Still bump cache_generation so the kernel knows the v1
            // cache is invalidated even on filesystems where it was
            // never written.
            if fs.superblock.cache_generation != u64::MAX {
                let trans = Transaction::start(&mut fs)
                    .context("failed to start transaction")?;
                fs.superblock.cache_generation = u64::MAX;
                trans
                    .commit(&mut fs)
                    .context("failed to commit transaction")?;
                fs.sync().context("failed to sync to disk")?;
            }
            println!(
                "no v1 free space cache found on {}",
                self.device.display()
            );
            return Ok(());
        }

        // Pass 2: apply.
        let mut trans = Transaction::start(&mut fs)
            .context("failed to start transaction")?;

        let mut total_extents_freed: usize = 0;
        for (bg_start, entry) in &entries {
            // Drop data refs first so the EXTENT_ITEMs are reclaimed
            // when the delayed refs flush.
            for ext in &entry.extents {
                if ext.disk_bytenr == 0 {
                    continue; // hole or inline; nothing to free
                }
                trans.delayed_refs.drop_data_ref(
                    ext.disk_bytenr,
                    ext.disk_num_bytes,
                    ROOT_TREE_OBJECTID,
                    entry.ino,
                    ext.file_offset,
                    1,
                );
                total_extents_freed += 1;
            }

            // Delete the FREE_SPACE_HEADER for this block group.
            delete_one_item(
                &mut trans,
                &mut fs,
                ROOT_TREE_OBJECTID,
                &DiskKey {
                    objectid: FREE_SPACE_OBJECTID,
                    key_type: KeyType::from_raw(FREE_SPACE_HEADER_KEY_TYPE),
                    offset: *bg_start,
                },
            )?;

            // Delete every EXTENT_DATA item for the cache inode.
            for ext in &entry.extents {
                delete_one_item(
                    &mut trans,
                    &mut fs,
                    ROOT_TREE_OBJECTID,
                    &DiskKey {
                        objectid: entry.ino,
                        key_type: KeyType::ExtentData,
                        offset: ext.file_offset,
                    },
                )?;
            }

            // Delete the INODE_ITEM (matches btrfs-progs which warns
            // and continues if the item is missing).
            let _ = delete_one_item(
                &mut trans,
                &mut fs,
                ROOT_TREE_OBJECTID,
                &DiskKey {
                    objectid: entry.ino,
                    key_type: KeyType::InodeItem,
                    offset: 0,
                },
            );
        }

        // Mark the v1 cache as fully invalidated so the kernel won't
        // try to load any leftover bits.
        fs.superblock.cache_generation = u64::MAX;

        trans
            .commit(&mut fs)
            .context("failed to commit transaction")?;
        fs.sync().context("failed to sync to disk")?;

        println!(
            "cleared v1 free space cache on {} ({} block group(s), {} data extent(s) freed)",
            self.device.display(),
            entries.len(),
            total_extents_freed,
        );
        Ok(())
    }

    fn clear_v2(&self) -> Result<()> {
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

        let fst_flag = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE);
        let fst_valid_flag =
            u64::from(raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID);
        let bgt_flag = u64::from(raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE);

        if fs.superblock.compat_ro_flags & bgt_flag != 0 {
            bail!(
                "cannot clear free space tree: filesystem has block-group-tree \
                 enabled, which requires free-space-tree to mount"
            );
        }

        if fs.superblock.compat_ro_flags & fst_flag == 0 {
            println!("no free space tree to clear");
            return Ok(());
        }

        let Some(fst_bytenr) = fs.root_bytenr(FREE_SPACE_TREE_OBJECTID) else {
            // The compat_ro bit was set but no root pointer exists.
            // Just clear the flags and write the superblock.
            fs.superblock.compat_ro_flags &= !(fst_flag | fst_valid_flag);
            let trans = Transaction::start(&mut fs)
                .context("failed to start transaction")?;
            trans
                .commit(&mut fs)
                .context("failed to commit transaction")?;
            fs.sync().context("failed to sync to disk")?;
            println!("cleared free space tree compat_ro flags");
            return Ok(());
        };

        let mut tree_blocks = Vec::new();
        collect_tree_blocks(&mut fs, fst_bytenr, &mut tree_blocks)
            .context("failed to walk free space tree")?;

        // Clear the compat_ro bits BEFORE the commit so the new
        // superblock written at the end of commit no longer advertises
        // an FST.
        fs.superblock.compat_ro_flags &= !(fst_flag | fst_valid_flag);

        let mut trans = Transaction::start(&mut fs)
            .context("failed to start transaction")?;

        for &(bytenr, level) in &tree_blocks {
            trans.delayed_refs.drop_ref(
                bytenr,
                true,
                FREE_SPACE_TREE_OBJECTID,
                level,
            );
            trans.pin_block(bytenr);
            fs.evict_block(bytenr);
        }

        // Delete the ROOT_ITEM for the FST from the root tree.
        let root_key = DiskKey {
            objectid: FREE_SPACE_TREE_OBJECTID,
            key_type: KeyType::RootItem,
            offset: 0,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            &mut fs,
            ROOT_TREE_OBJECTID,
            &root_key,
            &mut path,
            SearchIntent::Delete,
            true,
        )
        .context("failed to search root tree for free space tree entry")?;

        if found {
            let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                anyhow::anyhow!("no leaf in path for root tree deletion")
            })?;
            items::del_items(leaf, path.slots[0], 1);
            fs.mark_dirty(leaf);
        }
        path.release();

        // Drop the FST from the in-memory roots map so the commit's
        // update_free_space_tree pass early-returns (no FST root) and
        // the commit doesn't try to write a ROOT_ITEM we just deleted.
        fs.remove_root(FREE_SPACE_TREE_OBJECTID);

        trans
            .commit(&mut fs)
            .context("failed to commit transaction")?;
        fs.sync().context("failed to sync to disk")?;

        println!(
            "cleared free space tree on {} ({} blocks freed), kernel will rebuild it on next mount",
            self.device.display(),
            tree_blocks.len()
        );
        Ok(())
    }
}

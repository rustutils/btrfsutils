use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
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

/// Free space tree object ID (tree 10).
const FREE_SPACE_TREE_OBJECTID: u64 =
    raw::BTRFS_FREE_SPACE_TREE_OBJECTID as u64;

/// Root tree ID.
const ROOT_TREE_OBJECTID: u64 = 1;

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

impl Runnable for RescueClearSpaceCacheCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        match self.version {
            SpaceCacheVersion::V1 => bail!(
                "clearing v1 free space cache is not yet implemented: \
                 it requires data extent reference support in the \
                 transaction crate (see Stage G in transaction/PLAN.md)"
            ),
            SpaceCacheVersion::V2 => self.clear_v2(),
        }
    }
}

impl RescueClearSpaceCacheCommand {
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

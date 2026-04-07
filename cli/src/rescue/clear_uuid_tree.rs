use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::tree::{DiskKey, KeyType};
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

/// UUID tree object ID (tree 9).
const UUID_TREE_OBJECTID: u64 =
    btrfs_disk::raw::BTRFS_UUID_TREE_OBJECTID as u64;

/// Root tree ID.
const ROOT_TREE_OBJECTID: u64 = 1;

/// Delete uuid tree so that kernel can rebuild it at mount time
///
/// The UUID tree maps subvolume UUIDs to their root IDs. If it becomes
/// corrupted it can prevent mount or cause subvolume lookup failures.
/// Deleting it is safe because the kernel rebuilds it automatically on
/// the next mount.
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueClearUuidTreeCommand {
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

impl Runnable for RescueClearUuidTreeCommand {
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

        let Some(uuid_root_bytenr) = fs.root_bytenr(UUID_TREE_OBJECTID) else {
            println!("no uuid tree found, nothing to do");
            return Ok(());
        };

        // Walk the UUID tree to collect every block address (and level).
        // We don't need to delete items: we just need to drop the extent
        // refs for every block and remove the ROOT_ITEM. The kernel will
        // rebuild the tree on next mount.
        let mut tree_blocks = Vec::new();
        collect_tree_blocks(&mut fs, uuid_root_bytenr, &mut tree_blocks)
            .context("failed to walk uuid tree")?;

        let mut trans = Transaction::start(&mut fs)
            .context("failed to start transaction")?;

        // Queue a delayed ref drop for every block in the UUID tree, and
        // pin them so the allocator doesn't reuse the addresses before
        // commit.
        for &(bytenr, level) in &tree_blocks {
            trans.delayed_refs.drop_ref(
                bytenr,
                true,
                UUID_TREE_OBJECTID,
                level,
            );
            trans.pin_block(bytenr);
            fs.evict_block(bytenr);
        }

        // Delete the ROOT_ITEM for the UUID tree from the root tree.
        let root_key = DiskKey {
            objectid: UUID_TREE_OBJECTID,
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
        .context("failed to search root tree for uuid tree entry")?;

        if found {
            let leaf = path.nodes[0].as_mut().ok_or_else(|| {
                anyhow::anyhow!("no leaf in path for root tree deletion")
            })?;
            items::del_items(leaf, path.slots[0], 1);
            fs.mark_dirty(leaf);
        }
        path.release();

        // Remove the UUID tree from the in-memory roots map so commit
        // doesn't try to update a ROOT_ITEM we just deleted.
        fs.remove_root(UUID_TREE_OBJECTID);

        trans
            .commit(&mut fs)
            .context("failed to commit transaction")?;
        fs.sync().context("failed to sync to disk")?;

        println!(
            "Cleared uuid tree on {} ({} blocks freed), kernel will rebuild it on next mount",
            self.device.display(),
            tree_blocks.len()
        );
        Ok(())
    }
}

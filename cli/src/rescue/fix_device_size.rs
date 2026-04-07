use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    items::{DeviceExtent, DeviceItem},
    raw,
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    filesystem::Filesystem,
    path::BtrfsPath,
    search::{self, SearchIntent, next_leaf},
    transaction::Transaction,
};
use clap::Parser;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, Write},
    os::{fd::AsFd, unix::fs::FileTypeExt},
    path::PathBuf,
};

/// Tree id constants used here.
const CHUNK_TREE_OBJECTID: u64 = raw::BTRFS_CHUNK_TREE_OBJECTID as u64;
const DEV_TREE_OBJECTID: u64 = raw::BTRFS_DEV_TREE_OBJECTID as u64;
/// Special objectid that holds DEV_ITEM keys in the chunk tree.
const DEV_ITEMS_OBJECTID: u64 = raw::BTRFS_DEV_ITEMS_OBJECTID as u64;

/// Byte offset of `total_bytes` inside the on-disk `btrfs_dev_item`
/// (after the leading u64 `devid`).
const DEV_ITEM_TOTAL_BYTES_OFFSET: usize = 8;

/// Re-align device and super block sizes
///
/// Recomputes each device's `total_bytes` from its physical size and
/// the device extent layout, updates the corresponding `DEV_ITEM` in
/// the chunk tree (and the embedded `dev_item` in the superblock),
/// and rewrites the superblock's `total_bytes` to match the sum.
///
/// Cases handled:
///
/// - `dev_item.total_bytes` is misaligned to `sectorsize`: round it
///   down.
/// - `dev_item.total_bytes` is larger than the underlying block
///   device or backing file: shrink it to the actual size, but only
///   if no `DEV_EXTENT` covers or extends past that boundary
///   (otherwise we'd lose data).
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueFixDeviceSizeCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

/// One device's worth of state collected during the read pass.
struct DeviceFix {
    devid: u64,
    old_total: u64,
    new_total: u64,
}

/// Read the size in bytes of a block device or regular file.
fn underlying_size(file: &std::fs::File) -> Result<u64> {
    let meta = file.metadata().context("failed to stat device")?;
    if meta.file_type().is_block_device() {
        let size = btrfs_uapi::blkdev::device_size(file.as_fd())
            .context("BLKGETSIZE64 failed")?;
        Ok(size)
    } else {
        Ok(meta.len())
    }
}

/// Find the largest `(offset + length)` of any DEV_EXTENT belonging
/// to `devid` in the dev tree, or 0 if there are no extents.
fn last_dev_extent_end<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    devid: u64,
) -> Result<u64> {
    let start = DiskKey {
        objectid: devid,
        key_type: KeyType::DeviceExtent,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let _ = search::search_slot(
        None,
        fs,
        DEV_TREE_OBJECTID,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .context("failed to search dev tree")?;

    let mut max_end: u64 = 0;
    'outer: loop {
        {
            let Some(leaf) = path.nodes[0].as_ref() else {
                break;
            };
            let nritems = leaf.nritems() as usize;
            while path.slots[0] < nritems {
                let key = leaf.item_key(path.slots[0]);
                if key.objectid != devid
                    || key.key_type != KeyType::DeviceExtent
                {
                    break 'outer;
                }
                let dext = DeviceExtent::parse(leaf.item_data(path.slots[0]))
                    .ok_or_else(|| {
                    anyhow::anyhow!(
                        "failed to parse DEV_EXTENT at devid {devid} offset {}",
                        key.offset
                    )
                })?;
                let end = key.offset.saturating_add(dext.length);
                if end > max_end {
                    max_end = end;
                }
                path.slots[0] += 1;
            }
        }
        if !next_leaf(fs, &mut path).context("next_leaf failed")? {
            break;
        }
    }
    Ok(max_end)
}

/// Read pass: collect every DEV_ITEM in the chunk tree, decide
/// whether each needs fixing.
fn collect_device_fixes<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    actual_size: u64,
    sectorsize: u64,
) -> Result<Vec<DeviceFix>> {
    let start = DiskKey {
        objectid: DEV_ITEMS_OBJECTID,
        key_type: KeyType::DeviceItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let _ = search::search_slot(
        None,
        fs,
        CHUNK_TREE_OBJECTID,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )
    .context("failed to search chunk tree for DEV_ITEMs")?;

    let mut raw_items: Vec<(u64, u64)> = Vec::new();
    'outer: loop {
        {
            let Some(leaf) = path.nodes[0].as_ref() else {
                break;
            };
            let nritems = leaf.nritems() as usize;
            while path.slots[0] < nritems {
                let key = leaf.item_key(path.slots[0]);
                if key.objectid != DEV_ITEMS_OBJECTID {
                    break 'outer;
                }
                if key.key_type == KeyType::DeviceItem {
                    let di = DeviceItem::parse(leaf.item_data(path.slots[0]))
                        .ok_or_else(|| {
                        anyhow::anyhow!(
                            "failed to parse DEV_ITEM for devid {}",
                            key.offset
                        )
                    })?;
                    raw_items.push((di.devid, di.total_bytes));
                }
                path.slots[0] += 1;
            }
        }
        if !next_leaf(fs, &mut path).context("next_leaf failed")? {
            break;
        }
    }
    path.release();

    let mut out = Vec::new();
    for (devid, old_total) in raw_items {
        let mut new_total = old_total;

        if new_total % sectorsize != 0 {
            new_total -= new_total % sectorsize;
        }

        if new_total > actual_size {
            let extent_end = last_dev_extent_end(fs, devid)?;
            if extent_end > actual_size {
                bail!(
                    "devid {devid}: cannot shrink total_bytes from {old_total} \
                     to {actual_size}: a DEV_EXTENT covers up to {extent_end}, \
                     which is past the actual device size",
                );
            }
            new_total = actual_size - (actual_size % sectorsize);
        }

        if new_total != old_total {
            out.push(DeviceFix {
                devid,
                old_total,
                new_total,
            });
        }
    }

    Ok(out)
}

/// Sum the (corrected) `total_bytes` of every device in the chunk
/// tree. For devices listed in `fixes`, use the new value; for the
/// rest, use the on-disk value as-is.
fn sum_corrected_total_bytes<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    fixes: &[DeviceFix],
) -> Result<u64> {
    let start = DiskKey {
        objectid: DEV_ITEMS_OBJECTID,
        key_type: KeyType::DeviceItem,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let _ = search::search_slot(
        None,
        fs,
        CHUNK_TREE_OBJECTID,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;

    let mut total: u64 = 0;
    'outer: loop {
        {
            let Some(leaf) = path.nodes[0].as_ref() else {
                break;
            };
            let nritems = leaf.nritems() as usize;
            while path.slots[0] < nritems {
                let key = leaf.item_key(path.slots[0]);
                if key.objectid != DEV_ITEMS_OBJECTID {
                    break 'outer;
                }
                if key.key_type == KeyType::DeviceItem {
                    let di = DeviceItem::parse(leaf.item_data(path.slots[0]))
                        .ok_or_else(|| {
                        anyhow::anyhow!(
                            "failed to parse DEV_ITEM for devid {}",
                            key.offset
                        )
                    })?;
                    let value = fixes
                        .iter()
                        .find(|f| f.devid == di.devid)
                        .map_or(di.total_bytes, |f| f.new_total);
                    total = total.saturating_add(value);
                }
                path.slots[0] += 1;
            }
        }
        if !next_leaf(fs, &mut path)? {
            break;
        }
    }
    path.release();
    Ok(total)
}

impl Runnable for RescueFixDeviceSizeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        // The read pass and the algorithm below are correct, but
        // committing requires COWing the chunk tree. The transaction
        // crate's allocator always pulls from a metadata block group
        // (alloc_block in transaction.rs), so the new chunk-root
        // block ends up outside any SYSTEM chunk in `sys_chunk_array`
        // and the next mount cannot resolve it. See "Stage H — chunk
        // tree COW" in transaction/PLAN.md.
        bail!(
            "fix-device-size is not yet implemented: chunk tree COW \
             requires SYSTEM-block-group allocation in the transaction \
             crate (see Stage H in transaction/PLAN.md)"
        );

        #[allow(unreachable_code)]
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        #[allow(unreachable_code)]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)
            .with_context(|| {
                format!("failed to open '{}'", self.device.display())
            })?;

        let actual_size =
            underlying_size(&file).context("failed to get device size")?;

        let mut fs = Filesystem::open(file).with_context(|| {
            format!("failed to open filesystem on '{}'", self.device.display())
        })?;

        let sectorsize = u64::from(fs.superblock.sectorsize);
        let actual_aligned = actual_size - (actual_size % sectorsize);

        let fixes = collect_device_fixes(&mut fs, actual_aligned, sectorsize)?;

        let new_super_total = sum_corrected_total_bytes(&mut fs, &fixes)?;
        let old_super_total = fs.superblock.total_bytes;

        if fixes.is_empty() && new_super_total == old_super_total {
            println!("no device size related problem found");
            return Ok(());
        }

        let mut trans = Transaction::start(&mut fs)
            .context("failed to start transaction")?;

        // Apply each per-device fix.
        for fix in &fixes {
            let key = DiskKey {
                objectid: DEV_ITEMS_OBJECTID,
                key_type: KeyType::DeviceItem,
                offset: fix.devid,
            };
            let mut path = BtrfsPath::new();
            let found = search::search_slot(
                Some(&mut trans),
                &mut fs,
                CHUNK_TREE_OBJECTID,
                &key,
                &mut path,
                SearchIntent::ReadOnly,
                true,
            )
            .with_context(|| {
                format!("failed to search DEV_ITEM for devid {}", fix.devid)
            })?;
            if !found {
                bail!("DEV_ITEM for devid {} disappeared", fix.devid);
            }
            {
                let leaf = path.nodes[0].as_mut().unwrap();
                let data = leaf.item_data_mut(path.slots[0]);
                let off = DEV_ITEM_TOTAL_BYTES_OFFSET;
                data[off..off + 8]
                    .copy_from_slice(&fix.new_total.to_le_bytes());
                fs.mark_dirty(leaf);
            }
            path.release();

            // Mirror the change into the superblock's embedded
            // dev_item if this devid matches.
            if fs.superblock.dev_item.devid == fix.devid {
                fs.superblock.dev_item.total_bytes = fix.new_total;
            }
            println!(
                "devid {}: total_bytes {} -> {}",
                fix.devid, fix.old_total, fix.new_total
            );
        }

        if new_super_total != old_super_total {
            fs.superblock.total_bytes = new_super_total;
            println!(
                "superblock total_bytes {old_super_total} -> {new_super_total}"
            );
        }

        trans
            .commit(&mut fs)
            .context("failed to commit transaction")?;
        fs.sync().context("failed to sync to disk")?;

        println!("device size fix-up complete");
        Ok(())
    }
}

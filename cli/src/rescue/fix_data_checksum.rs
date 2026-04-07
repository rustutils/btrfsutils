use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    raw,
    superblock::ChecksumType,
    tree::{DiskKey, KeyType},
    util::btrfs_csum_data,
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
    path::PathBuf,
};

/// Csum tree id.
const CSUM_TREE_OBJECTID: u64 = raw::BTRFS_CSUM_TREE_OBJECTID as u64;
/// Special objectid that holds EXTENT_CSUM keys in the csum tree
/// (binds as `i32 = -10`; the kernel treats it as `-10ULL`, i.e.
/// `0xFFFFFFFF_FFFFFFF6`).
#[allow(clippy::cast_sign_loss)]
const EXTENT_CSUM_OBJECTID: u64 = raw::BTRFS_EXTENT_CSUM_OBJECTID as u64;

/// Fix data checksum mismatches
///
/// Walks the csum tree, recomputes the CRC32C of every covered data
/// block, and reports any mismatches. With `--mirror N`, mismatched
/// csums are rewritten in place using the data from mirror N (only
/// mirror 1 is currently supported, since multi-device write paths
/// are not yet implemented).
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueFixDataChecksumCommand {
    /// Device to operate on
    device: PathBuf,

    /// Readonly mode, only report errors without repair
    #[clap(short, long)]
    readonly: bool,

    /// Interactive mode, ignore the error by default
    #[clap(short, long)]
    interactive: bool,

    /// Update csum item using specified mirror
    #[clap(short, long)]
    mirror: Option<u32>,
}

/// One detected csum mismatch.
struct Mismatch {
    /// Logical address of the mismatching sector.
    logical: u64,
    /// New (computed) checksum bytes that should replace the stored one.
    new_csum: Vec<u8>,
}

impl Runnable for RescueFixDataChecksumCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if self.interactive {
            bail!("--interactive mode is not yet implemented");
        }
        if let Some(m) = self.mirror
            && m != 1
        {
            bail!(
                "only mirror 1 is supported (multi-device write paths are not implemented)"
            );
        }
        let repair = !self.readonly && self.mirror.is_some();

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

        let csum_type = fs.superblock.csum_type;
        if !matches!(csum_type, ChecksumType::Crc32) {
            bail!(
                "unsupported csum type {csum_type:?}: only CRC32C is supported",
            );
        }
        let csum_size = csum_type.size();
        let sectorsize = u64::from(fs.superblock.sectorsize);

        let mismatches = scan_csum_tree(&mut fs, csum_size, sectorsize)
            .context("failed to scan csum tree")?;

        if mismatches.is_empty() {
            println!("no data checksum mismatch found");
            return Ok(());
        }

        for m in &mismatches {
            println!("logical={} csum mismatch", m.logical);
        }

        if !repair {
            println!(
                "{} mismatch(es) found; rerun with --mirror 1 to repair",
                mismatches.len()
            );
            return Ok(());
        }

        apply_csum_updates(&mut fs, &mismatches, csum_size, sectorsize)
            .context("failed to apply csum updates")?;
        fs.sync().context("failed to sync to disk")?;

        println!(
            "{} csum item(s) updated using data from mirror 1",
            mismatches.len()
        );
        Ok(())
    }
}

/// Walk the csum tree, verifying every per-sector csum and collecting
/// mismatches.
fn scan_csum_tree<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    csum_size: usize,
    sectorsize: u64,
) -> Result<Vec<Mismatch>> {
    let start = DiskKey {
        objectid: EXTENT_CSUM_OBJECTID,
        key_type: KeyType::ExtentCsum,
        offset: 0,
    };
    let mut path = BtrfsPath::new();
    let _ = search::search_slot(
        None,
        fs,
        CSUM_TREE_OBJECTID,
        &start,
        &mut path,
        SearchIntent::ReadOnly,
        false,
    )?;

    // Collect (logical, stored_csum_bytes) pairs first so we don't
    // hold a borrow on the path while reading data.
    let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
    'outer: loop {
        {
            let Some(leaf) = path.nodes[0].as_ref() else {
                break;
            };
            let nritems = leaf.nritems() as usize;
            while path.slots[0] < nritems {
                let key = leaf.item_key(path.slots[0]);
                if key.key_type != KeyType::ExtentCsum {
                    if key.objectid != EXTENT_CSUM_OBJECTID {
                        break 'outer;
                    }
                    path.slots[0] += 1;
                    continue;
                }
                let data = leaf.item_data(path.slots[0]);
                // item_size = csum_size * (covered_sectors)
                let nsectors = data.len() / csum_size;
                for i in 0..nsectors {
                    let logical = key.offset + (i as u64) * sectorsize;
                    let stored =
                        data[i * csum_size..(i + 1) * csum_size].to_vec();
                    entries.push((logical, stored));
                }
                path.slots[0] += 1;
            }
        }
        if !next_leaf(fs, &mut path)? {
            break;
        }
    }
    path.release();

    let mut mismatches = Vec::new();
    let sector_usize = usize::try_from(sectorsize).unwrap();
    for (logical, stored) in entries {
        let buf = fs
            .reader_mut()
            .read_data(logical, sector_usize)
            .with_context(|| {
                format!("failed to read data at logical {logical}")
            })?;
        let computed = btrfs_csum_data(&buf).to_le_bytes();
        if computed[..csum_size] != stored[..] {
            mismatches.push(Mismatch {
                logical,
                new_csum: computed[..csum_size].to_vec(),
            });
        }
    }

    Ok(mismatches)
}

/// Apply csum updates to the csum tree. For each mismatch, search the
/// csum tree for the item containing the logical address, locate the
/// per-sector slot inside it, and rewrite the bytes in place.
fn apply_csum_updates<R: Read + Write + Seek>(
    fs: &mut Filesystem<R>,
    mismatches: &[Mismatch],
    csum_size: usize,
    sectorsize: u64,
) -> Result<()> {
    let mut trans =
        Transaction::start(fs).context("failed to start transaction")?;
    for m in mismatches {
        let key = DiskKey {
            objectid: EXTENT_CSUM_OBJECTID,
            key_type: KeyType::ExtentCsum,
            offset: m.logical,
        };
        let mut path = BtrfsPath::new();
        let found = search::search_slot(
            Some(&mut trans),
            fs,
            CSUM_TREE_OBJECTID,
            &key,
            &mut path,
            SearchIntent::ReadOnly,
            true,
        )?;

        // The csum item key.offset is the logical address of the
        // *first* sector in the item; if our exact key wasn't found,
        // step back one slot to land on the containing item.
        if !found && path.slots[0] > 0 {
            path.slots[0] -= 1;
        }

        let leaf = path.nodes[0].as_mut().ok_or_else(|| {
            anyhow::anyhow!(
                "no leaf in path for csum update at logical {}",
                m.logical
            )
        })?;
        let item_key = leaf.item_key(path.slots[0]);
        if item_key.key_type != KeyType::ExtentCsum {
            bail!("no EXTENT_CSUM item containing logical {}", m.logical);
        }
        let item_size = leaf.item_size(path.slots[0]) as usize;
        let nsectors = item_size / csum_size;
        let item_first = item_key.offset;
        let item_last_excl = item_first + (nsectors as u64) * sectorsize;
        if m.logical < item_first || m.logical >= item_last_excl {
            bail!(
                "csum item at logical {} does not cover {}",
                item_first,
                m.logical
            );
        }
        let sector_index =
            usize::try_from((m.logical - item_first) / sectorsize).unwrap();
        let off = sector_index * csum_size;
        let data = leaf.item_data_mut(path.slots[0]);
        data[off..off + csum_size].copy_from_slice(&m.new_csum);
        fs.mark_dirty(leaf);
        path.release();
    }
    trans.commit(fs).context("failed to commit transaction")?;
    Ok(())
}

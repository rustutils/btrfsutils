use super::{ReconstructionResult, ScanResult};
use anyhow::{Context, Result};
use btrfs_disk::{
    chunk::{ChunkTreeCache, chunk_item_bytes},
    items::BlockGroupFlags,
    raw,
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    Filesystem, Transaction,
    buffer::ITEM_SIZE,
    items,
    path::BtrfsPath,
    search::{self, SearchIntent},
};
use std::fs::File;

/// Chunk tree object ID.
const CHUNK_TREE_OBJECTID: u64 = raw::BTRFS_CHUNK_TREE_OBJECTID as u64;

/// Write the reconstructed chunk tree to disk.
///
/// Opens the filesystem with the reconstructed chunk cache (bypassing
/// the broken on-disk chunk tree), creates a fresh chunk tree via the
/// transaction crate, inserts all recovered DEV_ITEM and CHUNK_ITEM
/// records, and commits.
pub fn apply_chunk_tree(
    file: File,
    scan: &ScanResult,
    result: &ReconstructionResult,
) -> Result<()> {
    // Build a ChunkTreeCache from the reconstructed chunks.
    let chunk_cache = build_chunk_cache(result);

    // Identify SYSTEM chunks and serialize them for sys_chunk_array.
    let sectorsize = scan.nodesize.min(4096);
    let system_chunks: Vec<(u64, Vec<u8>)> = result
        .chunks
        .iter()
        .filter(|c| c.chunk.chunk_type.contains(BlockGroupFlags::SYSTEM))
        .map(|c| {
            let mapping = c.chunk.to_mapping(c.logical);
            let bytes = chunk_item_bytes(&mapping, sectorsize);
            (c.logical, bytes)
        })
        .collect();

    // Open filesystem with the reconstructed cache.
    let mut fs = Filesystem::open_with_chunk_cache(file, 0, chunk_cache)
        .context("failed to open filesystem with reconstructed chunk cache")?;

    // Start a transaction.
    let mut trans =
        Transaction::start(&mut fs).context("failed to start transaction")?;

    // Create a fresh empty chunk tree.
    trans
        .rebuild_chunk_tree(&mut fs, &system_chunks)
        .context("failed to create new chunk tree")?;

    // Insert all DEV_ITEM records.
    for dev in &result.devices {
        let key = DiskKey {
            objectid: u64::from(raw::BTRFS_DEV_ITEMS_OBJECTID),
            key_type: KeyType::DeviceItem,
            offset: dev.devid,
        };
        let mut data = Vec::new();
        dev.device.write_bytes(&mut data);
        insert_in_chunk_tree(&mut trans, &mut fs, &key, &data)?;
    }

    // Insert all CHUNK_ITEM records in logical order (already sorted).
    for chunk in &result.chunks {
        let key = DiskKey {
            objectid: u64::from(raw::BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            key_type: KeyType::ChunkItem,
            offset: chunk.logical,
        };
        let mapping = chunk.chunk.to_mapping(chunk.logical);
        let data = chunk_item_bytes(&mapping, sectorsize);
        insert_in_chunk_tree(&mut trans, &mut fs, &key, &data)?;
    }

    // Commit the transaction (updates superblock chunk_root etc.).
    trans
        .commit(&mut fs)
        .context("failed to commit chunk tree rebuild")?;

    // Sync to disk.
    fs.sync().context("failed to sync filesystem")?;

    Ok(())
}

/// Build a `ChunkTreeCache` from the reconstructed chunk records.
fn build_chunk_cache(result: &ReconstructionResult) -> ChunkTreeCache {
    let mut cache = ChunkTreeCache::default();
    for chunk in &result.chunks {
        let mapping = chunk.chunk.to_mapping(chunk.logical);
        cache.insert(mapping);
    }
    cache
}

/// Insert an item into the chunk tree (tree ID 3).
fn insert_in_chunk_tree(
    trans: &mut Transaction<File>,
    fs: &mut Filesystem<File>,
    key: &DiskKey,
    data: &[u8],
) -> Result<()> {
    let mut path = BtrfsPath::new();
    #[allow(clippy::cast_possible_truncation)]
    let found = search::search_slot(
        Some(&mut *trans),
        fs,
        CHUNK_TREE_OBJECTID,
        key,
        &mut path,
        SearchIntent::Insert((ITEM_SIZE + data.len()) as u32),
        true,
    )?;
    if found {
        path.release();
        anyhow::bail!("duplicate key {key:?} in chunk tree");
    }
    let leaf = path.nodes[0]
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("no leaf in path after search"))?;
    items::insert_item(leaf, path.slots[0], key, data)?;
    fs.mark_dirty(leaf);
    path.release();
    Ok(())
}

use super::{ChunkRecord, DevRecord, RecordSource, ScanResult};
use anyhow::{Result, bail};
use btrfs_disk::{
    items::{ChunkItem, DeviceItem},
    raw, superblock,
    tree::{KeyType, TreeBlock},
    util::btrfs_csum_data,
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    mem,
};

/// Minimum offset to start scanning (skip the superblock reserved area).
const SCAN_START: u64 = 1024 * 1024;

/// Size of the on-disk btrfs_header (csum[32] + fsid[16] + bytenr[8] +
/// flags[8] + chunk_tree_uuid[16] + generation[8] + owner[8] +
/// nritems[4] + level[1] = 101 bytes).
const HEADER_SIZE: usize = 101;

/// Chunk tree object ID.
const CHUNK_TREE_OBJECTID: u64 = raw::BTRFS_CHUNK_TREE_OBJECTID as u64;

/// Progress report interval (number of blocks between updates).
const PROGRESS_INTERVAL: u64 = 4096;

/// Scan a raw device for surviving chunk-tree data.
///
/// Reads every superblock mirror to find a valid one, parses bootstrap
/// chunks from the sys_chunk_array, then sweeps the device in nodesize
/// strides looking for chunk-tree leaves. Returns all recovered
/// CHUNK_ITEM and DEV_ITEM records with provenance metadata.
pub fn scan_device(file: &mut File) -> Result<ScanResult> {
    let sb = read_best_superblock(file)?;

    let fsid = sb.fsid;
    let has_metadata_uuid = sb.has_metadata_uuid();
    let metadata_uuid = sb.metadata_uuid;
    let nodesize = sb.nodesize;
    let chunk_root = sb.chunk_root;
    let chunk_root_level = sb.chunk_root_level;
    let sb_generation = sb.generation;

    // The UUID that tree block headers must match.
    let expected_fsid = if has_metadata_uuid {
        metadata_uuid
    } else {
        fsid
    };

    // Parse bootstrap chunks from sys_chunk_array.
    let (mut chunk_records, mut dev_records) = parse_bootstrap(&sb);

    // Add the superblock's embedded dev_item.
    dev_records.push(DevRecord {
        devid: sb.dev_item.devid,
        device: sb.dev_item.clone(),
        source: RecordSource::Bootstrap,
        generation: sb_generation,
    });

    // Determine device size.
    let device_size = file.seek(SeekFrom::End(0))?;

    // Scan the device.
    let start = SCAN_START.max(u64::from(nodesize));
    let nodesize_u64 = u64::from(nodesize);
    let total_blocks = device_size.saturating_sub(start) / nodesize_u64;

    let mut buf = vec![0u8; nodesize as usize];
    let mut candidates_checked: u64 = 0;
    let mut valid_blocks: u64 = 0;
    let mut chunk_tree_leaves: u64 = 0;
    let mut blocks_processed: u64 = 0;

    file.seek(SeekFrom::Start(start))?;
    let mut offset = start;

    while offset + nodesize_u64 <= device_size {
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // Quick reject: extract header fields without full parse.
        if buf.len() >= HEADER_SIZE {
            let (generation, owner, level) = quick_header(&buf);

            // Skip obviously invalid blocks.
            if generation != 0 && level <= 7 {
                candidates_checked += 1;

                // CRC32C validation.
                let stored_csum =
                    u32::from_le_bytes(buf[0..4].try_into().unwrap());
                let computed_csum = btrfs_csum_data(&buf[32..]);

                if stored_csum == computed_csum {
                    // fsid validation.
                    let block_fsid = &buf[32..48];
                    if block_fsid == expected_fsid.as_bytes() {
                        // Generation plausibility.
                        if generation <= sb_generation + 1 {
                            valid_blocks += 1;

                            // Only parse chunk-tree leaves fully.
                            if owner == CHUNK_TREE_OBJECTID && level == 0 {
                                chunk_tree_leaves += 1;
                                extract_chunk_tree_items(
                                    &buf,
                                    offset,
                                    generation,
                                    &mut chunk_records,
                                    &mut dev_records,
                                );
                            }
                        }
                    }
                }
            }
        }

        offset += nodesize_u64;
        blocks_processed += 1;

        if blocks_processed.is_multiple_of(PROGRESS_INTERVAL) {
            #[allow(clippy::cast_precision_loss)]
            let pct = if total_blocks > 0 {
                (blocks_processed as f64 / total_blocks as f64) * 100.0
            } else {
                100.0
            };
            eprint!(
                "\rScanning: {pct:.1}% ({blocks_processed} / {total_blocks} blocks)",
            );
        }
    }

    // Clear the progress line.
    if blocks_processed > 0 {
        eprintln!(
            "\rScanning: 100.0% ({blocks_processed} / {total_blocks} blocks)",
        );
    }

    Ok(ScanResult {
        fsid,
        metadata_uuid,
        has_metadata_uuid,
        nodesize,
        chunk_root,
        chunk_root_level,
        sb_generation,
        device_size,
        bytes_scanned: blocks_processed * nodesize_u64,
        candidates_checked,
        valid_blocks,
        chunk_tree_leaves,
        chunk_records,
        dev_records,
    })
}

/// Try all three superblock mirrors and return the first valid one.
fn read_best_superblock(file: &mut File) -> Result<superblock::Superblock> {
    for mirror in 0..3u32 {
        match superblock::read_superblock(file, mirror) {
            Ok(sb) if sb.magic_is_valid() => return Ok(sb),
            _ => {}
        }
    }
    bail!("no valid superblock found on any mirror")
}

/// Extract header fields directly from the buffer without allocating.
///
/// Layout (little-endian):
///   bytes 80..88: generation (u64)
///   bytes 88..96: owner (u64)
///   byte 100: level (u8)
fn quick_header(buf: &[u8]) -> (u64, u64, u8) {
    let generation = u64::from_le_bytes(buf[80..88].try_into().unwrap());
    let owner = u64::from_le_bytes(buf[88..96].try_into().unwrap());
    let level = buf[100];
    (generation, owner, level)
}

/// Parse bootstrap chunks from the superblock's sys_chunk_array.
///
/// Mirrors the logic in `btrfs_disk::chunk::seed_from_sys_chunk_array`
/// but produces `ChunkRecord` values with `RecordSource::Bootstrap`.
fn parse_bootstrap(
    sb: &superblock::Superblock,
) -> (Vec<ChunkRecord>, Vec<DevRecord>) {
    let array = &sb.sys_chunk_array[..sb.sys_chunk_array_size as usize];
    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let mut offset = 0usize;
    let mut chunks = Vec::new();
    let devs = Vec::new();

    while offset + disk_key_size <= array.len() {
        // The disk key is 17 bytes: objectid(8) + type(1) + offset(8).
        // We only need the offset (logical start) from bytes 9..17.
        let key_offset = u64::from_le_bytes(
            array[offset + 9..offset + 17].try_into().unwrap(),
        );
        offset += disk_key_size;

        if let Some(chunk) = ChunkItem::parse(&array[offset..]) {
            let consumed = chunk_item_size(&chunk);
            chunks.push(ChunkRecord {
                logical: key_offset,
                chunk: chunk.clone(),
                source: RecordSource::Bootstrap,
                generation: sb.generation,
            });
            offset += consumed;
        } else {
            break;
        }
    }

    (chunks, devs)
}

/// Compute the on-disk size of a chunk item (base + stripes).
fn chunk_item_size(chunk: &ChunkItem) -> usize {
    let base = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();
    base + chunk.num_stripes as usize * stripe_size
}

/// Parse a chunk-tree leaf and extract CHUNK_ITEM / DEV_ITEM records.
fn extract_chunk_tree_items(
    buf: &[u8],
    bytenr: u64,
    generation: u64,
    chunk_records: &mut Vec<ChunkRecord>,
    dev_records: &mut Vec<DevRecord>,
) {
    let block = TreeBlock::parse(buf);
    let TreeBlock::Leaf { items, .. } = &block else {
        return;
    };

    let source = RecordSource::ScannedLeaf { bytenr, generation };

    for (i, item) in items.iter().enumerate() {
        let Some(data) = block.item_data(i) else {
            continue;
        };

        match item.key.key_type {
            KeyType::ChunkItem => {
                if let Some(chunk) = ChunkItem::parse(data) {
                    chunk_records.push(ChunkRecord {
                        logical: item.key.offset,
                        chunk,
                        source,
                        generation,
                    });
                }
            }
            KeyType::DeviceItem => {
                if let Some(device) = DeviceItem::parse(data) {
                    dev_records.push(DevRecord {
                        devid: item.key.offset,
                        device,
                        source,
                        generation,
                    });
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::{
        chunk::seed_from_sys_chunk_array, superblock::Superblock,
    };

    #[test]
    fn quick_header_extracts_fields() {
        let mut buf = vec![0u8; HEADER_SIZE];
        // generation at 80..88
        buf[80..88].copy_from_slice(&42u64.to_le_bytes());
        // owner at 88..96
        buf[88..96].copy_from_slice(&3u64.to_le_bytes());
        // level at 100
        buf[100] = 1;

        let (generation, owner, level) = quick_header(&buf);
        assert_eq!(generation, 42);
        assert_eq!(owner, 3);
        assert_eq!(level, 1);
    }

    #[test]
    fn bootstrap_parsing_matches_seed() {
        // Build a synthetic sys_chunk_array with one SYSTEM chunk and
        // verify that parse_bootstrap produces matching records.
        use btrfs_disk::chunk::{
            ChunkMapping, Stripe, chunk_item_bytes, sys_chunk_array_append,
        };
        use uuid::Uuid;

        let mut sb = make_test_superblock();
        let mapping = ChunkMapping {
            logical: 0x100_0000,
            length: 8 * 1024 * 1024,
            stripe_len: 65536,
            chunk_type: 2, // SYSTEM
            num_stripes: 1,
            sub_stripes: 0,
            stripes: vec![Stripe {
                devid: 1,
                offset: 0x100_0000,
                dev_uuid: Uuid::nil(),
            }],
        };
        let bytes = chunk_item_bytes(&mapping, 4096);
        sys_chunk_array_append(
            &mut sb.sys_chunk_array,
            &mut sb.sys_chunk_array_size,
            0x100_0000,
            &bytes,
        )
        .unwrap();

        let (chunks, _devs) = parse_bootstrap(&sb);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].logical, 0x100_0000);
        assert_eq!(chunks[0].chunk.length, 8 * 1024 * 1024);
        assert!(chunks[0].source.is_bootstrap());

        // Cross-check against the standard parser.
        let cache = seed_from_sys_chunk_array(
            &sb.sys_chunk_array,
            sb.sys_chunk_array_size,
        );
        assert!(cache.lookup(0x100_0000).is_some());
    }

    /// Build a minimal Superblock for testing.
    fn make_test_superblock() -> Superblock {
        use btrfs_disk::{
            items::DeviceItem,
            superblock::{BackupRoot, ChecksumType},
        };
        use uuid::Uuid;

        Superblock {
            csum: [0; 32],
            fsid: Uuid::nil(),
            bytenr: 65536,
            flags: 0,
            magic: raw::BTRFS_MAGIC,
            generation: 10,
            root: 0,
            chunk_root: 0,
            log_root: 0,
            log_root_transid: 0,
            total_bytes: 1024 * 1024 * 1024,
            bytes_used: 0,
            root_dir_objectid: 0,
            num_devices: 1,
            sectorsize: 4096,
            nodesize: 16384,
            leafsize: 16384,
            stripesize: 4096,
            sys_chunk_array_size: 0,
            chunk_root_generation: 10,
            compat_flags: 0,
            compat_ro_flags: 0,
            incompat_flags: 0,
            csum_type: ChecksumType::Crc32,
            root_level: 0,
            chunk_root_level: 0,
            log_root_level: 0,
            dev_item: DeviceItem {
                devid: 1,
                total_bytes: 1024 * 1024 * 1024,
                bytes_used: 0,
                io_align: 4096,
                io_width: 4096,
                sector_size: 4096,
                dev_type: 0,
                generation: 0,
                start_offset: 0,
                dev_group: 0,
                seek_speed: 0,
                bandwidth: 0,
                uuid: Uuid::nil(),
                fsid: Uuid::nil(),
            },
            label: String::new(),
            cache_generation: 0,
            uuid_tree_generation: 0,
            metadata_uuid: Uuid::nil(),
            nr_global_roots: 0,
            backup_roots: std::array::from_fn(|_| BackupRoot::default()),
            sys_chunk_array: [0; 2048],
        }
    }
}

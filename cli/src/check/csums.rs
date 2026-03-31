use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    reader::{self, BlockReader},
    superblock::{ChecksumType, Superblock},
    tree::{KeyType, TreeBlock},
    util::raw_crc32c,
};
use std::io::{Read, Seek};

/// Check the checksum tree structure and optionally verify data checksums.
pub fn check_csums<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    sb: &Superblock,
    csum_root: u64,
    verify_data: bool,
    results: &mut CheckResults,
) {
    let csum_size = sb.csum_type.size() as u64;
    let sectorsize = u64::from(sb.sectorsize);
    let mut total_csum_bytes = 0u64;
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let csum_type = sb.csum_type;
    let can_verify = verify_data && csum_type == ChecksumType::Crc32;

    if verify_data && !can_verify {
        eprintln!(
            "warning: --check-data-csum not supported for \
             checksum type {}, skipping data verification",
            sb.csum_type
        );
    }

    // Collect csum items for potential data verification.
    let mut csum_items: Vec<CsumEntry> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                if item.key.key_type != KeyType::ExtentCsum {
                    continue;
                }
                let item_data =
                    &data[item.offset as usize..][..item.size as usize];

                // Each csum item covers a contiguous range of sectors.
                // The key offset is the logical byte address of the first
                // sector. The item data contains one checksum per sector.
                let logical_start = item.key.offset;
                let num_csums = item_data.len() as u64 / csum_size;
                total_csum_bytes += item_data.len() as u64;

                if can_verify {
                    csum_items.push(CsumEntry {
                        logical_start,
                        num_csums,
                        sectorsize,
                        csum_data: item_data.to_vec(),
                        csum_size: csum_size as usize,
                    });
                }
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) = reader::tree_walk_tolerant(
        reader,
        csum_root,
        &mut visitor,
        &mut on_error,
    ) {
        results.report(CheckError::ReadError {
            logical: csum_root,
            detail: format!("csum tree root: {e}"),
        });
        return;
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    results.total_csum_bytes = total_csum_bytes;

    // Verify data checksums if requested and supported.
    if can_verify {
        verify_data_csums(reader, &csum_items, results);
    }
}

struct CsumEntry {
    logical_start: u64,
    num_csums: u64,
    sectorsize: u64,
    csum_data: Vec<u8>,
    csum_size: usize,
}

fn verify_data_csums<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    entries: &[CsumEntry],
    results: &mut CheckResults,
) {
    for entry in entries {
        for i in 0..entry.num_csums {
            let logical = entry.logical_start + i * entry.sectorsize;
            let csum_off = (i as usize) * entry.csum_size;
            let stored = &entry.csum_data[csum_off..csum_off + entry.csum_size];

            let data =
                match reader.read_data(logical, entry.sectorsize as usize) {
                    Ok(d) => d,
                    Err(_) => {
                        results.report(CheckError::CsumMismatch { logical });
                        continue;
                    }
                };

            let computed = raw_crc32c(0, &data);
            let expected = u32::from_le_bytes(stored[..4].try_into().unwrap());
            if computed != expected {
                results.report(CheckError::CsumMismatch { logical });
            }
        }
    }
}

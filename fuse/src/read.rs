//! File read path: inline and regular uncompressed extent resolution.
//!
//! Compressed extents (zlib, zstd, lzo) are deferred to milestone M4.
//!
//! The on-disk layout for each `EXTENT_DATA` item:
//! - Fixed 21-byte header: generation (8) + `ram_bytes` (8) + compression (1)
//!   + encryption (1) + `other_encoding` (2) + `extent_type` (1).
//! - Inline: raw file bytes follow immediately.
//! - Regular/prealloc: `disk_bytenr` (8) + `disk_num_bytes` (8) + offset (8)
//!   + `num_bytes` (8).

use btrfs_disk::{
    items::{CompressionType, FileExtentBody, FileExtentItem, FileExtentType},
    reader::{BlockReader, Traversal, tree_walk},
    tree::{KeyType, TreeBlock},
};
use std::{io, mem};

/// Byte size of the fixed header that precedes inline or regular extent data.
const EXTENT_HEADER_SIZE: usize = 21;

/// A collected `EXTENT_DATA` item with its file position and raw bytes.
struct ExtentRec {
    /// Byte offset within the file where this extent begins (`key.offset`).
    file_pos: u64,
    item: FileExtentItem,
    /// Raw item payload (header + body), used to extract inline data.
    raw: Vec<u8>,
}

/// Read the target bytes of a symbolic link.
///
/// Symlink targets are stored as uncompressed inline `EXTENT_DATA` items.
/// Returns `None` if no matching extent item is found (caller should treat
/// that as `EIO`).
pub fn read_symlink<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
) -> io::Result<Option<Vec<u8>>> {
    let mut result = None;
    tree_walk(reader, fs_tree_root, Traversal::Dfs, &mut |block| {
        if result.is_some() {
            return;
        }
        let TreeBlock::Leaf { items, data, .. } = block else {
            return;
        };
        let hdr = mem::size_of::<btrfs_disk::raw::btrfs_header>();
        for item in items {
            if item.key.objectid != oid
                || item.key.key_type != KeyType::ExtentData
            {
                continue;
            }
            let start = hdr + item.offset as usize;
            let end = start + item.size as usize;
            if end > data.len() {
                continue;
            }
            let raw = &data[start..end];
            let Some(ext) = FileExtentItem::parse(raw) else {
                continue;
            };
            if ext.extent_type == FileExtentType::Inline {
                if ext.compression == CompressionType::None {
                    result = Some(raw[EXTENT_HEADER_SIZE..].to_vec());
                } else {
                    log::warn!(
                        "symlink oid={oid}: compressed inline extent \
                         ({}); returning empty target",
                        ext.compression.name()
                    );
                    result = Some(Vec::new());
                }
                return;
            }
        }
    })?;
    Ok(result)
}

/// Read bytes from a regular file.
///
/// Returns at most `size` bytes starting at `file_offset`, clamped to
/// `file_size`. Inline extents and regular uncompressed extents are handled
/// fully. Prealloc extents and sparse holes are returned as zeros. Compressed
/// extents return `ErrorKind::Unsupported` (milestone M4).
#[allow(clippy::too_many_lines)] // inherently complex: 4 extent variants × range math
pub fn read_file<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
    file_size: u64,
    file_offset: u64,
    size: u32,
) -> io::Result<Vec<u8>> {
    #[allow(clippy::cast_possible_truncation)]
    // file reads are bounded by usize on 64-bit
    let actual =
        u64::from(size).min(file_size.saturating_sub(file_offset)) as usize;
    if actual == 0 {
        return Ok(vec![]);
    }
    let req_end = file_offset + actual as u64;

    // Phase 1: collect all EXTENT_DATA items for this inode.
    let mut extents: Vec<ExtentRec> = Vec::new();
    tree_walk(reader, fs_tree_root, Traversal::Dfs, &mut |block| {
        let TreeBlock::Leaf { items, data, .. } = block else {
            return;
        };
        let hdr = mem::size_of::<btrfs_disk::raw::btrfs_header>();
        for item in items {
            if item.key.objectid != oid
                || item.key.key_type != KeyType::ExtentData
            {
                continue;
            }
            let start = hdr + item.offset as usize;
            let end = start + item.size as usize;
            if end > data.len() {
                continue;
            }
            if let Some(parsed) = FileExtentItem::parse(&data[start..end]) {
                extents.push(ExtentRec {
                    file_pos: item.key.offset,
                    item: parsed,
                    raw: data[start..end].to_vec(),
                });
            }
        }
    })?;

    // Phase 2: fill the output buffer from the collected extents.
    // Pre-zeroed so holes, prealloc, and gaps are returned as zeros.
    let mut out = vec![0u8; actual];

    #[allow(clippy::cast_possible_truncation)]
    // all offsets fit in usize on 64-bit Linux
    for rec in &extents {
        let ext_start = rec.file_pos;
        match (&rec.item.extent_type, &rec.item.body) {
            (
                FileExtentType::Inline,
                FileExtentBody::Inline { inline_size },
            ) => {
                let ext_end = ext_start + *inline_size as u64;
                let read_start = file_offset.max(ext_start);
                let read_end = req_end.min(ext_end);
                if read_start >= read_end {
                    continue;
                }
                if rec.item.compression != CompressionType::None {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!(
                            "compressed inline extent ({}); M4 not yet implemented",
                            rec.item.compression.name()
                        ),
                    ));
                }
                let len = (read_end - read_start) as usize;
                let src_off = (read_start - ext_start) as usize;
                let out_off = (read_start - file_offset) as usize;
                let inline = &rec.raw[EXTENT_HEADER_SIZE..];
                out[out_off..out_off + len]
                    .copy_from_slice(&inline[src_off..src_off + len]);
            }
            (
                FileExtentType::Regular,
                FileExtentBody::Regular {
                    disk_bytenr,
                    offset: disk_off,
                    num_bytes,
                    ..
                },
            ) => {
                if *disk_bytenr == 0 {
                    // Sparse hole: zeros already in out.
                    continue;
                }
                if rec.item.compression != CompressionType::None {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!(
                            "compressed extent ({}); M4 not yet implemented",
                            rec.item.compression.name()
                        ),
                    ));
                }
                let ext_end = ext_start + num_bytes;
                let read_start = file_offset.max(ext_start);
                let read_end = req_end.min(ext_end);
                if read_start >= read_end {
                    continue;
                }
                let len = (read_end - read_start) as usize;
                // Disk position: base + within-extent offset + how far into
                // the file extent we start reading.
                let disk_pos =
                    disk_bytenr + disk_off + (read_start - ext_start);
                let bytes = reader.read_data(disk_pos, len)?;
                let out_off = (read_start - file_offset) as usize;
                out[out_off..out_off + len].copy_from_slice(&bytes);
            }
            _ => {
                // Prealloc and unknown types: zeros already in out.
            }
        }
    }

    Ok(out)
}

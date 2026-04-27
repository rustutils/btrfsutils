//! File read path: inline and regular extent resolution with decompression.
//!
//! The on-disk layout for each `EXTENT_DATA` item:
//! - Fixed 21-byte header: generation (8) + `ram_bytes` (8) + compression (1)
//!   + encryption (1) + `other_encoding` (2) + `extent_type` (1).
//! - Inline: raw (or compressed) file bytes follow immediately.
//! - Regular/prealloc: `disk_bytenr` (8) + `disk_num_bytes` (8) + offset (8)
//!   + `num_bytes` (8).
//!
//! LZO framing: the on-disk data starts with a 4-byte LE total-size header,
//! followed by sector-sized chunks each prefixed with a 4-byte LE segment
//! length. If fewer than 4 bytes remain before the next sector boundary, skip
//! to align. The sector size is the filesystem `sectorsize` field.

use crate::cache::ExtentRecord;
use btrfs_disk::{
    items::{CompressionType, FileExtentBody, FileExtentItem, FileExtentType},
    reader::{BlockReader, Traversal, tree_walk},
    tree::{KeyType, TreeBlock},
};
use std::{io, io::Read, mem};

/// Byte size of the fixed header that precedes inline or regular extent data.
const EXTENT_HEADER_SIZE: usize = 21;

/// Decompress btrfs LZO format.
///
/// On-disk layout: 4-byte LE total size, then per-sector segments each with
/// a 4-byte LE length prefix followed by LZO1X compressed data. Segments are
/// padded to `sector_size` boundaries.
fn decompress_lzo(
    data: &[u8],
    output_len: usize,
    sector_size: usize,
) -> io::Result<Vec<u8>> {
    if data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "LZO data too short for header",
        ));
    }
    let total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if total_len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "LZO total length {total_len} exceeds data length {}",
                data.len()
            ),
        ));
    }

    let mut out = Vec::with_capacity(output_len);
    let mut pos = 4; // skip the 4-byte total-length header

    while pos < total_len && out.len() < output_len {
        // Pad to sector boundary if fewer than 4 bytes remain.
        let sector_remaining = sector_size - (pos % sector_size);
        if sector_remaining < 4 {
            if total_len - pos <= sector_remaining {
                break;
            }
            pos += sector_remaining;
        }

        if pos + 4 > total_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("LZO segment header truncated at offset {pos}"),
            ));
        }
        let seg_len =
            u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + seg_len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("LZO segment data truncated at offset {pos}"),
            ));
        }

        let remaining = (output_len - out.len()).min(sector_size);
        let mut seg_out = vec![0u8; remaining];
        lzokay::decompress::decompress(&data[pos..pos + seg_len], &mut seg_out)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("LZO decompression failed at offset {pos}: {e:?}"),
                )
            })?;
        out.extend_from_slice(&seg_out);
        pos += seg_len;
    }

    Ok(out)
}

/// Decompress a btrfs file extent payload.
///
/// `data` is the raw compressed bytes, `ram_bytes` is the expected
/// uncompressed size, and `sector_size` is the filesystem sectorsize (needed
/// for LZO per-sector framing).
fn decompress(
    data: &[u8],
    compression: CompressionType,
    ram_bytes: u64,
    sector_size: u32,
) -> io::Result<Vec<u8>> {
    #[allow(clippy::cast_possible_truncation)]
    // decompressed extents fit in memory on 64-bit
    let out_len = ram_bytes as usize;
    match compression {
        CompressionType::Zlib => {
            let mut decoder = flate2::read::ZlibDecoder::new(data);
            let mut out = vec![0u8; out_len];
            decoder.read_exact(&mut out).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("zlib decompression failed: {e}"),
                )
            })?;
            Ok(out)
        }
        CompressionType::Zstd => {
            zstd::bulk::decompress(data, out_len).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("zstd decompression failed: {e}"),
                )
            })
        }
        CompressionType::Lzo => {
            decompress_lzo(data, out_len, sector_size as usize)
        }
        other => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported compression type: {}", other.name()),
        )),
    }
}

/// Read the target bytes of a symbolic link.
///
/// Symlink targets are stored as inline `EXTENT_DATA` items, typically
/// uncompressed. `sector_size` is used if the (unusual) case of a compressed
/// inline symlink is encountered.
pub(crate) fn read_symlink<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
    sector_size: u32,
) -> io::Result<Option<Vec<u8>>> {
    let mut result: Option<io::Result<Vec<u8>>> = None;
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
                let payload = &raw[EXTENT_HEADER_SIZE..];
                result = Some(if ext.compression == CompressionType::None {
                    // `ram_bytes` is the logical size; the on-disk payload
                    // may be padded (e.g. mkfs.btrfs --rootdir stores a
                    // trailing NUL after symlink targets).
                    #[allow(clippy::cast_possible_truncation)]
                    let valid_len = (ext.ram_bytes as usize).min(payload.len());
                    Ok(payload[..valid_len].to_vec())
                } else {
                    decompress(
                        payload,
                        ext.compression,
                        ext.ram_bytes,
                        sector_size,
                    )
                });
                return;
            }
        }
    })?;
    result.transpose()
}

/// Walk the FS tree and collect every `EXTENT_DATA` item belonging to
/// `oid`. The returned records are in tree-walk order (i.e. ascending
/// file position, since the tree is key-sorted).
///
/// Used by [`crate::Filesystem`]'s extent-map cache so subsequent
/// reads on the same inode reuse the parsed records.
pub(crate) fn collect_extents<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
) -> io::Result<Vec<ExtentRecord>> {
    let mut extents: Vec<ExtentRecord> = Vec::new();
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
                extents.push(ExtentRecord {
                    file_pos: item.key.offset,
                    item: parsed,
                    raw: data[start..end].to_vec(),
                });
            }
        }
    })?;
    Ok(extents)
}

/// Read bytes from a regular file using a pre-collected extent map.
///
/// Returns at most `size` bytes starting at `file_offset`, clamped to
/// `file_size`. Inline, regular (compressed and uncompressed), and
/// prealloc extents are all handled. Sparse holes return zeros.
///
/// The extent map is supplied by the caller (typically built once via
/// [`collect_extents`] and cached) so repeated reads of the same file
/// don't re-walk the FS tree.
#[allow(clippy::too_many_lines)] // inherently complex: 4 extent variants × range math
pub(crate) fn read_file_with_map<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    extents: &[ExtentRecord],
    file_size: u64,
    file_offset: u64,
    size: u32,
    sector_size: u32,
) -> io::Result<Vec<u8>> {
    #[allow(clippy::cast_possible_truncation)]
    // file reads are bounded by usize on 64-bit
    let actual =
        u64::from(size).min(file_size.saturating_sub(file_offset)) as usize;
    if actual == 0 {
        return Ok(vec![]);
    }
    let req_end = file_offset + actual as u64;

    // Pre-zeroed so holes, prealloc, and gaps are returned as zeros.
    let mut out = vec![0u8; actual];

    #[allow(clippy::cast_possible_truncation)]
    // all offsets fit in usize on 64-bit Linux
    for rec in extents {
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
                let payload = &rec.raw[EXTENT_HEADER_SIZE..];
                let src = if rec.item.compression == CompressionType::None {
                    payload.to_vec()
                } else {
                    decompress(
                        payload,
                        rec.item.compression,
                        rec.item.ram_bytes,
                        sector_size,
                    )?
                };
                let len = (read_end - read_start) as usize;
                let src_off = (read_start - ext_start) as usize;
                let out_off = (read_start - file_offset) as usize;
                out[out_off..out_off + len]
                    .copy_from_slice(&src[src_off..src_off + len]);
            }
            (
                FileExtentType::Regular,
                FileExtentBody::Regular {
                    disk_bytenr,
                    disk_num_bytes,
                    offset: disk_off,
                    num_bytes,
                },
            ) => {
                if *disk_bytenr == 0 {
                    // Sparse hole: zeros already in out.
                    continue;
                }
                let ext_end = ext_start + num_bytes;
                let read_start = file_offset.max(ext_start);
                let read_end = req_end.min(ext_end);
                if read_start >= read_end {
                    continue;
                }

                if rec.item.compression == CompressionType::None {
                    // Uncompressed: read the precise byte range directly.
                    let len = (read_end - read_start) as usize;
                    let disk_pos =
                        disk_bytenr + disk_off + (read_start - ext_start);
                    let bytes = reader.read_data(disk_pos, len)?;
                    let out_off = (read_start - file_offset) as usize;
                    out[out_off..out_off + len].copy_from_slice(&bytes);
                } else {
                    // Compressed: read the full on-disk extent, decompress,
                    // then slice the requested range from the result.
                    let compressed = reader
                        .read_data(*disk_bytenr, *disk_num_bytes as usize)?;
                    let decompressed = decompress(
                        &compressed,
                        rec.item.compression,
                        rec.item.ram_bytes,
                        sector_size,
                    )?;
                    // disk_off is the offset within the decompressed data
                    // where this file extent's bytes begin.
                    let src_off =
                        (disk_off + (read_start - ext_start)) as usize;
                    let len = (read_end - read_start) as usize;
                    let out_off = (read_start - file_offset) as usize;
                    out[out_off..out_off + len]
                        .copy_from_slice(&decompressed[src_off..src_off + len]);
                }
            }
            _ => {
                // Prealloc and unknown types: zeros already in out.
            }
        }
    }

    Ok(out)
}

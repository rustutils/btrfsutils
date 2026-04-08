//! File read path: extent resolution and decompression.
//!
//! Not yet implemented — placeholder for milestones M2/M3/M4. The plan:
//!
//! - Look up `EXTENT_DATA` items for the inode covering `[off, off + size)`.
//! - For each `FileExtentItem`:
//!   - `Inline`: copy from the leaf payload (after stripping the 21-byte
//!     extent header) and decompress if `compression != None`.
//!   - `Regular { disk_bytenr: 0, .. }`: hole — fill with zeros.
//!   - `Regular { .. }` uncompressed: `BlockReader::read_data` at
//!     `disk_bytenr + offset` for `num_bytes`.
//!   - `Regular { .. }` compressed: read the full on-disk extent
//!     (`disk_num_bytes` bytes at `disk_bytenr`), decompress into a scratch
//!     buffer of `ram_bytes`, then slice `[offset .. offset + num_bytes]`.
//!   - `Prealloc`: zero-fill (we never expose unwritten data).
//!
//! Decompressors come from the workspace pins: `flate2`, `zstd`, `lzokay`.
//! LZO uses btrfs' per-sector framing — see how `stream/` decodes encoded
//! writes for the framing rules (clean-room: read its source, not
//! btrfs-progs).

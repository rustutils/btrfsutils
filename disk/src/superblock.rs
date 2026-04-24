//! # Reading and parsing the btrfs superblock from a block device
//!
//! The superblock is a 4096-byte structure stored at a fixed offset on disk
//! (primary at 64 KiB, with mirrors at 64 MiB and 256 GiB). It contains
//! the root pointers and metadata needed to bootstrap access to the rest
//! of the filesystem.

use crate::{items::DeviceItem, raw, util::btrfs_csum_data};
use bytes::BufMut;
use std::{
    fmt,
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};
use uuid::Uuid;

/// Size of a superblock on disk (4096 bytes).
/// From kernel-shared/ctree.h: `BTRFS_SUPER_INFO_SIZE`
const SUPER_INFO_SIZE: usize = 4096;

/// Byte offset of the primary superblock on disk (64 KiB).
/// From kernel-shared/ctree.h: `BTRFS_SUPER_INFO_OFFSET`
const SUPER_INFO_OFFSET: u64 = 65536;

/// Maximum number of superblock mirrors (3: primary + 2 copies).
/// From kernel-shared/disk-io.h: `BTRFS_SUPER_MIRROR_MAX`
pub const SUPER_MIRROR_MAX: u32 = 3;

/// Shift used to compute mirror offsets.
/// From kernel-shared/disk-io.h: `BTRFS_SUPER_MIRROR_SHIFT`
const SUPER_MIRROR_SHIFT: u32 = 12;

/// Compute the byte offset of the superblock mirror at `index`.
///
/// Mirror 0 is at 64 KiB, mirror 1 at 64 MiB, mirror 2 at 256 GiB.
#[must_use]
pub fn super_mirror_offset(index: u32) -> u64 {
    if index == 0 {
        SUPER_INFO_OFFSET
    } else {
        // 16 KiB << (12 * index)
        (16 * 1024u64) << (SUPER_MIRROR_SHIFT * index)
    }
}

/// Checksum algorithm used by the filesystem, stored in the superblock's
/// `csum_type` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    /// CRC32C (Castagnoli), the default and most common checksum algorithm.
    Crc32,
    /// xxHash64, a fast non-cryptographic hash.
    Xxhash,
    /// SHA-256, a cryptographic hash.
    Sha256,
    /// BLAKE2b-256, a cryptographic hash.
    Blake2,
    /// Unrecognized checksum type value.
    Unknown(u16),
}

impl ChecksumType {
    /// Parse from the raw on-disk u16 value.
    #[must_use]
    pub fn from_raw(val: u16) -> ChecksumType {
        match u32::from(val) {
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 => ChecksumType::Crc32,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH => ChecksumType::Xxhash,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 => ChecksumType::Sha256,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 => ChecksumType::Blake2,
            _ => ChecksumType::Unknown(val),
        }
    }

    /// Size in bytes of checksums for this algorithm.
    // Unknown falls back to 32 (BTRFS_CSUM_SIZE); same value as Sha256/Blake2
    // but for a different reason — suppress the match_same_arms lint.
    #[must_use]
    #[allow(clippy::match_same_arms)]
    pub fn size(&self) -> usize {
        match self {
            ChecksumType::Crc32 => 4,
            ChecksumType::Xxhash => 8,
            ChecksumType::Sha256 | ChecksumType::Blake2 => 32,
            ChecksumType::Unknown(_) => 32, // BTRFS_CSUM_SIZE
        }
    }
}

impl ChecksumType {
    /// Convert to the raw u16 value for on-disk storage.
    // All btrfs csum type constants fit in u16 (they are small enum values);
    // the u32 bindgen type is wider than necessary.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_raw(self) -> u16 {
        match self {
            ChecksumType::Crc32 => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 as u16
            }
            ChecksumType::Xxhash => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH as u16
            }
            ChecksumType::Sha256 => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 as u16
            }
            ChecksumType::Blake2 => {
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 as u16
            }
            ChecksumType::Unknown(v) => v,
        }
    }
}

impl fmt::Display for ChecksumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChecksumType::Crc32 => write!(f, "crc32c"),
            ChecksumType::Xxhash => write!(f, "xxhash64"),
            ChecksumType::Sha256 => write!(f, "sha256"),
            ChecksumType::Blake2 => write!(f, "blake2"),
            ChecksumType::Unknown(v) => write!(f, "unknown ({v})"),
        }
    }
}

/// A single backup root entry from the superblock's `super_roots` array.
///
/// The kernel maintains four rotating backup copies of the critical tree root
/// pointers. On mount failure, these can be used to recover an older consistent
/// state.
#[derive(Debug, Clone, Default)]
pub struct BackupRoot {
    /// Logical bytenr of the root tree root block.
    pub tree_root: u64,
    /// Generation of the root tree root.
    pub tree_root_gen: u64,
    /// Logical bytenr of the chunk tree root block.
    pub chunk_root: u64,
    /// Generation of the chunk tree root.
    pub chunk_root_gen: u64,
    /// Logical bytenr of the extent tree root block.
    pub extent_root: u64,
    /// Generation of the extent tree root.
    pub extent_root_gen: u64,
    /// Logical bytenr of the FS tree root block.
    pub fs_root: u64,
    /// Generation of the FS tree root.
    pub fs_root_gen: u64,
    /// Logical bytenr of the device tree root block.
    pub dev_root: u64,
    /// Generation of the device tree root.
    pub dev_root_gen: u64,
    /// Logical bytenr of the checksum tree root block.
    pub csum_root: u64,
    /// Generation of the checksum tree root.
    pub csum_root_gen: u64,
    /// Total bytes in the filesystem at backup time.
    pub total_bytes: u64,
    /// Bytes used at backup time.
    pub bytes_used: u64,
    /// Number of devices at backup time.
    pub num_devices: u64,
    /// B-tree level of the root tree root.
    pub tree_root_level: u8,
    /// B-tree level of the chunk tree root.
    pub chunk_root_level: u8,
    /// B-tree level of the extent tree root.
    pub extent_root_level: u8,
    /// B-tree level of the FS tree root.
    pub fs_root_level: u8,
    /// B-tree level of the device tree root.
    pub dev_root_level: u8,
    /// B-tree level of the checksum tree root.
    pub csum_root_level: u8,
}

/// Parsed btrfs superblock.
///
/// The superblock is the entry point for reading a btrfs filesystem. It lives
/// at a fixed offset on each device (see [`super_mirror_offset`]) and contains
/// the root pointers, feature flags, and embedded system chunk array needed to
/// bootstrap access to all other on-disk structures.
#[derive(Debug, Clone)]
pub struct Superblock {
    /// Checksum of everything past this field (bytes 32..4096).
    pub csum: [u8; 32],
    /// Filesystem UUID. Shared by all devices in a multi-device filesystem.
    pub fsid: Uuid,
    /// Physical byte offset where this superblock is stored on disk.
    pub bytenr: u64,
    /// Superblock flags (`BTRFS_SUPER_FLAG_*`).
    pub flags: u64,
    /// Magic number (`_BHRfS_M`). See [`Superblock::magic_is_valid`].
    pub magic: u64,
    /// Transaction generation of this superblock write.
    pub generation: u64,
    /// Logical bytenr of the root tree root block.
    pub root: u64,
    /// Logical bytenr of the chunk tree root block.
    pub chunk_root: u64,
    /// Logical bytenr of the log tree root block (0 if no log tree).
    pub log_root: u64,
    /// Transaction ID of the log tree root.
    pub log_root_transid: u64,
    /// Total usable bytes across all devices.
    pub total_bytes: u64,
    /// Total bytes used by data and metadata.
    pub bytes_used: u64,
    /// Objectid of the root directory (always 6).
    pub root_dir_objectid: u64,
    /// Number of devices in this filesystem.
    pub num_devices: u64,
    /// Minimum I/O alignment (typically 4096).
    pub sectorsize: u32,
    /// Size of tree blocks in bytes (typically 16384).
    pub nodesize: u32,
    /// Legacy field, equal to `nodesize` in modern filesystems.
    pub leafsize: u32,
    /// Stripe size for RAID (typically 65536).
    pub stripesize: u32,
    /// Number of valid bytes in the `sys_chunk_array`.
    pub sys_chunk_array_size: u32,
    /// Generation of the chunk tree root.
    pub chunk_root_generation: u64,
    /// Compatible feature flags.
    pub compat_flags: u64,
    /// Compatible read-only feature flags.
    pub compat_ro_flags: u64,
    /// Incompatible feature flags (e.g. `MIXED_GROUPS`, `SKINNY_METADATA`).
    pub incompat_flags: u64,
    /// Checksum algorithm for this filesystem.
    pub csum_type: ChecksumType,
    /// B-tree level of the root tree root.
    pub root_level: u8,
    /// B-tree level of the chunk tree root.
    pub chunk_root_level: u8,
    /// B-tree level of the log tree root.
    pub log_root_level: u8,
    /// Embedded device item describing this device.
    pub dev_item: DeviceItem,
    /// Filesystem label (up to 255 bytes, NUL-terminated on disk).
    pub label: String,
    /// Generation when the free space cache was written (v1 cache).
    pub cache_generation: u64,
    /// Generation when the UUID tree was last updated.
    pub uuid_tree_generation: u64,
    /// Metadata UUID (differs from `fsid` when `METADATA_UUID` incompat flag is set).
    pub metadata_uuid: Uuid,
    /// Number of global root entries (extent-tree-v2, not yet used).
    pub nr_global_roots: u64,
    /// Four rotating backup copies of critical tree root pointers.
    pub backup_roots: [BackupRoot; 4],
    /// Embedded chunk tree entries for bootstrapping the chunk cache.
    pub sys_chunk_array: [u8; 2048],
}

impl Superblock {
    /// Whether the magic bytes match `BTRFS_MAGIC`.
    #[must_use]
    pub fn magic_is_valid(&self) -> bool {
        self.magic == raw::BTRFS_MAGIC
    }

    /// Whether the `METADATA_UUID` incompat flag is set.
    #[must_use]
    pub fn has_metadata_uuid(&self) -> bool {
        self.incompat_flags
            & u64::from(raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID)
            != 0
    }

    /// Serialize the superblock to a 4096-byte buffer.
    ///
    /// The checksum field is written as-is from `self.csum`; call
    /// [`csum_superblock`] on the result to recompute it.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // Vec is pre-sized; try_into always succeeds
    pub fn to_bytes(&self) -> [u8; SUPER_INFO_SIZE] {
        type S = raw::btrfs_super_block;
        let mut v = Vec::with_capacity(SUPER_INFO_SIZE);

        v.put_slice(&self.csum); // 32 bytes
        debug_assert_eq!(v.len(), mem::offset_of!(S, fsid));
        v.put_slice(self.fsid.as_bytes());
        v.put_u64_le(self.bytenr);
        v.put_u64_le(self.flags);
        v.put_u64_le(self.magic);
        v.put_u64_le(self.generation);
        v.put_u64_le(self.root);
        v.put_u64_le(self.chunk_root);
        v.put_u64_le(self.log_root);
        v.put_u64_le(self.log_root_transid);
        v.put_u64_le(self.total_bytes);
        v.put_u64_le(self.bytes_used);
        v.put_u64_le(self.root_dir_objectid);
        v.put_u64_le(self.num_devices);
        debug_assert_eq!(v.len(), mem::offset_of!(S, sectorsize));
        v.put_u32_le(self.sectorsize);
        v.put_u32_le(self.nodesize);
        v.put_u32_le(self.leafsize);
        v.put_u32_le(self.stripesize);
        v.put_u32_le(self.sys_chunk_array_size);
        v.put_u64_le(self.chunk_root_generation);
        v.put_u64_le(self.compat_flags);
        v.put_u64_le(self.compat_ro_flags);
        v.put_u64_le(self.incompat_flags);
        v.put_u16_le(self.csum_type.to_raw());
        v.put_u8(self.root_level);
        v.put_u8(self.chunk_root_level);
        v.put_u8(self.log_root_level);
        debug_assert_eq!(v.len(), mem::offset_of!(S, dev_item));
        self.dev_item.write_bytes(&mut v);
        debug_assert_eq!(v.len(), mem::offset_of!(S, label));
        v.put_slice(&label_to_bytes(&self.label));
        debug_assert_eq!(v.len(), mem::offset_of!(S, cache_generation));
        v.put_u64_le(self.cache_generation);
        v.put_u64_le(self.uuid_tree_generation);
        v.put_slice(self.metadata_uuid.as_bytes());
        debug_assert_eq!(v.len(), mem::offset_of!(S, nr_global_roots));
        v.put_u64_le(self.nr_global_roots);
        // Zero-fill through remap_root, remap_root_generation,
        // remap_root_level, and reserved[] up to sys_chunk_array.
        let sys_chunk_off = mem::offset_of!(S, sys_chunk_array);
        v.put_bytes(0, sys_chunk_off - v.len());
        debug_assert_eq!(v.len(), sys_chunk_off);
        v.put_slice(&self.sys_chunk_array);
        // Backup roots come after sys_chunk_array.
        debug_assert_eq!(v.len(), mem::offset_of!(S, super_roots));
        for root in &self.backup_roots {
            root.write_bytes(&mut v);
        }
        // Pad with zeros to SUPER_INFO_SIZE (padding field).
        v.resize(SUPER_INFO_SIZE, 0);
        v.try_into().unwrap()
    }
}

impl BackupRoot {
    /// Serialize the backup root to a `BufMut`.
    fn write_bytes(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.tree_root);
        buf.put_u64_le(self.tree_root_gen);
        buf.put_u64_le(self.chunk_root);
        buf.put_u64_le(self.chunk_root_gen);
        buf.put_u64_le(self.extent_root);
        buf.put_u64_le(self.extent_root_gen);
        buf.put_u64_le(self.fs_root);
        buf.put_u64_le(self.fs_root_gen);
        buf.put_u64_le(self.dev_root);
        buf.put_u64_le(self.dev_root_gen);
        buf.put_u64_le(self.csum_root);
        buf.put_u64_le(self.csum_root_gen);
        buf.put_u64_le(self.total_bytes);
        buf.put_u64_le(self.bytes_used);
        buf.put_u64_le(self.num_devices);
        // unused_64[4] — 32 reserved bytes
        buf.put_bytes(0, 32);
        buf.put_u8(self.tree_root_level);
        buf.put_u8(self.chunk_root_level);
        buf.put_u8(self.extent_root_level);
        buf.put_u8(self.fs_root_level);
        buf.put_u8(self.dev_root_level);
        buf.put_u8(self.csum_root_level);
        // 10 unused bytes to fill btrfs_root_backup (168 bytes total)
        buf.put_bytes(0, 10);
    }
}

/// Convert a label string to the 256-byte on-disk format (NUL-terminated).
fn label_to_bytes(label: &str) -> [u8; 256] {
    let mut out = [0u8; 256];
    let bytes = label.as_bytes();
    let len = bytes.len().min(255);
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

/// Read the raw on-disk bytes into a packed bindgen struct.
fn read_raw_superblock(
    reader: &mut (impl Read + Seek),
    offset: u64,
) -> io::Result<raw::btrfs_super_block> {
    reader.seek(SeekFrom::Start(offset))?;

    let mut buf = [0u8; SUPER_INFO_SIZE];
    reader.read_exact(&mut buf)?;

    // SAFETY: btrfs_super_block is #[repr(C, packed)], exactly 4096 bytes,
    // and all-zeroes is a valid bit pattern. We just read into a byte buffer
    // so alignment is not an issue for the copy.
    let sb: raw::btrfs_super_block =
        unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };
    Ok(sb)
}

/// Helper: read a LE u64 field from a packed struct field (avoids misaligned reference).
macro_rules! le64 {
    ($field:expr) => {{
        let val = $field;
        u64::from_le(val)
    }};
}

macro_rules! le32 {
    ($field:expr) => {{
        let val = $field;
        u32::from_le(val)
    }};
}

macro_rules! le16 {
    ($field:expr) => {{
        let val = $field;
        u16::from_le(val)
    }};
}

fn parse_dev_item(d: &raw::btrfs_dev_item) -> DeviceItem {
    // Copy all fields to locals first — the struct is packed.
    let devid = le64!(d.devid);
    let total_bytes = le64!(d.total_bytes);
    let bytes_used = le64!(d.bytes_used);
    let io_align = le32!(d.io_align);
    let io_width = le32!(d.io_width);
    let sector_size = le32!(d.sector_size);
    let dev_type = le64!(d.type_);
    let generation = le64!(d.generation);
    let start_offset = le64!(d.start_offset);
    let dev_group = le32!(d.dev_group);
    let seek_speed = d.seek_speed;
    let bandwidth = d.bandwidth;
    let uuid = Uuid::from_bytes(d.uuid);
    let fsid = Uuid::from_bytes(d.fsid);

    DeviceItem {
        devid,
        total_bytes,
        bytes_used,
        io_align,
        io_width,
        sector_size,
        dev_type,
        generation,
        start_offset,
        dev_group,
        seek_speed,
        bandwidth,
        uuid,
        fsid,
    }
}

fn parse_backup_root(r: &raw::btrfs_root_backup) -> BackupRoot {
    BackupRoot {
        tree_root: le64!(r.tree_root),
        tree_root_gen: le64!(r.tree_root_gen),
        chunk_root: le64!(r.chunk_root),
        chunk_root_gen: le64!(r.chunk_root_gen),
        extent_root: le64!(r.extent_root),
        extent_root_gen: le64!(r.extent_root_gen),
        fs_root: le64!(r.fs_root),
        fs_root_gen: le64!(r.fs_root_gen),
        dev_root: le64!(r.dev_root),
        dev_root_gen: le64!(r.dev_root_gen),
        csum_root: le64!(r.csum_root),
        csum_root_gen: le64!(r.csum_root_gen),
        total_bytes: le64!(r.total_bytes),
        bytes_used: le64!(r.bytes_used),
        num_devices: le64!(r.num_devices),
        tree_root_level: r.tree_root_level,
        chunk_root_level: r.chunk_root_level,
        extent_root_level: r.extent_root_level,
        fs_root_level: r.fs_root_level,
        dev_root_level: r.dev_root_level,
        csum_root_level: r.csum_root_level,
    }
}

fn parse_label(raw_label: &[std::os::raw::c_char; 256]) -> String {
    let bytes: Vec<u8> = raw_label
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_superblock(sb: &raw::btrfs_super_block) -> Superblock {
    let mut sys_chunk_array = [0u8; 2048];
    sys_chunk_array.copy_from_slice(&sb.sys_chunk_array);

    Superblock {
        csum: sb.csum,
        fsid: Uuid::from_bytes(sb.fsid),
        bytenr: le64!(sb.bytenr),
        flags: le64!(sb.flags),
        magic: le64!(sb.magic),
        generation: le64!(sb.generation),
        root: le64!(sb.root),
        chunk_root: le64!(sb.chunk_root),
        log_root: le64!(sb.log_root),
        log_root_transid: le64!(sb.__unused_log_root_transid),
        total_bytes: le64!(sb.total_bytes),
        bytes_used: le64!(sb.bytes_used),
        root_dir_objectid: le64!(sb.root_dir_objectid),
        num_devices: le64!(sb.num_devices),
        sectorsize: le32!(sb.sectorsize),
        nodesize: le32!(sb.nodesize),
        leafsize: le32!(sb.__unused_leafsize),
        stripesize: le32!(sb.stripesize),
        sys_chunk_array_size: le32!(sb.sys_chunk_array_size),
        chunk_root_generation: le64!(sb.chunk_root_generation),
        compat_flags: le64!(sb.compat_flags),
        compat_ro_flags: le64!(sb.compat_ro_flags),
        incompat_flags: le64!(sb.incompat_flags),
        csum_type: ChecksumType::from_raw(le16!(sb.csum_type)),
        root_level: sb.root_level,
        chunk_root_level: sb.chunk_root_level,
        log_root_level: sb.log_root_level,
        dev_item: parse_dev_item(&sb.dev_item),
        label: parse_label(&sb.label),
        cache_generation: le64!(sb.cache_generation),
        uuid_tree_generation: le64!(sb.uuid_tree_generation),
        metadata_uuid: Uuid::from_bytes(sb.metadata_uuid),
        nr_global_roots: sb.nr_global_roots,
        backup_roots: [
            parse_backup_root(&sb.super_roots[0]),
            parse_backup_root(&sb.super_roots[1]),
            parse_backup_root(&sb.super_roots[2]),
            parse_backup_root(&sb.super_roots[3]),
        ],
        sys_chunk_array,
    }
}

/// Read and parse a btrfs superblock from a reader at the given mirror index
/// (0, 1, or 2).
///
/// # Errors
///
/// Returns an error if the underlying read or seek fails.
pub fn read_superblock(
    reader: &mut (impl Read + Seek),
    mirror: u32,
) -> io::Result<Superblock> {
    let offset = super_mirror_offset(mirror);
    read_superblock_at(reader, offset)
}

/// Read and parse a btrfs superblock from a reader at an explicit byte offset.
///
/// # Errors
///
/// Returns an error if the underlying read or seek fails.
pub fn read_superblock_at(
    reader: &mut (impl Read + Seek),
    offset: u64,
) -> io::Result<Superblock> {
    let raw = read_raw_superblock(reader, offset)?;
    Ok(parse_superblock(&raw))
}

/// Read the raw 4096-byte superblock from the primary mirror into a byte buffer.
///
/// # Errors
///
/// Returns an error if the underlying read or seek fails.
pub fn read_superblock_bytes(
    reader: &mut (impl Read + Seek),
) -> io::Result<[u8; SUPER_INFO_SIZE]> {
    read_superblock_bytes_at(reader, SUPER_INFO_OFFSET)
}

/// Read the raw 4096-byte superblock at an explicit byte offset.
///
/// # Errors
///
/// Returns an error if the underlying read or seek fails.
pub fn read_superblock_bytes_at(
    reader: &mut (impl Read + Seek),
    offset: u64,
) -> io::Result<[u8; SUPER_INFO_SIZE]> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; SUPER_INFO_SIZE];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Return `true` if the superblock buffer has valid magic and a matching CRC32C checksum.
///
/// Only CRC32C (`csum_type` == 0) is validated; other checksum types always return `false`.
#[must_use]
#[allow(clippy::missing_panics_doc)] // Slices are bounded by SUPER_INFO_SIZE; try_into always succeeds
pub fn superblock_is_valid(buf: &[u8; SUPER_INFO_SIZE]) -> bool {
    let magic_off = mem::offset_of!(raw::btrfs_super_block, magic);
    let magic =
        u64::from_le_bytes(buf[magic_off..magic_off + 8].try_into().unwrap());
    if magic != raw::BTRFS_MAGIC {
        return false;
    }
    let csum_type_off = mem::offset_of!(raw::btrfs_super_block, csum_type);
    let csum_type = u16::from_le_bytes(
        buf[csum_type_off..csum_type_off + 2].try_into().unwrap(),
    );
    if u32::from(csum_type) != raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 {
        return false;
    }
    let expected = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let actual = btrfs_csum_data(&buf[32..]);
    expected == actual
}

/// Extract the generation field from a raw superblock byte buffer.
#[must_use]
#[allow(clippy::missing_panics_doc)] // Slice is bounded by SUPER_INFO_SIZE; try_into always succeeds
pub fn superblock_generation(buf: &[u8; SUPER_INFO_SIZE]) -> u64 {
    let off = mem::offset_of!(raw::btrfs_super_block, generation);
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Recompute the CRC32C checksum of a superblock byte buffer in place.
///
/// Stores the 4-byte LE checksum at bytes 0..4, computed over bytes 32..4096.
/// Only CRC32C (`csum_type` == 0) is supported; returns an error for other
/// checksum types.
///
/// # Errors
///
/// Returns an error if the superblock uses a checksum type other than CRC32C.
#[allow(clippy::missing_panics_doc)] // Slice is bounded by SUPER_INFO_SIZE; try_into always succeeds
pub fn csum_superblock(buf: &mut [u8; SUPER_INFO_SIZE]) -> io::Result<()> {
    let csum_type_off = mem::offset_of!(raw::btrfs_super_block, csum_type);
    let csum_type = u16::from_le_bytes(
        buf[csum_type_off..csum_type_off + 2].try_into().unwrap(),
    );
    if u32::from(csum_type) != raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported checksum type {csum_type}"),
        ));
    }
    let csum = btrfs_csum_data(&buf[32..]);
    buf[0..4].copy_from_slice(&csum.to_le_bytes());
    buf[4..32].fill(0);
    Ok(())
}

/// Write a superblock buffer to all mirrors that fit within the device.
///
/// Updates the `bytenr` field per mirror before writing, then recomputes the
/// checksum. Queries the device size via `Seek::seek(End(0))`.
///
/// # Errors
///
/// Returns an error if the underlying I/O fails or the checksum type is unsupported.
pub fn write_superblock_all_mirrors(
    file: &mut (impl Read + Write + Seek),
    buf: &[u8; SUPER_INFO_SIZE],
) -> io::Result<()> {
    let device_size = file.seek(SeekFrom::End(0))?;
    let bytenr_off = mem::offset_of!(raw::btrfs_super_block, bytenr);

    for i in 0..SUPER_MIRROR_MAX {
        let offset = super_mirror_offset(i);
        if offset + SUPER_INFO_SIZE as u64 > device_size {
            break;
        }
        let mut mirror_buf = *buf;
        mirror_buf[bytenr_off..bytenr_off + 8]
            .copy_from_slice(&offset.to_le_bytes());
        csum_superblock(&mut mirror_buf)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&mirror_buf)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::c_char, io::Cursor, mem};

    // --- super_mirror_offset ---

    #[test]
    fn mirror_0_at_64k() {
        assert_eq!(super_mirror_offset(0), 65536);
    }

    #[test]
    fn mirror_1_at_64m() {
        assert_eq!(super_mirror_offset(1), 64 * 1024 * 1024);
    }

    #[test]
    fn mirror_2_at_256g() {
        assert_eq!(super_mirror_offset(2), 256 * 1024 * 1024 * 1024);
    }

    // --- ChecksumType ---

    #[test]
    fn csum_type_from_raw_known() {
        assert_eq!(
            ChecksumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 as u16
            ),
            ChecksumType::Crc32
        );
        assert_eq!(
            ChecksumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH as u16
            ),
            ChecksumType::Xxhash
        );
        assert_eq!(
            ChecksumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 as u16
            ),
            ChecksumType::Sha256
        );
        assert_eq!(
            ChecksumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 as u16
            ),
            ChecksumType::Blake2
        );
    }

    #[test]
    fn csum_type_from_raw_unknown() {
        assert_eq!(ChecksumType::from_raw(99), ChecksumType::Unknown(99));
    }

    #[test]
    fn csum_type_size() {
        assert_eq!(ChecksumType::Crc32.size(), 4);
        assert_eq!(ChecksumType::Xxhash.size(), 8);
        assert_eq!(ChecksumType::Sha256.size(), 32);
        assert_eq!(ChecksumType::Blake2.size(), 32);
        assert_eq!(ChecksumType::Unknown(99).size(), 32);
    }

    #[test]
    fn csum_type_display() {
        assert_eq!(format!("{}", ChecksumType::Crc32), "crc32c");
        assert_eq!(format!("{}", ChecksumType::Xxhash), "xxhash64");
        assert_eq!(format!("{}", ChecksumType::Sha256), "sha256");
        assert_eq!(format!("{}", ChecksumType::Blake2), "blake2");
        assert_eq!(format!("{}", ChecksumType::Unknown(99)), "unknown (99)");
    }

    // --- parse_label ---

    #[test]
    fn parse_label_normal() {
        let mut raw_label = [0 as c_char; 256];
        for (i, &b) in b"my-volume".iter().enumerate() {
            raw_label[i] = b as c_char;
        }
        assert_eq!(parse_label(&raw_label), "my-volume");
    }

    #[test]
    fn parse_label_empty() {
        let raw_label = [0 as c_char; 256];
        assert_eq!(parse_label(&raw_label), "");
    }

    #[test]
    fn parse_label_stops_at_nul() {
        let mut raw_label = [0 as c_char; 256];
        for (i, &b) in b"hello\0world".iter().enumerate() {
            raw_label[i] = b as c_char;
        }
        assert_eq!(parse_label(&raw_label), "hello");
    }

    // --- read_superblock with crafted image ---

    #[test]
    fn read_superblock_crafted() {
        let total_size = SUPER_INFO_OFFSET as usize + SUPER_INFO_SIZE;
        let mut buf = vec![0u8; total_size];

        let sb_start = SUPER_INFO_OFFSET as usize;

        // Set magic.
        let magic_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, magic);
        buf[magic_off..magic_off + 8]
            .copy_from_slice(&raw::BTRFS_MAGIC.to_le_bytes());

        // Set bytenr = SUPER_INFO_OFFSET.
        let bytenr_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, bytenr);
        buf[bytenr_off..bytenr_off + 8]
            .copy_from_slice(&SUPER_INFO_OFFSET.to_le_bytes());

        // Set generation.
        let gen_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, generation);
        buf[gen_off..gen_off + 8].copy_from_slice(&42u64.to_le_bytes());

        // Set nodesize.
        let ns_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, nodesize);
        buf[ns_off..ns_off + 4].copy_from_slice(&16384u32.to_le_bytes());

        // Set sectorsize.
        let ss_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, sectorsize);
        buf[ss_off..ss_off + 4].copy_from_slice(&4096u32.to_le_bytes());

        // Set a label.
        let label_off =
            sb_start + mem::offset_of!(raw::btrfs_super_block, label);
        buf[label_off..label_off + 4].copy_from_slice(b"test");

        let mut cursor = Cursor::new(buf);
        let sb = read_superblock(&mut cursor, 0).unwrap();

        assert!(sb.magic_is_valid());
        assert_eq!(sb.bytenr, SUPER_INFO_OFFSET);
        assert_eq!(sb.generation, 42);
        assert_eq!(sb.nodesize, 16384);
        assert_eq!(sb.sectorsize, 4096);
        assert_eq!(sb.label, "test");
    }

    #[test]
    fn read_superblock_bad_magic() {
        let total_size = SUPER_INFO_OFFSET as usize + SUPER_INFO_SIZE;
        let buf = vec![0u8; total_size];
        let mut cursor = Cursor::new(buf);
        let sb = read_superblock(&mut cursor, 0).unwrap();
        assert!(!sb.magic_is_valid());
    }

    #[test]
    fn read_superblock_too_short() {
        let buf = vec![0u8; 100]; // way too short for mirror 0
        let mut cursor = Cursor::new(buf);
        assert!(read_superblock(&mut cursor, 0).is_err());
    }

    // --- superblock_is_valid / superblock_generation ---

    fn make_valid_crc32c_buf() -> [u8; SUPER_INFO_SIZE] {
        let mut buf = [0u8; SUPER_INFO_SIZE];
        // Set magic.
        let magic_off = mem::offset_of!(raw::btrfs_super_block, magic);
        buf[magic_off..magic_off + 8]
            .copy_from_slice(&raw::BTRFS_MAGIC.to_le_bytes());
        // csum_type is already 0 (CRC32C).
        // Set generation = 99.
        let gen_off = mem::offset_of!(raw::btrfs_super_block, generation);
        buf[gen_off..gen_off + 8].copy_from_slice(&99u64.to_le_bytes());
        // Compute and store checksum.
        csum_superblock(&mut buf).unwrap();
        buf
    }

    #[test]
    fn superblock_is_valid_good() {
        let buf = make_valid_crc32c_buf();
        assert!(superblock_is_valid(&buf));
    }

    #[test]
    fn superblock_is_valid_bad_magic() {
        let mut buf = make_valid_crc32c_buf();
        let magic_off = mem::offset_of!(raw::btrfs_super_block, magic);
        buf[magic_off] ^= 0xff; // corrupt magic
        assert!(!superblock_is_valid(&buf));
    }

    #[test]
    fn superblock_is_valid_bad_csum() {
        let mut buf = make_valid_crc32c_buf();
        buf[0] ^= 0xff; // corrupt checksum
        assert!(!superblock_is_valid(&buf));
    }

    #[test]
    fn superblock_generation_reads_field() {
        let buf = make_valid_crc32c_buf();
        assert_eq!(superblock_generation(&buf), 99);
    }

    #[test]
    fn write_superblock_all_mirrors_updates_bytenr() {
        let buf = make_valid_crc32c_buf();
        // Device large enough for all 3 mirrors (256 GiB + 4096).
        // Use only primary + secondary (64 MiB + 4096) to keep test fast.
        let device_size = 64 * 1024 * 1024 + SUPER_INFO_SIZE;
        let mut device = vec![0u8; device_size];
        {
            let mut cursor = std::io::Cursor::new(&mut device);
            write_superblock_all_mirrors(&mut cursor, &buf).unwrap();
        }
        // Primary mirror (offset 64 KiB): bytenr should be 65536.
        let bytenr_off = mem::offset_of!(raw::btrfs_super_block, bytenr);
        let primary_bytenr = u64::from_le_bytes(
            device[SUPER_INFO_OFFSET as usize + bytenr_off
                ..SUPER_INFO_OFFSET as usize + bytenr_off + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(primary_bytenr, SUPER_INFO_OFFSET);
        // Secondary mirror (offset 64 MiB): bytenr should be 64 MiB.
        let mirror1_off = super_mirror_offset(1) as usize;
        let mirror1_bytenr = u64::from_le_bytes(
            device[mirror1_off + bytenr_off..mirror1_off + bytenr_off + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(mirror1_bytenr, super_mirror_offset(1));
        // Both mirrors should be valid after the write.
        let primary_buf: [u8; SUPER_INFO_SIZE] = device[SUPER_INFO_OFFSET
            as usize
            ..SUPER_INFO_OFFSET as usize + SUPER_INFO_SIZE]
            .try_into()
            .unwrap();
        assert!(superblock_is_valid(&primary_buf));
        let mirror1_buf: [u8; SUPER_INFO_SIZE] = device
            [mirror1_off..mirror1_off + SUPER_INFO_SIZE]
            .try_into()
            .unwrap();
        assert!(superblock_is_valid(&mirror1_buf));
    }

    #[test]
    fn to_bytes_roundtrip() {
        // Build a Superblock, serialize, then parse back and compare.
        let fsid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();
        let dev_uuid =
            Uuid::parse_str("cafebabe-cafe-babe-cafe-babecafebabe").unwrap();

        let sb = Superblock {
            csum: [0; 32],
            fsid,
            bytenr: SUPER_INFO_OFFSET,
            flags: 0,
            magic: raw::BTRFS_MAGIC,
            generation: 1,
            root: 0x10_0000,
            chunk_root: 0x10_8000,
            log_root: 0,
            log_root_transid: 0,
            total_bytes: 512 * 1024 * 1024,
            bytes_used: 7 * 16384,
            root_dir_objectid: 6,
            num_devices: 1,
            sectorsize: 4096,
            nodesize: 16384,
            leafsize: 16384,
            stripesize: 4096,
            sys_chunk_array_size: 0,
            chunk_root_generation: 1,
            compat_flags: 0,
            compat_ro_flags: 0,
            incompat_flags: 0,
            csum_type: ChecksumType::Crc32,
            root_level: 0,
            chunk_root_level: 0,
            log_root_level: 0,
            dev_item: DeviceItem {
                devid: 1,
                total_bytes: 512 * 1024 * 1024,
                bytes_used: 4 * 1024 * 1024,
                io_align: 4096,
                io_width: 4096,
                sector_size: 4096,
                dev_type: 0,
                generation: 0,
                start_offset: 0,
                dev_group: 0,
                seek_speed: 0,
                bandwidth: 0,
                uuid: dev_uuid,
                fsid,
            },
            label: "test-label".to_string(),
            cache_generation: 0,
            uuid_tree_generation: 0,
            metadata_uuid: Uuid::nil(),
            nr_global_roots: 0,
            backup_roots: std::array::from_fn(|_| BackupRoot {
                tree_root: 0,
                tree_root_gen: 0,
                chunk_root: 0,
                chunk_root_gen: 0,
                extent_root: 0,
                extent_root_gen: 0,
                fs_root: 0,
                fs_root_gen: 0,
                dev_root: 0,
                dev_root_gen: 0,
                csum_root: 0,
                csum_root_gen: 0,
                total_bytes: 0,
                bytes_used: 0,
                num_devices: 0,
                tree_root_level: 0,
                chunk_root_level: 0,
                extent_root_level: 0,
                fs_root_level: 0,
                dev_root_level: 0,
                csum_root_level: 0,
            }),
            sys_chunk_array: [0; 2048],
        };

        let mut bytes = sb.to_bytes();
        csum_superblock(&mut bytes).unwrap();

        // Parse back via read_superblock.
        let mut image = vec![0u8; SUPER_INFO_OFFSET as usize + SUPER_INFO_SIZE];
        image[SUPER_INFO_OFFSET as usize..].copy_from_slice(&bytes);
        let parsed = read_superblock(&mut Cursor::new(&image[..]), 0).unwrap();

        assert!(parsed.magic_is_valid());
        assert_eq!(parsed.fsid, fsid);
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.root, 0x10_0000);
        assert_eq!(parsed.chunk_root, 0x10_8000);
        assert_eq!(parsed.total_bytes, 512 * 1024 * 1024);
        assert_eq!(parsed.nodesize, 16384);
        assert_eq!(parsed.sectorsize, 4096);
        assert_eq!(parsed.label, "test-label");
        assert_eq!(parsed.num_devices, 1);
        assert_eq!(parsed.dev_item.devid, 1);
        assert_eq!(parsed.dev_item.uuid, dev_uuid);
    }
}

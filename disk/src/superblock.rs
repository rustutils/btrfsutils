//! # Reading and parsing the btrfs superblock from a block device
//!
//! The superblock is a 4096-byte structure stored at a fixed offset on disk
//! (primary at 64 KiB, with mirrors at 64 MiB and 256 GiB). It contains
//! the root pointers and metadata needed to bootstrap access to the rest
//! of the filesystem.

use crate::raw;
use std::{
    fmt,
    io::{self, Read, Seek, SeekFrom},
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
pub fn super_mirror_offset(index: u32) -> u64 {
    if index == 0 {
        SUPER_INFO_OFFSET
    } else {
        // 16 KiB << (12 * index)
        (16 * 1024u64) << (SUPER_MIRROR_SHIFT * index)
    }
}

/// Checksum algorithm used by the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsumType {
    Crc32,
    Xxhash,
    Sha256,
    Blake2,
    Unknown(u16),
}

impl CsumType {
    fn from_raw(val: u16) -> CsumType {
        match u32::from(val) {
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 => CsumType::Crc32,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH => CsumType::Xxhash,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 => CsumType::Sha256,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 => CsumType::Blake2,
            _ => CsumType::Unknown(val),
        }
    }

    /// Size in bytes of checksums for this algorithm.
    // Unknown falls back to 32 (BTRFS_CSUM_SIZE); same value as Sha256/Blake2
    // but for a different reason — suppress the match_same_arms lint.
    #[allow(clippy::match_same_arms)]
    pub fn size(&self) -> usize {
        match self {
            CsumType::Crc32 => 4,
            CsumType::Xxhash => 8,
            CsumType::Sha256 | CsumType::Blake2 => 32,
            CsumType::Unknown(_) => 32, // BTRFS_CSUM_SIZE
        }
    }
}

impl fmt::Display for CsumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CsumType::Crc32 => write!(f, "crc32c"),
            CsumType::Xxhash => write!(f, "xxhash64"),
            CsumType::Sha256 => write!(f, "sha256"),
            CsumType::Blake2 => write!(f, "blake2"),
            CsumType::Unknown(v) => write!(f, "unknown ({v})"),
        }
    }
}

/// Embedded device information from the superblock.
#[derive(Debug, Clone)]
pub struct DevItem {
    pub devid: u64,
    pub total_bytes: u64,
    pub bytes_used: u64,
    pub io_align: u32,
    pub io_width: u32,
    pub sector_size: u32,
    pub dev_type: u64,
    pub generation: u64,
    pub start_offset: u64,
    pub dev_group: u32,
    pub seek_speed: u8,
    pub bandwidth: u8,
    pub uuid: Uuid,
    pub fsid: Uuid,
}

/// A single backup root entry.
#[derive(Debug, Clone)]
pub struct BackupRoot {
    pub tree_root: u64,
    pub tree_root_gen: u64,
    pub chunk_root: u64,
    pub chunk_root_gen: u64,
    pub extent_root: u64,
    pub extent_root_gen: u64,
    pub fs_root: u64,
    pub fs_root_gen: u64,
    pub dev_root: u64,
    pub dev_root_gen: u64,
    pub csum_root: u64,
    pub csum_root_gen: u64,
    pub total_bytes: u64,
    pub bytes_used: u64,
    pub num_devices: u64,
    pub tree_root_level: u8,
    pub chunk_root_level: u8,
    pub extent_root_level: u8,
    pub fs_root_level: u8,
    pub dev_root_level: u8,
    pub csum_root_level: u8,
}

/// Parsed btrfs superblock.
#[derive(Debug, Clone)]
pub struct Superblock {
    pub csum: [u8; 32],
    pub fsid: Uuid,
    pub bytenr: u64,
    pub flags: u64,
    pub magic: u64,
    pub generation: u64,
    pub root: u64,
    pub chunk_root: u64,
    pub log_root: u64,
    pub log_root_transid: u64,
    pub total_bytes: u64,
    pub bytes_used: u64,
    pub root_dir_objectid: u64,
    pub num_devices: u64,
    pub sectorsize: u32,
    pub nodesize: u32,
    pub leafsize: u32,
    pub stripesize: u32,
    pub sys_chunk_array_size: u32,
    pub chunk_root_generation: u64,
    pub compat_flags: u64,
    pub compat_ro_flags: u64,
    pub incompat_flags: u64,
    pub csum_type: CsumType,
    pub root_level: u8,
    pub chunk_root_level: u8,
    pub log_root_level: u8,
    pub dev_item: DevItem,
    pub label: String,
    pub cache_generation: u64,
    pub uuid_tree_generation: u64,
    pub metadata_uuid: Uuid,
    pub nr_global_roots: u64,
    pub backup_roots: [BackupRoot; 4],
    pub sys_chunk_array: [u8; 2048],
}

impl Superblock {
    /// Whether the magic bytes match `BTRFS_MAGIC`.
    pub fn magic_is_valid(&self) -> bool {
        self.magic == raw::BTRFS_MAGIC
    }

    /// Whether the `METADATA_UUID` incompat flag is set.
    pub fn has_metadata_uuid(&self) -> bool {
        self.incompat_flags
            & u64::from(raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID)
            != 0
    }
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

fn parse_dev_item(d: &raw::btrfs_dev_item) -> DevItem {
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

    DevItem {
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
        .map(|&c| c.cast_unsigned())
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
        csum_type: CsumType::from_raw(le16!(sb.csum_type)),
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
pub fn read_superblock(
    reader: &mut (impl Read + Seek),
    mirror: u32,
) -> io::Result<Superblock> {
    let offset = super_mirror_offset(mirror);
    let raw = read_raw_superblock(reader, offset)?;
    Ok(parse_superblock(&raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Cursor, mem};

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

    // --- CsumType ---

    #[test]
    fn csum_type_from_raw_known() {
        assert_eq!(
            CsumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 as u16
            ),
            CsumType::Crc32
        );
        assert_eq!(
            CsumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH as u16
            ),
            CsumType::Xxhash
        );
        assert_eq!(
            CsumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 as u16
            ),
            CsumType::Sha256
        );
        assert_eq!(
            CsumType::from_raw(
                raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 as u16
            ),
            CsumType::Blake2
        );
    }

    #[test]
    fn csum_type_from_raw_unknown() {
        assert_eq!(CsumType::from_raw(99), CsumType::Unknown(99));
    }

    #[test]
    fn csum_type_size() {
        assert_eq!(CsumType::Crc32.size(), 4);
        assert_eq!(CsumType::Xxhash.size(), 8);
        assert_eq!(CsumType::Sha256.size(), 32);
        assert_eq!(CsumType::Blake2.size(), 32);
        assert_eq!(CsumType::Unknown(99).size(), 32);
    }

    #[test]
    fn csum_type_display() {
        assert_eq!(format!("{}", CsumType::Crc32), "crc32c");
        assert_eq!(format!("{}", CsumType::Xxhash), "xxhash64");
        assert_eq!(format!("{}", CsumType::Sha256), "sha256");
        assert_eq!(format!("{}", CsumType::Blake2), "blake2");
        assert_eq!(format!("{}", CsumType::Unknown(99)), "unknown (99)");
    }

    // --- parse_label ---

    #[test]
    fn parse_label_normal() {
        let mut raw_label = [0i8; 256];
        for (i, &b) in b"my-volume".iter().enumerate() {
            raw_label[i] = b as i8;
        }
        assert_eq!(parse_label(&raw_label), "my-volume");
    }

    #[test]
    fn parse_label_empty() {
        let raw_label = [0i8; 256];
        assert_eq!(parse_label(&raw_label), "");
    }

    #[test]
    fn parse_label_stops_at_nul() {
        let mut raw_label = [0i8; 256];
        for (i, &b) in b"hello\0world".iter().enumerate() {
            raw_label[i] = b as i8;
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
}

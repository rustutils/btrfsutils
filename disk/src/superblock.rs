//! # Superblock: reading and parsing the btrfs superblock from a block device
//!
//! The superblock is a 4096-byte structure stored at a fixed offset on disk
//! (primary at 64 KiB, with mirrors at 64 MiB and 256 GiB). It contains
//! the root pointers and metadata needed to bootstrap access to the rest
//! of the filesystem.

use crate::raw;
use std::{
    fmt,
    io::{self, Read, Seek, SeekFrom},
    mem,
};
use uuid::Uuid;

/// Size of a superblock on disk (4096 bytes).
/// From kernel-shared/ctree.h: BTRFS_SUPER_INFO_SIZE
const SUPER_INFO_SIZE: usize = 4096;

/// Byte offset of the primary superblock on disk (64 KiB).
/// From kernel-shared/ctree.h: BTRFS_SUPER_INFO_OFFSET
const SUPER_INFO_OFFSET: u64 = 65536;

/// Maximum number of superblock mirrors (3: primary + 2 copies).
/// From kernel-shared/disk-io.h: BTRFS_SUPER_MIRROR_MAX
pub const SUPER_MIRROR_MAX: u32 = 3;

/// Shift used to compute mirror offsets.
/// From kernel-shared/disk-io.h: BTRFS_SUPER_MIRROR_SHIFT
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
        match val as u32 {
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 => CsumType::Crc32,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH => CsumType::Xxhash,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 => CsumType::Sha256,
            raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 => CsumType::Blake2,
            _ => CsumType::Unknown(val),
        }
    }

    /// Size in bytes of checksums for this algorithm.
    pub fn size(&self) -> usize {
        match self {
            CsumType::Crc32 => 4,
            CsumType::Xxhash => 8,
            CsumType::Sha256 => 32,
            CsumType::Blake2 => 32,
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

    /// Whether the METADATA_UUID incompat flag is set.
    pub fn has_metadata_uuid(&self) -> bool {
        self.incompat_flags & raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64 != 0
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
    let sb: raw::btrfs_super_block = unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const _) };
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
pub fn read_superblock(reader: &mut (impl Read + Seek), mirror: u32) -> io::Result<Superblock> {
    let offset = super_mirror_offset(mirror);
    let raw = read_raw_superblock(reader, offset)?;
    Ok(parse_superblock(&raw))
}

/// Format the superblock flags as human-readable names.
fn format_super_flags(flags: u64) -> String {
    let known: &[(u64, &str)] = &[
        (raw::BTRFS_HEADER_FLAG_WRITTEN as u64, "WRITTEN"),
        (raw::BTRFS_HEADER_FLAG_RELOC as u64, "RELOC"),
        (raw::BTRFS_SUPER_FLAG_CHANGING_FSID as u64, "CHANGING_FSID"),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_FSID_V2 as u64,
            "CHANGING_FSID_V2",
        ),
        (raw::BTRFS_SUPER_FLAG_SEEDING as u64, "SEEDING"),
        (raw::BTRFS_SUPER_FLAG_METADUMP as u64, "METADUMP"),
        (raw::BTRFS_SUPER_FLAG_METADUMP_V2 as u64, "METADUMP_V2"),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_BG_TREE as u64,
            "CHANGING_BG_TREE",
        ),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_DATA_CSUM as u64,
            "CHANGING_DATA_CSUM",
        ),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_META_CSUM as u64,
            "CHANGING_META_CSUM",
        ),
    ];
    format_flag_names(flags, known)
}

/// Format the compat_ro flags as human-readable names.
fn format_compat_ro_flags(flags: u64) -> String {
    let known: &[(u64, &str)] = &[
        (
            raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE as u64,
            "FREE_SPACE_TREE",
        ),
        (
            raw::BTRFS_FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID as u64,
            "FREE_SPACE_TREE_VALID",
        ),
        (raw::BTRFS_FEATURE_COMPAT_RO_VERITY as u64, "VERITY"),
        (
            raw::BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE as u64,
            "BLOCK_GROUP_TREE",
        ),
    ];
    format_flag_names(flags, known)
}

/// Format the incompat flags as human-readable names.
fn format_incompat_flags(flags: u64) -> String {
    let known: &[(u64, &str)] = &[
        (
            raw::BTRFS_FEATURE_INCOMPAT_MIXED_BACKREF as u64,
            "MIXED_BACKREF",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_DEFAULT_SUBVOL as u64,
            "DEFAULT_SUBVOL",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_MIXED_GROUPS as u64,
            "MIXED_GROUPS",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_COMPRESS_LZO as u64,
            "COMPRESS_LZO",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_COMPRESS_ZSTD as u64,
            "COMPRESS_ZSTD",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_BIG_METADATA as u64,
            "BIG_METADATA",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64,
            "EXTENDED_IREF",
        ),
        (raw::BTRFS_FEATURE_INCOMPAT_RAID56 as u64, "RAID56"),
        (
            raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64,
            "SKINNY_METADATA",
        ),
        (raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64, "NO_HOLES"),
        (
            raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64,
            "METADATA_UUID",
        ),
        (raw::BTRFS_FEATURE_INCOMPAT_RAID1C34 as u64, "RAID1C34"),
        (raw::BTRFS_FEATURE_INCOMPAT_ZONED as u64, "ZONED"),
        (
            raw::BTRFS_FEATURE_INCOMPAT_EXTENT_TREE_V2 as u64,
            "EXTENT_TREE_V2",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_RAID_STRIPE_TREE as u64,
            "RAID_STRIPE_TREE",
        ),
        (
            raw::BTRFS_FEATURE_INCOMPAT_SIMPLE_QUOTA as u64,
            "SIMPLE_QUOTA",
        ),
    ];
    format_flag_names(flags, known)
}

fn format_flag_names(flags: u64, known: &[(u64, &str)]) -> String {
    if flags == 0 {
        return String::new();
    }
    let mut parts = Vec::new();
    let mut accounted = 0u64;
    for &(bit, name) in known {
        if flags & bit != 0 {
            parts.push(name.to_string());
            accounted |= bit;
        }
    }
    let unknown = flags & !accounted;
    if unknown != 0 {
        parts.push(format!("unknown(0x{unknown:x})"));
    }
    parts.join("|")
}

/// Format the magic bytes as printable ASCII (replacing non-printable with '.').
fn format_magic(magic: u64) -> String {
    let bytes = magic.to_le_bytes();
    bytes
        .iter()
        .map(|&b| if b.is_ascii_graphic() { b as char } else { '.' })
        .collect()
}

/// Print the superblock in the same format as `btrfs inspect-internal dump-super`.
pub fn print_superblock(sb: &Superblock, full: bool) {
    let csum_size = sb.csum_type.size();

    println!("csum_type\t\t{} ({})", le16_raw(sb.csum_type), sb.csum_type);
    println!("csum_size\t\t{csum_size}");

    print!("csum\t\t\t0x");
    for b in &sb.csum[..csum_size] {
        print!("{b:02x}");
    }
    println!();

    println!("bytenr\t\t\t{}", sb.bytenr);
    println!("flags\t\t\t0x{:x}", sb.flags);
    let super_flags = format_super_flags(sb.flags);
    if !super_flags.is_empty() {
        println!("\t\t\t( {super_flags} )");
    }

    let magic_str = format_magic(sb.magic);
    let magic_match = if sb.magic_is_valid() {
        "[match]"
    } else {
        "[DON'T MATCH]"
    };
    println!("magic\t\t\t{magic_str} {magic_match}");

    println!("fsid\t\t\t{}", sb.fsid.as_hyphenated());
    println!("metadata_uuid\t\t{}", sb.metadata_uuid.as_hyphenated());

    println!("label\t\t\t{}", sb.label);
    println!("generation\t\t{}", sb.generation);
    println!("root\t\t\t{}", sb.root);
    println!("sys_array_size\t\t{}", sb.sys_chunk_array_size);
    println!("chunk_root_generation\t{}", sb.chunk_root_generation);
    println!("root_level\t\t{}", sb.root_level);
    println!("chunk_root\t\t{}", sb.chunk_root);
    println!("chunk_root_level\t{}", sb.chunk_root_level);
    println!("log_root\t\t{}", sb.log_root);
    println!("log_root_transid (deprecated)\t{}", sb.log_root_transid);
    println!("log_root_level\t\t{}", sb.log_root_level);
    println!("total_bytes\t\t{}", sb.total_bytes);
    println!("bytes_used\t\t{}", sb.bytes_used);
    println!("sectorsize\t\t{}", sb.sectorsize);
    println!("nodesize\t\t{}", sb.nodesize);
    println!("leafsize (deprecated)\t{}", sb.leafsize);
    println!("stripesize\t\t{}", sb.stripesize);
    println!("root_dir\t\t{}", sb.root_dir_objectid);
    println!("num_devices\t\t{}", sb.num_devices);

    println!("compat_flags\t\t0x{:x}", sb.compat_flags);
    println!("compat_ro_flags\t\t0x{:x}", sb.compat_ro_flags);
    let ro_flags = format_compat_ro_flags(sb.compat_ro_flags);
    if !ro_flags.is_empty() {
        println!("\t\t\t( {ro_flags} )");
    }
    println!("incompat_flags\t\t0x{:x}", sb.incompat_flags);
    let ic_flags = format_incompat_flags(sb.incompat_flags);
    if !ic_flags.is_empty() {
        println!("\t\t\t( {ic_flags} )");
    }

    println!("cache_generation\t{}", sb.cache_generation);
    println!("uuid_tree_generation\t{}", sb.uuid_tree_generation);

    let d = &sb.dev_item;
    println!("dev_item.uuid\t\t{}", d.uuid.as_hyphenated());

    let fsid_match = if sb.has_metadata_uuid() {
        d.fsid == sb.metadata_uuid
    } else {
        d.fsid == sb.fsid
    };
    println!(
        "dev_item.fsid\t\t{} {}",
        d.fsid.as_hyphenated(),
        if fsid_match {
            "[match]"
        } else {
            "[DON'T MATCH]"
        }
    );

    println!("dev_item.type\t\t{}", d.dev_type);
    println!("dev_item.total_bytes\t{}", d.total_bytes);
    println!("dev_item.bytes_used\t{}", d.bytes_used);
    println!("dev_item.io_align\t{}", d.io_align);
    println!("dev_item.io_width\t{}", d.io_width);
    println!("dev_item.sector_size\t{}", d.sector_size);
    println!("dev_item.devid\t\t{}", d.devid);
    println!("dev_item.dev_group\t{}", d.dev_group);
    println!("dev_item.seek_speed\t{}", d.seek_speed);
    println!("dev_item.bandwidth\t{}", d.bandwidth);
    println!("dev_item.generation\t{}", d.generation);

    if full {
        println!("sys_chunk_array[{}]:", raw::BTRFS_SYSTEM_CHUNK_ARRAY_SIZE);
        print_sys_chunk_array(sb);
        println!("backup_roots[{}]:", raw::BTRFS_NUM_BACKUP_ROOTS);
        print_backup_roots(sb);
    }
}

fn print_sys_chunk_array(sb: &Superblock) {
    let array = &sb.sys_chunk_array[..sb.sys_chunk_array_size as usize];
    let mut offset = 0usize;

    let disk_key_size = mem::size_of::<raw::btrfs_disk_key>();
    let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    let stripe_size = mem::size_of::<raw::btrfs_stripe>();
    let mut item = 0;

    while offset + disk_key_size <= array.len() {
        let key_buf = &array[offset..offset + disk_key_size];
        let objectid = u64::from_le_bytes(key_buf[0..8].try_into().unwrap());
        let key_type = key_buf[8];
        let key_offset = u64::from_le_bytes(key_buf[9..17].try_into().unwrap());
        offset += disk_key_size;

        if offset + chunk_base_size > array.len() {
            break;
        }
        let chunk_buf = &array[offset..];
        let length = u64::from_le_bytes(chunk_buf[0..8].try_into().unwrap());
        let owner = u64::from_le_bytes(chunk_buf[8..16].try_into().unwrap());
        let stripe_len = u64::from_le_bytes(chunk_buf[16..24].try_into().unwrap());
        let chunk_type = u64::from_le_bytes(chunk_buf[24..32].try_into().unwrap());
        let io_align = u32::from_le_bytes(chunk_buf[32..36].try_into().unwrap());
        let io_width = u32::from_le_bytes(chunk_buf[36..40].try_into().unwrap());
        let sector_size = u32::from_le_bytes(chunk_buf[40..44].try_into().unwrap());
        let num_stripes = u16::from_le_bytes(chunk_buf[44..46].try_into().unwrap());
        let sub_stripes = u16::from_le_bytes(chunk_buf[46..48].try_into().unwrap());

        println!("\titem {item} key ({objectid} {key_type} {key_offset})");
        println!("\t\tlength {length} owner {owner} stripe_len {stripe_len} type {chunk_type:#x}");
        println!("\t\tio_align {io_align} io_width {io_width} sector_size {sector_size}");
        println!("\t\tnum_stripes {num_stripes} sub_stripes {sub_stripes}");

        let stripes_start = offset + chunk_base_size;
        for s in 0..num_stripes as usize {
            let s_off = stripes_start + s * stripe_size;
            if s_off + stripe_size > array.len() {
                break;
            }
            let s_buf = &array[s_off..s_off + stripe_size];
            let devid = u64::from_le_bytes(s_buf[0..8].try_into().unwrap());
            let s_offset = u64::from_le_bytes(s_buf[8..16].try_into().unwrap());
            let dev_uuid = Uuid::from_bytes(s_buf[16..32].try_into().unwrap());
            println!(
                "\t\t\tstripe {s} devid {devid} offset {s_offset}\n\t\t\tdev_uuid {}",
                dev_uuid.as_hyphenated()
            );
        }

        offset = stripes_start + num_stripes as usize * stripe_size;
        item += 1;
    }
}

fn print_backup_roots(sb: &Superblock) {
    for (i, r) in sb.backup_roots.iter().enumerate() {
        println!("\tbackup {i}:");
        println!("\t\ttree_root\t{}\tgen\t{}", r.tree_root, r.tree_root_gen);
        println!(
            "\t\tchunk_root\t{}\tgen\t{}",
            r.chunk_root, r.chunk_root_gen
        );
        println!(
            "\t\textent_root\t{}\tgen\t{}",
            r.extent_root, r.extent_root_gen
        );
        println!("\t\tfs_root\t\t{}\tgen\t{}", r.fs_root, r.fs_root_gen);
        println!("\t\tdev_root\t{}\tgen\t{}", r.dev_root, r.dev_root_gen);
        println!("\t\tcsum_root\t{}\tgen\t{}", r.csum_root, r.csum_root_gen);
        println!("\t\ttotal_bytes\t{}", r.total_bytes);
        println!("\t\tbytes_used\t{}", r.bytes_used);
        println!("\t\tnum_devices\t{}", r.num_devices);
        println!(
            "\t\tlevels\t\ttree {} chunk {} extent {} fs {} dev {} csum {}",
            r.tree_root_level,
            r.chunk_root_level,
            r.extent_root_level,
            r.fs_root_level,
            r.dev_root_level,
            r.csum_root_level,
        );
    }
}

/// Helper to get the raw u16 value from CsumType for display.
fn le16_raw(ct: CsumType) -> u16 {
    match ct {
        CsumType::Crc32 => raw::btrfs_csum_type_BTRFS_CSUM_TYPE_CRC32 as u16,
        CsumType::Xxhash => raw::btrfs_csum_type_BTRFS_CSUM_TYPE_XXHASH as u16,
        CsumType::Sha256 => raw::btrfs_csum_type_BTRFS_CSUM_TYPE_SHA256 as u16,
        CsumType::Blake2 => raw::btrfs_csum_type_BTRFS_CSUM_TYPE_BLAKE2 as u16,
        CsumType::Unknown(v) => v,
    }
}

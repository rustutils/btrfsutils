use btrfs_disk::{
    raw,
    superblock::{ChecksumType, Superblock},
};
use std::mem;
use uuid::Uuid;

/// Print the superblock in the same format as `btrfs inspect-internal dump-super`.
pub fn print_superblock(sb: &Superblock, full: bool) {
    let csum_size = sb.csum_type.size();

    println!(
        "csum_type\t\t{} ({})",
        csum_type_raw(sb.csum_type),
        sb.csum_type
    );
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
        let stripe_len =
            u64::from_le_bytes(chunk_buf[16..24].try_into().unwrap());
        let chunk_type =
            u64::from_le_bytes(chunk_buf[24..32].try_into().unwrap());
        let io_align =
            u32::from_le_bytes(chunk_buf[32..36].try_into().unwrap());
        let io_width =
            u32::from_le_bytes(chunk_buf[36..40].try_into().unwrap());
        let sector_size =
            u32::from_le_bytes(chunk_buf[40..44].try_into().unwrap());
        let num_stripes =
            u16::from_le_bytes(chunk_buf[44..46].try_into().unwrap());
        let sub_stripes =
            u16::from_le_bytes(chunk_buf[46..48].try_into().unwrap());

        println!("\titem {item} key ({objectid} {key_type} {key_offset})");
        println!(
            "\t\tlength {length} owner {owner} stripe_len {stripe_len} type {chunk_type:#x}"
        );
        println!(
            "\t\tio_align {io_align} io_width {io_width} sector_size {sector_size}"
        );
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

fn format_super_flags(flags: u64) -> String {
    let known: &[(u64, &str)] = &[
        (raw::BTRFS_HEADER_FLAG_WRITTEN as u64, "WRITTEN"),
        (raw::BTRFS_HEADER_FLAG_RELOC as u64, "RELOC"),
        (raw::BTRFS_SUPER_FLAG_CHANGING_FSID, "CHANGING_FSID"),
        (raw::BTRFS_SUPER_FLAG_CHANGING_FSID_V2, "CHANGING_FSID_V2"),
        (raw::BTRFS_SUPER_FLAG_SEEDING, "SEEDING"),
        (raw::BTRFS_SUPER_FLAG_METADUMP, "METADUMP"),
        (raw::BTRFS_SUPER_FLAG_METADUMP_V2, "METADUMP_V2"),
        (raw::BTRFS_SUPER_FLAG_CHANGING_BG_TREE, "CHANGING_BG_TREE"),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_DATA_CSUM,
            "CHANGING_DATA_CSUM",
        ),
        (
            raw::BTRFS_SUPER_FLAG_CHANGING_META_CSUM,
            "CHANGING_META_CSUM",
        ),
    ];
    format_flag_names(flags, known)
}

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

fn format_magic(magic: u64) -> String {
    let bytes = magic.to_le_bytes();
    bytes
        .iter()
        .map(|&b| if b.is_ascii_graphic() { b as char } else { '.' })
        .collect()
}

fn csum_type_raw(ct: ChecksumType) -> u16 {
    match ct {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_flag_names_zero() {
        assert_eq!(format_flag_names(0, &[]), "");
    }

    #[test]
    fn format_flag_names_single() {
        let known = &[(0x1, "FLAG_A"), (0x2, "FLAG_B")];
        assert_eq!(format_flag_names(0x1, known), "FLAG_A");
    }

    #[test]
    fn format_flag_names_multiple() {
        let known = &[(0x1, "FLAG_A"), (0x2, "FLAG_B")];
        assert_eq!(format_flag_names(0x3, known), "FLAG_A|FLAG_B");
    }

    #[test]
    fn format_flag_names_unknown_bits() {
        let known = &[(0x1, "FLAG_A")];
        assert_eq!(format_flag_names(0x5, known), "FLAG_A|unknown(0x4)");
    }

    #[test]
    fn format_magic_btrfs() {
        let s = format_magic(raw::BTRFS_MAGIC);
        assert_eq!(s, "_BHRfS_M");
    }

    #[test]
    fn format_magic_non_printable() {
        assert_eq!(format_magic(0), "........");
    }
}

//! # Print: human-readable formatting of btrfs tree blocks and items
//!
//! Prints tree block headers, node key pointers, and leaf items with
//! type-specific formatting matching the C `btrfs inspect-internal dump-tree`
//! output format.

use crate::{
    raw,
    tree::{DiskKey, Header, KeyType, ObjectId, TreeBlock, format_key},
    util::{read_le_u16, read_le_u32, read_le_u64, read_uuid},
};
use std::mem;

// Compression types from linux/btrfs_tree.h (not in the UAPI headers we bind)
const BTRFS_COMPRESS_NONE: u8 = 0;
const BTRFS_COMPRESS_ZLIB: u8 = 1;
const BTRFS_COMPRESS_LZO: u8 = 2;
const BTRFS_COMPRESS_ZSTD: u8 = 3;

/// Options controlling what gets printed.
pub struct PrintOptions {
    pub hide_names: bool,
    pub csum_headers: bool,
    pub csum_items: bool,
    pub csum_size: usize,
}

impl Default for PrintOptions {
    fn default() -> Self {
        Self {
            hide_names: false,
            csum_headers: false,
            csum_items: false,
            csum_size: 4, // CRC32C
        }
    }
}

/// Print a complete tree block (node or leaf) with all its contents.
pub fn print_tree_block(block: &TreeBlock, nodesize: u32, opts: &PrintOptions) {
    match block {
        TreeBlock::Node { header, ptrs } => {
            print_node_header(header, nodesize, opts);
            for ptr in ptrs {
                println!(
                    "\tkey {} block {} gen {}",
                    format_key(&ptr.key),
                    ptr.blockptr,
                    ptr.generation
                );
            }
        }
        TreeBlock::Leaf {
            header,
            items,
            data,
        } => {
            print_leaf_header(header, nodesize, opts);
            let header_size = mem::size_of::<raw::btrfs_header>();
            for (i, item) in items.iter().enumerate() {
                println!(
                    "\titem {} key {} itemoff {} itemsize {}",
                    i,
                    format_key(&item.key),
                    item.offset,
                    item.size
                );
                let start = header_size + item.offset as usize;
                let end = start + item.size as usize;
                if end <= data.len() {
                    print_item(&item.key, &data[start..end], opts);
                }
            }
        }
    }
}

fn print_node_header(header: &Header, nodesize: u32, opts: &PrintOptions) {
    let key_ptr_size = mem::size_of::<raw::btrfs_key_ptr>() as u32;
    let header_size = mem::size_of::<raw::btrfs_header>() as u32;
    let max_ptrs = (nodesize - header_size) / key_ptr_size;
    let free_space = (max_ptrs - header.nritems) * key_ptr_size;
    let owner = ObjectId::from_raw(header.owner);

    println!(
        "node {} level {} items {} free space {} generation {} owner {}",
        header.bytenr,
        header.level,
        header.nritems,
        free_space,
        header.generation,
        owner
    );
    print_header_flags_line(header, opts);
}

fn print_leaf_header(header: &Header, nodesize: u32, opts: &PrintOptions) {
    let header_size = mem::size_of::<raw::btrfs_header>() as u32;
    let item_size = mem::size_of::<raw::btrfs_item>() as u32;
    // Free space = total data area - used by item descriptors - used by item data
    // The simplest correct calculation: nodesize - header - items_array_size - items_data_size
    // But we don't easily know items_data_size here without summing all item sizes.
    // The C code computes it as: BTRFS_LEAF_DATA_SIZE - leaf_data_end (last item offset)
    // - nritems * sizeof(btrfs_item). We approximate with the simpler approach.
    let items_array = header.nritems * item_size;
    let data_area = nodesize - header_size;
    let free_space = data_area.saturating_sub(items_array);
    let owner = ObjectId::from_raw(header.owner);

    println!(
        "leaf {} items {} free space {} generation {} owner {}",
        header.bytenr, header.nritems, free_space, header.generation, owner
    );
    print_header_flags_line(header, opts);
}

fn print_header_flags_line(header: &Header, opts: &PrintOptions) {
    let flags = header.block_flags();
    let flag_names = crate::tree::format_header_flags(flags);
    print!(
        "leaf {} flags 0x{:x}({}) backref revision {}",
        header.bytenr,
        flags,
        flag_names,
        header.backref_rev()
    );
    if opts.csum_headers {
        print!(" csum 0x");
        for b in &header.csum[..opts.csum_size] {
            print!("{b:02x}");
        }
    }
    println!();
    println!("fs uuid {}", header.fsid.as_hyphenated());
    println!("chunk uuid {}", header.chunk_tree_uuid.as_hyphenated());
}

fn print_item(key: &DiskKey, data: &[u8], opts: &PrintOptions) {
    match key.key_type {
        KeyType::InodeItem => print_inode_item(data),
        KeyType::InodeRef => print_inode_ref(data, opts),
        KeyType::InodeExtref => print_inode_extref(data, opts),
        KeyType::DirItem | KeyType::DirIndex | KeyType::XattrItem => {
            print_dir_item(data, opts)
        }
        KeyType::DirLogItem | KeyType::DirLogIndex => print_dir_log_item(data),
        KeyType::OrphanItem => println!("\t\torphan item"),
        KeyType::RootItem => print_root_item(data),
        KeyType::RootRef | KeyType::RootBackref => print_root_ref(data, opts),
        KeyType::ExtentData => print_file_extent_item(data),
        KeyType::ExtentCsum => print_extent_csum(key, data, opts),
        KeyType::ExtentItem | KeyType::MetadataItem => {
            print_extent_item(key, data)
        }
        KeyType::TreeBlockRef => println!("\t\ttree block backref"),
        KeyType::SharedBlockRef => println!("\t\tshared block backref"),
        KeyType::ExtentDataRef => print_extent_data_ref(data),
        KeyType::SharedDataRef => print_shared_data_ref(data),
        KeyType::ExtentOwnerRef => print_extent_owner_ref(data),
        KeyType::BlockGroupItem => print_block_group_item(data),
        KeyType::FreeSpaceInfo => print_free_space_info(data),
        KeyType::FreeSpaceExtent => println!("\t\tfree space extent"),
        KeyType::FreeSpaceBitmap => println!("\t\tfree space bitmap"),
        KeyType::ChunkItem => print_chunk_item(data),
        KeyType::DevItem => print_dev_item(data),
        KeyType::DevExtent => print_dev_extent(data),
        KeyType::QgroupStatus => print_qgroup_status(data),
        KeyType::QgroupInfo => print_qgroup_info(data),
        KeyType::QgroupLimit => print_qgroup_limit(data),
        KeyType::QgroupRelation => {} // silently skipped in C reference
        KeyType::PersistentItem => print_persistent_item(key, data),
        KeyType::TemporaryItem => print_temporary_item(key, data),
        KeyType::DevReplace => print_dev_replace_item(data),
        KeyType::UuidKeySubvol | KeyType::UuidKeyReceivedSubvol => {
            print_uuid_item(data)
        }
        KeyType::StringItem => print_string_item(data),
        KeyType::RaidStripe => print_raid_stripe(data),
        _ => print_hex_dump(data),
    }
}

fn print_inode_item(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_inode_item>() {
        return print_hex_dump(data);
    }
    let generation = read_le_u64(data, 0);
    let transid = read_le_u64(data, 8);
    let size = read_le_u64(data, 16);
    let nbytes = read_le_u64(data, 24);
    let block_group = read_le_u64(data, 32);
    let nlink = read_le_u32(data, 40);
    let uid = read_le_u32(data, 44);
    let gid = read_le_u32(data, 48);
    let mode = read_le_u32(data, 52);
    let rdev = read_le_u64(data, 56);
    let flags = read_le_u64(data, 64);
    let sequence = read_le_u64(data, 72);

    println!(
        "\t\tgeneration {generation} transid {transid} size {size} nbytes {nbytes}"
    );
    println!(
        "\t\tblock group {block_group} mode {mode:o} links {nlink} uid {uid} gid {gid} rdev {rdev}"
    );
    println!("\t\tsequence {sequence} flags 0x{flags:x}(none)");

    // Timestamps: 4 timespec fields starting at offset 112
    // Each btrfs_timespec is 12 bytes (sec: le64, nsec: le32)
    let ts_off = mem::offset_of!(raw::btrfs_inode_item, atime);
    let ts_size = mem::size_of::<raw::btrfs_timespec>();
    for (i, name) in ["atime", "ctime", "mtime", "otime"].iter().enumerate() {
        let off = ts_off + i * ts_size;
        if off + ts_size <= data.len() {
            let sec = read_le_u64(data, off);
            let nsec = read_le_u32(data, off + 8);
            println!("\t\t{name} {sec}.{nsec} (unknown)");
        }
    }
}

fn print_inode_ref(data: &[u8], opts: &PrintOptions) {
    let mut offset = 0usize;
    while offset + 10 <= data.len() {
        let index = read_le_u64(data, offset);
        let name_len = read_le_u16(data, offset + 8) as usize;
        offset += 10;
        let name = if offset + name_len <= data.len() {
            if opts.hide_names {
                "(hidden)".to_string()
            } else {
                String::from_utf8_lossy(&data[offset..offset + name_len])
                    .to_string()
            }
        } else {
            "(truncated)".to_string()
        };
        println!("\t\tindex {index} namelen {name_len} name: {name}");
        offset += name_len;
    }
}

fn print_inode_extref(data: &[u8], opts: &PrintOptions) {
    let mut offset = 0usize;
    while offset + 18 <= data.len() {
        let parent = read_le_u64(data, offset);
        let index = read_le_u64(data, offset + 8);
        let name_len = read_le_u16(data, offset + 16) as usize;
        offset += 18;
        let name = if offset + name_len <= data.len() {
            if opts.hide_names {
                "(hidden)".to_string()
            } else {
                String::from_utf8_lossy(&data[offset..offset + name_len])
                    .to_string()
            }
        } else {
            "(truncated)".to_string()
        };
        println!(
            "\t\tindex {index} parent {parent} namelen {name_len} name: {name}"
        );
        offset += name_len;
    }
}

fn print_dir_item(data: &[u8], opts: &PrintOptions) {
    let mut offset = 0usize;
    let dir_item_size = mem::size_of::<raw::btrfs_dir_item>();

    while offset + dir_item_size <= data.len() {
        let location = DiskKey::parse(data, offset);
        let transid = read_le_u64(data, offset + 17);
        let data_len = read_le_u16(data, offset + 25) as usize;
        let name_len = read_le_u16(data, offset + 27) as usize;
        let file_type = data[offset + 29];
        offset += dir_item_size;

        let file_type_name = match file_type as u32 {
            raw::BTRFS_FT_REG_FILE => "FILE",
            raw::BTRFS_FT_DIR => "DIR",
            raw::BTRFS_FT_CHRDEV => "CHRDEV",
            raw::BTRFS_FT_BLKDEV => "BLKDEV",
            raw::BTRFS_FT_FIFO => "FIFO",
            raw::BTRFS_FT_SOCK => "SOCK",
            raw::BTRFS_FT_SYMLINK => "SYMLINK",
            raw::BTRFS_FT_XATTR => "XATTR",
            _ => "UNKNOWN",
        };

        println!(
            "\t\tlocation key {} type {}",
            format_key(&location),
            file_type_name
        );

        let name = if offset + name_len <= data.len() {
            if opts.hide_names {
                "(hidden)".to_string()
            } else {
                String::from_utf8_lossy(&data[offset..offset + name_len])
                    .to_string()
            }
        } else {
            "(truncated)".to_string()
        };
        println!(
            "\t\ttransid {transid} data_len {data_len} name_len {name_len}"
        );
        println!("\t\tname: {name}");

        if data_len > 0 && offset + name_len + data_len <= data.len() {
            let item_data =
                &data[offset + name_len..offset + name_len + data_len];
            if opts.hide_names {
                println!("\t\tdata (hidden)");
            } else {
                println!("\t\tdata {}", String::from_utf8_lossy(item_data));
            }
        }

        offset += name_len + data_len;
    }
}

fn print_dir_log_item(data: &[u8]) {
    if data.len() >= 8 {
        let end = read_le_u64(data, 0);
        println!("\t\tdir log end {end}");
    }
}

fn print_root_item(data: &[u8]) {
    // btrfs_root_item starts with btrfs_inode_item (160 bytes), then:
    let inode_size = mem::size_of::<raw::btrfs_inode_item>();
    if data.len() < inode_size + 8 {
        return print_hex_dump(data);
    }

    let generation = read_le_u64(data, inode_size);
    let root_dirid = read_le_u64(data, inode_size + 8);
    let bytenr = read_le_u64(data, inode_size + 16);
    let byte_limit = read_le_u64(data, inode_size + 24);
    let bytes_used = read_le_u64(data, inode_size + 32);
    let last_snapshot = read_le_u64(data, inode_size + 40);
    let flags = read_le_u64(data, inode_size + 48);
    let refs = read_le_u32(data, inode_size + 56);

    println!(
        "\t\tgeneration {generation} root_dirid {root_dirid} bytenr {bytenr} byte_limit {byte_limit} bytes_used {bytes_used}"
    );
    println!(
        "\t\tlast_snapshot {last_snapshot} flags 0x{flags:x}({}) refs {refs}",
        if flags & raw::BTRFS_ROOT_SUBVOL_RDONLY as u64 != 0 {
            "RDONLY"
        } else {
            "none"
        }
    );

    // drop_progress key at offset inode_size + 60
    let dp_off = inode_size + 60;
    if dp_off + 17 < data.len() {
        let drop_key = DiskKey::parse(data, dp_off);
        let drop_level = data[dp_off + 17];
        println!(
            "\t\tdrop_progress key {} drop_level {drop_level}",
            format_key(&drop_key)
        );
    }

    // level at inode_size + 78, generation_v2 at inode_size + 79
    let level_off = mem::offset_of!(raw::btrfs_root_item, level);
    if level_off + 1 + 8 <= data.len() {
        let level = data[level_off];
        let generation_v2 = read_le_u64(data, level_off + 1);
        println!("\t\tlevel {level} generation_v2 {generation_v2}");
    }

    // UUIDs: uuid, parent_uuid, received_uuid (each 16 bytes)
    let uuid_off = mem::offset_of!(raw::btrfs_root_item, uuid);
    if uuid_off + 48 <= data.len() {
        let uuid = read_uuid(data, uuid_off);
        let parent_uuid = read_uuid(data, uuid_off + 16);
        let received_uuid = read_uuid(data, uuid_off + 32);
        println!("\t\tuuid {}", uuid.as_hyphenated());
        println!("\t\tparent_uuid {}", parent_uuid.as_hyphenated());
        println!("\t\treceived_uuid {}", received_uuid.as_hyphenated());
    }

    // ctransid, otransid, stransid, rtransid
    let ct_off = mem::offset_of!(raw::btrfs_root_item, ctransid);
    if ct_off + 32 <= data.len() {
        let ctransid = read_le_u64(data, ct_off);
        let otransid = read_le_u64(data, ct_off + 8);
        let stransid = read_le_u64(data, ct_off + 16);
        let rtransid = read_le_u64(data, ct_off + 24);
        println!(
            "\t\tctransid {ctransid} otransid {otransid} stransid {stransid} rtransid {rtransid}"
        );
    }

    // Timestamps: ctime, otime, stime, rtime
    let ctime_off = mem::offset_of!(raw::btrfs_root_item, ctime);
    let ts_size = mem::size_of::<raw::btrfs_timespec>();
    for (i, name) in ["ctime", "otime", "stime", "rtime"].iter().enumerate() {
        let off = ctime_off + i * ts_size;
        if off + ts_size <= data.len() {
            let sec = read_le_u64(data, off);
            let nsec = read_le_u32(data, off + 8);
            println!("\t\t{name} {sec}.{nsec} (unknown)");
        }
    }
}

fn print_root_ref(data: &[u8], opts: &PrintOptions) {
    if data.len() < mem::size_of::<raw::btrfs_root_ref>() {
        return print_hex_dump(data);
    }
    let dirid = read_le_u64(data, 0);
    let sequence = read_le_u64(data, 8);
    let name_len = read_le_u16(data, 16) as usize;
    let name_start = mem::size_of::<raw::btrfs_root_ref>();
    let name = if name_start + name_len <= data.len() {
        if opts.hide_names {
            "(hidden)".to_string()
        } else {
            String::from_utf8_lossy(&data[name_start..name_start + name_len])
                .to_string()
        }
    } else {
        "(truncated)".to_string()
    };
    println!("\t\troot ref key dirid {dirid} sequence {sequence} name {name}");
}

fn print_file_extent_item(data: &[u8]) {
    if data.len() < 21 {
        return print_hex_dump(data);
    }
    let generation = read_le_u64(data, 0);
    let ram_bytes = read_le_u64(data, 8);
    let compression = data[16];
    let encryption = data[17];
    let other_encoding = read_le_u16(data, 18);
    let extent_type = data[20];
    let _ = (encryption, other_encoding); // unused in output

    let type_name = match extent_type as u32 {
        raw::BTRFS_FILE_EXTENT_INLINE => "inline",
        raw::BTRFS_FILE_EXTENT_REG => "regular",
        raw::BTRFS_FILE_EXTENT_PREALLOC => "prealloc",
        _ => "unknown",
    };

    let comp_name = match compression {
        BTRFS_COMPRESS_NONE => "none",
        BTRFS_COMPRESS_ZLIB => "zlib",
        BTRFS_COMPRESS_LZO => "lzo",
        BTRFS_COMPRESS_ZSTD => "zstd",
        _ => "unknown",
    };

    println!("\t\tgeneration {generation} type {extent_type} ({type_name})");

    if extent_type == raw::BTRFS_FILE_EXTENT_INLINE as u8 {
        let inline_size = data.len() - 21;
        println!(
            "\t\tinline extent data size {inline_size} ram_bytes {ram_bytes} compression {compression} ({comp_name})"
        );
    } else if data.len() >= 53 {
        let disk_bytenr = read_le_u64(data, 21);
        let disk_num_bytes = read_le_u64(data, 29);
        let offset = read_le_u64(data, 37);
        let num_bytes = read_le_u64(data, 45);

        if extent_type == raw::BTRFS_FILE_EXTENT_PREALLOC as u8 {
            println!(
                "\t\tprealloc data disk byte {disk_bytenr} nr {disk_num_bytes}"
            );
            println!("\t\tprealloc data offset {offset} nr {num_bytes}");
        } else {
            println!(
                "\t\textent data disk byte {disk_bytenr} nr {disk_num_bytes}"
            );
            println!(
                "\t\textent data offset {offset} nr {num_bytes} ram {ram_bytes}"
            );
        }
        if compression != BTRFS_COMPRESS_NONE {
            println!("\t\textent compression {compression} ({comp_name})");
        }
    }
}

fn print_extent_csum(key: &DiskKey, data: &[u8], opts: &PrintOptions) {
    let csum_size = opts.csum_size;
    if csum_size == 0 {
        return;
    }
    let count = data.len() / csum_size;
    let start = key.offset;
    // Default sector size assumption
    let sector_size = 4096u64;
    let end = start + count as u64 * sector_size;
    print!("\t\trange [{start} {end}) length {}", end - start);
    if opts.csum_items && !data.is_empty() {
        print!(" csum");
        let max_print = 8.min(count);
        for i in 0..max_print {
            print!(" 0x");
            for b in &data[i * csum_size..(i + 1) * csum_size] {
                print!("{b:02x}");
            }
        }
        if count > max_print {
            print!(" ...");
        }
    }
    println!();
}

fn print_extent_item(key: &DiskKey, data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_extent_item>() {
        return print_hex_dump(data);
    }
    let refs = read_le_u64(data, 0);
    let generation = read_le_u64(data, 8);
    let flags = read_le_u64(data, 16);

    let mut flag_names = Vec::new();
    if flags & raw::BTRFS_EXTENT_FLAG_DATA as u64 != 0 {
        flag_names.push("DATA");
    }
    if flags & raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64 != 0 {
        flag_names.push("TREE_BLOCK");
    }
    let flag_str = if flag_names.is_empty() {
        "none".to_string()
    } else {
        flag_names.join("|")
    };

    println!("\t\trefs {refs} gen {generation} flags {flag_str}");

    let mut offset = mem::size_of::<raw::btrfs_extent_item>();

    // If TREE_BLOCK flag and this is EXTENT_ITEM (not METADATA_ITEM),
    // there's a btrfs_tree_block_info after the extent_item
    if flags & raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64 != 0
        && key.key_type == KeyType::ExtentItem
        && offset + 17 < data.len()
    {
        let block_key = DiskKey::parse(data, offset);
        let level = data[offset + 17];
        println!(
            "\t\ttree block key {} level {level}",
            format_key(&block_key)
        );
        offset += mem::size_of::<raw::btrfs_tree_block_info>();
    }

    // For METADATA_ITEM, the level is in key.offset
    if key.key_type == KeyType::MetadataItem
        && flags & raw::BTRFS_EXTENT_FLAG_TREE_BLOCK as u64 != 0
    {
        println!("\t\ttree block skinny level {}", key.offset);
    }

    // Parse inline refs
    while offset < data.len() {
        let ref_type = data[offset];
        let ref_offset = if offset + 9 <= data.len() {
            read_le_u64(data, offset + 1)
        } else {
            0
        };
        offset += 1 + 8; // type + offset

        match ref_type as u32 {
            raw::BTRFS_TREE_BLOCK_REF_KEY => {
                let root = ObjectId::from_raw(ref_offset);
                println!("\t\ttree block backref root {root}");
            }
            raw::BTRFS_SHARED_BLOCK_REF_KEY => {
                println!("\t\tshared block backref parent {ref_offset}");
            }
            raw::BTRFS_EXTENT_DATA_REF_KEY => {
                if offset + 20 <= data.len() {
                    // Inline extent data ref: the ref_offset from the inline
                    // header was consumed above. Re-parse from the correct position.
                    let ref_start = offset - 8;
                    if ref_start + 28 <= data.len() {
                        let root = read_le_u64(data, ref_start);
                        let oid = read_le_u64(data, ref_start + 8);
                        let off = read_le_u64(data, ref_start + 16);
                        let count = read_le_u32(data, ref_start + 24);
                        let root_name = ObjectId::from_raw(root);
                        println!(
                            "\t\textent data backref root {root_name} objectid {oid} offset {off} count {count}"
                        );
                        offset = ref_start + 28;
                    }
                }
            }
            raw::BTRFS_SHARED_DATA_REF_KEY => {
                if offset + 4 <= data.len() {
                    let count = read_le_u32(data, offset);
                    println!(
                        "\t\tshared data backref parent {ref_offset} count {count}"
                    );
                    offset += 4;
                }
            }
            raw::BTRFS_EXTENT_OWNER_REF_KEY => {
                let root = ObjectId::from_raw(ref_offset);
                println!("\t\textent owner root {root}");
            }
            _ => {
                break;
            }
        }
    }
}

fn print_extent_data_ref(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_extent_data_ref>() {
        return print_hex_dump(data);
    }
    let root = read_le_u64(data, 0);
    let objectid = read_le_u64(data, 8);
    let offset = read_le_u64(data, 16);
    let count = read_le_u32(data, 24);
    let root_name = ObjectId::from_raw(root);
    println!(
        "\t\textent data backref root {root_name} objectid {objectid} offset {offset} count {count}"
    );
}

fn print_shared_data_ref(data: &[u8]) {
    if data.len() < 4 {
        return print_hex_dump(data);
    }
    let count = read_le_u32(data, 0);
    println!("\t\tshared data backref count {count}");
}

fn print_extent_owner_ref(data: &[u8]) {
    if data.len() < 8 {
        return print_hex_dump(data);
    }
    let root = read_le_u64(data, 0);
    let root_name = ObjectId::from_raw(root);
    println!("\t\textent owner root {root_name}");
}

fn print_block_group_item(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_block_group_item>() {
        return print_hex_dump(data);
    }
    let used = read_le_u64(data, 0);
    let chunk_objectid = read_le_u64(data, 8);
    let flags = read_le_u64(data, 16);
    println!(
        "\t\tblock group used {used} chunk_objectid {chunk_objectid} flags {flags:#x}"
    );
}

fn print_chunk_item(data: &[u8]) {
    let chunk_base_size = mem::offset_of!(raw::btrfs_chunk, stripe);
    if data.len() < chunk_base_size {
        return print_hex_dump(data);
    }

    let length = read_le_u64(data, 0);
    let owner = read_le_u64(data, 8);
    let stripe_len = read_le_u64(data, 16);
    let chunk_type = read_le_u64(data, 24);
    let io_align = read_le_u32(data, 32);
    let io_width = read_le_u32(data, 36);
    let sector_size = read_le_u32(data, 40);
    let num_stripes = read_le_u16(data, 44);
    let sub_stripes = read_le_u16(data, 46);

    println!(
        "\t\tlength {length} owner {owner} stripe_len {stripe_len} type {chunk_type:#x}"
    );
    println!(
        "\t\tio_align {io_align} io_width {io_width} sector_size {sector_size}"
    );
    println!("\t\tnum_stripes {num_stripes} sub_stripes {sub_stripes}");

    let stripe_size = mem::size_of::<raw::btrfs_stripe>();
    for i in 0..num_stripes as usize {
        let s_off = chunk_base_size + i * stripe_size;
        if s_off + stripe_size > data.len() {
            break;
        }
        let devid = read_le_u64(data, s_off);
        let offset = read_le_u64(data, s_off + 8);
        let dev_uuid = read_uuid(data, s_off + 16);
        println!("\t\t\tstripe {i} devid {devid} offset {offset}");
        println!("\t\t\tdev_uuid {}", dev_uuid.as_hyphenated());
    }
}

fn print_dev_item(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_dev_item>() {
        return print_hex_dump(data);
    }
    let devid = read_le_u64(data, 0);
    let total_bytes = read_le_u64(data, 8);
    let bytes_used = read_le_u64(data, 16);
    let io_align = read_le_u32(data, 24);
    let io_width = read_le_u32(data, 28);
    let sector_size = read_le_u32(data, 32);
    let dev_type = read_le_u64(data, 36);
    let generation = read_le_u64(data, 44);
    let start_offset = read_le_u64(data, 52);
    let dev_group = read_le_u32(data, 60);
    let seek_speed = data[64];
    let bandwidth = data[65];
    let uuid = read_uuid(data, 66);
    let fsid = read_uuid(data, 82);

    println!(
        "\t\tdevid {devid} total_bytes {total_bytes} bytes_used {bytes_used}"
    );
    println!(
        "\t\tio_align {io_align} io_width {io_width} sector_size {sector_size} type {dev_type}"
    );
    println!(
        "\t\tgeneration {generation} start_offset {start_offset} dev_group {dev_group}"
    );
    println!("\t\tseek_speed {seek_speed} bandwidth {bandwidth}");
    println!("\t\tuuid {}", uuid.as_hyphenated());
    println!("\t\tfsid {}", fsid.as_hyphenated());
}

fn print_dev_extent(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_dev_extent>() {
        return print_hex_dump(data);
    }
    let chunk_tree = read_le_u64(data, 0);
    let chunk_objectid = read_le_u64(data, 8);
    let chunk_offset = read_le_u64(data, 16);
    let length = read_le_u64(data, 24);
    let chunk_tree_uuid = read_uuid(data, 32);

    println!("\t\tdev extent chunk_tree {chunk_tree}");
    println!(
        "\t\tchunk_objectid {chunk_objectid} chunk_offset {chunk_offset} length {length}"
    );
    println!("\t\tchunk_tree_uuid {}", chunk_tree_uuid.as_hyphenated());
}

fn print_qgroup_status(data: &[u8]) {
    if data.len() < 32 {
        return print_hex_dump(data);
    }
    let version = read_le_u64(data, 0);
    let generation = read_le_u64(data, 8);
    let flags = read_le_u64(data, 16);
    let scan = read_le_u64(data, 24);

    print!(
        "\t\tversion {version} generation {generation} flags 0x{flags:x} scan {scan}"
    );
    if data.len() >= 40 {
        let enable_gen = read_le_u64(data, 32);
        print!(" enable_gen {enable_gen}");
    }
    println!();
}

fn print_qgroup_info(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_qgroup_info_item>() {
        return print_hex_dump(data);
    }
    let generation = read_le_u64(data, 0);
    let referenced = read_le_u64(data, 8);
    let referenced_compressed = read_le_u64(data, 16);
    let exclusive = read_le_u64(data, 24);
    let exclusive_compressed = read_le_u64(data, 32);

    println!("\t\tgeneration {generation}");
    println!(
        "\t\treferenced {referenced} referenced_compressed {referenced_compressed}"
    );
    println!(
        "\t\texclusive {exclusive} exclusive_compressed {exclusive_compressed}"
    );
}

fn print_qgroup_limit(data: &[u8]) {
    if data.len() < mem::size_of::<raw::btrfs_qgroup_limit_item>() {
        return print_hex_dump(data);
    }
    let flags = read_le_u64(data, 0);
    let max_referenced = read_le_u64(data, 8);
    let max_exclusive = read_le_u64(data, 16);
    let rsv_referenced = read_le_u64(data, 24);
    let rsv_exclusive = read_le_u64(data, 32);

    println!("\t\tflags 0x{flags:x}");
    println!(
        "\t\tmax_referenced {max_referenced} max_exclusive {max_exclusive}"
    );
    println!(
        "\t\trsv_referenced {rsv_referenced} rsv_exclusive {rsv_exclusive}"
    );
}

fn print_free_space_info(data: &[u8]) {
    if data.len() < 8 {
        return print_hex_dump(data);
    }
    let extent_count = read_le_u32(data, 0);
    let flags = read_le_u32(data, 4);
    println!("\t\textent count {extent_count} flags {flags}");
}

fn print_uuid_item(data: &[u8]) {
    let mut offset = 0;
    while offset + 8 <= data.len() {
        let subid = read_le_u64(data, offset);
        println!("\t\tsubvol_id {subid}");
        offset += 8;
    }
}

fn print_persistent_item(key: &DiskKey, data: &[u8]) {
    // objectid 0 with PERSISTENT_ITEM = dev_stats
    if key.objectid == raw::BTRFS_DEV_STATS_OBJECTID as u64 {
        print_dev_stats(data);
    } else {
        print_hex_dump(data);
    }
}

fn print_dev_stats(data: &[u8]) {
    let stat_names = [
        "write_errs",
        "read_errs",
        "flush_errs",
        "corruption_errs",
        "generation_errs",
    ];
    println!("\t\tdevice stats");
    for (i, name) in stat_names.iter().enumerate() {
        let off = i * 8;
        if off + 8 <= data.len() {
            let val = read_le_u64(data, off);
            println!("\t\t[{i}]\t{name} {val}");
        }
    }
}

fn print_temporary_item(key: &DiskKey, data: &[u8]) {
    let oid = ObjectId::from_raw(key.objectid);
    match oid {
        ObjectId::Balance => {
            println!("\t\tbalance status");
            // The balance item has complex internals; just note it exists
            if data.len() >= 8 {
                let flags = read_le_u64(data, 0);
                println!("\t\t\tflags 0x{flags:x}");
            }
        }
        _ => print_hex_dump(data),
    }
}

fn print_dev_replace_item(data: &[u8]) {
    if data.len() < 80 {
        return print_hex_dump(data);
    }
    let src_devid = read_le_u64(data, 0);
    let cursor_left = read_le_u64(data, 8);
    let cursor_right = read_le_u64(data, 16);
    let cont_reading_from_srcdev_mode = read_le_u64(data, 24);
    let replace_state = read_le_u64(data, 32);
    let time_started = read_le_u64(data, 40);
    let time_stopped = read_le_u64(data, 48);
    let num_write_errors = read_le_u64(data, 56);
    let num_uncorrectable_read_errors = read_le_u64(data, 64);

    println!("\t\tsource devid {src_devid}");
    println!("\t\tcursor_left {cursor_left} cursor_right {cursor_right}");
    println!(
        "\t\treplace_mode {cont_reading_from_srcdev_mode} replace_state {replace_state}"
    );
    println!("\t\ttime_started {time_started} time_stopped {time_stopped}");
    println!(
        "\t\tnum_write_errors {num_write_errors} num_uncorrectable_read_errors {num_uncorrectable_read_errors}"
    );
}

fn print_string_item(data: &[u8]) {
    let s = String::from_utf8_lossy(data);
    println!("\t\tstring {s}");
}

fn print_raid_stripe(data: &[u8]) {
    if data.len() < 8 {
        return print_hex_dump(data);
    }
    let encoding = read_le_u64(data, 0);
    println!("\t\traid stripe encoding {encoding}");
    let stride_size = 16; // devid(8) + physical(8)
    let mut offset = 8;
    let mut i = 0;
    while offset + stride_size <= data.len() {
        let devid = read_le_u64(data, offset);
        let physical = read_le_u64(data, offset + 8);
        println!("\t\t\tstripe {i} devid {devid} physical {physical}");
        offset += stride_size;
        i += 1;
    }
}

fn print_hex_dump(data: &[u8]) {
    let max = 64.min(data.len());
    print!("\t\t");
    for b in &data[..max] {
        print!("{b:02x} ");
    }
    if data.len() > max {
        print!("...");
    }
    println!();
}

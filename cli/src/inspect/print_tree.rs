use btrfs_disk::{
    items::{self, FileExtentBody, InlineRef, ItemPayload},
    raw,
    tree::{Header, ObjectId, TreeBlock, format_header_flags, format_key},
};
use std::mem;

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
            csum_size: 4,
        }
    }
}

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
                    let payload =
                        items::parse_item_payload(&item.key, &data[start..end]);
                    print_payload(&item.key, &payload, opts);
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
    let flag_names = format_header_flags(flags);
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

fn name_or_hidden(data: &[u8], hide: bool) -> String {
    if hide {
        "(hidden)".to_string()
    } else {
        String::from_utf8_lossy(data).to_string()
    }
}

fn print_payload(
    key: &btrfs_disk::tree::DiskKey,
    payload: &ItemPayload,
    opts: &PrintOptions,
) {
    match payload {
        ItemPayload::InodeItem(v) => {
            println!(
                "\t\tgeneration {} transid {} size {} nbytes {}",
                v.generation, v.transid, v.size, v.nbytes
            );
            println!(
                "\t\tblock group {} mode {:o} links {} uid {} gid {} rdev {}",
                v.block_group, v.mode, v.nlink, v.uid, v.gid, v.rdev
            );
            println!("\t\tsequence {} flags 0x{:x}(none)", v.sequence, v.flags);
            for (name, ts) in [
                ("atime", &v.atime),
                ("ctime", &v.ctime),
                ("mtime", &v.mtime),
                ("otime", &v.otime),
            ] {
                println!("\t\t{name} {}.{} (unknown)", ts.sec, ts.nsec);
            }
        }
        ItemPayload::InodeRef(refs) => {
            for r in refs {
                let name = name_or_hidden(&r.name, opts.hide_names);
                println!(
                    "\t\tindex {} namelen {} name: {name}",
                    r.index,
                    r.name.len()
                );
            }
        }
        ItemPayload::InodeExtref(refs) => {
            for r in refs {
                let name = name_or_hidden(&r.name, opts.hide_names);
                println!(
                    "\t\tindex {} parent {} namelen {} name: {name}",
                    r.index,
                    r.parent,
                    r.name.len()
                );
            }
        }
        ItemPayload::DirItem(entries) => {
            for d in entries {
                println!(
                    "\t\tlocation key {} type {}",
                    format_key(&d.location),
                    d.file_type.name()
                );
                let name = name_or_hidden(&d.name, opts.hide_names);
                println!(
                    "\t\ttransid {} data_len {} name_len {}",
                    d.transid,
                    d.data.len(),
                    d.name.len()
                );
                println!("\t\tname: {name}");
                if !d.data.is_empty() {
                    if opts.hide_names {
                        println!("\t\tdata (hidden)");
                    } else {
                        println!(
                            "\t\tdata {}",
                            String::from_utf8_lossy(&d.data)
                        );
                    }
                }
            }
        }
        ItemPayload::DirLogItem { end } => {
            println!("\t\tdir log end {end}");
        }
        ItemPayload::OrphanItem => {
            println!("\t\torphan item");
        }
        ItemPayload::RootItem(v) => {
            println!(
                "\t\tgeneration {} root_dirid {} bytenr {} byte_limit {} bytes_used {}",
                v.generation,
                v.root_dirid,
                v.bytenr,
                v.byte_limit,
                v.bytes_used
            );
            println!(
                "\t\tlast_snapshot {} flags 0x{:x}({}) refs {}",
                v.last_snapshot,
                v.flags,
                if v.is_rdonly() { "RDONLY" } else { "none" },
                v.refs
            );
            println!(
                "\t\tdrop_progress key {} drop_level {}",
                format_key(&v.drop_progress),
                v.drop_level
            );
            println!("\t\tlevel {} generation_v2 {}", v.level, v.generation_v2);
            println!("\t\tuuid {}", v.uuid.as_hyphenated());
            println!("\t\tparent_uuid {}", v.parent_uuid.as_hyphenated());
            println!("\t\treceived_uuid {}", v.received_uuid.as_hyphenated());
            println!(
                "\t\tctransid {} otransid {} stransid {} rtransid {}",
                v.ctransid, v.otransid, v.stransid, v.rtransid
            );
            for (name, ts) in [
                ("ctime", &v.ctime),
                ("otime", &v.otime),
                ("stime", &v.stime),
                ("rtime", &v.rtime),
            ] {
                println!("\t\t{name} {}.{} (unknown)", ts.sec, ts.nsec);
            }
        }
        ItemPayload::RootRef(v) => {
            let name = name_or_hidden(&v.name, opts.hide_names);
            println!(
                "\t\troot ref key dirid {} sequence {} name {name}",
                v.dirid, v.sequence
            );
        }
        ItemPayload::FileExtentItem(v) => {
            println!(
                "\t\tgeneration {} type {} ({})",
                v.generation,
                v.extent_type.to_raw(),
                v.extent_type.name()
            );
            match &v.body {
                FileExtentBody::Inline { inline_size } => {
                    println!(
                        "\t\tinline extent data size {inline_size} ram_bytes {} compression {} ({})",
                        v.ram_bytes,
                        v.compression.to_raw(),
                        v.compression.name()
                    );
                }
                FileExtentBody::Regular {
                    disk_bytenr,
                    disk_num_bytes,
                    offset,
                    num_bytes,
                } => {
                    if v.extent_type == items::FileExtentType::Prealloc {
                        println!(
                            "\t\tprealloc data disk byte {disk_bytenr} nr {disk_num_bytes}"
                        );
                        println!(
                            "\t\tprealloc data offset {offset} nr {num_bytes}"
                        );
                    } else {
                        println!(
                            "\t\textent data disk byte {disk_bytenr} nr {disk_num_bytes}"
                        );
                        println!(
                            "\t\textent data offset {offset} nr {num_bytes} ram {}",
                            v.ram_bytes
                        );
                    }
                    if v.compression != items::CompressionType::None {
                        println!(
                            "\t\textent compression {} ({})",
                            v.compression.to_raw(),
                            v.compression.name()
                        );
                    }
                }
            }
        }
        ItemPayload::ExtentCsum { data } => {
            let csum_size = opts.csum_size;
            if csum_size == 0 {
                return;
            }
            let count = data.len() / csum_size;
            let start = key.offset;
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
        ItemPayload::ExtentItem(v) => {
            println!(
                "\t\trefs {} gen {} flags {}",
                v.refs,
                v.generation,
                v.flag_names()
            );
            if let (Some(bk), Some(level)) =
                (&v.tree_block_key, v.tree_block_level)
            {
                println!("\t\ttree block key {} level {level}", format_key(bk));
            }
            if let Some(level) = v.skinny_level {
                println!("\t\ttree block skinny level {level}");
            }
            for iref in &v.inline_refs {
                match iref {
                    InlineRef::TreeBlockBackref { root } => {
                        println!(
                            "\t\ttree block backref root {}",
                            ObjectId::from_raw(*root)
                        );
                    }
                    InlineRef::SharedBlockBackref { parent } => {
                        println!("\t\tshared block backref parent {parent}");
                    }
                    InlineRef::ExtentDataBackref {
                        root,
                        objectid,
                        offset,
                        count,
                    } => {
                        println!(
                            "\t\textent data backref root {} objectid {objectid} offset {offset} count {count}",
                            ObjectId::from_raw(*root)
                        );
                    }
                    InlineRef::SharedDataBackref { parent, count } => {
                        println!(
                            "\t\tshared data backref parent {parent} count {count}"
                        );
                    }
                    InlineRef::ExtentOwnerRef { root } => {
                        println!(
                            "\t\textent owner root {}",
                            ObjectId::from_raw(*root)
                        );
                    }
                }
            }
        }
        ItemPayload::TreeBlockRef => println!("\t\ttree block backref"),
        ItemPayload::SharedBlockRef => println!("\t\tshared block backref"),
        ItemPayload::ExtentDataRef(v) => {
            println!(
                "\t\textent data backref root {} objectid {} offset {} count {}",
                ObjectId::from_raw(v.root),
                v.objectid,
                v.offset,
                v.count
            );
        }
        ItemPayload::SharedDataRef(v) => {
            println!("\t\tshared data backref count {}", v.count);
        }
        ItemPayload::ExtentOwnerRef { root } => {
            println!("\t\textent owner root {}", ObjectId::from_raw(*root));
        }
        ItemPayload::BlockGroupItem(v) => {
            println!(
                "\t\tblock group used {} chunk_objectid {} flags {:#x}",
                v.used, v.chunk_objectid, v.flags
            );
        }
        ItemPayload::FreeSpaceInfo(v) => {
            println!("\t\textent count {} flags {}", v.extent_count, v.flags);
        }
        ItemPayload::FreeSpaceExtent => println!("\t\tfree space extent"),
        ItemPayload::FreeSpaceBitmap => println!("\t\tfree space bitmap"),
        ItemPayload::ChunkItem(v) => {
            println!(
                "\t\tlength {} owner {} stripe_len {} type {:#x}",
                v.length, v.owner, v.stripe_len, v.chunk_type
            );
            println!(
                "\t\tio_align {} io_width {} sector_size {}",
                v.io_align, v.io_width, v.sector_size
            );
            println!(
                "\t\tnum_stripes {} sub_stripes {}",
                v.num_stripes, v.sub_stripes
            );
            for (i, s) in v.stripes.iter().enumerate() {
                println!(
                    "\t\t\tstripe {i} devid {} offset {}",
                    s.devid, s.offset
                );
                println!("\t\t\tdev_uuid {}", s.dev_uuid.as_hyphenated());
            }
        }
        ItemPayload::DevItem(v) => {
            println!(
                "\t\tdevid {} total_bytes {} bytes_used {}",
                v.devid, v.total_bytes, v.bytes_used
            );
            println!(
                "\t\tio_align {} io_width {} sector_size {} type {}",
                v.io_align, v.io_width, v.sector_size, v.dev_type
            );
            println!(
                "\t\tgeneration {} start_offset {} dev_group {}",
                v.generation, v.start_offset, v.dev_group
            );
            println!(
                "\t\tseek_speed {} bandwidth {}",
                v.seek_speed, v.bandwidth
            );
            println!("\t\tuuid {}", v.uuid.as_hyphenated());
            println!("\t\tfsid {}", v.fsid.as_hyphenated());
        }
        ItemPayload::DevExtent(v) => {
            println!("\t\tdev extent chunk_tree {}", v.chunk_tree);
            println!(
                "\t\tchunk_objectid {} chunk_offset {} length {}",
                v.chunk_objectid, v.chunk_offset, v.length
            );
            println!(
                "\t\tchunk_tree_uuid {}",
                v.chunk_tree_uuid.as_hyphenated()
            );
        }
        ItemPayload::QgroupStatus(v) => {
            print!(
                "\t\tversion {} generation {} flags 0x{:x} scan {}",
                v.version, v.generation, v.flags, v.scan
            );
            if let Some(eg) = v.enable_gen {
                print!(" enable_gen {eg}");
            }
            println!();
        }
        ItemPayload::QgroupInfo(v) => {
            println!("\t\tgeneration {}", v.generation);
            println!(
                "\t\treferenced {} referenced_compressed {}",
                v.referenced, v.referenced_compressed
            );
            println!(
                "\t\texclusive {} exclusive_compressed {}",
                v.exclusive, v.exclusive_compressed
            );
        }
        ItemPayload::QgroupLimit(v) => {
            println!("\t\tflags 0x{:x}", v.flags);
            println!(
                "\t\tmax_referenced {} max_exclusive {}",
                v.max_referenced, v.max_exclusive
            );
            println!(
                "\t\trsv_referenced {} rsv_exclusive {}",
                v.rsv_referenced, v.rsv_exclusive
            );
        }
        ItemPayload::QgroupRelation => {}
        ItemPayload::DevStats(v) => {
            println!("\t\tdevice stats");
            for (i, (name, val)) in v.values.iter().enumerate() {
                println!("\t\t[{i}]\t{name} {val}");
            }
        }
        ItemPayload::BalanceItem { flags } => {
            println!("\t\tbalance status");
            println!("\t\t\tflags 0x{flags:x}");
        }
        ItemPayload::DevReplace(v) => {
            println!("\t\tsource devid {}", v.src_devid);
            println!(
                "\t\tcursor_left {} cursor_right {}",
                v.cursor_left, v.cursor_right
            );
            println!(
                "\t\treplace_mode {} replace_state {}",
                v.replace_mode, v.replace_state
            );
            println!(
                "\t\ttime_started {} time_stopped {}",
                v.time_started, v.time_stopped
            );
            println!(
                "\t\tnum_write_errors {} num_uncorrectable_read_errors {}",
                v.num_write_errors, v.num_uncorrectable_read_errors
            );
        }
        ItemPayload::UuidItem(v) => {
            for id in &v.subvol_ids {
                println!("\t\tsubvol_id {id}");
            }
        }
        ItemPayload::StringItem(data) => {
            println!("\t\tstring {}", String::from_utf8_lossy(data));
        }
        ItemPayload::RaidStripe(v) => {
            println!("\t\traid stripe encoding {}", v.encoding);
            for (i, s) in v.stripes.iter().enumerate() {
                println!(
                    "\t\t\tstripe {i} devid {} physical {}",
                    s.devid, s.physical
                );
            }
        }
        ItemPayload::Unknown(data) => {
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
    }
}

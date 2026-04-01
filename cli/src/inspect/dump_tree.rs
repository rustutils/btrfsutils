use super::print_tree::{self, PrintOptions};
use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    reader::{self, Traversal},
    superblock::Superblock,
    tree::{ObjectId, TreeBlock},
};
use clap::Parser;
use std::{collections::BTreeMap, path::PathBuf};

/// Dump tree blocks from a btrfs device or image file.
///
/// Reads raw btrfs tree blocks from a block device or filesystem image and
/// prints their contents in a human-readable format. By default all trees
/// are printed. Use -t to select a specific tree, -b to print a specific
/// block, or -e/-d/-u to print subsets.
#[derive(Parser, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct DumpTreeCommand {
    /// Path to a btrfs block device or image file
    path: PathBuf,

    /// Print only extent-related trees (extent tree and device tree)
    #[clap(short = 'e', long)]
    extents: bool,

    /// Print only device-related trees (root tree, chunk tree, device tree)
    #[clap(short = 'd', long)]
    device: bool,

    /// Print only short root node info
    #[clap(short = 'r', long)]
    roots: bool,

    /// Print root node info and backup roots
    #[clap(short = 'R', long)]
    backups: bool,

    /// Print only the UUID tree
    #[clap(short = 'u', long)]
    uuid: bool,

    /// Print only the specified tree (by name or numeric ID).
    ///
    /// Tree names: root, extent, chunk, dev, fs, csum, uuid, quota,
    /// free-space, block-group, raid-stripe, remap, tree-log, data-reloc.
    /// Numeric IDs (e.g. 5 for the filesystem tree) are also accepted.
    #[clap(short = 't', long)]
    tree: Option<String>,

    /// Print only the block at this logical byte number (repeatable)
    #[clap(short = 'b', long, num_args = 1)]
    block: Vec<u64>,

    /// With -b, also print all child blocks
    #[clap(long)]
    follow: bool,

    /// Use breadth-first traversal (default)
    #[clap(long)]
    bfs: bool,

    /// Use depth-first traversal
    #[clap(long)]
    dfs: bool,

    /// Hide filenames, subvolume names, xattr names and data
    #[clap(long)]
    hide_names: bool,

    /// Print checksums stored in metadata block headers
    #[clap(long)]
    csum_headers: bool,

    /// Print checksums stored in checksum items
    #[clap(long)]
    csum_items: bool,

    /// Do not scan for additional devices (accepted for compatibility,
    /// has no effect since this implementation reads from a single device)
    #[clap(long)]
    noscan: bool,
}

impl Runnable for DumpTreeCommand {
    #[allow(clippy::too_many_lines)]
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        println!("btrfs-cli v{}", env!("CARGO_PKG_VERSION"));

        let file = open_path(&self.path)?;

        let open = reader::filesystem_open(file).with_context(|| {
            format!(
                "failed to open btrfs filesystem on '{}'",
                self.path.display()
            )
        })?;

        let traversal = if self.dfs {
            Traversal::Dfs
        } else {
            Traversal::Bfs
        };

        let csum_size = open.superblock.csum_type.size();
        let nodesize = open.superblock.nodesize;
        let opts = PrintOptions {
            hide_names: self.hide_names,
            csum_headers: self.csum_headers,
            csum_items: self.csum_items,
            csum_size,
        };

        let mut reader = open.reader;
        let sb = &open.superblock;
        let tree_roots = &open.tree_roots;
        let mut print = |block: &TreeBlock| {
            print_tree::print_tree_block(block, nodesize, &opts);
        };

        // --block: print specific blocks
        if !self.block.is_empty() {
            for &logical in &self.block {
                reader::block_visit(
                    &mut reader,
                    logical,
                    self.follow,
                    traversal,
                    &mut print,
                )?;
            }
            return Ok(());
        }

        // --tree: print a specific tree
        if let Some(ref tree_name) = self.tree {
            let tree_id = parse_tree_id(tree_name)?;
            let root_bytenr = find_tree_root(tree_id, sb, tree_roots)?;
            reader::tree_walk(&mut reader, root_bytenr, traversal, &mut print)?;
            return Ok(());
        }

        // --roots / --backups: print short root info
        if self.roots || self.backups {
            print_roots(sb, tree_roots);
            if self.backups {
                print_backup_roots(sb);
            }
            return Ok(());
        }

        // --uuid: print only UUID tree
        if self.uuid {
            if let Some(&(root, _)) =
                tree_roots.get(&ObjectId::UuidTree.to_raw())
            {
                reader::tree_walk(&mut reader, root, traversal, &mut print)?;
            } else {
                bail!("UUID tree not found");
            }
            return Ok(());
        }

        // --extents: extent and device trees
        if self.extents {
            for &tree_id in
                &[ObjectId::ExtentTree.to_raw(), ObjectId::DevTree.to_raw()]
            {
                if let Some(&(root, _)) = tree_roots.get(&tree_id) {
                    let name = ObjectId::from_raw(tree_id);
                    println!("{name}:");
                    reader::tree_walk(
                        &mut reader,
                        root,
                        traversal,
                        &mut print,
                    )?;
                    println!();
                }
            }
            return Ok(());
        }

        // --device: root tree, chunk tree, device tree
        if self.device {
            println!("ROOT_TREE:");
            reader::tree_walk(&mut reader, sb.root, traversal, &mut print)?;
            println!();

            println!("CHUNK_TREE:");
            reader::tree_walk(
                &mut reader,
                sb.chunk_root,
                traversal,
                &mut print,
            )?;
            println!();

            if let Some(&(root, _)) =
                tree_roots.get(&ObjectId::DevTree.to_raw())
            {
                println!("DEV_TREE:");
                reader::tree_walk(&mut reader, root, traversal, &mut print)?;
                println!();
            }
            return Ok(());
        }

        // Default: print all trees
        println!("root tree");
        reader::tree_walk(&mut reader, sb.root, traversal, &mut print)?;
        println!("chunk tree");
        reader::tree_walk(&mut reader, sb.chunk_root, traversal, &mut print)?;

        let mut sorted_roots: Vec<_> = tree_roots.iter().collect();
        sorted_roots.sort_by_key(|&(id, _)| *id);

        for &(&tree_id, &(root_bytenr, key_offset)) in &sorted_roots {
            let label = tree_label(tree_id);
            let oid = ObjectId::from_raw(tree_id);
            println!("{label} key ({oid} ROOT_ITEM {key_offset}) ");
            reader::tree_walk(&mut reader, root_bytenr, traversal, &mut print)?;
        }

        println!("total bytes {}", sb.total_bytes);
        println!("bytes used {}", sb.bytes_used);
        println!("uuid {}", sb.fsid.as_hyphenated());

        Ok(())
    }
}

fn tree_label(tree_id: u64) -> &'static str {
    match ObjectId::from_raw(tree_id) {
        ObjectId::RootTree => "root tree",
        ObjectId::ExtentTree => "extent tree",
        ObjectId::ChunkTree => "chunk tree",
        ObjectId::DevTree => "device tree",
        ObjectId::FsTree => "fs tree",
        ObjectId::CsumTree => "checksum tree",
        ObjectId::QuotaTree => "quota tree",
        ObjectId::UuidTree => "uuid tree",
        ObjectId::FreeSpaceTree => "free space tree",
        ObjectId::BlockGroupTree => "block group tree",
        ObjectId::RaidStripeTree => "raid stripe tree",
        ObjectId::RemapTree => "remap tree",
        ObjectId::DataRelocTree => "data reloc tree",
        ObjectId::TreeLog => "log tree",
        _ => "file tree",
    }
}

fn parse_tree_id(name: &str) -> Result<u64> {
    if let Some(oid) = ObjectId::from_tree_name(name) {
        Ok(oid.to_raw())
    } else {
        bail!("unknown tree name '{name}'");
    }
}

fn find_tree_root(
    tree_id: u64,
    sb: &Superblock,
    tree_roots: &BTreeMap<u64, (u64, u64)>,
) -> Result<u64> {
    if tree_id == ObjectId::RootTree.to_raw() {
        return Ok(sb.root);
    }
    if tree_id == ObjectId::ChunkTree.to_raw() {
        return Ok(sb.chunk_root);
    }
    if tree_id == ObjectId::TreeLog.to_raw() && sb.log_root != 0 {
        return Ok(sb.log_root);
    }

    tree_roots
        .get(&tree_id)
        .map(|&(bytenr, _)| bytenr)
        .ok_or_else(|| {
            let name = ObjectId::from_raw(tree_id);
            anyhow::anyhow!("tree {name} (id {tree_id}) not found")
        })
}

fn print_roots(sb: &Superblock, tree_roots: &BTreeMap<u64, (u64, u64)>) {
    println!("root tree bytenr {} level {}", sb.root, sb.root_level);
    println!(
        "chunk tree bytenr {} level {}",
        sb.chunk_root, sb.chunk_root_level
    );
    if sb.log_root != 0 {
        println!(
            "log tree bytenr {} level {}",
            sb.log_root, sb.log_root_level
        );
    }
    for (&tree_id, &(bytenr, _)) in tree_roots {
        let name = ObjectId::from_raw(tree_id);
        println!("tree {name} (id {tree_id}) bytenr {bytenr}");
    }
}

fn print_backup_roots(sb: &Superblock) {
    for (i, root) in sb.backup_roots.iter().enumerate() {
        println!("backup {i}:");
        println!(
            "\ttree_root {} tree_root_gen {}",
            root.tree_root, root.tree_root_gen
        );
        println!(
            "\tchunk_root {} chunk_root_gen {}",
            root.chunk_root, root.chunk_root_gen
        );
        println!(
            "\textent_root {} extent_root_gen {}",
            root.extent_root, root.extent_root_gen
        );
        println!(
            "\tfs_root {} fs_root_gen {}",
            root.fs_root, root.fs_root_gen
        );
        println!(
            "\tdev_root {} dev_root_gen {}",
            root.dev_root, root.dev_root_gen
        );
        println!(
            "\tcsum_root {} csum_root_gen {}",
            root.csum_root, root.csum_root_gen
        );
    }
}

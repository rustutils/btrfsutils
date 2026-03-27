use crate::{Format, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    print::PrintOptions,
    reader::{self, Traversal},
    tree::ObjectId,
};
use clap::Parser;
use std::{fs::File, path::PathBuf};

/// Dump tree blocks from a btrfs device or image file.
///
/// Reads raw btrfs tree blocks from a block device or filesystem image and
/// prints their contents in a human-readable format. By default all trees
/// are printed. Use -t to select a specific tree, -b to print a specific
/// block, or -e/-d/-u to print subsets.
#[derive(Parser, Debug)]
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
}

impl Runnable for DumpTreeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        let open = reader::open_filesystem(file).with_context(|| {
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
        let opts = PrintOptions {
            hide_names: self.hide_names,
            csum_headers: self.csum_headers,
            csum_items: self.csum_items,
            csum_size,
        };

        let mut reader = open.reader;
        let sb = &open.superblock;
        let tree_roots = &open.tree_roots;

        // --block: print specific blocks
        if !self.block.is_empty() {
            for &logical in &self.block {
                reader::print_block(
                    &mut reader,
                    logical,
                    self.follow,
                    traversal,
                    &opts,
                )?;
            }
            return Ok(());
        }

        // --tree: print a specific tree
        if let Some(ref tree_name) = self.tree {
            let tree_id = parse_tree_id(tree_name)?;
            let root_bytenr = find_tree_root(tree_id, sb, tree_roots)?;
            reader::walk_tree(&mut reader, root_bytenr, traversal, &opts)?;
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
            if let Some(&root) = tree_roots
                .get(&(btrfs_disk::raw::BTRFS_UUID_TREE_OBJECTID as u64))
            {
                reader::walk_tree(&mut reader, root, traversal, &opts)?;
            } else {
                bail!("UUID tree not found");
            }
            return Ok(());
        }

        // --extents: extent and device trees
        if self.extents {
            for &tree_id in &[
                btrfs_disk::raw::BTRFS_EXTENT_TREE_OBJECTID as u64,
                btrfs_disk::raw::BTRFS_DEV_TREE_OBJECTID as u64,
            ] {
                if let Some(&root) = tree_roots.get(&tree_id) {
                    let name = ObjectId::from_raw(tree_id);
                    println!("{name}:");
                    reader::walk_tree(&mut reader, root, traversal, &opts)?;
                    println!();
                }
            }
            return Ok(());
        }

        // --device: root tree, chunk tree, device tree
        if self.device {
            // Root tree
            println!("ROOT_TREE:");
            reader::walk_tree(&mut reader, sb.root, traversal, &opts)?;
            println!();

            // Chunk tree
            println!("CHUNK_TREE:");
            reader::walk_tree(&mut reader, sb.chunk_root, traversal, &opts)?;
            println!();

            // Device tree
            if let Some(&root) = tree_roots
                .get(&(btrfs_disk::raw::BTRFS_DEV_TREE_OBJECTID as u64))
            {
                println!("DEV_TREE:");
                reader::walk_tree(&mut reader, root, traversal, &opts)?;
                println!();
            }
            return Ok(());
        }

        // Default: print all trees
        // First: root tree
        reader::walk_tree(&mut reader, sb.root, traversal, &opts)?;

        // Then: chunk tree
        reader::walk_tree(&mut reader, sb.chunk_root, traversal, &opts)?;

        // Then: all trees found in the root tree
        // Sort by tree ID for deterministic output
        let mut sorted_roots: Vec<_> = tree_roots.iter().collect();
        sorted_roots.sort_by_key(|&(id, _)| *id);

        for (_, root_bytenr) in &sorted_roots {
            reader::walk_tree(&mut reader, **root_bytenr, traversal, &opts)?;
        }

        Ok(())
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
    sb: &btrfs_disk::superblock::Superblock,
    tree_roots: &std::collections::BTreeMap<u64, u64>,
) -> Result<u64> {
    // Special cases: root tree and chunk tree are in the superblock
    if tree_id == btrfs_disk::raw::BTRFS_ROOT_TREE_OBJECTID as u64 {
        return Ok(sb.root);
    }
    if tree_id == btrfs_disk::raw::BTRFS_CHUNK_TREE_OBJECTID as u64 {
        return Ok(sb.chunk_root);
    }
    if tree_id == btrfs_disk::raw::BTRFS_TREE_LOG_OBJECTID as u64
        && sb.log_root != 0
    {
        return Ok(sb.log_root);
    }

    tree_roots.get(&tree_id).copied().ok_or_else(|| {
        let name = ObjectId::from_raw(tree_id);
        anyhow::anyhow!("tree {name} (id {tree_id}) not found")
    })
}

fn print_roots(
    sb: &btrfs_disk::superblock::Superblock,
    tree_roots: &std::collections::BTreeMap<u64, u64>,
) {
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
    for (&tree_id, &bytenr) in tree_roots {
        let name = ObjectId::from_raw(tree_id);
        println!("tree {name} (id {tree_id}) bytenr {bytenr}");
    }
}

fn print_backup_roots(sb: &btrfs_disk::superblock::Superblock) {
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

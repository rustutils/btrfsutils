use crate::{
    Format, Runnable,
    util::{SizeFormat, fmt_size},
};
use anyhow::{Context, Result};
use btrfs_disk::{
    raw,
    reader::{self, TreeStats, tree_stats_collect},
};
use clap::Parser;
use std::{path::PathBuf, time::Instant};

/// Print statistics about trees in a btrfs filesystem
///
/// Reads the filesystem image or block device directly (no mount required).
/// By default prints statistics for the root, extent, checksum, and fs trees.
/// Use -t to restrict to a single tree.
///
/// When the filesystem is mounted the numbers may be slightly inaccurate due
/// to concurrent modifications.
#[derive(Parser, Debug)]
pub struct TreeStatsCommand {
    /// Path to a btrfs block device or image file
    device: PathBuf,

    /// Print sizes in raw bytes instead of human-readable form
    #[clap(short = 'b', long = "raw")]
    raw: bool,

    /// Only print stats for the given tree (name or numeric ID)
    #[clap(short = 't', long = "tree")]
    tree: Option<String>,
}

/// Map a tree name string or decimal integer to a tree object ID.
fn parse_tree_id(s: &str) -> Result<u64> {
    match s {
        "root" => Ok(u64::from(raw::BTRFS_ROOT_TREE_OBJECTID)),
        "extent" => Ok(u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID)),
        "chunk" => Ok(u64::from(raw::BTRFS_CHUNK_TREE_OBJECTID)),
        "dev" => Ok(u64::from(raw::BTRFS_DEV_TREE_OBJECTID)),
        "fs" => Ok(u64::from(raw::BTRFS_FS_TREE_OBJECTID)),
        "csum" | "checksum" => Ok(u64::from(raw::BTRFS_CSUM_TREE_OBJECTID)),
        "quota" => Ok(u64::from(raw::BTRFS_QUOTA_TREE_OBJECTID)),
        "uuid" => Ok(u64::from(raw::BTRFS_UUID_TREE_OBJECTID)),
        "free-space" | "free_space" => {
            Ok(u64::from(raw::BTRFS_FREE_SPACE_TREE_OBJECTID))
        }
        "data-reloc" | "data_reloc" => {
            Ok(raw::BTRFS_DATA_RELOC_TREE_OBJECTID as u64)
        }
        _ => s.parse::<u64>().with_context(|| {
            format!("cannot parse tree id '{s}' (expected a name or number)")
        }),
    }
}

fn tree_name(id: u64) -> String {
    match id as u32 {
        x if x == raw::BTRFS_ROOT_TREE_OBJECTID => "root tree".to_string(),
        x if x == raw::BTRFS_EXTENT_TREE_OBJECTID => "extent tree".to_string(),
        x if x == raw::BTRFS_CHUNK_TREE_OBJECTID => "chunk tree".to_string(),
        x if x == raw::BTRFS_DEV_TREE_OBJECTID => "dev tree".to_string(),
        x if x == raw::BTRFS_FS_TREE_OBJECTID => "fs tree".to_string(),
        x if x == raw::BTRFS_CSUM_TREE_OBJECTID => "csum tree".to_string(),
        x if x == raw::BTRFS_QUOTA_TREE_OBJECTID => "quota tree".to_string(),
        x if x == raw::BTRFS_UUID_TREE_OBJECTID => "uuid tree".to_string(),
        x if x == raw::BTRFS_FREE_SPACE_TREE_OBJECTID => {
            "free-space tree".to_string()
        }
        x if x as i32 == raw::BTRFS_DATA_RELOC_TREE_OBJECTID => {
            "data-reloc tree".to_string()
        }
        _ => format!("tree {id}"),
    }
}

fn print_stats(
    name: &str,
    stats: &TreeStats,
    elapsed_secs: u64,
    elapsed_usecs: u32,
    fmt: &SizeFormat,
) {
    println!("Calculating size of {name}");
    println!("\tTotal size: {}", fmt_size(stats.total_bytes, fmt));
    println!("\t\tInline data: {}", fmt_size(stats.total_inline, fmt));
    println!("\tTotal seeks: {}", stats.total_seeks);
    println!("\t\tForward seeks: {}", stats.forward_seeks);
    println!("\t\tBackward seeks: {}", stats.backward_seeks);
    let avg_seek = if stats.total_seeks > 0 {
        stats.total_seek_len / stats.total_seeks
    } else {
        0
    };
    println!("\t\tAvg seek len: {}", fmt_size(avg_seek, fmt));

    // When no seeks occurred, the C reference sets total_clusters=1, min=0.
    let (total_clusters, min_cluster, max_cluster, avg_cluster) =
        if stats.min_cluster_size == u64::MAX {
            (1u64, 0u64, stats.max_cluster_size, 0u64)
        } else {
            let avg = if stats.total_clusters > 0 {
                stats.total_cluster_size / stats.total_clusters
            } else {
                0
            };
            (
                stats.total_clusters,
                stats.min_cluster_size,
                stats.max_cluster_size,
                avg,
            )
        };
    println!("\tTotal clusters: {total_clusters}");
    println!("\t\tAvg cluster size: {}", fmt_size(avg_cluster, fmt));
    println!("\t\tMin cluster size: {}", fmt_size(min_cluster, fmt));
    println!("\t\tMax cluster size: {}", fmt_size(max_cluster, fmt));

    let spread = stats.highest_bytenr.saturating_sub(stats.lowest_bytenr);
    println!("\tTotal disk spread: {}", fmt_size(spread, fmt));
    println!("\tTotal read time: {elapsed_secs} s {elapsed_usecs} us");
    println!("\tLevels: {}", stats.levels);
    println!("\tTotal nodes: {}", stats.total_nodes);

    for i in 0..stats.levels as usize {
        let count = stats.node_counts.get(i).copied().unwrap_or(0);
        if i == 0 {
            println!("\t\tOn level {i}: {count:8}");
        } else {
            let child_count =
                stats.node_counts.get(i - 1).copied().unwrap_or(0);
            let fanout = if count > 0 { child_count / count } else { 0 };
            println!("\t\tOn level {i}: {count:8}  (avg fanout {fanout})");
        }
    }
}

impl Runnable for TreeStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = crate::util::open_path(&self.device)?;
        let mut fs = reader::filesystem_open(file).with_context(|| {
            format!("failed to open '{}'", self.device.display())
        })?;

        let size_fmt = if self.raw {
            SizeFormat::Raw
        } else {
            SizeFormat::HumanIec
        };

        if let Some(ref tree_spec) = self.tree {
            let tree_id = parse_tree_id(tree_spec)?;
            let (root_logical, _) =
                fs.tree_roots.get(&tree_id).copied().ok_or_else(|| {
                    anyhow::anyhow!("tree {tree_id} not found in filesystem")
                })?;

            let name = tree_name(tree_id);
            let start = Instant::now();
            let stats = tree_stats_collect(&mut fs.reader, root_logical, true)
                .with_context(|| format!("failed to walk {name}"))?;
            let elapsed = start.elapsed();
            print_stats(
                &name,
                &stats,
                elapsed.as_secs(),
                elapsed.subsec_micros(),
                &size_fmt,
            );
        } else {
            // Default: root, extent, csum, fs trees.
            // The root tree is bootstrapped from the superblock, not a
            // ROOT_ITEM, so its logical address comes from superblock.root.
            let root_tree_logical = fs.superblock.root;
            let default_trees: &[(u64, u64, bool)] = &[
                (
                    u64::from(raw::BTRFS_ROOT_TREE_OBJECTID),
                    root_tree_logical,
                    false,
                ),
                (
                    u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID),
                    fs.tree_roots
                        .get(&u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID))
                        .map_or(0, |&(l, _)| l),
                    false,
                ),
                (
                    u64::from(raw::BTRFS_CSUM_TREE_OBJECTID),
                    fs.tree_roots
                        .get(&u64::from(raw::BTRFS_CSUM_TREE_OBJECTID))
                        .map_or(0, |&(l, _)| l),
                    false,
                ),
                (
                    u64::from(raw::BTRFS_FS_TREE_OBJECTID),
                    fs.tree_roots
                        .get(&u64::from(raw::BTRFS_FS_TREE_OBJECTID))
                        .map_or(0, |&(l, _)| l),
                    true,
                ),
            ];

            for &(tree_id, root_logical, find_inline) in default_trees {
                if root_logical == 0 {
                    continue;
                }
                let name = tree_name(tree_id);
                let start = Instant::now();
                let stats = tree_stats_collect(
                    &mut fs.reader,
                    root_logical,
                    find_inline,
                )
                .with_context(|| format!("failed to walk {name}"))?;
                let elapsed = start.elapsed();
                print_stats(
                    &name,
                    &stats,
                    elapsed.as_secs(),
                    elapsed.subsec_micros(),
                    &size_fmt,
                );
            }
        }

        Ok(())
    }
}

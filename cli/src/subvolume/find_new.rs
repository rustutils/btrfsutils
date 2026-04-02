use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_disk::items::{FileExtentBody, FileExtentItem, FileExtentType};
use btrfs_uapi::{
    filesystem::sync,
    inode::ino_paths,
    raw::BTRFS_EXTENT_DATA_KEY,
    subvolume::subvolume_info,
    tree_search::{SearchKey, tree_search},
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// List the recently modified files in a subvolume
///
/// Prints all files that have been modified since the given generation number.
/// The generation can be found with `btrfs subvolume show`.
#[derive(Parser, Debug)]
pub struct SubvolumeFindNewCommand {
    /// Path to the subvolume to search
    path: PathBuf,

    /// Only show files modified at or after this generation number
    last_gen: u64,
}

impl Runnable for SubvolumeFindNewCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        // Sync first so we see the latest data.
        sync(file.as_fd()).with_context(|| {
            format!("failed to sync '{}'", self.path.display())
        })?;

        // Get the current generation for the "transid marker" output.
        let info = subvolume_info(file.as_fd()).with_context(|| {
            format!(
                "failed to get subvolume info for '{}'",
                self.path.display()
            )
        })?;
        let max_gen = info.generation;

        // Search tree 0 (the subvolume's own tree, relative to the fd) for
        // EXTENT_DATA_KEY items.  The min_transid filter restricts results to
        // items whose metadata block was written at or after last_gen.
        let mut key = SearchKey::for_type(0, BTRFS_EXTENT_DATA_KEY);
        key.min_transid = self.last_gen;

        let mut cache_ino: u64 = 0;
        let mut cache_name: Option<String> = None;

        tree_search(file.as_fd(), key, |hdr, data| {
            let Some(fe) = FileExtentItem::parse(data) else {
                return Ok(());
            };

            if fe.generation < self.last_gen {
                return Ok(());
            }

            let compressed =
                !matches!(fe.compression, btrfs_disk::items::CompressionType::None);

            let (disk_start, disk_offset, len) = match &fe.body {
                FileExtentBody::Regular {
                    disk_bytenr,
                    num_bytes,
                    offset,
                    ..
                } => (*disk_bytenr, *offset, *num_bytes),
                FileExtentBody::Inline { inline_size } => {
                    (0, 0, *inline_size as u64)
                }
            };

            // Resolve inode to path (with caching for consecutive extents
            // of the same inode).
            let name = if hdr.objectid == cache_ino {
                cache_name.as_deref().unwrap_or("unknown")
            } else {
                let resolved = match ino_paths(file.as_fd(), hdr.objectid) {
                    Ok(paths) if !paths.is_empty() => {
                        Some(paths.into_iter().next().unwrap())
                    }
                    _ => None,
                };
                cache_ino = hdr.objectid;
                cache_name = resolved;
                cache_name.as_deref().unwrap_or("unknown")
            };

            // Build flags string.
            let mut flags = String::new();
            if compressed {
                flags.push_str("COMPRESS");
            }
            if fe.extent_type == FileExtentType::Prealloc {
                if !flags.is_empty() {
                    flags.push('|');
                }
                flags.push_str("PREALLOC");
            }
            if fe.extent_type == FileExtentType::Inline {
                if !flags.is_empty() {
                    flags.push('|');
                }
                flags.push_str("INLINE");
            }
            if flags.is_empty() {
                flags.push_str("NONE");
            }

            println!(
                "inode {} file offset {} len {} disk start {} offset {} gen {} flags {flags} {name}",
                hdr.objectid, hdr.offset, len, disk_start, disk_offset, fe.generation,
            );

            Ok(())
        })
        .with_context(|| format!("tree search failed for '{}'", self.path.display()))?;

        println!("transid marker was {max_gen}");

        Ok(())
    }
}

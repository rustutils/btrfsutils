use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::{
    filesystem::sync,
    inode::ino_paths,
    raw::{
        BTRFS_EXTENT_DATA_KEY, BTRFS_FILE_EXTENT_INLINE,
        BTRFS_FILE_EXTENT_PREALLOC, BTRFS_FILE_EXTENT_REG,
        btrfs_file_extent_item,
    },
    subvolume::subvolume_info,
    tree_search::{SearchKey, tree_search},
};
use clap::Parser;
use std::{fs::File, mem, os::unix::io::AsFd, path::PathBuf};

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

fn rle64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

impl Runnable for SubvolumeFindNewCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

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
        let mut key = SearchKey::for_type(0, BTRFS_EXTENT_DATA_KEY as u32);
        key.min_transid = self.last_gen;

        let mut cache_ino: u64 = 0;
        let mut cache_name: Option<String> = None;

        tree_search(file.as_fd(), key, |hdr, data| {
            let gen_off = mem::offset_of!(btrfs_file_extent_item, generation);
            let type_off = mem::offset_of!(btrfs_file_extent_item, type_);
            let compression_off = mem::offset_of!(btrfs_file_extent_item, compression);

            // Need at least enough data to read the generation and type fields.
            if data.len() < type_off + 1 {
                return Ok(());
            }

            let found_gen = rle64(data, gen_off);
            if found_gen < self.last_gen {
                return Ok(());
            }

            let extent_type = data[type_off];
            let compressed = data.get(compression_off).copied().unwrap_or(0) != 0;

            let (disk_start, disk_offset, len) =
                if extent_type == BTRFS_FILE_EXTENT_REG as u8
                    || extent_type == BTRFS_FILE_EXTENT_PREALLOC as u8
                {
                    let disk_bytenr_off = mem::offset_of!(btrfs_file_extent_item, disk_bytenr);
                    let offset_off = mem::offset_of!(btrfs_file_extent_item, offset);
                    let num_bytes_off = mem::offset_of!(btrfs_file_extent_item, num_bytes);

                    if data.len() < num_bytes_off + 8 {
                        return Ok(());
                    }
                    (
                        rle64(data, disk_bytenr_off),
                        rle64(data, offset_off),
                        rle64(data, num_bytes_off),
                    )
                } else if extent_type == BTRFS_FILE_EXTENT_INLINE as u8 {
                    let ram_bytes_off = mem::offset_of!(btrfs_file_extent_item, ram_bytes);
                    if data.len() < ram_bytes_off + 8 {
                        return Ok(());
                    }
                    (0, 0, rle64(data, ram_bytes_off))
                } else {
                    return Ok(());
                };

            // Resolve inode to path (with caching for consecutive extents
            // of the same inode).
            let name = if hdr.objectid == cache_ino {
                cache_name.as_deref().unwrap_or("unknown")
            } else {
                let resolved = match ino_paths(file.as_fd(), hdr.objectid) {
                    Ok(paths) if !paths.is_empty() => Some(paths.into_iter().next().unwrap()),
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
            if extent_type == BTRFS_FILE_EXTENT_PREALLOC as u8 {
                if !flags.is_empty() {
                    flags.push('|');
                }
                flags.push_str("PREALLOC");
            }
            if extent_type == BTRFS_FILE_EXTENT_INLINE as u8 {
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
                hdr.objectid, hdr.offset, len, disk_start, disk_offset, found_gen,
            );

            Ok(())
        })
        .with_context(|| format!("tree search failed for '{}'", self.path.display()))?;

        println!("transid marker was {max_gen}");

        Ok(())
    }
}

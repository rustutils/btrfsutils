use super::print_super;
use crate::{Format, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_disk::superblock::{self, SUPER_MIRROR_MAX};
use clap::Parser;
use std::{fs::File, path::PathBuf};

/// Dump the btrfs superblock from a device or image file.
///
/// Reads and displays the superblock stored on a btrfs block device or
/// filesystem image. By default only the primary superblock (mirror 0,
/// at offset 64 KiB) is printed. Use -a to print all mirrors, or -s to
/// select a specific one.
#[derive(Parser, Debug)]
pub struct DumpSuperCommand {
    /// Path to a btrfs block device or image file
    path: PathBuf,

    /// Print full information including sys_chunk_array and backup roots
    #[clap(short = 'f', long)]
    full: bool,

    /// Print all superblock mirrors (0, 1, 2)
    #[clap(short = 'a', long)]
    all: bool,

    /// Print only this superblock mirror (0, 1, or 2)
    #[clap(short = 's', long = "super", value_parser = clap::value_parser!(u32).range(..SUPER_MIRROR_MAX as i64))]
    mirror: Option<u32>,

    /// Read the superblock from this byte offset instead of using a mirror index
    #[clap(long, conflicts_with_all = ["mirror", "all"])]
    bytenr: Option<u64>,

    /// Attempt to print superblocks with bad magic
    #[clap(short = 'F', long)]
    force: bool,
}

impl Runnable for DumpSuperCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let mut file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        // Collect (offset, label) pairs for each superblock to print.
        let offsets: Vec<(u64, String)> = if let Some(bytenr) = self.bytenr {
            vec![(bytenr, format!("bytenr={bytenr}"))]
        } else if self.all {
            (0..SUPER_MIRROR_MAX)
                .map(|m| {
                    let off = superblock::super_mirror_offset(m);
                    (off, format!("bytenr={off}"))
                })
                .collect()
        } else {
            let m = self.mirror.unwrap_or(0);
            let off = superblock::super_mirror_offset(m);
            vec![(off, format!("bytenr={off}"))]
        };

        for (i, (offset, label)) in offsets.iter().enumerate() {
            if i > 0 {
                println!();
            }

            println!("superblock: {label}, device={}", self.path.display());

            let sb = match superblock::read_superblock_at(&mut file, *offset) {
                Ok(sb) => sb,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!(
                        "superblock at {label} beyond end of device, skipping"
                    );
                    continue;
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "failed to read superblock at {label} from '{}'",
                            self.path.display()
                        )
                    });
                }
            };

            if !sb.magic_is_valid() && !self.force {
                if self.all {
                    println!(
                        "superblock at {label} has bad magic, skipping (use -F to force)"
                    );
                    continue;
                }
                bail!(
                    "bad magic on superblock at {label} of '{}' (use -F to force)",
                    self.path.display()
                );
            }

            println!(
                "---------------------------------------------------------"
            );
            print_super::print_superblock(&sb, self.full);
        }

        Ok(())
    }
}

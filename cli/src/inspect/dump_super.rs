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
    #[clap(short = 's', long = "super")]
    mirror: Option<u32>,

    /// Attempt to print superblocks with bad magic
    #[clap(short = 'F', long)]
    force: bool,
}

impl Runnable for DumpSuperCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let mut file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let mirrors: Vec<u32> = if self.all {
            (0..SUPER_MIRROR_MAX).collect()
        } else if let Some(m) = self.mirror {
            if m >= SUPER_MIRROR_MAX {
                bail!("mirror index must be 0, 1, or 2 (got {m})");
            }
            vec![m]
        } else {
            vec![0]
        };

        for (i, &mirror) in mirrors.iter().enumerate() {
            if i > 0 {
                println!();
            }

            let offset = superblock::super_mirror_offset(mirror);
            println!(
                "superblock: bytenr={offset}, device={}",
                self.path.display()
            );

            let sb = match superblock::read_superblock(&mut file, mirror) {
                Ok(sb) => sb,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("superblock mirror {mirror} beyond end of device, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "failed to read superblock mirror {mirror} from '{}'",
                            self.path.display()
                        )
                    });
                }
            };

            if !sb.magic_is_valid() && !self.force {
                if self.all {
                    println!(
                        "superblock mirror {mirror} has bad magic, skipping (use -F to force)"
                    );
                    continue;
                }
                bail!(
                    "bad magic on superblock mirror {mirror} of '{}' (use -F to force)",
                    self.path.display()
                );
            }

            println!("---------------------------------------------------------");
            superblock::print_superblock(&sb, self.full);
        }

        Ok(())
    }
}

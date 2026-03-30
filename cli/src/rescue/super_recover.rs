use crate::{Format, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::superblock::{
    SUPER_MIRROR_MAX, read_superblock_bytes_at, super_mirror_offset,
    superblock_generation, superblock_is_valid, write_superblock_all_mirrors,
};
use clap::Parser;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, Write},
    path::PathBuf,
};

/// Recover bad superblocks from good copies
///
/// Reads all superblock mirrors and validates their checksums and magic.
/// If any mirrors are corrupted, the best valid copy (highest generation)
/// is written back to all mirrors.
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueSuperRecoverCommand {
    /// Path to the device
    device: PathBuf,

    /// Assume an answer of 'yes' to all questions
    #[clap(short = 'y', long)]
    yes: bool,
}

struct MirrorRecord {
    bytenr: u64,
    buf: [u8; 4096],
    generation: u64,
}

impl Runnable for RescueSuperRecoverCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        let mut file = File::open(&self.device).with_context(|| {
            format!("failed to open '{}'", self.device.display())
        })?;

        let mut good: Vec<MirrorRecord> = Vec::new();
        let mut bad: Vec<MirrorRecord> = Vec::new();

        for i in 0..SUPER_MIRROR_MAX {
            let bytenr = super_mirror_offset(i);
            match read_superblock_bytes_at(&mut file, bytenr) {
                Ok(buf) => {
                    if superblock_is_valid(&buf) {
                        let generation = superblock_generation(&buf);
                        good.push(MirrorRecord {
                            bytenr,
                            buf,
                            generation,
                        });
                    } else {
                        let generation = superblock_generation(&buf);
                        bad.push(MirrorRecord {
                            bytenr,
                            buf,
                            generation,
                        });
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Mirror offset is beyond end of device — skip silently.
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "failed to read mirror {} from '{}'",
                            i,
                            self.device.display()
                        )
                    });
                }
            }
        }

        // Demote good mirrors with generation below the maximum to bad.
        if let Some(max_gen) = good.iter().map(|r| r.generation).max() {
            let (keep, demote): (Vec<_>, Vec<_>) =
                good.into_iter().partition(|r| r.generation == max_gen);
            good = keep;
            bad.extend(demote);
        }

        println!("[All good supers]:");
        for r in &good {
            println!("\t\tdevice name = {}", self.device.display());
            println!("\t\tsuperblock bytenr = {}", r.bytenr);
            println!();
        }
        println!("[All bad supers]:");
        for r in &bad {
            println!("\t\tdevice name = {}", self.device.display());
            println!("\t\tsuperblock bytenr = {}", r.bytenr);
            println!();
        }

        if bad.is_empty() {
            println!("All superblocks are valid, no need to recover");
            return Ok(());
        }

        if good.is_empty() {
            bail!("no valid superblock found on '{}'", self.device.display());
        }

        if !self.yes {
            print!(
                "Make sure this is a btrfs disk otherwise the tool will destroy other fs, Are you sure? (yes/no): "
            );
            io::stdout().flush()?;
            let stdin = io::stdin();
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            if line.trim() != "yes" {
                bail!("aborted by user");
            }
        }

        // Write the best good mirror to all mirrors.
        let source = &good[0];
        let mut file_rw = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)
            .with_context(|| {
                format!(
                    "failed to open '{}' for writing",
                    self.device.display()
                )
            })?;
        write_superblock_all_mirrors(&mut file_rw, &source.buf).with_context(
            || {
                format!(
                    "failed to write superblocks to '{}'",
                    self.device.display()
                )
            },
        )?;

        println!("Recovered bad superblocks successfully");
        Ok(())
    }
}

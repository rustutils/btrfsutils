use crate::{RunContext, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_disk::items::{ChunkItem, DeviceItem};
use clap::Parser;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};
use uuid::Uuid;

mod apply;
mod reconstruct;
mod report;
mod scan;

/// Recover the chunk tree by scanning the device for surviving chunk data.
///
/// Sweeps the raw device for tree blocks that belong to the chunk tree,
/// extracts CHUNK_ITEM and DEV_ITEM records, resolves conflicts between
/// duplicates found at different generations, and reports whether a
/// coherent chunk tree can be reconstructed.
///
/// By default this is a read-only scan that reports whether recovery is
/// possible. Use --apply to actually write the reconstructed chunk tree.
#[derive(Parser, Debug)]
pub struct RescueChunkRecoverCommand {
    /// Assume an answer of 'yes' to all questions
    #[clap(short = 'y', long)]
    pub yes: bool,

    /// Write the reconstructed chunk tree to disk
    #[clap(long)]
    pub apply: bool,

    /// Device to recover
    pub device: PathBuf,
}

impl Runnable for RescueChunkRecoverCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if crate::util::is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        let mut file = File::open(&self.device).with_context(|| {
            format!("failed to open '{}'", self.device.display())
        })?;

        eprintln!("Phase 1: scanning device for chunk tree data...");
        let scan_result = scan::scan_device(&mut file)?;

        eprintln!("Phase 2: reconstructing chunk tree...");
        let reconstruction = reconstruct::reconstruct(&scan_result)?;

        report::print_report(&self.device, &scan_result, &reconstruction);

        if !reconstruction.chunk_root_covered {
            bail!(
                "chunk root logical address {:#x} is not covered by any recovered chunk",
                scan_result.chunk_root,
            );
        }

        if !self.apply {
            println!(
                "\nDry run complete. Use --apply to write the reconstructed chunk tree.",
            );
            return Ok(());
        }

        if !self.yes {
            eprint!(
                "\nAbout to write reconstructed chunk tree ({} chunks, {} devices). Continue? [y/N] ",
                reconstruction.chunks.len(),
                reconstruction.devices.len(),
            );
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if !answer.trim().eq_ignore_ascii_case("y") {
                bail!("aborted by user");
            }
        }

        // Reopen with write access for the apply phase.
        drop(file);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)
            .with_context(|| {
                format!(
                    "failed to open '{}' for writing",
                    self.device.display()
                )
            })?;

        eprintln!("Phase 4: writing reconstructed chunk tree...");
        apply::apply_chunk_tree(file, &scan_result, &reconstruction)?;

        println!("Chunk tree successfully written.");
        Ok(())
    }
}

// --- Shared types ---

/// Where a recovered record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordSource {
    /// From the superblock's sys_chunk_array (bootstrap).
    Bootstrap,
    /// From a chunk-tree leaf found during the raw device scan.
    ScannedLeaf {
        /// Physical byte offset on device where the leaf was found.
        bytenr: u64,
        /// Generation of the leaf header.
        generation: u64,
    },
}

impl RecordSource {
    fn is_bootstrap(self) -> bool {
        matches!(self, Self::Bootstrap)
    }
}

/// A recovered CHUNK_ITEM with provenance.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    /// Logical start address of this chunk (the key offset).
    pub logical: u64,
    /// The parsed chunk item.
    pub chunk: ChunkItem,
    /// Where this record was found.
    pub source: RecordSource,
    /// Generation of the tree block containing this record.
    pub generation: u64,
}

/// A recovered DEV_ITEM with provenance.
#[derive(Debug, Clone)]
pub struct DevRecord {
    /// Device ID.
    pub devid: u64,
    /// The parsed device item.
    pub device: DeviceItem,
    /// Where this record was found.
    pub source: RecordSource,
    /// Generation of the tree block containing this record.
    pub generation: u64,
}

/// A conflict that was resolved during reconstruction.
#[derive(Debug)]
pub enum Conflict {
    /// Two DEV_ITEM records for the same devid.
    DevItem {
        devid: u64,
        winner_gen: u64,
        loser_gen: u64,
    },
    /// Two CHUNK_ITEM records for the same logical start.
    ChunkItem {
        logical: u64,
        winner_gen: u64,
        loser_gen: u64,
        bootstrap_won: bool,
    },
}

/// A non-fatal warning from reconstruction.
#[derive(Debug)]
pub enum Warning {
    /// A chunk stripe references a devid with no corresponding DEV_ITEM.
    DanglingStripeRef { logical: u64, devid: u64 },
}

/// Output of Phase 1: raw device scan.
pub struct ScanResult {
    pub fsid: Uuid,
    pub metadata_uuid: Uuid,
    pub has_metadata_uuid: bool,
    pub nodesize: u32,
    pub chunk_root: u64,
    pub chunk_root_level: u8,
    pub sb_generation: u64,
    pub device_size: u64,
    pub bytes_scanned: u64,
    pub candidates_checked: u64,
    pub valid_blocks: u64,
    pub chunk_tree_leaves: u64,
    pub chunk_records: Vec<ChunkRecord>,
    pub dev_records: Vec<DevRecord>,
}

/// Output of Phase 2: chunk tree reconstruction.
pub struct ReconstructionResult {
    /// Deduplicated, conflict-resolved chunks sorted by logical start.
    pub chunks: Vec<ChunkRecord>,
    /// Deduplicated device records.
    pub devices: Vec<DevRecord>,
    /// Conflicts that were resolved.
    pub conflicts: Vec<Conflict>,
    /// Non-fatal warnings.
    pub warnings: Vec<Warning>,
    /// Whether the superblock's chunk_root logical address is covered.
    pub chunk_root_covered: bool,
}

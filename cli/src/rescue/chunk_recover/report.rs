use super::{Conflict, ReconstructionResult, ScanResult, Warning};
use crate::util::human_bytes;
use std::path::Path;

/// Print the chunk-recover scan and reconstruction report.
pub fn print_report(
    device: &Path,
    scan: &ScanResult,
    result: &ReconstructionResult,
) {
    println!("Chunk recovery scan for {}", device.display());
    println!();

    // Phase 1 stats.
    println!("Phase 1: raw device scan");
    println!("  Device size:          {}", human_bytes(scan.device_size));
    println!(
        "  Bytes scanned:        {}",
        human_bytes(scan.bytes_scanned),
    );
    println!("  Candidates checked:   {}", scan.candidates_checked);
    println!("  Valid tree blocks:     {}", scan.valid_blocks);
    println!("  Chunk tree leaves:    {}", scan.chunk_tree_leaves);
    println!("  Chunk records found:  {}", scan.chunk_records.len());
    println!("  Device records found: {}", scan.dev_records.len());
    println!();

    // Phase 2 stats.
    println!("Phase 2: chunk tree reconstruction");

    if result.conflicts.is_empty() {
        println!("  Conflicts resolved:   0");
    } else {
        println!("  Conflicts resolved:   {}", result.conflicts.len());
        for conflict in &result.conflicts {
            print_conflict(conflict);
        }
    }

    if result.warnings.is_empty() {
        println!("  Warnings:             0");
    } else {
        println!("  Warnings:             {}", result.warnings.len());
        for warning in &result.warnings {
            print_warning(warning);
        }
    }

    println!("  Recovered chunks:     {}", result.chunks.len());
    println!("  Recovered devices:    {}", result.devices.len());
    println!();

    // Chunk table.
    if !result.chunks.is_empty() {
        println!("Phase 3: recovered chunk map");
        println!(
            "  {:>18}  {:>10}  {:>14}  {:>8}  {:>7}",
            "Logical", "Length", "Type", "Profile", "Stripes",
        );

        for chunk in &result.chunks {
            let flags = chunk.chunk.chunk_type;
            println!(
                "  {:#018x}  {:>10}  {:>14}  {:>8}  {:>7}",
                chunk.logical,
                human_bytes(chunk.chunk.length),
                flags.type_name(),
                flags.profile_name(),
                chunk.chunk.num_stripes,
            );
        }
        println!();
    }

    // Coverage.
    println!(
        "Chunk root ({:#x}) covered: {}",
        scan.chunk_root,
        if result.chunk_root_covered {
            "yes"
        } else {
            "NO"
        },
    );

    // Final status.
    if result.chunk_root_covered {
        println!(
            "\nResult: chunk tree can be reconstructed ({} chunks, {} devices)",
            result.chunks.len(),
            result.devices.len(),
        );
    } else {
        println!(
            "\nResult: chunk tree CANNOT be reconstructed \
             (chunk root not covered)",
        );
    }
}

fn print_conflict(conflict: &Conflict) {
    match conflict {
        Conflict::DevItem {
            devid,
            winner_gen,
            loser_gen,
        } => {
            println!(
                "    DEV_ITEM devid={devid}: \
                 generation {winner_gen} beat generation {loser_gen}",
            );
        }
        Conflict::ChunkItem {
            logical,
            winner_gen,
            loser_gen,
            bootstrap_won,
        } => {
            if *bootstrap_won {
                println!(
                    "    CHUNK_ITEM logical={logical:#x}: \
                     bootstrap wins tie at generation {winner_gen}",
                );
            } else {
                println!(
                    "    CHUNK_ITEM logical={logical:#x}: \
                     generation {winner_gen} beat generation {loser_gen}",
                );
            }
        }
    }
}

fn print_warning(warning: &Warning) {
    match warning {
        Warning::DanglingStripeRef { logical, devid } => {
            println!(
                "    chunk {logical:#x}: stripe references \
                 unknown devid {devid}",
            );
        }
    }
}

use super::{
    ChunkRecord, Conflict, DevRecord, ReconstructionResult, ScanResult, Warning,
};
use anyhow::{Result, bail};
use std::collections::{BTreeMap, HashSet};

/// Reconstruct a coherent chunk map from scan results.
///
/// Deduplicates records, resolves conflicts by generation and bootstrap
/// priority, detects overlaps, validates stripe-to-device references,
/// and checks that the chunk_root logical address is covered.
pub fn reconstruct(scan: &ScanResult) -> Result<ReconstructionResult> {
    let mut conflicts = Vec::new();
    let mut warnings = Vec::new();

    let devices = dedup_dev_items(&scan.dev_records, &mut conflicts)?;
    let chunks = dedup_chunk_items(&scan.chunk_records, &mut conflicts)?;

    check_overlaps(&chunks)?;

    let dev_ids: HashSet<u64> = devices.iter().map(|d| d.devid).collect();
    check_stripe_refs(&chunks, &dev_ids, &mut warnings);

    let chunk_root_covered = chunks.iter().any(|c| {
        scan.chunk_root >= c.logical
            && scan.chunk_root < c.logical + c.chunk.length
    });

    Ok(ReconstructionResult {
        chunks,
        devices,
        conflicts,
        warnings,
        chunk_root_covered,
    })
}

/// Deduplicate DEV_ITEM records by devid.
///
/// When multiple records exist for the same devid, the one with the
/// highest generation wins. Equal generations from different sources
/// is an ambiguous state that cannot be resolved automatically.
fn dedup_dev_items(
    records: &[DevRecord],
    conflicts: &mut Vec<Conflict>,
) -> Result<Vec<DevRecord>> {
    let mut by_devid: BTreeMap<u64, Vec<&DevRecord>> = BTreeMap::new();
    for rec in records {
        by_devid.entry(rec.devid).or_default().push(rec);
    }

    let mut result = Vec::with_capacity(by_devid.len());
    for (devid, mut group) in by_devid {
        group.sort_by_key(|c| std::cmp::Reverse(c.generation));

        if group.len() > 1 {
            let best = group[0];
            let second = group[1];

            if best.generation == second.generation
                && !same_source(&best.source, &second.source)
            {
                bail!(
                    "ambiguous DEV_ITEM for devid {devid}: two records \
                     at generation {} from different sources",
                    best.generation,
                );
            }

            for loser in &group[1..] {
                if loser.generation != best.generation {
                    conflicts.push(Conflict::DevItem {
                        devid,
                        winner_gen: best.generation,
                        loser_gen: loser.generation,
                    });
                }
            }
        }

        result.push(group[0].clone());
    }

    Ok(result)
}

/// Deduplicate CHUNK_ITEM records by logical start address.
///
/// When multiple records exist for the same logical start:
/// 1. Bootstrap records win ties (same generation).
/// 2. Otherwise, highest generation wins.
/// 3. Equal generations from non-bootstrap sources is ambiguous.
fn dedup_chunk_items(
    records: &[ChunkRecord],
    conflicts: &mut Vec<Conflict>,
) -> Result<Vec<ChunkRecord>> {
    let mut by_logical: BTreeMap<u64, Vec<&ChunkRecord>> = BTreeMap::new();
    for rec in records {
        by_logical.entry(rec.logical).or_default().push(rec);
    }

    let mut result = Vec::with_capacity(by_logical.len());
    for (logical, mut group) in by_logical {
        // Sort: bootstrap first, then by generation descending.
        group.sort_by(|a, b| {
            let a_boot = a.source.is_bootstrap();
            let b_boot = b.source.is_bootstrap();
            b_boot.cmp(&a_boot).then(b.generation.cmp(&a.generation))
        });

        if group.len() > 1 {
            let best = group[0];
            let second = group[1];

            if best.generation == second.generation
                && !best.source.is_bootstrap()
                && !second.source.is_bootstrap()
                && !same_source(&best.source, &second.source)
            {
                bail!(
                    "ambiguous CHUNK_ITEM at logical {logical:#x}: two \
                     records at generation {} from different sources",
                    best.generation,
                );
            }

            let bootstrap_won = best.source.is_bootstrap()
                && best.generation == second.generation;

            for loser in &group[1..] {
                if loser.generation != best.generation || bootstrap_won {
                    conflicts.push(Conflict::ChunkItem {
                        logical,
                        winner_gen: best.generation,
                        loser_gen: loser.generation,
                        bootstrap_won,
                    });
                }
            }
        }

        result.push(group[0].clone());
    }

    Ok(result)
}

/// Check for overlapping chunk ranges.
///
/// Chunks are already sorted by logical start (BTreeMap iteration order).
/// Two chunks overlap if the first's end extends past the second's start.
fn check_overlaps(chunks: &[ChunkRecord]) -> Result<()> {
    for pair in chunks.windows(2) {
        let a = &pair[0];
        let b = &pair[1];
        let a_end = a.logical + a.chunk.length;
        if a_end > b.logical {
            bail!(
                "overlapping chunks: [{:#x}..{:#x}) and [{:#x}..{:#x})",
                a.logical,
                a_end,
                b.logical,
                b.logical + b.chunk.length,
            );
        }
    }
    Ok(())
}

/// Warn about chunk stripes referencing devids with no DEV_ITEM.
fn check_stripe_refs(
    chunks: &[ChunkRecord],
    dev_ids: &HashSet<u64>,
    warnings: &mut Vec<Warning>,
) {
    for chunk in chunks {
        for stripe in &chunk.chunk.stripes {
            if !dev_ids.contains(&stripe.devid) {
                warnings.push(Warning::DanglingStripeRef {
                    logical: chunk.logical,
                    devid: stripe.devid,
                });
            }
        }
    }
}

/// Check whether two sources refer to the same leaf.
fn same_source(a: &super::RecordSource, b: &super::RecordSource) -> bool {
    match (a, b) {
        (super::RecordSource::Bootstrap, super::RecordSource::Bootstrap) => {
            true
        }
        (
            super::RecordSource::ScannedLeaf {
                bytenr: a_b,
                generation: a_g,
            },
            super::RecordSource::ScannedLeaf {
                bytenr: b_b,
                generation: b_g,
            },
        ) => a_b == b_b && a_g == b_g,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{super::RecordSource, *};
    use btrfs_disk::items::{BlockGroupFlags, ChunkItem, ChunkStripe};
    use uuid::Uuid;

    fn make_chunk(logical: u64, length: u64, devid: u64) -> ChunkItem {
        ChunkItem {
            length,
            owner: 256,
            stripe_len: 65536,
            chunk_type: BlockGroupFlags::DATA,
            io_align: 4096,
            io_width: 4096,
            sector_size: 4096,
            num_stripes: 1,
            sub_stripes: 0,
            stripes: vec![ChunkStripe {
                devid,
                offset: logical,
                dev_uuid: Uuid::nil(),
            }],
        }
    }

    fn make_chunk_record(
        logical: u64,
        length: u64,
        generation: u64,
        source: RecordSource,
    ) -> ChunkRecord {
        ChunkRecord {
            logical,
            chunk: make_chunk(logical, length, 1),
            source,
            generation,
        }
    }

    fn make_dev_record(
        devid: u64,
        generation: u64,
        source: RecordSource,
    ) -> DevRecord {
        use btrfs_disk::items::DeviceItem;

        DevRecord {
            devid,
            device: DeviceItem {
                devid,
                total_bytes: 1024 * 1024 * 1024,
                bytes_used: 0,
                io_align: 4096,
                io_width: 4096,
                sector_size: 4096,
                dev_type: 0,
                generation,
                start_offset: 0,
                dev_group: 0,
                seek_speed: 0,
                bandwidth: 0,
                uuid: Uuid::nil(),
                fsid: Uuid::nil(),
            },
            source,
            generation,
        }
    }

    fn scanned(bytenr: u64, generation: u64) -> RecordSource {
        RecordSource::ScannedLeaf { bytenr, generation }
    }

    // --- DEV_ITEM dedup tests ---

    #[test]
    fn dev_item_single_record() {
        let records = vec![make_dev_record(1, 10, scanned(0, 10))];
        let mut conflicts = Vec::new();
        let result = dedup_dev_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].devid, 1);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn dev_item_newer_generation_wins() {
        let records = vec![
            make_dev_record(1, 8, scanned(100, 8)),
            make_dev_record(1, 10, scanned(200, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_dev_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].generation, 10);
        assert_eq!(conflicts.len(), 1);
        match &conflicts[0] {
            Conflict::DevItem {
                devid,
                winner_gen,
                loser_gen,
            } => {
                assert_eq!(*devid, 1);
                assert_eq!(*winner_gen, 10);
                assert_eq!(*loser_gen, 8);
            }
            _ => panic!("expected DevItem conflict"),
        }
    }

    #[test]
    fn dev_item_same_generation_different_source_is_ambiguous() {
        let records = vec![
            make_dev_record(1, 10, scanned(100, 10)),
            make_dev_record(1, 10, scanned(200, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_dev_items(&records, &mut conflicts);
        assert!(result.is_err());
    }

    #[test]
    fn dev_item_same_source_same_generation_ok() {
        let records = vec![
            make_dev_record(1, 10, scanned(100, 10)),
            make_dev_record(1, 10, scanned(100, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_dev_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
    }

    // --- CHUNK_ITEM dedup tests ---

    #[test]
    fn chunk_item_single_record() {
        let records = vec![make_chunk_record(
            0x100_0000,
            0x800_0000,
            10,
            scanned(100, 10),
        )];
        let mut conflicts = Vec::new();
        let result = dedup_chunk_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn chunk_item_newer_generation_wins() {
        let records = vec![
            make_chunk_record(0x100_0000, 0x800_0000, 8, scanned(100, 8)),
            make_chunk_record(0x100_0000, 0x800_0000, 10, scanned(200, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_chunk_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].generation, 10);
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn chunk_item_bootstrap_wins_tie() {
        let records = vec![
            make_chunk_record(
                0x100_0000,
                0x800_0000,
                10,
                RecordSource::Bootstrap,
            ),
            make_chunk_record(0x100_0000, 0x800_0000, 10, scanned(200, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_chunk_items(&records, &mut conflicts).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].source.is_bootstrap());
        assert_eq!(conflicts.len(), 1);
        match &conflicts[0] {
            Conflict::ChunkItem { bootstrap_won, .. } => {
                assert!(*bootstrap_won);
            }
            _ => panic!("expected ChunkItem conflict"),
        }
    }

    #[test]
    fn chunk_item_same_gen_different_source_is_ambiguous() {
        let records = vec![
            make_chunk_record(0x100_0000, 0x800_0000, 10, scanned(100, 10)),
            make_chunk_record(0x100_0000, 0x800_0000, 10, scanned(200, 10)),
        ];
        let mut conflicts = Vec::new();
        let result = dedup_chunk_items(&records, &mut conflicts);
        assert!(result.is_err());
    }

    // --- Overlap detection ---

    #[test]
    fn no_overlap_adjacent() {
        let chunks = vec![
            make_chunk_record(0, 0x100_0000, 10, scanned(100, 10)),
            make_chunk_record(0x100_0000, 0x100_0000, 10, scanned(200, 10)),
        ];
        assert!(check_overlaps(&chunks).is_ok());
    }

    #[test]
    fn overlap_detected() {
        let chunks = vec![
            make_chunk_record(0, 0x200_0000, 10, scanned(100, 10)),
            make_chunk_record(0x100_0000, 0x200_0000, 10, scanned(200, 10)),
        ];
        assert!(check_overlaps(&chunks).is_err());
    }

    // --- Stripe-to-device validation ---

    #[test]
    fn dangling_stripe_ref() {
        let chunks = vec![make_chunk_record(
            0x100_0000,
            0x800_0000,
            10,
            scanned(100, 10),
        )];
        let dev_ids: HashSet<u64> = HashSet::from([99]); // devid 1 not present
        let mut warnings = Vec::new();
        check_stripe_refs(&chunks, &dev_ids, &mut warnings);
        assert_eq!(warnings.len(), 1);
        match &warnings[0] {
            Warning::DanglingStripeRef { logical, devid } => {
                assert_eq!(*logical, 0x100_0000);
                assert_eq!(*devid, 1);
            }
        }
    }

    #[test]
    fn valid_stripe_ref() {
        let chunks = vec![make_chunk_record(
            0x100_0000,
            0x800_0000,
            10,
            scanned(100, 10),
        )];
        let dev_ids: HashSet<u64> = HashSet::from([1]);
        let mut warnings = Vec::new();
        check_stripe_refs(&chunks, &dev_ids, &mut warnings);
        assert!(warnings.is_empty());
    }

    // --- Coverage check ---

    #[test]
    fn chunk_root_covered() {
        let scan = make_scan_result(
            0x200_0000,
            vec![make_chunk_record(0, 0x400_0000, 10, scanned(100, 10))],
        );
        let result = reconstruct(&scan).unwrap();
        assert!(result.chunk_root_covered);
    }

    #[test]
    fn chunk_root_not_covered() {
        let scan = make_scan_result(
            0x500_0000,
            vec![make_chunk_record(0, 0x400_0000, 10, scanned(100, 10))],
        );
        let result = reconstruct(&scan).unwrap();
        assert!(!result.chunk_root_covered);
    }

    fn make_scan_result(
        chunk_root: u64,
        chunk_records: Vec<ChunkRecord>,
    ) -> ScanResult {
        ScanResult {
            fsid: Uuid::nil(),
            metadata_uuid: Uuid::nil(),
            has_metadata_uuid: false,
            nodesize: 16384,
            chunk_root,
            chunk_root_level: 0,
            sb_generation: 10,
            device_size: 1024 * 1024 * 1024,
            bytes_scanned: 1024 * 1024 * 1024,
            candidates_checked: 0,
            valid_blocks: 0,
            chunk_tree_leaves: 0,
            chunk_records,
            dev_records: vec![make_dev_record(1, 10, scanned(100, 10))],
        }
    }
}

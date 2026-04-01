use super::errors::{CheckError, CheckResults};
use btrfs_disk::{
    items::RootRef,
    reader::{self, BlockReader},
    tree::{KeyType, TreeBlock},
};
use std::{
    collections::BTreeMap,
    io::{Read, Seek},
};

/// Header size in a btrfs tree block (bytes before item data area).
const HEADER_SIZE: usize = std::mem::size_of::<btrfs_disk::raw::btrfs_header>();

/// A collected `ROOT_REF` or `ROOT_BACKREF` entry.
struct RefEntry {
    dirid: u64,
    sequence: u64,
    name: Vec<u8>,
}

/// Check `ROOT_REF`/`ROOT_BACKREF` consistency in the root tree.
///
/// For every `ROOT_REF` (parent → child) there must be a matching
/// `ROOT_BACKREF` (child → parent) with identical dirid, sequence, and
/// name, and vice versa.
pub fn check_root_refs<R: Read + Seek>(
    reader: &mut BlockReader<R>,
    root_bytenr: u64,
    results: &mut CheckResults,
) {
    // Collect all ROOT_REF and ROOT_BACKREF items from the root tree.
    // Key: (child_root_id, parent_root_id) → RefEntry
    let mut forward_refs: BTreeMap<(u64, u64), RefEntry> = BTreeMap::new();
    let mut back_refs: BTreeMap<(u64, u64), RefEntry> = BTreeMap::new();
    let mut read_errors: Vec<(u64, String)> = Vec::new();

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                let start = HEADER_SIZE + item.offset as usize;
                let item_data = &data[start..][..item.size as usize];

                match item.key.key_type {
                    KeyType::RootRef => {
                        // ROOT_REF: objectid = parent, offset = child
                        let parent = item.key.objectid;
                        let child = item.key.offset;
                        if let Some(rr) = RootRef::parse(item_data) {
                            forward_refs.insert(
                                (child, parent),
                                RefEntry {
                                    dirid: rr.dirid,
                                    sequence: rr.sequence,
                                    name: rr.name,
                                },
                            );
                        }
                    }
                    KeyType::RootBackref => {
                        // ROOT_BACKREF: objectid = child, offset = parent
                        let child = item.key.objectid;
                        let parent = item.key.offset;
                        if let Some(rr) = RootRef::parse(item_data) {
                            back_refs.insert(
                                (child, parent),
                                RefEntry {
                                    dirid: rr.dirid,
                                    sequence: rr.sequence,
                                    name: rr.name,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    };

    let mut on_error = |logical: u64, err: &std::io::Error| {
        read_errors.push((logical, err.to_string()));
    };

    if let Err(e) = reader::tree_walk_tolerant(
        reader,
        root_bytenr,
        &mut visitor,
        &mut on_error,
    ) {
        results.report(CheckError::ReadError {
            logical: root_bytenr,
            detail: format!("root tree: {e}"),
        });
        return;
    }

    for (logical, detail) in read_errors {
        results.report(CheckError::ReadError { logical, detail });
    }

    // Check: every ROOT_REF has a matching ROOT_BACKREF.
    for (&(child, parent), fwd) in &forward_refs {
        match back_refs.get(&(child, parent)) {
            None => {
                results
                    .report(CheckError::RootBackrefMissing { child, parent });
            }
            Some(back) => {
                check_fields_match(child, parent, fwd, back, results);
            }
        }
    }

    // Check: every ROOT_BACKREF has a matching ROOT_REF.
    for &(child, parent) in back_refs.keys() {
        if !forward_refs.contains_key(&(child, parent)) {
            results.report(CheckError::RootRefMissing { child, parent });
        }
    }
}

fn check_fields_match(
    child: u64,
    parent: u64,
    fwd: &RefEntry,
    back: &RefEntry,
    results: &mut CheckResults,
) {
    if fwd.dirid != back.dirid {
        results.report(CheckError::RootRefMismatch {
            child,
            parent,
            detail: format!(
                "dirid mismatch: ROOT_REF has {}, ROOT_BACKREF has {}",
                fwd.dirid, back.dirid
            ),
        });
    }
    if fwd.sequence != back.sequence {
        results.report(CheckError::RootRefMismatch {
            child,
            parent,
            detail: format!(
                "sequence mismatch: ROOT_REF has {}, ROOT_BACKREF has {}",
                fwd.sequence, back.sequence
            ),
        });
    }
    if fwd.name != back.name {
        let fwd_name = String::from_utf8_lossy(&fwd.name);
        let back_name = String::from_utf8_lossy(&back.name);
        results.report(CheckError::RootRefMismatch {
            child,
            parent,
            detail: format!(
                "name mismatch: ROOT_REF has '{fwd_name}', ROOT_BACKREF has '{back_name}'"
            ),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_fields_match_identical() {
        let fwd = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let back = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let mut results = CheckResults::new(0);
        check_fields_match(257, 5, &fwd, &back, &mut results);
        assert_eq!(results.error_count, 0);
    }

    #[test]
    fn check_fields_match_dirid_mismatch() {
        let fwd = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let back = RefEntry {
            dirid: 512,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let mut results = CheckResults::new(0);
        check_fields_match(257, 5, &fwd, &back, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn check_fields_match_sequence_mismatch() {
        let fwd = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let back = RefEntry {
            dirid: 256,
            sequence: 7,
            name: b"subvol1".to_vec(),
        };
        let mut results = CheckResults::new(0);
        check_fields_match(257, 5, &fwd, &back, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn check_fields_match_name_mismatch() {
        let fwd = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let back = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol2".to_vec(),
        };
        let mut results = CheckResults::new(0);
        check_fields_match(257, 5, &fwd, &back, &mut results);
        assert_eq!(results.error_count, 1);
    }

    #[test]
    fn check_fields_match_all_mismatch() {
        let fwd = RefEntry {
            dirid: 256,
            sequence: 3,
            name: b"subvol1".to_vec(),
        };
        let back = RefEntry {
            dirid: 512,
            sequence: 7,
            name: b"subvol2".to_vec(),
        };
        let mut results = CheckResults::new(0);
        check_fields_match(257, 5, &fwd, &back, &mut results);
        assert_eq!(results.error_count, 3);
    }
}

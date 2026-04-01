use super::errors::{CheckError, CheckResults};
use btrfs_disk::superblock::{self, SUPER_MIRROR_MAX, super_mirror_offset};
use std::io::{Read, Seek};

/// Validate all superblock mirrors, reporting errors for invalid ones.
///
/// Returns the number of valid mirrors found.
pub fn check_superblocks<R: Read + Seek>(
    reader: &mut R,
    results: &mut CheckResults,
) -> u32 {
    let mut valid_count = 0;

    for mirror in 0..SUPER_MIRROR_MAX {
        let offset = super_mirror_offset(mirror);
        match superblock::read_superblock_bytes_at(reader, offset) {
            Ok(buf) => {
                if superblock::superblock_is_valid(&buf) {
                    valid_count += 1;
                } else {
                    results.report(CheckError::SuperblockInvalid {
                        mirror,
                        detail: "invalid checksum or magic".into(),
                    });
                }
            }
            Err(e) => {
                // Mirror may be beyond device size — not an error for
                // mirrors 1 and 2 on small devices.
                if mirror == 0 {
                    results.report(CheckError::SuperblockInvalid {
                        mirror,
                        detail: format!("read failed: {e}"),
                    });
                }
            }
        }
    }

    valid_count
}

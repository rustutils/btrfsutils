//! Inode-number translation between FUSE and btrfs.
//!
//! FUSE requires the root directory to have inode `1`. In btrfs, the root
//! directory of a subvolume is `BTRFS_FIRST_FREE_OBJECTID = 256`. For v1 we
//! expose a single subvolume, so the mapping is just a swap of `1` and `256`;
//! every other objectid passes through unchanged.
//!
//! When we add multi-subvolume support, this module should grow into a real
//! `(subvol_id, objectid) -> fuse_ino` table. Keep all translation here so
//! that change is localised.

/// btrfs `BTRFS_FIRST_FREE_OBJECTID` — root directory of a subvolume.
pub const BTRFS_ROOT_DIR: u64 = 256;

/// FUSE inode number for the filesystem root.
pub const FUSE_ROOT: u64 = 1;

/// Map a FUSE inode number to a btrfs objectid in the active FS tree.
#[must_use]
pub fn fuse_to_btrfs(ino: u64) -> u64 {
    match ino {
        FUSE_ROOT | BTRFS_ROOT_DIR => BTRFS_ROOT_DIR,
        other => other,
    }
}

/// Map a btrfs objectid to a FUSE inode number.
#[must_use]
pub fn btrfs_to_fuse(objectid: u64) -> u64 {
    if objectid == BTRFS_ROOT_DIR {
        FUSE_ROOT
    } else {
        objectid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_swap_round_trips() {
        assert_eq!(fuse_to_btrfs(FUSE_ROOT), BTRFS_ROOT_DIR);
        assert_eq!(btrfs_to_fuse(BTRFS_ROOT_DIR), FUSE_ROOT);
    }

    #[test]
    fn other_inodes_pass_through() {
        for ino in [257u64, 1024, 1 << 40] {
            assert_eq!(fuse_to_btrfs(ino), ino);
            assert_eq!(btrfs_to_fuse(ino), ino);
        }
    }
}

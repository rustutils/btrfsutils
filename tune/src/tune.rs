use anyhow::{Context, Result, bail};
use btrfs_disk::{
    raw,
    superblock::{
        read_superblock_bytes, superblock_is_valid,
        write_superblock_all_mirrors,
    },
};
use std::{
    io::{Read, Seek, Write},
    mem,
};

/// Read the little-endian u64 at `offset` in a superblock buffer.
fn read_u64(buf: &[u8; 4096], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Write a little-endian u64 at `offset` in a superblock buffer.
fn write_u64(buf: &mut [u8; 4096], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

/// OR the given incompat feature flags into the superblock and write it back
/// to all mirrors.
///
/// Returns without writing if the flags are already set.
pub fn set_incompat_flags(
    file: &mut (impl Read + Write + Seek),
    flags: u64,
) -> Result<()> {
    let mut buf =
        read_superblock_bytes(file).context("failed to read superblock")?;

    if !superblock_is_valid(&buf) {
        bail!("superblock is invalid (bad magic or checksum)");
    }

    let offset = mem::offset_of!(raw::btrfs_super_block, incompat_flags);
    let current = read_u64(&buf, offset);

    if current & flags == flags {
        println!("feature flags are already set");
        return Ok(());
    }

    write_u64(&mut buf, offset, current | flags);

    write_superblock_all_mirrors(file, &buf)
        .context("failed to write superblock")?;

    println!("incompat feature flags updated successfully");
    Ok(())
}

/// Set or clear the seeding flag on the superblock.
///
/// Setting the seeding flag is rejected if the filesystem has a dirty log
/// or if the METADATA_UUID incompat flag is set. Clearing requires user
/// confirmation unless `force` is true.
pub fn update_seeding_flag(
    file: &mut (impl Read + Write + Seek),
    set: bool,
    force: bool,
) -> Result<()> {
    let mut buf =
        read_superblock_bytes(file).context("failed to read superblock")?;

    if !superblock_is_valid(&buf) {
        bail!("superblock is invalid (bad magic or checksum)");
    }

    let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
    let incompat_off = mem::offset_of!(raw::btrfs_super_block, incompat_flags);
    let log_root_off = mem::offset_of!(raw::btrfs_super_block, log_root);

    let super_flags = read_u64(&buf, flags_off);
    let incompat_flags = read_u64(&buf, incompat_off);
    let log_root = read_u64(&buf, log_root_off);

    let seeding = raw::BTRFS_SUPER_FLAG_SEEDING;
    let metadata_uuid = raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64;

    if incompat_flags & metadata_uuid != 0 {
        bail!(
            "SEED flag cannot be changed on a metadata-uuid changed filesystem"
        );
    }

    let new_flags = if set {
        if super_flags & seeding != 0 {
            if force {
                return Ok(());
            }
            eprintln!("WARNING: seeding flag is already set");
            bail!("seeding flag is already set (use -f to ignore)");
        }
        if log_root != 0 {
            bail!("filesystem has a dirty log, not setting seed flag");
        }
        super_flags | seeding
    } else {
        if super_flags & seeding == 0 {
            eprintln!("WARNING: seeding flag is not set");
            bail!("seeding flag is not set");
        }
        if !force {
            bail!(
                "clearing the seeding flag may cause derived devices not to be \
                 mountable — use -f to force"
            );
        }
        eprintln!("WARNING: seeding flag cleared");
        super_flags & !seeding
    };

    write_u64(&mut buf, flags_off, new_flags);

    write_superblock_all_mirrors(file, &buf)
        .context("failed to write superblock")?;

    if set {
        println!("seeding flag set successfully");
    } else {
        println!("seeding flag cleared successfully");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal valid superblock buffer for testing.
    fn make_test_superblock() -> [u8; 4096] {
        use btrfs_disk::superblock::csum_superblock;

        let mut buf = [0u8; 4096];

        // Set magic.
        let magic_off = mem::offset_of!(raw::btrfs_super_block, magic);
        buf[magic_off..magic_off + 8]
            .copy_from_slice(&raw::BTRFS_MAGIC.to_le_bytes());

        // csum_type = 0 (CRC32C).
        let csum_type_off = mem::offset_of!(raw::btrfs_super_block, csum_type);
        buf[csum_type_off..csum_type_off + 2]
            .copy_from_slice(&0u16.to_le_bytes());

        // Set bytenr to the primary superblock offset.
        let bytenr_off = mem::offset_of!(raw::btrfs_super_block, bytenr);
        buf[bytenr_off..bytenr_off + 8].copy_from_slice(
            &btrfs_disk::superblock::super_mirror_offset(0).to_le_bytes(),
        );

        csum_superblock(&mut buf).unwrap();
        buf
    }

    /// Create a fake device (in-memory) with the superblock at offset 64K.
    fn make_test_device(sb: &[u8; 4096]) -> Vec<u8> {
        let size =
            btrfs_disk::superblock::super_mirror_offset(0) as usize + 4096;
        let mut dev = vec![0u8; size];
        let off = btrfs_disk::superblock::super_mirror_offset(0) as usize;
        dev[off..off + 4096].copy_from_slice(sb);
        dev
    }

    #[test]
    fn set_incompat_flags_adds_flags() {
        let sb = make_test_superblock();
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        let flag = raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64;
        set_incompat_flags(&mut cursor, flag).unwrap();

        // Re-read and verify.
        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let offset = mem::offset_of!(raw::btrfs_super_block, incompat_flags);
        let result = read_u64(&updated, offset);
        assert_ne!(result & flag, 0, "EXTENDED_IREF flag should be set");
    }

    #[test]
    fn set_incompat_flags_noop_when_already_set() {
        let mut sb = make_test_superblock();
        let flag = raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64;

        // Pre-set the flag.
        let offset = mem::offset_of!(raw::btrfs_super_block, incompat_flags);
        write_u64(&mut sb, offset, flag);
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();

        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        // Should succeed without error (already set).
        set_incompat_flags(&mut cursor, flag).unwrap();
    }

    #[test]
    fn seeding_rejects_dirty_log() {
        let mut sb = make_test_superblock();
        let log_root_off = mem::offset_of!(raw::btrfs_super_block, log_root);
        write_u64(&mut sb, log_root_off, 12345);
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();

        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        let err = update_seeding_flag(&mut cursor, true, false).unwrap_err();
        assert!(
            err.to_string().contains("dirty log"),
            "expected dirty log error, got: {err}"
        );
    }

    #[test]
    fn seeding_rejects_metadata_uuid() {
        let mut sb = make_test_superblock();
        let incompat_off =
            mem::offset_of!(raw::btrfs_super_block, incompat_flags);
        write_u64(
            &mut sb,
            incompat_off,
            raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64,
        );
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();

        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        let err = update_seeding_flag(&mut cursor, true, false).unwrap_err();
        assert!(
            err.to_string().contains("metadata-uuid"),
            "expected metadata-uuid error, got: {err}"
        );
    }

    #[test]
    fn seeding_set_and_clear() {
        let sb = make_test_superblock();
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        // Set seeding flag.
        update_seeding_flag(&mut cursor, true, false).unwrap();

        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        let flags = read_u64(&updated, flags_off);
        assert_ne!(flags & raw::BTRFS_SUPER_FLAG_SEEDING, 0);

        // Clear with force.
        update_seeding_flag(&mut cursor, false, true).unwrap();

        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let flags = read_u64(&updated, flags_off);
        assert_eq!(flags & raw::BTRFS_SUPER_FLAG_SEEDING, 0);
    }

    #[test]
    fn seeding_clear_requires_force() {
        let mut sb = make_test_superblock();
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        write_u64(&mut sb, flags_off, raw::BTRFS_SUPER_FLAG_SEEDING);
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();

        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        let err = update_seeding_flag(&mut cursor, false, false).unwrap_err();
        assert!(
            err.to_string().contains("force"),
            "expected force error, got: {err}"
        );
    }
}

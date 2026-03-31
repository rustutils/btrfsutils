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
use uuid::Uuid;

/// Read the little-endian u64 at `offset` in a superblock buffer.
fn read_u64(buf: &[u8; 4096], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Write a little-endian u64 at `offset` in a superblock buffer.
fn write_u64(buf: &mut [u8; 4096], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

/// Read a UUID (16 bytes) at `offset` in a superblock buffer.
fn read_uuid(buf: &[u8; 4096], offset: usize) -> Uuid {
    Uuid::from_bytes(buf[offset..offset + 16].try_into().unwrap())
}

/// Write a UUID (16 bytes) at `offset` in a superblock buffer.
fn write_uuid(buf: &mut [u8; 4096], offset: usize, uuid: Uuid) {
    buf[offset..offset + 16].copy_from_slice(uuid.as_bytes());
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

/// Change the visible filesystem UUID using the metadata_uuid mechanism.
///
/// This changes the user-visible fsid field while preserving the original
/// UUID in the metadata_uuid field with the METADATA_UUID incompat flag.
/// Unlike a full fsid rewrite (`-u`/`-U`), this does not require traversing
/// every tree block on disk.
///
/// Three cases are handled:
/// 1. First change: copy current fsid to metadata_uuid, set new fsid,
///    enable METADATA_UUID incompat flag.
/// 2. Already changed, new fsid differs from metadata_uuid: just update fsid.
/// 3. Already changed, new fsid equals metadata_uuid: restore original state
///    by clearing the METADATA_UUID flag and zeroing metadata_uuid.
pub fn set_metadata_uuid(
    file: &mut (impl Read + Write + Seek),
    new_fsid: Uuid,
) -> Result<()> {
    let mut buf =
        read_superblock_bytes(file).context("failed to read superblock")?;

    if !superblock_is_valid(&buf) {
        bail!("superblock is invalid (bad magic or checksum)");
    }

    let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
    let incompat_off = mem::offset_of!(raw::btrfs_super_block, incompat_flags);
    let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
    let metadata_uuid_off =
        mem::offset_of!(raw::btrfs_super_block, metadata_uuid);

    let super_flags = read_u64(&buf, flags_off);
    let incompat_flags = read_u64(&buf, incompat_off);
    let current_fsid = read_uuid(&buf, fsid_off);

    let seeding = raw::BTRFS_SUPER_FLAG_SEEDING;
    if super_flags & seeding != 0 {
        bail!("cannot set metadata UUID on a seed device");
    }

    let metadata_uuid_flag = raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64;
    let fsid_changed = incompat_flags & metadata_uuid_flag != 0;

    if new_fsid == current_fsid {
        println!("fsid is already {new_fsid}");
        return Ok(());
    }

    // Step 1: set the in-progress flag and write to disk.
    let changing_v2 = raw::BTRFS_SUPER_FLAG_CHANGING_FSID_V2;
    write_u64(&mut buf, flags_off, super_flags | changing_v2);
    write_superblock_all_mirrors(file, &buf)
        .context("failed to write superblock (step 1: set in-progress flag)")?;

    // Step 2: apply the UUID change.
    let metadata_uuid = read_uuid(&buf, metadata_uuid_off);

    if fsid_changed && new_fsid == metadata_uuid {
        // Restoring fsid to the original metadata_uuid value: clear the
        // METADATA_UUID flag and zero out the metadata_uuid field.
        write_uuid(&mut buf, fsid_off, new_fsid);
        write_u64(&mut buf, incompat_off, incompat_flags & !metadata_uuid_flag);
        write_uuid(&mut buf, metadata_uuid_off, Uuid::nil());
    } else if fsid_changed {
        // Already has METADATA_UUID set, just change the visible fsid.
        write_uuid(&mut buf, fsid_off, new_fsid);
    } else {
        // First time: save the original fsid as metadata_uuid.
        write_u64(&mut buf, incompat_off, incompat_flags | metadata_uuid_flag);
        write_uuid(&mut buf, metadata_uuid_off, current_fsid);
        write_uuid(&mut buf, fsid_off, new_fsid);
    }

    // Clear the in-progress flag and write final state.
    let updated_flags = read_u64(&buf, flags_off) & !changing_v2;
    write_u64(&mut buf, flags_off, updated_flags);

    write_superblock_all_mirrors(file, &buf)
        .context("failed to write superblock (step 2: apply UUID change)")?;

    println!("metadata UUID changed to {new_fsid}");
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

    // --- set_metadata_uuid tests ---

    /// Helper to make a superblock with a known fsid.
    fn make_test_superblock_with_fsid(fsid: Uuid) -> [u8; 4096] {
        let mut sb = make_test_superblock();
        let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
        write_uuid(&mut sb, fsid_off, fsid);
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();
        sb
    }

    #[test]
    fn metadata_uuid_first_change() {
        let original = Uuid::from_bytes([0xAA; 16]);
        let new = Uuid::from_bytes([0xBB; 16]);
        let sb = make_test_superblock_with_fsid(original);
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        set_metadata_uuid(&mut cursor, new).unwrap();

        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
        let metadata_uuid_off =
            mem::offset_of!(raw::btrfs_super_block, metadata_uuid);
        let incompat_off =
            mem::offset_of!(raw::btrfs_super_block, incompat_flags);
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);

        // fsid should be the new UUID.
        assert_eq!(read_uuid(&updated, fsid_off), new);
        // metadata_uuid should hold the original.
        assert_eq!(read_uuid(&updated, metadata_uuid_off), original);
        // METADATA_UUID incompat flag should be set.
        let incompat = read_u64(&updated, incompat_off);
        let flag = raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64;
        assert_ne!(incompat & flag, 0);
        // In-progress flag should be cleared.
        let flags = read_u64(&updated, flags_off);
        assert_eq!(flags & raw::BTRFS_SUPER_FLAG_CHANGING_FSID_V2, 0);
    }

    #[test]
    fn metadata_uuid_second_change() {
        let original = Uuid::from_bytes([0xAA; 16]);
        let first_new = Uuid::from_bytes([0xBB; 16]);
        let second_new = Uuid::from_bytes([0xCC; 16]);
        let sb = make_test_superblock_with_fsid(original);
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        // First change.
        set_metadata_uuid(&mut cursor, first_new).unwrap();
        // Second change.
        set_metadata_uuid(&mut cursor, second_new).unwrap();

        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
        let metadata_uuid_off =
            mem::offset_of!(raw::btrfs_super_block, metadata_uuid);

        // fsid should be the second new UUID.
        assert_eq!(read_uuid(&updated, fsid_off), second_new);
        // metadata_uuid should still hold the original.
        assert_eq!(read_uuid(&updated, metadata_uuid_off), original);
    }

    #[test]
    fn metadata_uuid_restore_original() {
        let original = Uuid::from_bytes([0xAA; 16]);
        let new = Uuid::from_bytes([0xBB; 16]);
        let sb = make_test_superblock_with_fsid(original);
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        // Change to new UUID.
        set_metadata_uuid(&mut cursor, new).unwrap();
        // Restore back to original.
        set_metadata_uuid(&mut cursor, original).unwrap();

        let updated = read_superblock_bytes(&mut cursor).unwrap();
        let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
        let metadata_uuid_off =
            mem::offset_of!(raw::btrfs_super_block, metadata_uuid);
        let incompat_off =
            mem::offset_of!(raw::btrfs_super_block, incompat_flags);

        // fsid should be restored to original.
        assert_eq!(read_uuid(&updated, fsid_off), original);
        // metadata_uuid should be zeroed.
        assert_eq!(read_uuid(&updated, metadata_uuid_off), Uuid::nil());
        // METADATA_UUID flag should be cleared.
        let incompat = read_u64(&updated, incompat_off);
        let flag = raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID as u64;
        assert_eq!(incompat & flag, 0);
    }

    #[test]
    fn metadata_uuid_noop_when_same() {
        let fsid = Uuid::from_bytes([0xAA; 16]);
        let sb = make_test_superblock_with_fsid(fsid);
        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        // Should succeed without error (already the same).
        set_metadata_uuid(&mut cursor, fsid).unwrap();
    }

    #[test]
    fn metadata_uuid_rejects_seeding() {
        let mut sb =
            make_test_superblock_with_fsid(Uuid::from_bytes([0xAA; 16]));
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        write_u64(&mut sb, flags_off, raw::BTRFS_SUPER_FLAG_SEEDING);
        btrfs_disk::superblock::csum_superblock(&mut sb).unwrap();

        let mut dev = make_test_device(&sb);
        let mut cursor = Cursor::new(&mut dev[..]);

        let err = set_metadata_uuid(&mut cursor, Uuid::from_bytes([0xBB; 16]))
            .unwrap_err();
        assert!(
            err.to_string().contains("seed"),
            "expected seed error, got: {err}"
        );
    }
}

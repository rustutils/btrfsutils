//! # Superblock and tree block modification operations
//!
//! All functions operate on an unmounted filesystem via direct reads and
//! writes to the underlying block device or image file.

use anyhow::{Context, Result, bail};
use btrfs_disk::{
    raw, reader,
    superblock::{
        read_superblock_bytes, superblock_is_valid,
        write_superblock_all_mirrors,
    },
    tree::{KeyType, TreeBlock},
    util::{csum_tree_block, write_uuid as disk_write_uuid},
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
///
/// # Errors
///
/// Returns an error if the superblock is invalid or the write fails.
pub fn set_incompat_flags(
    file: &mut (impl Read + Write + Seek),
    flags: u64,
) -> Result<()> {
    let offset = mem::offset_of!(raw::btrfs_super_block, incompat_flags);

    // Pre-check: read to see if already set (avoids unnecessary write).
    let buf =
        read_superblock_bytes(file).context("failed to read superblock")?;
    if !superblock_is_valid(&buf) {
        bail!("superblock is invalid (bad magic or checksum)");
    }
    let current = read_u64(&buf, offset);
    if current & flags == flags {
        println!("feature flags are already set");
        return Ok(());
    }

    read_validate_mutate_write(file, |sb| {
        let current = read_u64(sb, offset);
        write_u64(sb, offset, current | flags);
    })?;

    println!("incompat feature flags updated successfully");
    Ok(())
}

/// Set or clear the seeding flag on the superblock.
///
/// Setting the seeding flag is rejected if the filesystem has a dirty log
/// or if the `METADATA_UUID` incompat flag is set. Clearing requires user
/// confirmation unless `force` is true.
///
/// # Errors
///
/// Returns an error if the superblock is invalid, the filesystem has a dirty
/// log, the `METADATA_UUID` flag is set, clearing is attempted without `force`,
/// or the write fails.
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
    let metadata_uuid = u64::from(raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID);

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

/// Change the visible filesystem UUID using the `metadata_uuid` mechanism.
///
/// This changes the user-visible fsid field while preserving the original
/// UUID in the `metadata_uuid` field with the `METADATA_UUID` incompat flag.
/// Unlike a full fsid rewrite (`-u`/`-U`), this does not require traversing
/// every tree block on disk.
///
/// Three cases are handled:
/// 1. First change: copy current fsid to `metadata_uuid`, set new fsid,
///    enable `METADATA_UUID` incompat flag.
/// 2. Already changed, new fsid differs from `metadata_uuid`: just update fsid.
/// 3. Already changed, new fsid equals `metadata_uuid`: restore original state
///    by clearing the `METADATA_UUID` flag and zeroing `metadata_uuid`.
///
/// # Errors
///
/// Returns an error if the superblock is invalid, the filesystem is a seed
/// device, or the write fails.
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

    let metadata_uuid_flag =
        u64::from(raw::BTRFS_FEATURE_INCOMPAT_METADATA_UUID);
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

/// Rewrite the filesystem UUID in every tree block header and device item
/// on disk.
///
/// Unlike `set_metadata_uuid` (which only modifies the superblock), this
/// traverses the extent tree to find every tree block and patches the fsid
/// and `chunk_tree_uuid` in each header. It also patches the device fsid in
/// chunk tree `DEV_ITEM` entries and the superblock's embedded `dev_item`.
///
/// The operation is crash-safe: `BTRFS_SUPER_FLAG_CHANGING_FSID` is set
/// before any writes and cleared only after all blocks are updated.
///
/// # Panics
///
/// Panics if an on-disk extent item is too short to contain the expected flags field.
///
/// # Errors
///
/// Returns an error if the filesystem cannot be opened, the extent tree root
/// is missing, or any block read/write fails.
#[allow(clippy::too_many_lines)] // fsid rewrite is a single logical operation
pub fn change_uuid<R: Read + Write + Seek>(
    file: R,
    new_fsid: Uuid,
) -> Result<()> {
    // Bootstrap the filesystem: read superblock, chunk cache, tree roots.
    let open =
        reader::filesystem_open(file).context("failed to open filesystem")?;
    let mut reader = open.reader;
    let sb = open.superblock;

    let old_fsid = sb.fsid;
    if new_fsid == old_fsid {
        println!("fsid is already {new_fsid}");
        return Ok(());
    }

    let new_chunk_tree_uuid = Uuid::new_v4();
    let header_size = mem::size_of::<raw::btrfs_header>();
    let header_fsid_off = mem::offset_of!(raw::btrfs_header, fsid);
    let header_chunk_uuid_off =
        mem::offset_of!(raw::btrfs_header, chunk_tree_uuid);

    println!("current fsid: {old_fsid}");
    println!("new fsid:     {new_fsid}");

    // Step 1: set CHANGING_FSID flag and write new fsid to superblock.
    read_validate_mutate_write(reader.inner_mut(), |sb_buf| {
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        let fsid_off = mem::offset_of!(raw::btrfs_super_block, fsid);
        let changing = raw::BTRFS_SUPER_FLAG_CHANGING_FSID;
        let flags = read_u64(sb_buf, flags_off);
        write_u64(sb_buf, flags_off, flags | changing);
        write_uuid(sb_buf, fsid_off, new_fsid);
    })
    .context("failed to write superblock (set CHANGING_FSID)")?;

    // Step 2: collect all tree block addresses from the extent tree, then
    // patch each one.
    let extent_root = open
        .tree_roots
        .get(&u64::from(raw::BTRFS_EXTENT_TREE_OBJECTID))
        .map(|(bytenr, _)| *bytenr)
        .context("extent tree root not found")?;

    let mut tree_block_addrs = Vec::new();
    let tree_block_flag = u64::from(raw::BTRFS_EXTENT_FLAG_TREE_BLOCK);
    let flags_off = mem::offset_of!(raw::btrfs_extent_item, flags);
    reader::tree_walk(
        &mut reader,
        extent_root,
        reader::Traversal::Dfs,
        &mut |block| {
            if let TreeBlock::Leaf { items, data, .. } = block {
                for item in items {
                    let is_extent = item.key.key_type == KeyType::ExtentItem
                        || item.key.key_type == KeyType::MetadataItem;
                    if !is_extent {
                        continue;
                    }
                    let start = header_size + item.offset as usize + flags_off;
                    if start + 8 > data.len() {
                        continue;
                    }
                    let flags = u64::from_le_bytes(
                        data[start..start + 8].try_into().unwrap(),
                    );
                    if flags & tree_block_flag != 0 {
                        tree_block_addrs.push(item.key.objectid);
                    }
                }
            }
        },
    )
    .context("failed to walk extent tree")?;

    println!("patching {} tree block headers...", tree_block_addrs.len());

    for &bytenr in &tree_block_addrs {
        let mut buf = reader.read_block(bytenr).with_context(|| {
            format!("failed to read tree block at {bytenr}")
        })?;
        disk_write_uuid(&mut buf, header_fsid_off, &new_fsid);
        disk_write_uuid(&mut buf, header_chunk_uuid_off, &new_chunk_tree_uuid);
        csum_tree_block(&mut buf);
        reader.write_block(bytenr, &buf).with_context(|| {
            format!("failed to write tree block at {bytenr}")
        })?;
    }

    // Step 3: patch device fsid in chunk tree DEV_ITEM entries.
    let dev_fsid_off = mem::offset_of!(raw::btrfs_dev_item, fsid);
    reader::tree_walk_mut(&mut reader, sb.chunk_root, &mut |buf, block| {
        let TreeBlock::Leaf { items, .. } = block else {
            return false;
        };
        let mut modified = false;
        for item in items {
            if item.key.key_type != KeyType::DeviceItem {
                continue;
            }
            let start = header_size + item.offset as usize + dev_fsid_off;
            if start + 16 <= buf.len() {
                disk_write_uuid(buf, start, &new_fsid);
                modified = true;
            }
        }
        modified
    })
    .context("failed to patch chunk tree DEV_ITEMs")?;

    // Step 4: patch superblock dev_item fsid and clear CHANGING_FSID.
    read_validate_mutate_write(reader.inner_mut(), |sb_buf| {
        let dev_item_fsid_off =
            mem::offset_of!(raw::btrfs_super_block, dev_item)
                + mem::offset_of!(raw::btrfs_dev_item, fsid);
        write_uuid(sb_buf, dev_item_fsid_off, new_fsid);

        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        let changing = raw::BTRFS_SUPER_FLAG_CHANGING_FSID;
        let flags = read_u64(sb_buf, flags_off);
        write_u64(sb_buf, flags_off, flags & !changing);
    })
    .context("failed to write superblock (clear CHANGING_FSID)")?;

    println!("fsid changed to {new_fsid}");
    Ok(())
}

/// Read the superblock, validate it, apply a mutation, and write it back
/// to all mirrors. Returns the buffer so callers can inspect it after.
fn read_validate_mutate_write(
    file: &mut (impl Read + Write + Seek),
    f: impl FnOnce(&mut [u8; 4096]),
) -> Result<[u8; 4096]> {
    let mut sb_buf =
        read_superblock_bytes(file).context("failed to read superblock")?;
    if !superblock_is_valid(&sb_buf) {
        bail!("superblock is invalid (bad magic or checksum)");
    }
    f(&mut sb_buf);
    write_superblock_all_mirrors(file, &sb_buf)
        .context("failed to write superblock")?;
    Ok(sb_buf)
}

/// Convert an unmounted single-device filesystem to use the v2 free
/// space tree (`FREE_SPACE_TREE` `compat_ro` feature).
///
/// Opens the device through the transaction crate, runs
/// [`btrfs_transaction::convert::convert_to_free_space_tree`] inside
/// a fresh transaction, and commits.
///
/// # Errors
///
/// Returns an error if the device cannot be opened, the transaction
/// crate refuses the conversion (FST already enabled, stale FST
/// root present, v1 cache items still in the root tree), or the
/// commit fails.
pub fn convert_to_free_space_tree(path: &std::path::Path) -> Result<()> {
    use btrfs_transaction::{
        convert, filesystem::Filesystem, transaction::Transaction,
    };
    use std::fs::OpenOptions;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))?;

    let mut fs = Filesystem::open(file).with_context(|| {
        format!("failed to open filesystem on '{}'", path.display())
    })?;

    let mut trans =
        Transaction::start(&mut fs).context("failed to start transaction")?;

    convert::convert_to_free_space_tree(&mut trans, &mut fs)
        .context("free space tree conversion failed")?;

    trans
        .commit(&mut fs)
        .context("failed to commit conversion transaction")?;
    fs.sync().context("failed to sync to disk")?;

    println!("converted '{}' to free space tree (v2)", path.display());
    Ok(())
}

/// Convert an unmounted single-device filesystem to use the block
/// group tree (`BLOCK_GROUP_TREE` `compat_ro` feature).
///
/// Opens the device through the transaction crate, runs
/// [`btrfs_transaction::convert::convert_to_block_group_tree`]
/// inside a fresh transaction, and commits.
///
/// # Errors
///
/// Returns an error if the device cannot be opened, the transaction
/// crate refuses the conversion (BGT already enabled, FST not
/// enabled, stale BGT root present), or the commit fails.
pub fn convert_to_block_group_tree(path: &std::path::Path) -> Result<()> {
    use btrfs_transaction::{
        convert, filesystem::Filesystem, transaction::Transaction,
    };
    use std::fs::OpenOptions;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))?;

    let mut fs = Filesystem::open(file).with_context(|| {
        format!("failed to open filesystem on '{}'", path.display())
    })?;

    let mut trans =
        Transaction::start(&mut fs).context("failed to start transaction")?;

    convert::convert_to_block_group_tree(&mut trans, &mut fs)
        .context("block group tree conversion failed")?;

    trans
        .commit(&mut fs)
        .context("failed to commit conversion transaction")?;
    fs.sync().context("failed to sync to disk")?;

    println!("converted '{}' to block group tree", path.display());
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

    // --- change_uuid tests ---

    /// Create a real btrfs filesystem image using mkfs, returning the image
    /// bytes.
    fn make_mkfs_image() -> Vec<u8> {
        use btrfs_mkfs::{
            args::Profile,
            mkfs::{DeviceInfo, MkfsConfig},
            write::ChecksumType,
        };

        let size: u64 = 256 * 1024 * 1024;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().set_len(size).unwrap();

        let cfg = MkfsConfig {
            nodesize: 16384,
            sectorsize: 4096,
            devices: vec![DeviceInfo {
                devid: 1,
                path: tmp.path().to_path_buf(),
                total_bytes: size,
                dev_uuid: Uuid::from_bytes([0xAB; 16]),
            }],
            label: None,
            fs_uuid: Uuid::from_bytes([0xDE; 16]),
            chunk_tree_uuid: Uuid::from_bytes([0xCD; 16]),
            incompat_flags: MkfsConfig::default_incompat_flags(),
            compat_ro_flags: MkfsConfig::default_compat_ro_flags(),
            data_profile: Profile::Single,
            metadata_profile: Profile::Dup,
            csum_type: ChecksumType::Crc32c,
            creation_time: Some(1000000),
            quota: false,
            squota: false,
        };
        btrfs_mkfs::mkfs::make_btrfs(&cfg).unwrap();

        std::fs::read(tmp.path()).unwrap()
    }

    #[test]
    fn change_uuid_rewrites_all_headers() {
        let mut image = make_mkfs_image();
        let new_fsid = Uuid::from_bytes([0xFF; 16]);

        // Apply the full UUID change.
        let cursor = Cursor::new(&mut image[..]);
        change_uuid(cursor, new_fsid).unwrap();

        // Re-open and verify.
        let cursor = Cursor::new(&image[..]);
        let open = reader::filesystem_open(cursor).unwrap();
        let mut rdr = open.reader;

        // Superblock should have the new fsid.
        assert_eq!(
            open.superblock.fsid, new_fsid,
            "superblock fsid not updated"
        );

        // CHANGING_FSID flag should be cleared.
        let sb_buf =
            read_superblock_bytes(&mut Cursor::new(&image[..])).unwrap();
        let flags_off = mem::offset_of!(raw::btrfs_super_block, flags);
        let flags = read_u64(&sb_buf, flags_off);
        assert_eq!(
            flags & raw::BTRFS_SUPER_FLAG_CHANGING_FSID,
            0,
            "CHANGING_FSID flag not cleared"
        );

        // Walk all trees and verify every tree block header has the new fsid.
        let all_roots: Vec<u64> = open
            .tree_roots
            .values()
            .map(|(bytenr, _)| *bytenr)
            .chain(std::iter::once(open.superblock.chunk_root))
            .chain(std::iter::once(open.superblock.root))
            .collect();

        for root in all_roots {
            verify_tree_fsid(&mut rdr, root, &new_fsid);
        }

        // Verify superblock dev_item fsid.
        let dev_item_fsid_off =
            mem::offset_of!(raw::btrfs_super_block, dev_item)
                + mem::offset_of!(raw::btrfs_dev_item, fsid);
        let dev_fsid = read_uuid(&sb_buf, dev_item_fsid_off);
        assert_eq!(dev_fsid, new_fsid, "superblock dev_item fsid not updated");
    }

    /// Recursively verify that all tree block headers have the expected fsid.
    fn verify_tree_fsid<R: Read + Seek>(
        reader: &mut reader::BlockReader<R>,
        logical: u64,
        expected_fsid: &Uuid,
    ) {
        let block = reader.read_tree_block(logical).unwrap();
        assert_eq!(
            &block.header().fsid,
            expected_fsid,
            "tree block at {logical} has wrong fsid: {} (expected {expected_fsid})",
            block.header().fsid
        );

        if let TreeBlock::Node { ptrs, .. } = &block {
            for ptr in ptrs {
                verify_tree_fsid(reader, ptr.blockptr, expected_fsid);
            }
        }
    }
}

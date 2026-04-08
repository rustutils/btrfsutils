//! # Entry point logic for btrfs-tune
//!
//! Extracted from `main()` so it can be called from both the standalone
//! `btrfs-tune` binary and the `btrfs tune` CLI subcommand.

use crate::args::Arguments;
use anyhow::{Context, Result, bail};
use btrfs_disk::raw;
use btrfs_uapi::filesystem::is_mounted;
use std::{
    fs::{self, OpenOptions},
    os::unix::fs::FileTypeExt,
};
use uuid::Uuid;

/// Run btrfs-tune with the given parsed arguments.
///
/// # Errors
///
/// Returns an error if no operation is specified, the device is not a block
/// device or regular file, the device is mounted, or any tuning operation fails.
pub fn run(args: &Arguments) -> Result<()> {
    let has_legacy = args.extref || args.skinny_metadata || args.no_holes;
    let has_seeding = args.seeding.is_some();
    let has_metadata_uuid =
        args.metadata_uuid || args.set_metadata_uuid.is_some();
    let has_uuid_rewrite = args.random_uuid || args.set_uuid.is_some();
    let has_convert_fst = args.convert_to_free_space_tree;

    if !has_legacy
        && !has_seeding
        && !has_metadata_uuid
        && !has_uuid_rewrite
        && !has_convert_fst
    {
        bail!("at least one option must be specified (see --help)");
    }

    let meta = fs::metadata(&args.device).with_context(|| {
        format!("cannot access '{}'", args.device.display())
    })?;
    let ft = meta.file_type();
    if !ft.is_block_device() && !ft.is_file() {
        bail!(
            "'{}' is not a block device or regular file",
            args.device.display()
        );
    }

    if is_mounted(&args.device).unwrap_or(false) {
        bail!("'{}' is currently mounted", args.device.display());
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&args.device)
        .with_context(|| {
            format!("failed to open '{}'", args.device.display())
        })?;

    if has_legacy {
        let mut flags = 0u64;
        if args.extref {
            flags |= u64::from(raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF);
        }
        if args.skinny_metadata {
            flags |= u64::from(raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA);
        }
        if args.no_holes {
            flags |= u64::from(raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES);
        }
        crate::tune::set_incompat_flags(&mut file, flags)?;
    }

    if let Some(set) = args.seeding {
        crate::tune::update_seeding_flag(&mut file, set, args.force)?;
    }

    if let Some(uuid) = args.set_metadata_uuid {
        crate::tune::set_metadata_uuid(&mut file, uuid)?;
    } else if args.metadata_uuid {
        let uuid = Uuid::new_v4();
        crate::tune::set_metadata_uuid(&mut file, uuid)?;
    }

    if let Some(uuid) = args.set_uuid {
        crate::tune::change_uuid(&mut file, uuid)?;
    } else if args.random_uuid {
        let uuid = Uuid::new_v4();
        crate::tune::change_uuid(&mut file, uuid)?;
    }

    if has_convert_fst {
        // Drop the bare-file handle: the transaction crate opens
        // its own handle on the same path.
        drop(file);
        crate::tune::convert_to_free_space_tree(&args.device)?;
    }

    Ok(())
}

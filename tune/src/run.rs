//! # Entry point logic for btrfs-tune
//!
//! Extracted from `main()` so it can be called from both the standalone
//! `btrfs-tune` binary and the `btrfs tune` CLI subcommand.

use crate::args::Arguments;
use anyhow::{Context, Result, bail};
use btrfs_disk::raw;
use std::{
    fs::{self, File, OpenOptions},
    io::BufRead,
    os::unix::fs::FileTypeExt,
    path::Path,
};
use uuid::Uuid;

/// Return `true` if `device` appears as a source in `/proc/mounts`.
fn is_mounted(device: &Path) -> bool {
    let Ok(canon) = fs::canonicalize(device) else {
        return false;
    };
    let Ok(f) = File::open("/proc/mounts") else {
        return false;
    };
    let reader = std::io::BufReader::new(f);
    for line in reader.lines().map_while(Result::ok) {
        let mut fields = line.split_whitespace();
        if let Some(src) = fields.next()
            && let Ok(src_canon) = fs::canonicalize(src)
            && src_canon == canon
        {
            return true;
        }
    }
    false
}

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

    if !has_legacy && !has_seeding && !has_metadata_uuid && !has_uuid_rewrite {
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

    if is_mounted(&args.device) {
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

    Ok(())
}

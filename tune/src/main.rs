use anyhow::{Context, Result, bail};
use btrfs_disk::raw;
use btrfs_tune::args::Arguments;
use clap::Parser;
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

fn main() -> Result<()> {
    let args = Arguments::parse();

    let has_legacy = args.extref || args.skinny_metadata || args.no_holes;
    let has_seeding = args.seeding.is_some();
    let has_metadata_uuid =
        args.metadata_uuid || args.set_metadata_uuid.is_some();

    if !has_legacy && !has_seeding && !has_metadata_uuid {
        bail!("at least one option must be specified (see --help)");
    }

    // Validate the device path.
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
            flags |= raw::BTRFS_FEATURE_INCOMPAT_EXTENDED_IREF as u64;
        }
        if args.skinny_metadata {
            flags |= raw::BTRFS_FEATURE_INCOMPAT_SKINNY_METADATA as u64;
        }
        if args.no_holes {
            flags |= raw::BTRFS_FEATURE_INCOMPAT_NO_HOLES as u64;
        }
        btrfs_tune::tune::set_incompat_flags(&mut file, flags)?;
    }

    if let Some(set) = args.seeding {
        btrfs_tune::tune::update_seeding_flag(&mut file, set, args.force)?;
    }

    if let Some(uuid) = args.set_metadata_uuid {
        btrfs_tune::tune::set_metadata_uuid(&mut file, uuid)?;
    } else if args.metadata_uuid {
        let uuid = Uuid::new_v4();
        btrfs_tune::tune::set_metadata_uuid(&mut file, uuid)?;
    }

    Ok(())
}

use anyhow::{Result, bail};
use btrfs_mkfs::{
    args::{Arguments, ChecksumArg},
    mkfs,
};
use clap::Parser;
use std::os::unix::fs::FileTypeExt;
use uuid::Uuid;

fn main() -> Result<()> {
    let args = Arguments::parse();

    if args.devices.len() > 1 {
        bail!("multi-device mkfs is not yet implemented");
    }

    let path = &args.devices[0];

    // Resolve sizes.
    let nodesize = args.nodesize.map(|s| s.0 as u32).unwrap_or(16384);
    let sectorsize = args.sectorsize.map(|s| s.0 as u32).unwrap_or(4096);

    // Validate nodesize/sectorsize.
    if !nodesize.is_power_of_two() || nodesize < sectorsize || nodesize > 65536
    {
        bail!(
            "invalid nodesize {nodesize}: must be a power of 2, \
             >= sectorsize ({sectorsize}), and <= 64K"
        );
    }
    if !sectorsize.is_power_of_two() || sectorsize < 4096 {
        bail!("invalid sectorsize {sectorsize}: must be a power of 2 >= 4096");
    }

    // Validate checksum algorithm.
    if let Some(csum) = args.checksum
        && csum != ChecksumArg::Crc32c
    {
        bail!(
            "checksum algorithm '{}' is not yet supported; only crc32c is available",
            csum
        );
    }

    // Validate label.
    if let Some(ref label) = args.label
        && label.len() >= 256
    {
        bail!("label too long: {} bytes (max 255)", label.len());
    }

    // Determine device size.
    let total_bytes = if let Some(byte_count) = args.byte_count {
        byte_count.0
    } else {
        mkfs::device_size(path)?
    };

    // Minimum size check.
    let min_size = mkfs::minimum_device_size(nodesize);
    if total_bytes < min_size {
        bail!(
            "device too small: {} bytes, need at least {} bytes ({} MiB)",
            total_bytes,
            min_size,
            min_size / (1024 * 1024)
        );
    }

    // Device safety checks (block devices only).
    let metadata = std::fs::metadata(path)
        .ok()
        .filter(|m| m.file_type().is_block_device());
    if metadata.is_some() {
        if mkfs::is_device_mounted(path)? {
            bail!(
                "'{}' is mounted; refusing to format a mounted device",
                path.display()
            );
        }
        if !args.force && mkfs::has_btrfs_superblock(path) {
            bail!(
                "'{}' already contains a btrfs filesystem; use -f to force",
                path.display()
            );
        }
    }

    // Generate or parse UUIDs.
    let fs_uuid = args.filesystem_uuid.unwrap_or_else(Uuid::new_v4);
    let dev_uuid = args.device_uuid.unwrap_or_else(Uuid::new_v4);
    let chunk_tree_uuid = Uuid::new_v4();

    let mut cfg = mkfs::MkfsConfig {
        nodesize,
        sectorsize,
        total_bytes,
        label: args.label,
        fs_uuid,
        dev_uuid,
        chunk_tree_uuid,
        incompat_flags: mkfs::MkfsConfig::default_incompat_flags(),
        compat_ro_flags: mkfs::MkfsConfig::default_compat_ro_flags(),
    };

    // Apply user-specified feature flags.
    cfg.apply_features(&args.features)?;

    if !args.quiet {
        eprintln!("Creating btrfs filesystem on {}", path.display());
        eprintln!(
            "  Label:          {}",
            cfg.label.as_deref().unwrap_or("(none)")
        );
        eprintln!("  UUID:           {}", cfg.fs_uuid);
        eprintln!("  Node size:      {}", cfg.nodesize);
        eprintln!("  Sector size:    {}", cfg.sectorsize);
        eprintln!(
            "  Filesystem size: {} ({} bytes)",
            human_size(cfg.total_bytes),
            cfg.total_bytes
        );
    }

    // TRIM the device before writing (unless -K).
    if metadata.is_some() && !args.nodiscard {
        if !args.quiet {
            eprintln!("Performing full device TRIM...");
        }
        if let Err(e) = mkfs::discard_device(path, total_bytes) {
            // TRIM failure is non-fatal (device may not support it).
            eprintln!("WARNING: discard failed: {e:#}");
        }
    }

    mkfs::make_btrfs(path, &cfg)?;

    if !args.quiet {
        eprintln!("Done.");
    }

    Ok(())
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    for &unit in UNITS {
        if value < 1024.0 {
            return if value.fract() == 0.0 {
                format!("{:.0} {unit}", value)
            } else {
                format!("{:.2} {unit}", value)
            };
        }
        value /= 1024.0;
    }
    format!("{:.2} EiB", value)
}

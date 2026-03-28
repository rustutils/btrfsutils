use anyhow::{Result, bail};
use btrfs_mkfs::{args::Arguments, mkfs};
use clap::Parser;
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

    // Generate or parse UUIDs.
    let fs_uuid = args.filesystem_uuid.unwrap_or_else(Uuid::new_v4);
    let dev_uuid = args.device_uuid.unwrap_or_else(Uuid::new_v4);
    let chunk_tree_uuid = Uuid::new_v4();

    let cfg = mkfs::MkfsConfig {
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

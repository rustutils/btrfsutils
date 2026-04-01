//! # Entry point logic for mkfs
//!
//! Extracted from `main()` so it can be called from both the standalone
//! `btrfs-mkfs` binary and the `btrfs mkfs` CLI subcommand.

use crate::{
    args::{Arguments, ChecksumArg, CompressAlgorithm, Profile},
    mkfs::{self, DeviceInfo},
    rootdir::CompressConfig,
    write::ChecksumType,
};
use anyhow::{Result, bail};
use std::os::unix::fs::FileTypeExt;
use uuid::Uuid;

/// Run mkfs with the given parsed arguments.
pub fn run(args: &Arguments) -> Result<()> {
    let nodesize = args.nodesize.map(|s| s.0 as u32).unwrap_or(16384);
    let sectorsize = args.sectorsize.map(|s| s.0 as u32).unwrap_or(4096);

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

    let csum_type = match args.checksum {
        None | Some(ChecksumArg::Crc32c) => ChecksumType::Crc32c,
        Some(ChecksumArg::Xxhash) => ChecksumType::Xxhash64,
        Some(ChecksumArg::Sha256) => ChecksumType::Sha256,
        Some(ChecksumArg::Blake2) => ChecksumType::Blake2b,
    };

    if let Some(ref label) = args.label
        && label.len() >= 256
    {
        bail!("label too long: {} bytes (max 255)", label.len());
    }

    let num_devices = args.devices.len();
    let metadata_profile =
        args.metadata_profile.unwrap_or(if num_devices > 1 {
            Profile::Raid1
        } else {
            Profile::Dup
        });
    let data_profile = args.data_profile.unwrap_or(Profile::Single);

    if num_devices < metadata_profile.min_devices() {
        bail!(
            "metadata profile {} requires at least {} devices, got {}",
            metadata_profile,
            metadata_profile.min_devices(),
            num_devices
        );
    }
    if num_devices < data_profile.min_devices() {
        bail!(
            "data profile {} requires at least {} devices, got {}",
            data_profile,
            data_profile.min_devices(),
            num_devices
        );
    }

    let mut devices = Vec::with_capacity(num_devices);
    for (i, dev_path) in args.devices.iter().enumerate() {
        let devid = (i + 1) as u64;
        let total_bytes = if let Some(byte_count) = args.byte_count {
            byte_count.0
        } else {
            mkfs::device_size(dev_path)?
        };

        let min_size = mkfs::minimum_device_size(nodesize);
        if total_bytes < min_size {
            bail!(
                "device '{}' too small: {} bytes, need at least {} bytes ({} MiB)",
                dev_path.display(),
                total_bytes,
                min_size,
                min_size / (1024 * 1024)
            );
        }

        let is_block = std::fs::metadata(dev_path)
            .ok()
            .is_some_and(|m| m.file_type().is_block_device());
        if is_block {
            if mkfs::is_device_mounted(dev_path)? {
                bail!(
                    "'{}' is mounted; refusing to format a mounted device",
                    dev_path.display()
                );
            }
            if !args.force && mkfs::has_btrfs_superblock(dev_path) {
                bail!(
                    "'{}' already contains a btrfs filesystem; use -f to force",
                    dev_path.display()
                );
            }
        }

        let dev_uuid = if i == 0 {
            args.device_uuid.unwrap_or_else(Uuid::new_v4)
        } else {
            Uuid::new_v4()
        };

        devices.push(DeviceInfo {
            devid,
            path: dev_path.clone(),
            total_bytes,
            dev_uuid,
        });
    }

    let fs_uuid = args.filesystem_uuid.unwrap_or_else(Uuid::new_v4);
    let chunk_tree_uuid = Uuid::new_v4();

    let mut cfg = mkfs::MkfsConfig {
        nodesize,
        sectorsize,
        devices,
        label: args.label.clone(),
        fs_uuid,
        chunk_tree_uuid,
        incompat_flags: mkfs::MkfsConfig::default_incompat_flags(),
        compat_ro_flags: mkfs::MkfsConfig::default_compat_ro_flags(),
        data_profile,
        metadata_profile,
        csum_type,
        creation_time: None,
    };

    cfg.apply_features(&args.features)?;

    if !args.quiet {
        let device_names: Vec<_> = cfg
            .devices
            .iter()
            .map(|d| d.path.display().to_string())
            .collect();
        eprintln!("Creating btrfs filesystem on {}", device_names.join(", "));
        eprintln!(
            "  Label:          {}",
            cfg.label.as_deref().unwrap_or("(none)")
        );
        eprintln!("  UUID:           {}", cfg.fs_uuid);
        eprintln!("  Node size:      {}", cfg.nodesize);
        eprintln!("  Sector size:    {}", cfg.sectorsize);
        eprintln!(
            "  Filesystem size: {} ({} bytes)",
            human_size(cfg.total_bytes()),
            cfg.total_bytes()
        );
        if num_devices > 1 {
            eprintln!("  Data profile:   {}", cfg.data_profile);
            eprintln!("  Metadata profile: {}", cfg.metadata_profile);
        }
    }

    if !args.nodiscard {
        for dev in &cfg.devices {
            let is_block = std::fs::metadata(&dev.path)
                .ok()
                .is_some_and(|m| m.file_type().is_block_device());
            if is_block {
                if !args.quiet {
                    eprintln!(
                        "Performing full device TRIM on {}...",
                        dev.path.display()
                    );
                }
                if let Err(e) = mkfs::discard_device(&dev.path, dev.total_bytes)
                {
                    eprintln!(
                        "WARNING: discard failed on {}: {e:#}",
                        dev.path.display()
                    );
                }
            }
        }
    }

    if let Some(ref rootdir) = args.rootdir {
        if !rootdir.is_dir() {
            bail!("'{}' is not a directory", rootdir.display());
        }
        let algorithm = args
            .compress
            .as_ref()
            .map(|c| c.algorithm)
            .unwrap_or(CompressAlgorithm::No);
        if algorithm == CompressAlgorithm::Lzo {
            bail!(
                "LZO compression is not yet supported for --rootdir \
                 (btrfs LZO uses a per-sector format that is not yet implemented)"
            );
        }
        let compress = CompressConfig {
            algorithm,
            level: args.compress.as_ref().and_then(|c| c.level),
        };
        if !args.quiet {
            eprintln!("  Rootdir:        {}", rootdir.display());
            if compress.algorithm != CompressAlgorithm::No {
                eprintln!("  Compression:    {:?}", compress.algorithm);
            }
        }
        mkfs::make_btrfs_with_rootdir(
            &cfg,
            rootdir,
            compress,
            &args.inode_flags,
            args.shrink,
        )?;
    } else {
        mkfs::make_btrfs(&cfg)?;
    }

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

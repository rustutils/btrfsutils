use crate::{
    RunContext, Runnable,
    util::{is_mounted, open_path, parse_size_with_suffix},
};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    items::DeviceItem,
    tree::{DiskKey, KeyType},
};
use btrfs_transaction::{
    filesystem::Filesystem,
    path::BtrfsPath,
    search::{self, SearchIntent},
    transaction::Transaction,
};
use btrfs_uapi::filesystem::{ResizeAmount, ResizeArgs, resize};
use clap::Parser;
use std::{
    fs::OpenOptions,
    os::unix::io::AsFd,
    path::{Path, PathBuf},
};

/// Resize a mounted btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemResizeCommand {
    /// Wait if there is another exclusive operation running, otherwise error
    #[clap(long)]
    pub enqueue: bool,

    /// Resize a filesystem stored in a file image (unmounted)
    #[clap(long)]
    pub offline: bool,

    /// New size for the filesystem, e.g. "1G", "+512M", "-1G", "max", "cancel",
    /// or "devid:ID:SIZE" to target a specific device
    pub size: String,

    pub path: PathBuf,
}

fn parse_resize_amount(s: &str) -> Result<ResizeAmount> {
    if s == "cancel" {
        return Ok(ResizeAmount::Cancel);
    }
    if s == "max" {
        return Ok(ResizeAmount::Max);
    }
    let (modifier, rest) = if let Some(r) = s.strip_prefix('+') {
        (1i32, r)
    } else if let Some(r) = s.strip_prefix('-') {
        (-1i32, r)
    } else {
        (0i32, s)
    };
    let bytes = parse_size_with_suffix(rest)?;
    Ok(match modifier {
        1 => ResizeAmount::Add(bytes),
        -1 => ResizeAmount::Sub(bytes),
        _ => ResizeAmount::Set(bytes),
    })
}

fn parse_resize_args(s: &str) -> Result<ResizeArgs> {
    if let Some(colon) = s.find(':')
        && let Ok(devid) = s[..colon].parse::<u64>()
    {
        let amount = parse_resize_amount(&s[colon + 1..])?;
        return Ok(ResizeArgs::new(amount).with_devid(devid));
    }
    Ok(ResizeArgs::new(parse_resize_amount(s)?))
}

impl Runnable for FilesystemResizeCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        if self.offline {
            if self.enqueue {
                bail!("--enqueue is not compatible with --offline");
            }
            return run_offline(&self.path, &self.size, ctx);
        }

        if self.enqueue {
            bail!("--enqueue is not yet implemented");
        }

        let args = parse_resize_args(&self.size).with_context(|| {
            format!("invalid resize argument: '{}'", self.size)
        })?;

        let file = open_path(&self.path)?;

        resize(file.as_fd(), args).with_context(|| {
            format!("resize failed on '{}'", self.path.display())
        })?;

        Ok(())
    }

    fn supports_dry_run(&self) -> bool {
        // Only the offline path honors --dry-run; the online ioctl
        // path always commits.
        self.offline
    }
}

/// Resize a btrfs filesystem on an unmounted image or block device.
///
/// Only grow is supported. Only single-device filesystems are
/// supported. The filesystem must not be mounted. The amount is
/// parsed with [`parse_resize_amount`]; the `cancel` keyword is
/// rejected (there is no pending operation to cancel). A `devid:`
/// prefix is accepted as long as it names the sole device.
///
/// The operation updates `DEV_ITEM.total_bytes` in the chunk tree
/// and `superblock.total_bytes`, commits a transaction, and
/// finally truncates the backing file to the new size when the
/// target is a regular file (block devices are left alone).
#[allow(clippy::too_many_lines)]
fn run_offline(path: &Path, amount: &str, ctx: &RunContext) -> Result<()> {
    if is_mounted(path) {
        bail!("{} must not be mounted to use --offline", path.display());
    }

    let args = parse_resize_args(amount)
        .with_context(|| format!("invalid resize argument: '{amount}'"))?;

    if matches!(args.amount, ResizeAmount::Cancel) {
        bail!("cannot cancel an offline resize");
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))?;

    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat '{}'", path.display()))?;
    let is_regular_file = metadata.file_type().is_file();

    let mut fs = Filesystem::open(file).with_context(|| {
        format!("failed to open filesystem on '{}'", path.display())
    })?;

    if fs.superblock.num_devices != 1 {
        bail!(
            "multi-device filesystems are not supported with --offline ({} devices)",
            fs.superblock.num_devices
        );
    }

    let sectorsize = u64::from(fs.superblock.sectorsize);
    let old_total = fs.superblock.total_bytes;
    let devid = fs.superblock.dev_item.devid;
    let old_device_bytes = fs.superblock.dev_item.total_bytes;

    if let Some(requested_devid) = args.devid
        && requested_devid != devid
    {
        bail!(
            "invalid device id {requested_devid} (only devid {devid} is present)"
        );
    }

    // Resolve the requested amount to an absolute new device size.
    let new_device_bytes = match args.amount {
        ResizeAmount::Set(bytes) => bytes,
        ResizeAmount::Add(bytes) => old_device_bytes
            .checked_add(bytes)
            .context("resize overflow")?,
        ResizeAmount::Sub(_) => {
            bail!("offline resize does not support shrinking")
        }
        ResizeAmount::Max => {
            // For images and block devices, "max" means the full
            // backing size. For block devices we would need to
            // query the partition size via ioctl; for regular
            // files we use the file length.
            if is_regular_file {
                metadata.len()
            } else {
                bail!("--offline max is only supported on regular file images");
            }
        }
        ResizeAmount::Cancel => unreachable!("rejected above"),
    };

    // Round down to the sector size, matching btrfs-progs.
    let new_device_bytes = (new_device_bytes / sectorsize) * sectorsize;
    if new_device_bytes < old_device_bytes {
        bail!("offline resize does not support shrinking");
    }
    if new_device_bytes == old_device_bytes {
        if !ctx.quiet {
            println!(
                "{}: already at the requested size ({} bytes)",
                path.display(),
                old_device_bytes
            );
        }
        return Ok(());
    }

    let diff = new_device_bytes - old_device_bytes;
    let new_total_bytes = old_total
        .checked_add(diff)
        .context("superblock total_bytes overflow")?;

    if !ctx.quiet {
        println!(
            "resize '{}' from {} to {} ({:+} bytes){}",
            path.display(),
            old_device_bytes,
            new_device_bytes,
            diff.cast_signed(),
            if ctx.dry_run { " [dry-run]" } else { "" },
        );
    }

    if ctx.dry_run {
        return Ok(());
    }

    // Start a transaction and patch the DEV_ITEM in the chunk tree.
    let mut trans =
        Transaction::start(&mut fs).context("failed to start transaction")?;

    let key = DiskKey {
        objectid: 1, // BTRFS_DEV_ITEMS_OBJECTID
        key_type: KeyType::DeviceItem,
        offset: devid,
    };
    let mut bpath = BtrfsPath::new();
    let found = search::search_slot(
        Some(&mut trans),
        &mut fs,
        3, // chunk tree
        &key,
        &mut bpath,
        SearchIntent::ReadOnly,
        true, // COW
    )
    .context("failed to search chunk tree for DEV_ITEM")?;
    if !found {
        bpath.release();
        bail!("DEV_ITEM for devid {devid} not found in chunk tree");
    }

    {
        let leaf = bpath.nodes[0]
            .as_mut()
            .context("DEV_ITEM search returned no leaf")?;
        let slot = bpath.slots[0];
        let data = leaf.item_data(slot);
        let mut item = DeviceItem::parse(data).with_context(|| {
            format!("failed to parse DEV_ITEM for devid {devid}")
        })?;
        item.total_bytes = new_device_bytes;
        let mut buf = Vec::with_capacity(data.len());
        item.write_bytes(&mut buf);
        let target = leaf.item_data_mut(slot);
        if target.len() < buf.len() {
            bpath.release();
            bail!("DEV_ITEM payload smaller than expected");
        }
        target[..buf.len()].copy_from_slice(&buf);
        fs.mark_dirty(leaf);
    }
    bpath.release();

    // Update the in-memory superblock. The commit step writes it
    // back to all mirrors; the embedded dev_item is kept in sync so
    // a subsequent mount sees consistent bootstrap values.
    fs.superblock.total_bytes = new_total_bytes;
    fs.superblock.dev_item.total_bytes = new_device_bytes;

    trans
        .commit(&mut fs)
        .context("failed to commit transaction")?;
    fs.sync().context("failed to sync to disk")?;

    // For regular file images, extend the file length to match.
    // Block devices are sized by the underlying partition, so we
    // leave them alone.
    if is_regular_file {
        let f = fs.reader_mut().single_device_mut();
        f.set_len(new_device_bytes).with_context(|| {
            format!(
                "failed to resize backing file '{}' to {new_device_bytes}",
                path.display()
            )
        })?;
    }

    Ok(())
}

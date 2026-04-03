use crate::{
    RunContext, Runnable,
    util::{human_bytes, open_path},
};
use anyhow::{Context, Result};
use btrfs_disk::{
    items::DeviceExtent,
    reader,
    tree::{KeyType, TreeBlock},
};
use clap::Parser;
use std::{
    fs::File,
    io::{Read, Seek},
    os::unix::io::AsFd,
    path::PathBuf,
};

/// Print the minimum size a device can be shrunk to.
///
/// Returns the minimum size in bytes that the specified device can be
/// resized to without losing data. The device id 1 is used by default.
/// Requires CAP_SYS_ADMIN (unless --offline is used).
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct MinDevSizeCommand {
    /// Specify the device id to query
    #[arg(long = "id", default_value = "1")]
    devid: u64,

    /// Read directly from an unmounted device or image file instead of
    /// a mounted filesystem. Does not require CAP_SYS_ADMIN.
    #[clap(long)]
    pub offline: bool,

    /// Path to a file or directory on the btrfs filesystem, or a block
    /// device / image file when --offline is used
    path: PathBuf,
}

impl Runnable for MinDevSizeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let size = if self.offline {
            self.compute_offline()?
        } else {
            self.compute_online()?
        };

        println!("{} bytes ({})", size, human_bytes(size));
        Ok(())
    }
}

impl MinDevSizeCommand {
    fn compute_online(&self) -> Result<u64> {
        let file = open_path(&self.path)?;
        btrfs_uapi::device::device_min_size(file.as_fd(), self.devid)
            .with_context(|| {
                format!(
                    "failed to determine min device size for devid {} on '{}'",
                    self.devid,
                    self.path.display()
                )
            })
    }

    fn compute_offline(&self) -> Result<u64> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        let mut open = reader::filesystem_open(file).with_context(|| {
            format!(
                "failed to open btrfs filesystem on '{}'",
                self.path.display()
            )
        })?;

        let dev_extents =
            collect_dev_extents(&mut open.reader, &open.tree_roots, self.devid);

        Ok(btrfs_uapi::device::compute_min_size(&dev_extents))
    }
}

/// Walk the device tree to collect all device extents for a given devid.
///
/// Returns `(physical_start, length)` pairs in ascending physical order.
fn collect_dev_extents<R: Read + Seek>(
    block_reader: &mut reader::BlockReader<R>,
    tree_roots: &std::collections::BTreeMap<u64, (u64, u64)>,
    devid: u64,
) -> Vec<(u64, u64)> {
    let mut dev_extents: Vec<(u64, u64)> = Vec::new();

    let dev_root = tree_roots
        .get(&u64::from(btrfs_disk::raw::BTRFS_DEV_TREE_OBJECTID))
        .map(|&(bytenr, _)| bytenr);

    let Some(dev_root) = dev_root else {
        return dev_extents;
    };

    let mut visitor = |_raw: &[u8], block: &TreeBlock| {
        if let TreeBlock::Leaf { items, data, .. } = block {
            for item in items {
                if item.key.key_type != KeyType::DeviceExtent {
                    continue;
                }
                if item.key.objectid != devid {
                    continue;
                }
                let start =
                    std::mem::size_of::<btrfs_disk::raw::btrfs_header>()
                        + item.offset as usize;
                let item_data = &data[start..][..item.size as usize];
                if let Some(de) = DeviceExtent::parse(item_data) {
                    dev_extents.push((item.key.offset, de.length));
                }
            }
        }
    };

    let _ = reader::tree_walk_tolerant(
        block_reader,
        dev_root,
        &mut visitor,
        &mut |_, _| {},
    );

    // Ensure ascending physical order (tree walk is DFS by key, so this
    // should already be sorted, but be safe).
    dev_extents.sort_by_key(|&(start, _)| start);
    dev_extents
}

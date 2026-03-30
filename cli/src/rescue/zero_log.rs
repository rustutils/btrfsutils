use crate::{Format, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::superblock::{
    read_superblock_bytes, write_superblock_all_mirrors,
};
use clap::Parser;
use std::{fs::OpenOptions, mem, path::PathBuf};

/// Clear the tree log (usable if it's corrupted and prevents mount)
///
/// The log tree is used for fsync durability. If it becomes corrupted it can
/// prevent the filesystem from mounting. Clearing it forces a full fsync of
/// all previously synced data on the next mount. No data is lost — only the
/// durability guarantee of uncommitted fsyncs.
///
/// The device must not be mounted.
#[derive(Parser, Debug)]
pub struct RescueZeroLogCommand {
    /// Path to the btrfs device
    device: PathBuf,
}

impl Runnable for RescueZeroLogCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if is_mounted(&self.device) {
            bail!("{} is currently mounted", self.device.display());
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)
            .with_context(|| {
                format!("failed to open '{}'", self.device.display())
            })?;

        let mut buf = read_superblock_bytes(&mut file).with_context(|| {
            format!(
                "failed to read superblock from '{}'",
                self.device.display()
            )
        })?;

        use btrfs_disk::raw;
        let log_root_off = mem::offset_of!(raw::btrfs_super_block, log_root);
        let log_root_level_off =
            mem::offset_of!(raw::btrfs_super_block, log_root_level);

        let log_root = u64::from_le_bytes(
            buf[log_root_off..log_root_off + 8].try_into().unwrap(),
        );
        let log_root_level = buf[log_root_level_off];

        println!(
            "Clearing log on {}, previous log_root {log_root}, level {log_root_level}",
            self.device.display()
        );

        buf[log_root_off..log_root_off + 8].fill(0);
        buf[log_root_level_off] = 0;

        write_superblock_all_mirrors(&mut file, &buf).with_context(|| {
            format!("failed to write superblock to '{}'", self.device.display())
        })?;

        Ok(())
    }
}

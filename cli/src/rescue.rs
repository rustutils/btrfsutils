use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod chunk_recover;
mod clear_ino_cache;
mod clear_space_cache;
mod clear_uuid_tree;
mod create_control_device;
mod fix_data_checksum;
mod fix_device_size;
mod super_recover;
mod zero_log;

use chunk_recover::RescueChunkRecoverCommand;
use clear_ino_cache::RescueClearInoCacheCommand;
use clear_space_cache::RescueClearSpaceCacheCommand;
use clear_uuid_tree::RescueClearUuidTreeCommand;
use create_control_device::RescueCreateControlDeviceCommand;
use fix_data_checksum::RescueFixDataChecksumCommand;
use fix_device_size::RescueFixDeviceSizeCommand;
use super_recover::RescueSuperRecoverCommand;
use zero_log::RescueZeroLogCommand;

/// Toolbox for specific rescue operations
#[derive(Parser, Debug)]
pub struct RescueCommand {
    #[clap(subcommand)]
    pub subcommand: RescueSubcommand,
}

impl Runnable for RescueCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            RescueSubcommand::ChunkRecover(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::SuperRecover(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::ZeroLog(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::FixDeviceSize(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::FixDataChecksum(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::CreateControlDevice(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::ClearInoCache(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::ClearSpaceCache(cmd) => cmd.run(format, dry_run),
            RescueSubcommand::ClearUuidTree(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum RescueSubcommand {
    /// Recover the chunk tree by scanning the devices one by one
    ChunkRecover(RescueChunkRecoverCommand),
    /// Recover bad superblocks from good copies
    SuperRecover(RescueSuperRecoverCommand),
    /// Clear the tree log (usable if it's corrupted and prevents mount)
    ZeroLog(RescueZeroLogCommand),
    /// Re-align device and super block sizes
    FixDeviceSize(RescueFixDeviceSizeCommand),
    /// Fix data checksum mismatches
    FixDataChecksum(RescueFixDataChecksumCommand),
    /// Create /dev/btrfs-control
    CreateControlDevice(RescueCreateControlDeviceCommand),
    /// Remove leftover items pertaining to the deprecated inode cache feature
    ClearInoCache(RescueClearInoCacheCommand),
    /// Completely remove the v1 or v2 free space cache
    ClearSpaceCache(RescueClearSpaceCacheCommand),
    /// Delete uuid tree so that kernel can rebuild it at mount time
    ClearUuidTree(RescueClearUuidTreeCommand),
}

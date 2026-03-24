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

/// Toolbox for specific rescue operations.
///
/// Provide emergency recovery tools for damaged or unrecoverable filesystems.
/// These operations are potentially dangerous and should only be used when
/// the filesystem cannot be mounted or accessed through normal means.
/// Most rescue operations require CAP_SYS_ADMIN and an unmounted filesystem.
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
    ChunkRecover(RescueChunkRecoverCommand),
    SuperRecover(RescueSuperRecoverCommand),
    ZeroLog(RescueZeroLogCommand),
    FixDeviceSize(RescueFixDeviceSizeCommand),
    FixDataChecksum(RescueFixDataChecksumCommand),
    CreateControlDevice(RescueCreateControlDeviceCommand),
    ClearInoCache(RescueClearInoCacheCommand),
    ClearSpaceCache(RescueClearSpaceCacheCommand),
    ClearUuidTree(RescueClearUuidTreeCommand),
}

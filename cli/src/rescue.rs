use crate::{CommandGroup, Runnable};
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

pub use self::{
    chunk_recover::*, clear_ino_cache::*, clear_space_cache::*,
    clear_uuid_tree::*, create_control_device::*, fix_data_checksum::*,
    fix_device_size::*, super_recover::*, zero_log::*,
};

/// Toolbox for specific rescue operations.
///
/// Provide emergency recovery tools for damaged or unrecoverable filesystems.
/// These operations are potentially dangerous and should only be used when
/// the filesystem cannot be mounted or accessed through normal means.
/// Most rescue operations require CAP_SYS_ADMIN and an unmounted filesystem.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
#[clap(arg_required_else_help = true)]
pub struct RescueCommand {
    #[clap(subcommand)]
    pub subcommand: RescueSubcommand,
}

impl CommandGroup for RescueCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            RescueSubcommand::ChunkRecover(cmd) => cmd,
            RescueSubcommand::SuperRecover(cmd) => cmd,
            RescueSubcommand::ZeroLog(cmd) => cmd,
            RescueSubcommand::FixDeviceSize(cmd) => cmd,
            RescueSubcommand::FixDataChecksum(cmd) => cmd,
            RescueSubcommand::CreateControlDevice(cmd) => cmd,
            RescueSubcommand::ClearInoCache(cmd) => cmd,
            RescueSubcommand::ClearSpaceCache(cmd) => cmd,
            RescueSubcommand::ClearUuidTree(cmd) => cmd,
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

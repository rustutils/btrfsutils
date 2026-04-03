use crate::{CommandGroup, Runnable};
use clap::Parser;

mod create;
mod delete;
mod find_new;
mod flags;
mod get_default;
mod list;
mod set_default;
mod show;
mod snapshot;
mod sync;

pub use self::{
    create::*, delete::*, find_new::*, flags::*, get_default::*, list::*,
    set_default::*, show::*, snapshot::*, sync::*,
};

/// Create, delete, list, and manage btrfs subvolumes and snapshots.
///
/// Subvolumes are independent filesystem trees within a btrfs filesystem,
/// allowing flexible storage organization, snapshots, and quota management.
/// Snapshots are read-only or read-write copies of subvolumes at a point in time.
/// Most operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct SubvolumeCommand {
    #[clap(subcommand)]
    pub subcommand: SubvolumeSubcommand,
}

impl CommandGroup for SubvolumeCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            SubvolumeSubcommand::Create(cmd) => cmd,
            SubvolumeSubcommand::Delete(cmd) => cmd,
            SubvolumeSubcommand::Snapshot(cmd) => cmd,
            SubvolumeSubcommand::Show(cmd) => cmd,
            SubvolumeSubcommand::List(cmd) => cmd,
            SubvolumeSubcommand::GetDefault(cmd) => cmd,
            SubvolumeSubcommand::SetDefault(cmd) => cmd,
            SubvolumeSubcommand::GetFlags(cmd) => cmd,
            SubvolumeSubcommand::SetFlags(cmd) => cmd,
            SubvolumeSubcommand::FindNew(cmd) => cmd,
            SubvolumeSubcommand::Sync(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum SubvolumeSubcommand {
    Create(SubvolumeCreateCommand),
    #[clap(alias = "del")]
    Delete(SubvolumeDeleteCommand),
    Snapshot(SubvolumeSnapshotCommand),
    Show(SubvolumeShowCommand),
    List(SubvolumeListCommand),
    GetDefault(SubvolumeGetDefaultCommand),
    SetDefault(SubvolumeSetDefaultCommand),
    GetFlags(SubvolumeGetFlagsCommand),
    SetFlags(SubvolumeSetFlagsCommand),
    FindNew(SubvolumeFindNewCommand),
    Sync(SubvolumeSyncCommand),
}

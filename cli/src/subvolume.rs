use crate::{Format, Runnable};
use anyhow::Result;
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
pub struct SubvolumeCommand {
    #[clap(subcommand)]
    pub subcommand: SubvolumeSubcommand,
}

impl Runnable for SubvolumeCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            SubvolumeSubcommand::Create(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::Delete(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::Snapshot(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::Show(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::List(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::GetDefault(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::SetDefault(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::GetFlags(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::SetFlags(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::FindNew(cmd) => cmd.run(format, dry_run),
            SubvolumeSubcommand::Sync(cmd) => cmd.run(format, dry_run),
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

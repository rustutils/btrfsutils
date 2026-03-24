use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod create;
mod delete;
mod flags;
mod get_default;
mod list;
mod set_default;
mod show;
mod snapshot;

use create::SubvolumeCreateCommand;
use delete::SubvolumeDeleteCommand;
use flags::{SubvolumeGetFlagsCommand, SubvolumeSetFlagsCommand};
use get_default::SubvolumeGetDefaultCommand;
use list::SubvolumeListCommand;
use set_default::SubvolumeSetDefaultCommand;
use show::SubvolumeShowCommand;
use snapshot::SubvolumeSnapshotCommand;

/// Create, delete, list, and manage btrfs subvolumes and snapshots
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
        }
    }
}

#[derive(Parser, Debug)]
pub enum SubvolumeSubcommand {
    /// Create a new subvolume at each given path
    Create(SubvolumeCreateCommand),
    /// Delete one or more subvolumes or snapshots
    #[clap(alias = "del")]
    Delete(SubvolumeDeleteCommand),
    /// Create a snapshot of a subvolume
    Snapshot(SubvolumeSnapshotCommand),
    /// Show detailed information about a subvolume
    Show(SubvolumeShowCommand),
    /// List subvolumes and snapshots in the filesystem
    List(SubvolumeListCommand),
    /// Show the default subvolume of a filesystem
    GetDefault(SubvolumeGetDefaultCommand),
    /// Set the default subvolume of a filesystem
    SetDefault(SubvolumeSetDefaultCommand),
    /// Show the flags of a subvolume
    GetFlags(SubvolumeGetFlagsCommand),
    /// Set the flags of a subvolume
    SetFlags(SubvolumeSetFlagsCommand),
}

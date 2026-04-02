use crate::{Format, RunContext, Runnable};
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
#[allow(clippy::doc_markdown)]
pub struct SubvolumeCommand {
    #[clap(subcommand)]
    pub subcommand: SubvolumeSubcommand,
}

impl Runnable for SubvolumeCommand {
    fn supported_formats(&self) -> &[Format] {
        match &self.subcommand {
            SubvolumeSubcommand::Show(cmd) => cmd.supported_formats(),
            SubvolumeSubcommand::List(cmd) => cmd.supported_formats(),
            SubvolumeSubcommand::GetDefault(cmd) => cmd.supported_formats(),
            _ => &[Format::Text, Format::Modern],
        }
    }

    fn supports_dry_run(&self) -> bool {
        matches!(self.subcommand, SubvolumeSubcommand::Delete(_))
    }

    fn run(&self, ctx: &RunContext) -> Result<()> {
        match &self.subcommand {
            SubvolumeSubcommand::Create(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::Delete(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::Snapshot(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::Show(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::List(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::GetDefault(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::SetDefault(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::GetFlags(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::SetFlags(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::FindNew(cmd) => cmd.run(ctx),
            SubvolumeSubcommand::Sync(cmd) => cmd.run(ctx),
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

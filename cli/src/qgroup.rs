use crate::{CommandGroup, Runnable};
use clap::Parser;

mod assign;
mod clear_stale;
mod create;
mod destroy;
mod limit;
mod remove;
mod show;

pub use self::{
    assign::*, clear_stale::*, create::*, destroy::*, limit::*, remove::*,
    show::*,
};

/// Manage quota groups.
///
/// Quota groups (qgroups) track and limit disk usage at the subvolume
/// level. Each subvolume automatically gets a level-0 qgroup (e.g.
/// 0/256 for subvolume 256) that tracks how much data it references
/// and how much is exclusive to it. Qgroups cannot be applied to
/// individual files or directories within a subvolume, only to whole
/// subvolumes. This is why per-directory quotas on btrfs require
/// creating separate subvolumes.
///
/// Higher-level qgroups (1/0, 2/0, ...) are user-created containers
/// that aggregate the usage of their member subvolumes and can enforce
/// a shared limit across all of them. For example, assigning several
/// user home subvolumes to a single level-1 qgroup lets you cap their
/// combined usage.
///
/// Quotas must be enabled first with "btrfs quota enable". Most
/// operations require CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
#[clap(arg_required_else_help = true)]
pub struct QgroupCommand {
    #[clap(subcommand)]
    pub subcommand: QgroupSubcommand,
}

impl CommandGroup for QgroupCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            QgroupSubcommand::Assign(cmd) => cmd,
            QgroupSubcommand::Remove(cmd) => cmd,
            QgroupSubcommand::Create(cmd) => cmd,
            QgroupSubcommand::Destroy(cmd) => cmd,
            QgroupSubcommand::Show(cmd) => cmd,
            QgroupSubcommand::Limit(cmd) => cmd,
            QgroupSubcommand::ClearStale(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum QgroupSubcommand {
    Assign(QgroupAssignCommand),
    Remove(QgroupRemoveCommand),
    Create(QgroupCreateCommand),
    Destroy(QgroupDestroyCommand),
    Show(QgroupShowCommand),
    Limit(QgroupLimitCommand),
    #[command(name = "clear-stale")]
    ClearStale(QgroupClearStaleCommand),
}

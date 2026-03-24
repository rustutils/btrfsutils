use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod get;
mod list;
mod set;

use get::PropertyGetCommand;
use list::PropertyListCommand;
use set::PropertySetCommand;

/// Object type for property operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PropertyObjectType {
    Inode,
    Subvol,
    Filesystem,
    Device,
}

/// Modify properties of filesystem objects.
///
/// Get, set, and list properties of filesystem objects including subvolumes,
/// inodes, the filesystem itself, and devices. Properties control various
/// aspects of filesystem behavior such as read-only status, compression,
/// and labels. Most property operations require CAP_SYS_ADMIN or appropriate
/// filesystem permissions.
#[derive(Parser, Debug)]
pub struct PropertyCommand {
    #[clap(subcommand)]
    pub subcommand: PropertySubcommand,
}

impl Runnable for PropertyCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            PropertySubcommand::Get(cmd) => cmd.run(format, dry_run),
            PropertySubcommand::Set(cmd) => cmd.run(format, dry_run),
            PropertySubcommand::List(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum PropertySubcommand {
    Get(PropertyGetCommand),
    Set(PropertySetCommand),
    List(PropertyListCommand),
}

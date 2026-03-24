use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

mod get;
mod list;
mod set;

use get::PropertyGetCommand;
use list::PropertyListCommand;
use set::PropertySetCommand;

/// Modify properties of filesystem objects
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
    /// Get a property value of a btrfs object
    Get(PropertyGetCommand),
    /// Set a property on a btrfs object
    Set(PropertySetCommand),
    /// List available properties with their descriptions for the given object
    List(PropertyListCommand),
}

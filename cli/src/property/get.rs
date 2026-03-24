use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

/// Object type for property operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PropertyObjectType {
    Inode,
    Subvol,
    Filesystem,
    Device,
}

/// Get a property value of a btrfs object
///
/// If no name is specified, all properties for the object are printed.
#[derive(Parser, Debug)]
pub struct PropertyGetCommand {
    /// Object type (inode, subvol, filesystem, device)
    #[clap(short = 't', long = "type")]
    pub object_type: Option<PropertyObjectType>,

    /// Path to the btrfs object
    pub object: String,

    /// Property name to retrieve
    pub name: Option<String>,
}

impl Runnable for PropertyGetCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement btrfs property get")
    }
}

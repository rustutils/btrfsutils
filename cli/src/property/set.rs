use super::get::PropertyObjectType;
use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;

/// Set a property on a btrfs object
#[derive(Parser, Debug)]
pub struct PropertySetCommand {
    /// Path to the btrfs object
    pub object: String,

    /// Name of the property to set
    pub name: String,

    /// Value to assign to the property
    pub value: String,

    /// Object type (inode, subvol, filesystem, device)
    #[clap(short = 't', long = "type")]
    pub object_type: Option<PropertyObjectType>,

    /// Force the change
    #[clap(short = 'f', long)]
    pub force: bool,
}

impl Runnable for PropertySetCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement btrfs property set")
    }
}

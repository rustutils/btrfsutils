use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use super::get::PropertyObjectType;

/// List available properties with their descriptions for the given object
#[derive(Parser, Debug)]
pub struct PropertyListCommand {
    /// Btrfs object path to list properties for
    pub object: PathBuf,

    /// Object type (inode, subvol, filesystem, device)
    #[clap(short = 't', long = "type")]
    pub object_type: Option<PropertyObjectType>,
}

impl Runnable for PropertyListCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement btrfs property list")
    }
}

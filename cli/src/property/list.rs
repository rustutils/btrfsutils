use super::{
    PropertyObjectType, detect_object_types, property_description,
    property_names,
};
use crate::{Format, Runnable};
use anyhow::{Result, anyhow, bail};
use clap::Parser;
use std::path::PathBuf;

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
        // Detect object type if not specified
        let detected_types = detect_object_types(&self.object);
        let target_type = if let Some(t) = self.object_type {
            t
        } else {
            // If ambiguous, require the user to specify
            if detected_types.len() > 1 {
                bail!(
                    "object type is ambiguous, please use option -t (detected: {detected_types:?})"
                );
            }
            detected_types
                .first()
                .copied()
                .ok_or_else(|| anyhow!("object is not a btrfs object"))?
        };

        for name in property_names(target_type) {
            println!("{:<20}{}", name, property_description(name));
        }

        Ok(())
    }
}

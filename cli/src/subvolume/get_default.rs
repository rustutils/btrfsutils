use crate::{
    Format, Runnable,
    util::{open_path, print_json},
};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{FS_TREE_OBJECTID, subvolume_default_get};
use clap::Parser;
use serde::Serialize;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show the default subvolume of a filesystem
#[derive(Parser, Debug)]
pub struct SubvolumeGetDefaultCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

#[derive(Serialize)]
struct DefaultSubvolJson {
    id: u64,
    name: String,
}

impl Runnable for SubvolumeGetDefaultCommand {
    fn run(&self, format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        let default_id =
            subvolume_default_get(file.as_fd()).with_context(|| {
                format!(
                    "failed to get default subvolume for '{}'",
                    self.path.display()
                )
            })?;

        let name = if default_id == FS_TREE_OBJECTID {
            "FS_TREE".to_string()
        } else {
            format!("{default_id}")
        };

        match format {
            Format::Text => {
                if default_id == FS_TREE_OBJECTID {
                    println!("ID 5 (FS_TREE)");
                } else {
                    println!("ID {default_id}");
                }
            }
            Format::Json => {
                print_json(
                    "default-subvolume",
                    &DefaultSubvolJson {
                        id: default_id,
                        name,
                    },
                )?;
            }
        }

        Ok(())
    }
}

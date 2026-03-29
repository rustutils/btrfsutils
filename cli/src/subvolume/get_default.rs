use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{FS_TREE_OBJECTID, subvolume_default_get};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show the default subvolume of a filesystem
#[derive(Parser, Debug)]
pub struct SubvolumeGetDefaultCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
}

impl Runnable for SubvolumeGetDefaultCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        let default_id =
            subvolume_default_get(file.as_fd()).with_context(|| {
                format!(
                    "failed to get default subvolume for '{}'",
                    self.path.display()
                )
            })?;

        if default_id == FS_TREE_OBJECTID {
            println!("ID 5 (FS_TREE)");
        } else {
            // TODO: resolve name via BTRFS_IOC_GET_SUBVOL_INFO + path lookup
            println!("ID {}", default_id);
        }

        Ok(())
    }
}

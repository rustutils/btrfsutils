use crate::{RunContext, Runnable};
use anyhow::{Context, Result, anyhow};
use btrfs_uapi::subvolume::{
    FS_TREE_OBJECTID, subvolume_default_set, subvolume_info,
};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Set the default subvolume of a filesystem.
///
/// Accepts either a subvolume path (btrfs subvolume set-default /mnt/fs/subvol)
/// or a numeric ID with a filesystem path (btrfs subvolume set-default 256 /mnt/fs).
#[derive(Parser, Debug)]
pub struct SubvolumeSetDefaultCommand {
    /// Subvolume path OR numeric subvolume ID
    #[clap(required = true)]
    subvol_or_id: String,

    /// Filesystem mount point (required when first arg is a numeric ID)
    path: Option<PathBuf>,
}

impl Runnable for SubvolumeSetDefaultCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if let Ok(id) = self.subvol_or_id.parse::<u64>() {
            let mount = self.path.as_ref().ok_or_else(|| {
                anyhow!("a filesystem path is required when specifying a subvolume ID")
            })?;

            let file = File::open(mount).with_context(|| {
                format!("failed to open '{}'", mount.display())
            })?;

            let id_to_use = if id == 0 { FS_TREE_OBJECTID } else { id };

            subvolume_default_set(file.as_fd(), id_to_use).with_context(
                || {
                    format!(
                        "failed to set default subvolume to ID {} on '{}'",
                        id_to_use,
                        mount.display()
                    )
                },
            )?;

            println!("Set default subvolume to ID {id_to_use}");
        } else {
            let subvol_path = PathBuf::from(&self.subvol_or_id);

            let file = File::open(&subvol_path).with_context(|| {
                format!("failed to open '{}'", subvol_path.display())
            })?;

            let info = subvolume_info(file.as_fd()).with_context(|| {
                format!(
                    "failed to get subvolume info for '{}'",
                    subvol_path.display()
                )
            })?;

            subvolume_default_set(file.as_fd(), info.id).with_context(
                || {
                    format!(
                        "failed to set default subvolume to '{}' (ID {})",
                        subvol_path.display(),
                        info.id
                    )
                },
            )?;

            println!(
                "Set default subvolume to '{}' (ID {})",
                subvol_path.display(),
                info.id
            );
        }

        Ok(())
    }
}

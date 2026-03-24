use super::PropertyObjectType;
use crate::{Format, Runnable};
use anyhow::{Result, anyhow, bail};
use clap::Parser;
use std::{
    fs::File,
    os::unix::fs::{FileTypeExt, MetadataExt},
    os::unix::io::AsFd,
    path::PathBuf,
};

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
        let detected_types = detect_object_types(&self.object)?;
        let target_type = match self.object_type {
            Some(t) => t,
            None => {
                // If ambiguous, require the user to specify
                if detected_types.len() > 1 {
                    bail!(
                        "object type is ambiguous, please use option -t (detected: {:?})",
                        detected_types
                    );
                }
                detected_types
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("object is not a btrfs object"))?
            }
        };

        list_properties(target_type, &self.object)?;

        Ok(())
    }
}

fn detect_object_types(path: &PathBuf) -> Result<Vec<PropertyObjectType>> {
    let mut types = Vec::new();

    // Try to stat the path
    let metadata = std::fs::metadata(path).ok();

    if let Some(metadata) = metadata {
        // All files on btrfs are inodes
        types.push(PropertyObjectType::Inode);

        // Check if it's a block device (device property)
        if metadata.file_type().is_block_device() {
            types.push(PropertyObjectType::Device);
        }

        // Try to get subvolume info - if it works, it's a subvolume
        if let Ok(file) = File::open(path) {
            if btrfs_uapi::subvolume::subvolume_info(file.as_fd()).is_ok() {
                // If the inode is BTRFS_FIRST_FREE_OBJECTID, it's a subvolume or filesystem root
                if metadata.ino() == 256 {
                    types.push(PropertyObjectType::Subvol);

                    // Check if it's the filesystem root
                    if is_filesystem_root(path).unwrap_or(false) {
                        types.push(PropertyObjectType::Filesystem);
                    }
                }
            }
        }
    }

    Ok(types)
}

fn is_filesystem_root(path: &PathBuf) -> Result<bool> {
    let canonical = std::fs::canonicalize(path)?;
    let parent = canonical.parent();

    // The filesystem root is the mount point
    // We can check if the parent has a different filesystem
    if let Some(parent) = parent {
        let canonical_metadata = std::fs::metadata(&canonical)?;
        let parent_metadata = std::fs::metadata(parent)?;

        // Different device numbers means we crossed a filesystem boundary
        if canonical_metadata.dev() != parent_metadata.dev() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn list_properties(obj_type: PropertyObjectType, _path: &PathBuf) -> Result<()> {
    match obj_type {
        PropertyObjectType::Subvol => {
            println!("{:<20}{}", "ro", "read-only status of a subvolume");
        }
        PropertyObjectType::Filesystem | PropertyObjectType::Device => {
            println!("{:<20}{}", "label", "label of the filesystem");
        }
        PropertyObjectType::Inode => {
            println!(
                "{:<20}{}",
                "compression", "compression algorithm for the file or directory"
            );
        }
    }

    Ok(())
}

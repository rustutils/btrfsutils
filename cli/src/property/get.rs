use super::PropertyObjectType;
use crate::{Format, Runnable};
use anyhow::{Context, Result, anyhow, bail};
use btrfs_uapi::{filesystem::label_get, subvolume::subvolume_flags_get};
use clap::Parser;
use std::{
    fs::File,
    os::unix::{
        fs::{FileTypeExt, MetadataExt},
        io::AsFd,
    },
    path::PathBuf,
};

/// Get a property value of a btrfs object
///
/// If no name is specified, all properties for the object are printed.
#[derive(Parser, Debug)]
pub struct PropertyGetCommand {
    /// Object type (inode, subvol, filesystem, device)
    #[clap(short = 't', long = "type")]
    pub object_type: Option<PropertyObjectType>,

    /// Path to the btrfs object
    pub object: PathBuf,

    /// Property name to retrieve
    pub name: Option<String>,
}

impl Runnable for PropertyGetCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.object)
            .with_context(|| format!("failed to open '{}'", self.object.display()))?;

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

        // If a specific property is requested, get it
        if let Some(name) = &self.name {
            get_property(&file, target_type, name, &self.object)?;
        } else {
            // Otherwise, list all properties for this object type
            list_properties(target_type, &self.object)?;
        }

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

fn get_property(
    file: &File,
    obj_type: PropertyObjectType,
    name: &str,
    path: &PathBuf,
) -> Result<()> {
    match (obj_type, name) {
        (PropertyObjectType::Subvol, "ro") => {
            let flags = subvolume_flags_get(file.as_fd()).with_context(|| {
                format!("failed to get read-only flag for '{}'", path.display())
            })?;
            let is_readonly = flags.contains(btrfs_uapi::subvolume::SubvolumeFlags::RDONLY);
            println!("ro={}", if is_readonly { "true" } else { "false" });
        }
        (PropertyObjectType::Filesystem, "label") | (PropertyObjectType::Device, "label") => {
            let label = label_get(file.as_fd())
                .with_context(|| format!("failed to get label for '{}'", path.display()))?;
            println!("label={}", label.to_bytes().escape_ascii());
        }
        (PropertyObjectType::Inode, "compression") => {
            // Try to get compression xattr
            get_compression_property(file, path)?;
        }
        _ => {
            bail!(
                "property '{}' is not applicable to object type {:?}",
                name,
                obj_type
            );
        }
    }

    Ok(())
}

fn get_compression_property(file: &File, path: &PathBuf) -> Result<()> {
    use nix::libc::{ENODATA, fgetxattr};
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let xattr_name = "btrfs.compression\0";

    // SAFETY: fgetxattr is safe to call with a valid fd and valid string pointer
    let result = unsafe {
        fgetxattr(
            fd,
            xattr_name.as_ptr() as *const i8,
            std::ptr::null_mut(),
            0,
        )
    };

    if result < 0 {
        let errno = nix::errno::Errno::last_raw();
        if errno == ENODATA {
            // Attribute doesn't exist; compression not set
            return Ok(());
        } else {
            return Err(anyhow::anyhow!(
                "failed to get compression for '{}': {}",
                path.display(),
                nix::errno::Errno::from_raw(errno)
            ));
        }
    }

    let len = result as usize;
    let mut buf = vec![0u8; len];

    // SAFETY: fgetxattr is safe to call with a valid fd, valid buffer, and valid string pointer
    let result = unsafe {
        fgetxattr(
            fd,
            xattr_name.as_ptr() as *const i8,
            buf.as_mut_ptr() as *mut std::ffi::c_void,
            len,
        )
    };

    if result < 0 {
        return Err(anyhow::anyhow!(
            "failed to get compression for '{}'",
            path.display()
        ));
    }

    let value = String::from_utf8_lossy(&buf);
    println!("compression={}", value);

    Ok(())
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

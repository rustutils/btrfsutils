use super::PropertyObjectType;
use crate::{Format, Runnable};
use anyhow::{Context, Result, anyhow, bail};
use btrfs_uapi::{
    filesystem::label_set,
    subvolume::{SubvolumeFlags, subvolume_flags_get, subvolume_flags_set},
};
use clap::Parser;
use std::{
    ffi::CString,
    fs::File,
    os::unix::{
        fs::{FileTypeExt, MetadataExt},
        io::AsFd,
    },
    path::PathBuf,
};

/// Set a property on a btrfs object
#[derive(Parser, Debug)]
pub struct PropertySetCommand {
    /// Path to the btrfs object
    pub object: PathBuf,

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

        set_property(
            &file,
            target_type,
            &self.name,
            &self.value,
            self.force,
            &self.object,
        )?;

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

fn set_property(
    file: &File,
    obj_type: PropertyObjectType,
    name: &str,
    value: &str,
    force: bool,
    path: &PathBuf,
) -> Result<()> {
    match (obj_type, name) {
        (PropertyObjectType::Subvol, "ro") => {
            set_readonly_property(file, value, force, path)?;
        }
        (PropertyObjectType::Filesystem, "label") | (PropertyObjectType::Device, "label") => {
            let cstring =
                CString::new(value.as_bytes()).context("label must not contain null bytes")?;
            label_set(file.as_fd(), &cstring)
                .with_context(|| format!("failed to set label for '{}'", path.display()))?;
        }
        (PropertyObjectType::Inode, "compression") => {
            set_compression_property(file, value, path)?;
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

fn set_readonly_property(file: &File, value: &str, force: bool, path: &PathBuf) -> Result<()> {
    let new_readonly = match value {
        "true" => true,
        "false" => false,
        _ => bail!("invalid value for property: {}", value),
    };

    let current_flags = subvolume_flags_get(file.as_fd())
        .with_context(|| format!("failed to get flags for '{}'", path.display()))?;
    let is_readonly = current_flags.contains(SubvolumeFlags::RDONLY);

    // No change if already in desired state
    if is_readonly == new_readonly {
        return Ok(());
    }

    // If going from ro to rw, check for received_uuid
    if is_readonly && !new_readonly {
        let info = btrfs_uapi::subvolume::subvolume_info(file.as_fd())
            .with_context(|| format!("failed to get subvolume info for '{}'", path.display()))?;

        if !info.received_uuid.is_nil() {
            if !force {
                bail!(
                    "cannot flip ro->rw with received_uuid set, use force option -f if you really want to unset the read-only status. \
                     The value of received_uuid is used for incremental send, consider making a snapshot instead."
                );
            }
        }
    }

    let mut new_flags = current_flags;
    if new_readonly {
        new_flags |= SubvolumeFlags::RDONLY;
    } else {
        new_flags &= !SubvolumeFlags::RDONLY;
    }

    subvolume_flags_set(file.as_fd(), new_flags)
        .with_context(|| format!("failed to set flags for '{}'", path.display()))?;

    // Clear received_uuid after flipping ro→rw with force.  This must
    // happen after the flag change (the kernel rejects SET_RECEIVED_SUBVOL
    // on a read-only subvolume). If it fails, warn but don't error —
    // matching the C reference behaviour.
    if is_readonly && !new_readonly && force {
        let info = btrfs_uapi::subvolume::subvolume_info(file.as_fd()).ok();
        if let Some(info) = info {
            if !info.received_uuid.is_nil() {
                eprintln!(
                    "clearing received_uuid (was {})",
                    info.received_uuid.as_hyphenated()
                );
                if let Err(e) =
                    btrfs_uapi::send_receive::received_subvol_set(file.as_fd(), &uuid::Uuid::nil(), 0)
                {
                    eprintln!(
                        "WARNING: failed to clear received_uuid on '{}': {e}",
                        path.display()
                    );
                }
            }
        }
    }

    Ok(())
}

fn set_compression_property(file: &File, value: &str, path: &PathBuf) -> Result<()> {
    use nix::libc::fsetxattr;
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let xattr_name = "btrfs.compression\0";

    // SAFETY: fsetxattr is safe to call with a valid fd and valid string pointers
    let result = unsafe {
        fsetxattr(
            fd,
            xattr_name.as_ptr() as *const i8,
            value.as_ptr() as *const std::ffi::c_void,
            value.len(),
            0,
        )
    };

    if result < 0 {
        return Err(anyhow::anyhow!(
            "failed to set compression for '{}': {}",
            path.display(),
            nix::errno::Errno::last()
        ));
    }

    Ok(())
}

use super::{PropertyObjectType, detect_object_types, property_names};
use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result, anyhow, bail};
use btrfs_uapi::{filesystem::label_get, subvolume::subvolume_flags_get};
use clap::Parser;
use std::{
    fs::File,
    os::unix::io::AsFd,
    path::{Path, PathBuf},
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
        let file = open_path(&self.object)?;

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

        // If a specific property is requested, get it
        if let Some(name) = &self.name {
            get_property(&file, target_type, name, &self.object)?;
        } else {
            // Otherwise, list all properties with their values
            for name in property_names(target_type) {
                // Best-effort: print what we can, skip errors for individual properties
                let _ = get_property(&file, target_type, name, &self.object);
            }
        }

        Ok(())
    }
}

fn get_property(
    file: &File,
    obj_type: PropertyObjectType,
    name: &str,
    path: &Path,
) -> Result<()> {
    match (obj_type, name) {
        (PropertyObjectType::Subvol, "ro") => {
            let flags =
                subvolume_flags_get(file.as_fd()).with_context(|| {
                    format!(
                        "failed to get read-only flag for '{}'",
                        path.display()
                    )
                })?;
            let is_readonly =
                flags.contains(btrfs_uapi::subvolume::SubvolumeFlags::RDONLY);
            println!("ro={}", if is_readonly { "true" } else { "false" });
        }
        (
            PropertyObjectType::Filesystem | PropertyObjectType::Device,
            "label",
        ) => {
            let label = label_get(file.as_fd()).with_context(|| {
                format!("failed to get label for '{}'", path.display())
            })?;
            println!("label={}", label.to_bytes().escape_ascii());
        }
        (PropertyObjectType::Inode, "compression") => {
            get_compression_property(file, path)?;
        }
        _ => {
            bail!(
                "property '{name}' is not applicable to object type {obj_type:?}"
            );
        }
    }

    Ok(())
}

fn get_compression_property(file: &File, path: &Path) -> Result<()> {
    use nix::libc::{ENODATA, fgetxattr};
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let xattr_name = "btrfs.compression\0";

    // SAFETY: fgetxattr is safe to call with a valid fd and valid string pointer
    let result = unsafe {
        fgetxattr(fd, xattr_name.as_ptr().cast(), std::ptr::null_mut(), 0)
    };

    if result < 0 {
        let errno = nix::errno::Errno::last_raw();
        if errno == ENODATA {
            // Attribute doesn't exist; compression not set
            return Ok(());
        }
        return Err(anyhow::anyhow!(
            "failed to get compression for '{}': {}",
            path.display(),
            nix::errno::Errno::from_raw(errno)
        ));
    }

    #[allow(clippy::cast_sign_loss)] // result is positive after the check above
    let len = result as usize;
    let mut buf = vec![0u8; len];

    // SAFETY: fgetxattr is safe to call with a valid fd, valid buffer, and valid string pointer
    let result = unsafe {
        fgetxattr(fd, xattr_name.as_ptr().cast(), buf.as_mut_ptr().cast(), len)
    };

    if result < 0 {
        return Err(anyhow::anyhow!(
            "failed to get compression for '{}'",
            path.display()
        ));
    }

    let value = String::from_utf8_lossy(&buf);
    println!("compression={value}");

    Ok(())
}

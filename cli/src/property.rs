use crate::{CommandGroup, Runnable};
use anyhow::Result;
use clap::Parser;
use std::{
    fs::{self, File},
    os::unix::{
        fs::{FileTypeExt, MetadataExt},
        io::AsFd,
    },
    path::Path,
};

mod get;
mod list;
mod set;

pub use self::{get::*, list::*, set::*};

/// Object type for property operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PropertyObjectType {
    Inode,
    Subvol,
    Filesystem,
    Device,
}

/// Modify properties of filesystem objects.
///
/// Get, set, and list properties of filesystem objects including subvolumes,
/// inodes, the filesystem itself, and devices. Properties control various
/// aspects of filesystem behavior such as read-only status, compression,
/// and labels. Most property operations require CAP_SYS_ADMIN or appropriate
/// filesystem permissions.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
#[clap(arg_required_else_help = true)]
pub struct PropertyCommand {
    #[clap(subcommand)]
    pub subcommand: PropertySubcommand,
}

impl CommandGroup for PropertyCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            PropertySubcommand::Get(cmd) => cmd,
            PropertySubcommand::Set(cmd) => cmd,
            PropertySubcommand::List(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum PropertySubcommand {
    Get(PropertyGetCommand),
    Set(PropertySetCommand),
    List(PropertyListCommand),
}

/// Metadata attributes used to classify a filesystem object.
#[allow(clippy::struct_excessive_bools)]
struct ObjectAttrs {
    /// Whether the path could be stat'd at all.
    exists: bool,
    /// Whether the path is a block device.
    is_block_device: bool,
    /// Inode number of the path.
    ino: u64,
    /// Whether `subvolume_info` succeeded on the path.
    is_subvolume: bool,
    /// Whether the path is the filesystem root (mount point).
    is_fs_root: bool,
}

/// Classify a filesystem object based on its attributes.
///
/// Returns the list of applicable object types. The order matches the C
/// reference: inode first, then device, subvol, filesystem.
fn classify_object(attrs: &ObjectAttrs) -> Vec<PropertyObjectType> {
    if !attrs.exists {
        return Vec::new();
    }

    let mut types = Vec::new();

    // All files on btrfs are inodes.
    types.push(PropertyObjectType::Inode);

    // Block devices get the Device type.
    if attrs.is_block_device {
        types.push(PropertyObjectType::Device);
    }

    // Subvolume roots have inode 256 (BTRFS_FIRST_FREE_OBJECTID) and
    // respond to subvolume_info.
    if attrs.is_subvolume && attrs.ino == 256 {
        types.push(PropertyObjectType::Subvol);

        // The filesystem root is a subvolume that is also a mount point.
        if attrs.is_fs_root {
            types.push(PropertyObjectType::Filesystem);
        }
    }

    types
}

/// Probe the filesystem to build `ObjectAttrs` for the given path.
fn probe_object_attrs(path: &Path) -> ObjectAttrs {
    let Ok(metadata) = fs::metadata(path) else {
        return ObjectAttrs {
            exists: false,
            is_block_device: false,
            ino: 0,
            is_subvolume: false,
            is_fs_root: false,
        };
    };

    let is_block_device = metadata.file_type().is_block_device();
    let ino = metadata.ino();

    let is_subvolume = File::open(path).ok().is_some_and(|f| {
        btrfs_uapi::subvolume::subvolume_info(f.as_fd()).is_ok()
    });

    let is_fs_root = is_filesystem_root(path).unwrap_or(false);

    ObjectAttrs {
        exists: true,
        is_block_device,
        ino,
        is_subvolume,
        is_fs_root,
    }
}

/// Detect which object types a path could be.
fn detect_object_types(path: &Path) -> Vec<PropertyObjectType> {
    classify_object(&probe_object_attrs(path))
}

/// Check whether `path` is a filesystem root (mount point) by comparing
/// device numbers with its parent directory.
fn is_filesystem_root(path: &Path) -> Result<bool> {
    let canonical = fs::canonicalize(path)?;
    let parent = canonical.parent();

    if let Some(parent) = parent {
        let canonical_metadata = fs::metadata(&canonical)?;
        let parent_metadata = fs::metadata(parent)?;

        if canonical_metadata.dev() != parent_metadata.dev() {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Return the property names applicable to the given object type.
fn property_names(obj_type: PropertyObjectType) -> &'static [&'static str] {
    match obj_type {
        PropertyObjectType::Inode => &["compression"],
        PropertyObjectType::Subvol => &["ro"],
        PropertyObjectType::Filesystem | PropertyObjectType::Device => {
            &["label"]
        }
    }
}

/// Return a human-readable description for a property name.
fn property_description(name: &str) -> &'static str {
    match name {
        "ro" => "read-only status of a subvolume",
        "label" => "label of the filesystem",
        "compression" => "compression algorithm for the file or directory",
        _ => "unknown property",
    }
}

/// Check whether `name` is a valid property for `obj_type`.
#[cfg(test)]
fn is_valid_property(obj_type: PropertyObjectType, name: &str) -> bool {
    property_names(obj_type).contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(
        is_block_device: bool,
        ino: u64,
        is_subvolume: bool,
        is_fs_root: bool,
    ) -> ObjectAttrs {
        ObjectAttrs {
            exists: true,
            is_block_device,
            ino,
            is_subvolume,
            is_fs_root,
        }
    }

    // --- classify_object ---

    #[test]
    fn classify_regular_file() {
        let types = classify_object(&attrs(false, 1000, false, false));
        assert_eq!(types, vec![PropertyObjectType::Inode]);
    }

    #[test]
    fn classify_block_device() {
        let types = classify_object(&attrs(true, 1000, false, false));
        assert_eq!(
            types,
            vec![PropertyObjectType::Inode, PropertyObjectType::Device]
        );
    }

    #[test]
    fn classify_subvolume() {
        let types = classify_object(&attrs(false, 256, true, false));
        assert_eq!(
            types,
            vec![PropertyObjectType::Inode, PropertyObjectType::Subvol]
        );
    }

    #[test]
    fn classify_subvolume_at_mount_point() {
        let types = classify_object(&attrs(false, 256, true, true));
        assert_eq!(
            types,
            vec![
                PropertyObjectType::Inode,
                PropertyObjectType::Subvol,
                PropertyObjectType::Filesystem,
            ]
        );
    }

    #[test]
    fn classify_nonexistent_path() {
        let a = ObjectAttrs {
            exists: false,
            is_block_device: false,
            ino: 0,
            is_subvolume: false,
            is_fs_root: false,
        };
        assert!(classify_object(&a).is_empty());
    }

    #[test]
    fn classify_subvolume_info_ok_but_wrong_ino() {
        // subvolume_info succeeds but inode is not 256 — should not detect as subvol.
        let types = classify_object(&attrs(false, 500, true, false));
        assert_eq!(types, vec![PropertyObjectType::Inode]);
    }

    #[test]
    fn classify_ino_256_but_not_subvolume() {
        // Inode 256 but subvolume_info fails — should not detect as subvol.
        let types = classify_object(&attrs(false, 256, false, false));
        assert_eq!(types, vec![PropertyObjectType::Inode]);
    }

    #[test]
    fn classify_fs_root_requires_subvolume() {
        // is_fs_root but not a subvolume — filesystem type should not appear.
        let types = classify_object(&attrs(false, 256, false, true));
        assert_eq!(types, vec![PropertyObjectType::Inode]);
    }

    // --- property_names ---

    #[test]
    fn property_names_inode() {
        assert_eq!(property_names(PropertyObjectType::Inode), &["compression"]);
    }

    #[test]
    fn property_names_subvol() {
        assert_eq!(property_names(PropertyObjectType::Subvol), &["ro"]);
    }

    #[test]
    fn property_names_filesystem() {
        assert_eq!(property_names(PropertyObjectType::Filesystem), &["label"]);
    }

    #[test]
    fn property_names_device() {
        assert_eq!(property_names(PropertyObjectType::Device), &["label"]);
    }

    // --- is_valid_property ---

    #[test]
    fn valid_property_ro_on_subvol() {
        assert!(is_valid_property(PropertyObjectType::Subvol, "ro"));
    }

    #[test]
    fn invalid_property_ro_on_inode() {
        assert!(!is_valid_property(PropertyObjectType::Inode, "ro"));
    }

    #[test]
    fn valid_property_label_on_filesystem() {
        assert!(is_valid_property(PropertyObjectType::Filesystem, "label"));
    }

    #[test]
    fn valid_property_label_on_device() {
        assert!(is_valid_property(PropertyObjectType::Device, "label"));
    }

    #[test]
    fn invalid_property_label_on_subvol() {
        assert!(!is_valid_property(PropertyObjectType::Subvol, "label"));
    }

    #[test]
    fn valid_property_compression_on_inode() {
        assert!(is_valid_property(PropertyObjectType::Inode, "compression"));
    }

    #[test]
    fn invalid_property_unknown() {
        assert!(!is_valid_property(PropertyObjectType::Inode, "nosuch"));
    }

    // --- property_description ---

    #[test]
    fn description_known_properties() {
        assert_eq!(
            property_description("ro"),
            "read-only status of a subvolume"
        );
        assert_eq!(property_description("label"), "label of the filesystem");
        assert_eq!(
            property_description("compression"),
            "compression algorithm for the file or directory"
        );
    }

    #[test]
    fn description_unknown_property() {
        assert_eq!(property_description("nosuch"), "unknown property");
    }
}

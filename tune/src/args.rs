use clap::Parser;
use std::path::PathBuf;
use uuid::Uuid;

/// Tune various btrfs filesystem parameters on an unmounted device.
///
/// Only one operation group may be specified per invocation. The legacy
/// feature flags (-r, -x, -n) can be combined with each other but not
/// with the seeding flag (-S).
#[derive(Parser, Debug)]
#[command(version, name = "btrfs-tune")]
pub struct Arguments {
    /// Enable extended inode refs (extref)
    #[arg(short = 'r', conflicts_with = "seeding")]
    pub extref: bool,

    /// Enable skinny metadata extent refs
    #[arg(short = 'x', conflicts_with = "seeding")]
    pub skinny_metadata: bool,

    /// Enable no-holes feature
    #[arg(short = 'n', conflicts_with = "seeding")]
    pub no_holes: bool,

    /// Set (1) or clear (0) the seeding flag
    #[arg(short = 'S', value_parser = parse_seeding_value,
        conflicts_with_all = ["metadata_uuid", "set_metadata_uuid"])]
    pub seeding: Option<bool>,

    /// Change fsid to a random UUID via the metadata_uuid mechanism
    #[arg(short = 'm', conflicts_with_all = ["extref", "skinny_metadata",
        "no_holes", "seeding", "set_metadata_uuid"])]
    pub metadata_uuid: bool,

    /// Change fsid to the given UUID via the metadata_uuid mechanism
    #[arg(short = 'M', value_name = "UUID",
        conflicts_with_all = ["extref", "skinny_metadata", "no_holes",
        "seeding", "metadata_uuid"])]
    pub set_metadata_uuid: Option<Uuid>,

    /// Allow dangerous operations without confirmation
    #[arg(short = 'f', long)]
    pub force: bool,

    /// Path to the btrfs device
    pub device: PathBuf,
}

fn parse_seeding_value(s: &str) -> Result<bool, String> {
    match s {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("value must be 0 or 1".to_string()),
    }
}

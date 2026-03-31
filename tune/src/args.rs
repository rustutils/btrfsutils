//! # CLI argument definitions for `btrfs-tune`

use clap::{ArgGroup, Parser};
use std::path::PathBuf;
use uuid::Uuid;

const HEADING_FEATURES: &str = "Legacy feature flags";
const HEADING_UUID: &str = "UUID";
const HEADING_SEEDING: &str = "Seeding";

/// Modify btrfs filesystem parameters on an unmounted device by writing
/// directly to the on-disk superblock and tree structures.
///
/// Operations are grouped into feature flags, UUID changes, and seeding.
/// Only one group may be used per invocation. The legacy feature flags
/// (-r, -x, -n) can be combined with each other but not with other groups.
///
/// The legacy feature flags enable on-disk format features that were once
/// optional but are now enabled by default on all new filesystems created
/// by mkfs.btrfs. They exist for upgrading old filesystems in place.
#[derive(Parser, Debug)]
#[command(version, name = "btrfs-tune")]
#[command(group = ArgGroup::new("uuid_change")
    .args(["metadata_uuid", "set_metadata_uuid", "random_uuid", "set_uuid"])
    .conflicts_with_all(["extref", "skinny_metadata", "no_holes", "seeding"]))]
pub struct Arguments {
    /// Enable extended inode refs (extref)
    #[arg(short = 'r', help_heading = HEADING_FEATURES)]
    pub extref: bool,

    /// Enable skinny metadata extent refs
    #[arg(short = 'x', help_heading = HEADING_FEATURES)]
    pub skinny_metadata: bool,

    /// Enable no-holes feature
    #[arg(short = 'n', help_heading = HEADING_FEATURES)]
    pub no_holes: bool,

    /// Change fsid to a random UUID via the metadata_uuid mechanism
    #[arg(short = 'm', group = "uuid_change", help_heading = HEADING_UUID)]
    pub metadata_uuid: bool,

    /// Change fsid to the given UUID via the metadata_uuid mechanism
    #[arg(short = 'M', value_name = "UUID", group = "uuid_change",
        help_heading = HEADING_UUID)]
    pub set_metadata_uuid: Option<Uuid>,

    /// Rewrite fsid to a random UUID (rewrites all tree block headers)
    #[arg(short = 'u', group = "uuid_change", help_heading = HEADING_UUID)]
    pub random_uuid: bool,

    /// Rewrite fsid to the given UUID (rewrites all tree block headers)
    #[arg(short = 'U', value_name = "UUID", group = "uuid_change",
        help_heading = HEADING_UUID)]
    pub set_uuid: Option<Uuid>,

    /// Set (1) or clear (0) the seeding flag
    #[arg(short = 'S', value_parser = parse_seeding_value,
        conflicts_with_all = ["extref", "skinny_metadata", "no_holes"],
        help_heading = HEADING_SEEDING)]
    pub seeding: Option<bool>,

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

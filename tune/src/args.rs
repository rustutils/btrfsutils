//! # CLI argument definitions for `btrfs-tune`

use clap::{ArgGroup, Parser};
use std::path::PathBuf;
use uuid::Uuid;

const HEADING_FEATURES: &str = "Legacy feature flags";
const HEADING_UUID: &str = "UUID";
const HEADING_SEEDING: &str = "Seeding";
const HEADING_CONVERT: &str = "Format conversions";

/// Modify btrfs filesystem parameters and features on an unmounted device.
///
/// These commands work by by writing directly to the on-disk superblock
/// and tree structures.
///
/// Operations are grouped into feature flags, UUID changes, and seeding.
/// Only one group may be used per invocation. The legacy feature flags
/// (-r, -x, -n) can be combined with each other but not with other groups.
///
/// The legacy feature flags enable on-disk format features that were once
/// optional but are now enabled by default on all new filesystems created
/// by mkfs.btrfs. They exist for upgrading old filesystems in place.
#[derive(Parser, Debug)]
#[command(
    version,
    name = "btrfs-tune",
    arg_required_else_help = true,
    max_term_width = 100
)]
#[command(group = ArgGroup::new("uuid_change")
    .args(["metadata_uuid", "set_metadata_uuid", "random_uuid", "set_uuid"])
    .conflicts_with_all(["extref", "skinny_metadata", "no_holes", "seeding"]))]
#[command(group = ArgGroup::new("convert")
    .args(["convert_to_free_space_tree", "convert_to_block_group_tree"])
    .multiple(true)
    .conflicts_with_all([
        "extref", "skinny_metadata", "no_holes",
        "seeding", "metadata_uuid", "set_metadata_uuid",
        "random_uuid", "set_uuid",
    ]))]
#[allow(clippy::struct_excessive_bools, clippy::doc_markdown)]
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

    /// Convert the filesystem to use the free space tree (v2 cache).
    /// The filesystem must not already have the free-space-tree
    /// feature enabled, must not have a stale free space tree root,
    /// and must not have any v1 free-space-cache items present
    /// (clear them with `btrfs rescue clear-space-cache` first).
    #[arg(long, help_heading = HEADING_CONVERT)]
    pub convert_to_free_space_tree: bool,

    /// Convert the filesystem to use the block group tree. The
    /// filesystem must not already have the block-group-tree feature
    /// enabled and must already have the free-space-tree feature
    /// enabled (the kernel requires FST for BGT). When combined with
    /// --convert-to-free-space-tree, both conversions run in
    /// sequence.
    #[arg(long, help_heading = HEADING_CONVERT)]
    pub convert_to_block_group_tree: bool,

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

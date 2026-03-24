use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// List subvolume quota groups
#[derive(Parser, Debug)]
pub struct QgroupShowCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Print parent qgroup id
    #[clap(short = 'p')]
    pub print_parent: bool,

    /// Print child qgroup id
    #[clap(short = 'c')]
    pub print_child: bool,

    /// Print limit of referenced size
    #[clap(short = 'r')]
    pub print_rfer_limit: bool,

    /// Print limit of exclusive size
    #[clap(short = 'e')]
    pub print_excl_limit: bool,

    /// List all qgroups impacting path, including ancestral qgroups
    #[clap(short = 'F')]
    pub filter_all: bool,

    /// List all qgroups impacting path, excluding ancestral qgroups
    #[clap(short = 'f')]
    pub filter_direct: bool,

    /// Show raw numbers in bytes
    #[clap(long)]
    pub raw: bool,

    /// Show human friendly numbers, base 1024 (default)
    #[clap(long)]
    pub human_readable: bool,

    /// Use 1024 as a base (IEC units)
    #[clap(long)]
    pub iec: bool,

    /// Use 1000 as a base (SI units)
    #[clap(long)]
    pub si: bool,

    /// Show sizes in KiB
    #[clap(long)]
    pub kbytes: bool,

    /// Show sizes in MiB
    #[clap(long)]
    pub mbytes: bool,

    /// Show sizes in GiB
    #[clap(long)]
    pub gbytes: bool,

    /// Show sizes in TiB
    #[clap(long)]
    pub tbytes: bool,

    /// Sort by a comma-separated list of fields (qgroupid, rfer, excl, max_rfer, max_excl, path)
    #[clap(long)]
    pub sort: Option<String>,

    /// Force a sync before getting quota information
    #[clap(long)]
    pub sync: bool,
}

impl Runnable for QgroupShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement qgroup show")
    }
}

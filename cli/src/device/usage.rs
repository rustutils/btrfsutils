use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Show detailed information about internal allocations in devices
#[derive(Parser, Debug)]
pub struct DeviceUsageCommand {
    /// Path(s) to a mounted btrfs filesystem
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,

    /// Show raw numbers in bytes
    #[clap(short = 'b', long, overrides_with_all = ["human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub raw: bool,

    /// Show human-friendly numbers using base 1024 (default)
    #[clap(short = 'h', long, overrides_with_all = ["raw", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub human_readable: bool,

    /// Show human-friendly numbers using base 1000
    #[clap(short = 'H', overrides_with_all = ["raw", "human_readable", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub human_base1000: bool,

    /// Use 1024 as a base (KiB, MiB, GiB, TiB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "human_base1000", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub iec: bool,

    /// Use 1000 as a base (kB, MB, GB, TB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub si: bool,

    /// Show sizes in KiB, or kB with --si
    #[clap(short = 'k', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "mbytes", "gbytes", "tbytes"])]
    pub kbytes: bool,

    /// Show sizes in MiB, or MB with --si
    #[clap(short = 'm', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "gbytes", "tbytes"])]
    pub mbytes: bool,

    /// Show sizes in GiB, or GB with --si
    #[clap(short = 'g', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "tbytes"])]
    pub gbytes: bool,

    /// Show sizes in TiB, or TB with --si
    #[clap(short = 't', long, overrides_with_all = ["raw", "human_readable", "human_base1000", "iec", "si", "kbytes", "mbytes", "gbytes"])]
    pub tbytes: bool,
}

impl Runnable for DeviceUsageCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement device usage")
    }
}

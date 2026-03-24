use crate::{Format, Runnable};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Replace a device in the filesystem.
///
/// The source device can be specified either as a path (e.g. /dev/sdb) or as a
/// numeric device ID. The target device will be used to replace the source. The
/// filesystem must be mounted at mount_point.
#[derive(Parser, Debug)]
pub struct ReplaceStartCommand {
    /// Source device path or devid to replace
    pub source: String,

    /// Target device that will replace the source
    pub target: PathBuf,

    /// Mount point of the filesystem
    pub mount_point: PathBuf,

    /// Only read from srcdev if no other zero-defect mirror exists
    #[clap(short = 'r')]
    pub redundancy_only: bool,

    /// Force using and overwriting targetdev even if it contains a valid btrfs filesystem
    #[clap(short = 'f')]
    pub force: bool,

    /// Do not background the replace operation; wait for it to finish
    #[clap(short = 'B')]
    pub no_background: bool,

    /// Wait if there's another exclusive operation running, instead of returning an error
    #[clap(long)]
    pub enqueue: bool,

    /// Do not perform whole device TRIM on the target device
    #[clap(short = 'K', long)]
    pub nodiscard: bool,
}

impl Runnable for ReplaceStartCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement replace start")
    }
}

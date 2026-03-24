use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

use crate::{Format, Runnable, util::human_bytes};

/// Print the minimum size a device can be shrunk to.
///
/// Returns the minimum size in bytes that the specified device can be
/// resized to without losing data. The device id 1 is used by default.
/// Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct MinDevSizeCommand {
    /// Specify the device id to query
    #[arg(long = "id", default_value = "1")]
    devid: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for MinDevSizeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let size =
            btrfs_uapi::dev_extent::min_dev_size(file.as_fd(), self.devid).with_context(|| {
                format!(
                    "failed to determine min device size for devid {} on '{}'",
                    self.devid,
                    self.path.display()
                )
            })?;

        println!("{} bytes ({})", size, human_bytes(size));
        Ok(())
    }
}

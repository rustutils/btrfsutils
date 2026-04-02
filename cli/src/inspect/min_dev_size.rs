use crate::{
    RunContext, Runnable,
    util::{human_bytes, open_path},
};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Print the minimum size a device can be shrunk to.
///
/// Returns the minimum size in bytes that the specified device can be
/// resized to without losing data. The device id 1 is used by default.
/// Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct MinDevSizeCommand {
    /// Specify the device id to query
    #[arg(long = "id", default_value = "1")]
    devid: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for MinDevSizeCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        let size = btrfs_uapi::device::device_min_size(
            file.as_fd(),
            self.devid,
        )
        .with_context(|| {
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

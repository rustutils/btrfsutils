use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Receive subvolumes from a stream
#[derive(Parser, Debug)]
pub struct ReceiveCommand {
    /// Mount point of the destination filesystem (not required with --dump)
    mount: Option<PathBuf>,

    /// Read the stream from FILE instead of stdin
    #[clap(short = 'f')]
    file: Option<PathBuf>,

    /// Terminate after receiving an end-cmd marker
    #[clap(short = 'e')]
    terminate_on_end: bool,

    /// Confine the process to <mount> using chroot
    #[clap(short = 'C', long)]
    chroot: bool,

    /// Terminate after NERR errors (0 means unlimited)
    #[clap(short = 'E', long)]
    max_errors: Option<u64>,

    /// The root mount point of the destination filesystem
    #[clap(short = 'm', long = "root-mount")]
    root_mount: Option<PathBuf>,

    /// Always decompress instead of using encoded I/O
    #[clap(long)]
    force_decompress: bool,

    /// Dump stream metadata without requiring the mount parameter
    #[clap(long)]
    dump: bool,
}

impl Runnable for ReceiveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement receive")
    }
}

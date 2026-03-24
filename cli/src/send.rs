use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use crate::{Format, Runnable};

/// Send the subvolume(s) to stdout.
///
/// Generate a stream representation of one or more subvolumes that can be
/// transmitted over the network or stored for later restoration. Streams
/// are incremental and can be based on a parent subvolume to only send
/// changes. The stream output is in btrfs send format and can be received
/// with the receive command. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct SendCommand {
    /// Subvolume(s) to send
    #[clap(required = true)]
    subvolumes: Vec<PathBuf>,

    /// Omit end-cmd marker between subvolumes
    #[clap(short = 'e')]
    omit_end_cmd: bool,

    /// Send an incremental stream from <parent> to the subvolume
    #[clap(short = 'p', long)]
    parent: Option<PathBuf>,

    /// Use this snapshot as a clone source (may be given multiple times)
    #[clap(short = 'c', long = "clone-src")]
    clone_src: Vec<PathBuf>,

    /// Write output to a file instead of stdout
    #[clap(short = 'f', long)]
    outfile: Option<PathBuf>,

    /// Send in NO_FILE_DATA mode
    #[clap(long)]
    no_data: bool,

    /// Use send protocol version N
    #[clap(long)]
    proto: Option<u64>,

    /// Send compressed data directly without decompressing
    #[clap(long)]
    compressed_data: bool,
}

impl Runnable for SendCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        todo!("implement send")
    }
}

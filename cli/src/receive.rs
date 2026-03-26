mod dump;
mod ops;

use crate::{Format, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_disk::stream::{StreamCommand, StreamReader};
use clap::Parser;
use ops::ReceiveContext;
use std::{fs::File, io, path::PathBuf};

/// Receive subvolumes from a stream.
///
/// Read a btrfs send stream and recreate subvolumes on the destination filesystem.
/// Streams can be received incrementally based on a parent subvolume to only
/// apply changes. Multiple streams can be received in sequence. The destination
/// filesystem must be mounted and writable. Requires CAP_SYS_ADMIN.
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
        let input: Box<dyn io::Read> = match &self.file {
            Some(path) => Box::new(
                File::open(path).with_context(|| format!("cannot open '{}'", path.display()))?,
            ),
            None => Box::new(io::stdin()),
        };

        if self.dump {
            return dump::dump_stream(input);
        }

        let mount = self
            .mount
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("mount point is required (unless --dump)"))?;

        if !mount.is_dir() {
            bail!("'{}' is not a directory", mount.display());
        }

        // The input file must be opened before chroot (it may be outside
        // the mount point). The stream reader consumes the input.
        let mut reader = StreamReader::new(input)?;

        let dest = if self.chroot {
            // Confine the process to the mount point. After this, all paths
            // in the stream are resolved relative to "/".
            let mount_cstr = std::ffi::CString::new(
                mount
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("mount path is not valid UTF-8"))?,
            )
            .context("mount path contains null byte")?;

            if unsafe { nix::libc::chroot(mount_cstr.as_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context(format!("failed to chroot to '{}'", mount.display()));
            }
            if unsafe { nix::libc::chdir(c"/".as_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("failed to chdir to / after chroot");
            }
            eprintln!("Chroot to {}", mount.display());
            PathBuf::from("/")
        } else {
            mount.clone()
        };

        let mut ctx = ReceiveContext::new(&dest)?;
        let max_errors = self.max_errors.unwrap_or(0);
        let mut error_count = 0u64;
        let mut received_subvol = false;

        loop {
            match reader.next_command() {
                Err(e) => {
                    error_count += 1;
                    eprintln!("ERROR: {e:#}");
                    if max_errors > 0 && error_count >= max_errors {
                        bail!("too many errors ({error_count}), aborting");
                    }
                    continue;
                }
                Ok(None) => {
                    // EOF — finalize and exit.
                    break;
                }
                Ok(Some(StreamCommand::End)) => {
                    ctx.close_write_fd();
                    ctx.finish_subvol()?;
                    received_subvol = false;

                    if self.terminate_on_end {
                        return Ok(());
                    }

                    // Try to read the next stream header for multi-stream input.
                    // If there's more data, the next call to next_command() on a
                    // new reader will pick it up. We re-create the reader with the
                    // remaining input.
                    let inner = reader.into_inner();
                    match StreamReader::new(inner) {
                        Ok(new_reader) => {
                            reader = new_reader;
                        }
                        Err(_) => {
                            // No more streams.
                            return Ok(());
                        }
                    }
                    continue;
                }
                Ok(Some(cmd)) => {
                    if matches!(
                        &cmd,
                        StreamCommand::Subvol { .. } | StreamCommand::Snapshot { .. }
                    ) {
                        received_subvol = true;
                    }
                    if let Err(e) = ctx.process_command(&cmd) {
                        error_count += 1;
                        eprintln!("ERROR: {e:#}");
                        if max_errors > 0 && error_count >= max_errors {
                            bail!("too many errors ({error_count}), aborting");
                        }
                    }
                }
            }
        }

        // Finalize the last subvolume if we received one.
        if received_subvol {
            ctx.finish_subvol()?;
        }

        Ok(())
    }
}

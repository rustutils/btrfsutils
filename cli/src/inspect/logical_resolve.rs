use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Get file system paths for the given logical address
#[derive(Parser, Debug)]
pub struct LogicalResolveCommand {
    /// Logical address
    logical: u64,

    /// Path to a file or directory on the btrfs filesystem
    path: PathBuf,

    /// Skip the path resolving and print the inodes instead
    #[clap(short = 'P', long)]
    skip_paths: bool,

    /// Ignore offsets when matching references
    #[clap(short = 'o', long)]
    ignore_offset: bool,

    /// Set inode container's size
    #[clap(short = 's', long)]
    bufsize: Option<u64>,
}

impl Runnable for LogicalResolveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let results = btrfs_uapi::inode::logical_ino(
            fd,
            self.logical,
            self.ignore_offset,
            self.bufsize,
        )
        .context(
            "failed to look up logical address (is this a btrfs filesystem?)",
        )?;

        if results.is_empty() {
            eprintln!("no results found for logical address {}", self.logical);
        } else if self.skip_paths {
            // Just print inode, offset, root
            for result in results {
                println!(
                    "inode {} offset {} root {}",
                    result.inode, result.offset, result.root
                );
            }
        } else {
            // Resolve paths for each inode
            for result in results {
                match btrfs_uapi::inode::ino_paths(fd, result.inode) {
                    Ok(paths) => {
                        if paths.is_empty() {
                            println!(
                                "inode {} offset {} root {} <no path>",
                                result.inode, result.offset, result.root
                            );
                        } else {
                            for path in paths {
                                println!("{}", path);
                            }
                        }
                    }
                    Err(_) => {
                        println!(
                            "inode {} offset {} root {} <error resolving path>",
                            result.inode, result.offset, result.root
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

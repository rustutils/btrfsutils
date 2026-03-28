use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::defrag::{
    CompressSpec, CompressType, DefragRangeArgs, defrag_range,
};
use clap::Parser;
use std::{
    fs::{self, File},
    os::unix::io::AsFd,
    path::PathBuf,
};

const HEADING_COMPRESSION: &str = "Compression";
const HEADING_RANGE: &str = "Range";

/// Defragment files or directories on a btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemDefragCommand {
    /// Be verbose, print file names as they are defragmented
    #[clap(long, short)]
    pub verbose: bool,

    /// Defragment files in subdirectories recursively
    #[clap(long, short)]
    pub recursive: bool,

    /// Flush data to disk immediately after defragmentation
    #[clap(long, short)]
    pub flush: bool,

    /// Compress the file while defragmenting (optionally specify type: zlib, lzo, zstd)
    #[clap(long, short, conflicts_with = "nocomp", help_heading = HEADING_COMPRESSION)]
    pub compress: Option<Option<CompressType>>,

    /// Compression level (used together with --compress)
    #[clap(long = "level", short = 'L', requires = "compress", help_heading = HEADING_COMPRESSION)]
    pub compress_level: Option<i8>,

    /// Disable compression during defragmentation
    #[clap(long, conflicts_with = "compress", help_heading = HEADING_COMPRESSION)]
    pub nocomp: bool,

    /// Defragment only bytes starting at this offset
    #[clap(long, short, help_heading = HEADING_RANGE)]
    pub start: Option<u64>,

    /// Defragment only this many bytes
    #[clap(long, help_heading = HEADING_RANGE)]
    pub len: Option<u64>,

    /// Target extent size threshold in bytes; extents larger than this are
    /// considered already defragmented
    #[clap(long, short, help_heading = HEADING_RANGE)]
    pub target: Option<u64>,

    /// Process the file in steps of this size rather than all at once
    #[clap(long, help_heading = HEADING_RANGE)]
    pub step: Option<u64>,

    /// One or more files or directories to defragment
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for FilesystemDefragCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let compress = self.compress.as_ref().map(|ct| CompressSpec {
            compress_type: ct.unwrap_or(CompressType::Zlib),
            level: self.compress_level,
        });

        let mut args = DefragRangeArgs::new();
        if let Some(start) = self.start {
            args = args.start(start);
        }
        if let Some(len) = self.len {
            args = args.len(len);
        }
        if let Some(thresh) = self.target {
            args = args.extent_thresh(thresh as u32);
        }
        if self.flush {
            args = args.flush();
        }
        if self.nocomp {
            args = args.nocomp();
        } else if let Some(spec) = compress {
            args = args.compress(spec);
        }

        let mut errors = 0u64;

        for path in &self.paths {
            let meta = fs::symlink_metadata(path).with_context(|| {
                format!("cannot access '{}'", path.display())
            })?;

            if self.recursive && meta.is_dir() {
                errors += self.defrag_recursive(path, &args)?;
            } else {
                if let Err(e) = self.defrag_one(path, &args) {
                    eprintln!("error: {e:#}");
                    errors += 1;
                }
            }
        }

        if errors > 0 {
            anyhow::bail!("{errors} error(s) during defragmentation");
        }

        Ok(())
    }
}

impl FilesystemDefragCommand {
    /// Defragment a single file.
    fn defrag_one(
        &self,
        path: &std::path::Path,
        args: &DefragRangeArgs,
    ) -> Result<()> {
        if self.verbose {
            println!("{}", path.display());
        }
        let file = File::open(path)
            .with_context(|| format!("failed to open '{}'", path.display()))?;

        if let Some(step) = self.step {
            self.defrag_in_steps(&file, path, args, step)?;
        } else {
            defrag_range(file.as_fd(), args).with_context(|| {
                format!("defrag failed on '{}'", path.display())
            })?;
        }
        Ok(())
    }

    /// Walk a directory tree and defragment every regular file.
    ///
    /// Does not follow symlinks and does not cross filesystem boundaries,
    /// matching the C reference's `nftw(path, cb, 10, FTW_MOUNT | FTW_PHYS)`.
    fn defrag_recursive(
        &self,
        dir: &std::path::Path,
        args: &DefragRangeArgs,
    ) -> Result<u64> {
        use std::os::unix::fs::MetadataExt;

        let dir_dev = fs::metadata(dir)
            .with_context(|| format!("cannot stat '{}'", dir.display()))?
            .dev();

        let mut errors = 0u64;
        let mut stack = vec![dir.to_path_buf()];

        while let Some(current) = stack.pop() {
            let entries = match fs::read_dir(&current) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!(
                        "error: cannot read '{}': {e}",
                        current.display()
                    );
                    errors += 1;
                    continue;
                }
            };

            for entry in entries {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("error: directory entry read failed: {e}");
                        errors += 1;
                        continue;
                    }
                };

                let path = entry.path();

                // Use symlink_metadata to avoid following symlinks (FTW_PHYS).
                let meta = match fs::symlink_metadata(&path) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!(
                            "error: cannot stat '{}': {e}",
                            path.display()
                        );
                        errors += 1;
                        continue;
                    }
                };

                if meta.is_dir() {
                    // Don't cross filesystem boundaries (FTW_MOUNT).
                    if meta.dev() == dir_dev {
                        stack.push(path);
                    }
                } else if meta.is_file()
                    && let Err(e) = self.defrag_one(&path, args)
                {
                    eprintln!("error: {e:#}");
                    errors += 1;
                }
                // Skip symlinks, sockets, fifos, etc.
            }
        }

        Ok(errors)
    }

    /// Process a file in fixed-size steps, flushing between each step.
    ///
    /// Matches `defrag_range_in_steps` from the C reference.
    fn defrag_in_steps(
        &self,
        file: &File,
        path: &std::path::Path,
        args: &DefragRangeArgs,
        step: u64,
    ) -> Result<()> {
        use std::os::unix::fs::MetadataExt;

        let file_size = file.metadata()?.size();
        let mut offset = args.start;
        let end = if args.len == u64::MAX {
            u64::MAX
        } else {
            args.start.saturating_add(args.len)
        };

        while offset < end {
            // Re-check file size each iteration in case it changed.
            let current_size = file.metadata()?.size();
            if offset >= current_size {
                break;
            }

            let remaining = end.saturating_sub(offset).min(step);
            let mut step_args = args.clone();
            step_args.start = offset;
            step_args.len = remaining;
            // Always flush between steps.
            step_args.flush = true;

            defrag_range(file.as_fd(), &step_args).with_context(|| {
                format!(
                    "defrag failed on '{}' at offset {offset}",
                    path.display()
                )
            })?;

            offset = match offset.checked_add(step) {
                Some(next) => next,
                None => break, // overflow means we've covered the whole file
            };
        }

        // If the file grew since we started, the original file_size might be
        // less than the current size, but we only defrag through `end`.
        let _ = file_size;

        Ok(())
    }
}

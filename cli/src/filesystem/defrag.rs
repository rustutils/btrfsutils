use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::defrag::{CompressSpec, CompressType, DefragRangeArgs, defrag_range};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

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
    #[clap(long, short, conflicts_with = "nocomp")]
    pub compress: Option<Option<CompressType>>,

    /// Compression level (used together with --compress)
    #[clap(long = "level", short = 'L', requires = "compress")]
    pub compress_level: Option<i8>,

    /// Disable compression during defragmentation
    #[clap(long, conflicts_with = "compress")]
    pub nocomp: bool,

    /// Defragment only bytes starting at this offset
    #[clap(long, short)]
    pub start: Option<u64>,

    /// Defragment only this many bytes
    #[clap(long)]
    pub len: Option<u64>,

    /// Target extent size threshold in bytes; extents larger than this are
    /// considered already defragmented
    #[clap(long, short)]
    pub target: Option<u64>,

    /// Process the file in steps of this size rather than all at once
    #[clap(long)]
    pub step: Option<u64>,

    /// One or more files or directories to defragment
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for FilesystemDefragCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.recursive {
            anyhow::bail!("--recursive is not yet implemented");
        }

        if self.step.is_some() {
            anyhow::bail!("--step is not yet implemented");
        }

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

        for path in &self.paths {
            if self.verbose {
                println!("{}", path.display());
            }
            let file =
                File::open(path).with_context(|| format!("failed to open '{}'", path.display()))?;
            defrag_range(file.as_fd(), &args)
                .with_context(|| format!("defrag failed on '{}'", path.display()))?;
        }

        Ok(())
    }
}

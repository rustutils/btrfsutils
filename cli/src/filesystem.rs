use super::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::label::{label_get, label_set};
use btrfs_uapi::space::space_info;
use btrfs_uapi::sync::sync;
use clap::{Args, Parser};
use std::ffi::CString;
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsFd;
use std::path::PathBuf;

/// Overall filesystem tasks and information
#[derive(Parser, Debug)]
pub struct FilesystemCommand {
    #[clap(subcommand)]
    pub subcommand: FilesystemSubcommand,
}

impl Runnable for FilesystemCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            FilesystemSubcommand::Df(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Du(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Show(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Sync(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Defragment(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Resize(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Label(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Usage(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::Mkswapfile(cmd) => cmd.run(format, dry_run),
            FilesystemSubcommand::CommitStats(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum FilesystemSubcommand {
    Df(FilesystemDfCommand),
    Du(FilesystemDuCommand),
    Show(FilesystemShowCommand),
    Sync(FilesystemSyncCommand),
    #[clap(alias = "defrag")]
    Defragment(FilesystemDefragCommand),
    Resize(FilesystemResizeCommand),
    Label(FilesystemLabelCommand),
    Usage(FilesystemUsageCommand),
    Mkswapfile(FilesystemMkswapfileCommand),
    CommitStats(FilesystemCommitStatsCommand),
}

/// Unit display mode flags, shared by subcommands that output sizes.
#[derive(Args, Debug)]
pub struct UnitMode {
    /// Show raw numbers in bytes
    #[clap(long, overrides_with_all = ["human_readable", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub raw: bool,

    /// Show human-friendly numbers using base 1024 (default)
    #[clap(long, short = 'h', overrides_with_all = ["raw", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub human_readable: bool,

    /// Use 1024 as a base (KiB, MiB, GiB, TiB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub iec: bool,

    /// Use 1000 as a base (kB, MB, GB, TB)
    #[clap(long, overrides_with_all = ["raw", "human_readable", "iec", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub si: bool,

    /// Show sizes in KiB, or kB with --si
    #[clap(long, overrides_with_all = ["raw", "human_readable", "iec", "si", "mbytes", "gbytes", "tbytes"])]
    pub kbytes: bool,

    /// Show sizes in MiB, or MB with --si
    #[clap(long, overrides_with_all = ["raw", "human_readable", "iec", "si", "kbytes", "gbytes", "tbytes"])]
    pub mbytes: bool,

    /// Show sizes in GiB, or GB with --si
    #[clap(long, overrides_with_all = ["raw", "human_readable", "iec", "si", "kbytes", "mbytes", "tbytes"])]
    pub gbytes: bool,

    /// Show sizes in TiB, or TB with --si
    #[clap(long, overrides_with_all = ["raw", "human_readable", "iec", "si", "kbytes", "mbytes", "gbytes"])]
    pub tbytes: bool,
}

/// Show space usage information for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemDfCommand {
    pub path: PathBuf,
}

/// Summarize disk usage of each file
#[derive(Parser, Debug)]
pub struct FilesystemDuCommand {
    /// Display only a total for each argument
    #[clap(long, short)]
    pub summarize: bool,

    #[clap(flatten)]
    pub units: UnitMode,

    /// One or more paths to summarize
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

/// Show information about one or more mounted or unmounted filesystems
#[derive(Parser, Debug)]
pub struct FilesystemShowCommand {
    /// Search all devices, including unmounted ones
    #[clap(long, short = 'd')]
    pub all_devices: bool,

    /// Search only mounted filesystems
    #[clap(long, short)]
    pub mounted: bool,

    /// Path, UUID, device or label to show (shows all if omitted)
    pub filter: Option<String>,
}

/// Force a sync on a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemSyncCommand {
    pub path: PathBuf,
}

/// Defragment files or directories on a btrfs filesystem
///
/// Defragments the given files or directories. Optionally compresses the
/// data during defragmentation and can operate recursively on directories.
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
    #[clap(long, short)]
    pub compress: Option<Option<String>>,

    /// Compression level (used together with --compress)
    #[clap(long = "level", short = 'L')]
    pub compress_level: Option<i8>,

    /// Disable compression during defragmentation
    #[clap(long)]
    pub nocomp: bool,

    /// Defragment only bytes starting at this offset
    #[clap(long, short)]
    pub start: Option<u64>,

    /// Defragment only this many bytes
    #[clap(long, short)]
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

/// Resize a mounted btrfs filesystem
///
/// The size argument can be a number with an optional suffix (K, M, G, T),
/// "max" to grow to the device size, or "cancel" to cancel a running resize.
/// Optionally prefix with a device ID as "devid:size".
#[derive(Parser, Debug)]
pub struct FilesystemResizeCommand {
    /// Wait if there is another exclusive operation running, otherwise error
    #[clap(long)]
    pub enqueue: bool,

    /// Resize a filesystem stored in a file image (unmounted)
    #[clap(long)]
    pub offline: bool,

    /// New size for the filesystem, e.g. "1G", "+512M", "-1G", "max", "cancel",
    /// or "devid:<id>:<size>" to target a specific device
    pub size: String,

    pub path: PathBuf,
}

/// Get or set the label of a btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemLabelCommand {
    /// The device or mount point to operate on
    pub path: PathBuf,

    /// The new label to set (if omitted, the current label is printed)
    pub new_label: Option<std::ffi::OsString>,
}

/// Create a swapfile on a btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemMkswapfileCommand {
    /// Size of the swapfile (default: 2GiB)
    #[clap(long, short)]
    pub size: Option<String>,

    /// UUID to embed in the swap header (clear, random, time, or explicit UUID;
    /// default: random)
    #[clap(long = "uuid", short = 'U')]
    pub uuid: Option<String>,

    /// Path to the swapfile to create
    pub path: PathBuf,
}

/// Show commit statistics for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemCommitStatsCommand {
    /// Print stats then reset the max_commit_ms counter (requires root)
    #[clap(long, short = 'z')]
    pub reset: bool,

    pub path: PathBuf,
}

/// Show detailed information about internal filesystem usage
#[derive(Parser, Debug)]
pub struct FilesystemUsageCommand {
    #[clap(flatten)]
    pub units: UnitMode,

    /// Use base 1000 for human-readable sizes
    #[clap(short = 'H')]
    pub human_si: bool,

    /// Show data in tabular format
    #[clap(short = 'T')]
    pub tabular: bool,

    /// One or more mount points to show usage for
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for FilesystemDuCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

impl Runnable for FilesystemUsageCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

/// Format a byte count as a human-readable string using binary prefixes.
fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.2}{}", UNITS[unit])
    }
}

impl Runnable for FilesystemDfCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let entries = space_info(file.as_fd())
            .with_context(|| format!("failed to get space info for '{}'", self.path.display()))?;

        for entry in &entries {
            println!(
                "{}, {}: total={}, used={}",
                entry.flags.type_name(),
                entry.flags.profile_name(),
                human_bytes(entry.total_bytes),
                human_bytes(entry.used_bytes),
            );
        }

        Ok(())
    }
}

impl Runnable for FilesystemShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

impl Runnable for FilesystemSyncCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        sync(file.as_fd()).with_context(|| format!("failed to sync '{}'", self.path.display()))?;
        Ok(())
    }
}

impl Runnable for FilesystemDefragCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

impl Runnable for FilesystemResizeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

impl Runnable for FilesystemLabelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        match &self.new_label {
            None => {
                let label = label_get(file.as_fd()).with_context(|| {
                    format!("failed to get label for '{}'", self.path.display())
                })?;
                // Escape non-printable and non-ASCII bytes so the output is
                // always safe to display in a terminal.
                println!("{}", label.to_bytes().escape_ascii());
            }
            Some(new_label) => {
                let cstring = CString::new(new_label.as_bytes())
                    .context("label must not contain null bytes")?;
                label_set(file.as_fd(), &cstring).with_context(|| {
                    format!("failed to set label for '{}'", self.path.display())
                })?;
            }
        }
        Ok(())
    }
}

impl Runnable for FilesystemMkswapfileCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

impl Runnable for FilesystemCommitStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

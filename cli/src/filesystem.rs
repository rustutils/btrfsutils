use super::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::{
    defrag::{CompressSpec, CompressType, DefragRangeArgs, defrag_range},
    label::{label_get, label_set},
    resize::{ResizeAmount, ResizeArgs, resize},
    space::space_info,
    sync::sync,
};
use clap::{Args, Parser};
use nix::{
    fcntl::{FallocateFlags, fallocate},
    libc,
};
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    os::unix::{
        ffi::OsStrExt,
        fs::OpenOptionsExt,
        io::{AsFd, AsRawFd},
    },
    path::PathBuf,
    str::FromStr,
};
use uuid::Uuid;

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
    #[clap(long, short, conflicts_with = "nocomp")]
    pub compress: Option<Option<String>>,

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
    /// Size of the swapfile
    #[clap(long, short, default_value = "2G")]
    pub size: String,

    /// UUID to embed in the swap header (clear, random, time, or explicit UUID;
    /// default: random)
    #[clap(long = "uuid", short = 'U')]
    pub uuid: Option<ParsedUuid>,

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
        if self.recursive {
            // TODO: implement recursive directory walking
            anyhow::bail!("--recursive is not yet implemented");
        }

        if self.step.is_some() {
            // TODO: implement chunked defrag
            anyhow::bail!("--step is not yet implemented");
        }

        // Build the compress spec from --compress and --level.
        let compress = match &self.compress {
            None => None,
            Some(type_str) => {
                let compress_type = match type_str.as_deref() {
                    // -c with no argument defaults to zlib, matching the C tool.
                    None | Some("") => CompressType::Zlib,
                    Some(s) => CompressType::from_str(s)
                        .with_context(|| format!("unknown compress type: {s}"))?,
                };
                Some(CompressSpec {
                    compress_type,
                    level: self.compress_level,
                })
            }
        };

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

fn parse_size_with_suffix(s: &str) -> Result<u64> {
    let (num_str, suffix) = match s.find(|c: char| c.is_alphabetic()) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let n: u64 = num_str
        .parse()
        .with_context(|| format!("invalid size number: '{num_str}'"))?;
    let multiplier: u64 = match suffix.to_uppercase().as_str() {
        "" => 1,
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        "T" => 1024u64.pow(4),
        "P" => 1024u64.pow(5),
        "E" => 1024u64.pow(6),
        _ => anyhow::bail!("unknown size suffix: '{suffix}'"),
    };
    n.checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("size overflow: '{s}'"))
}

fn parse_resize_amount(s: &str) -> Result<ResizeAmount> {
    if s == "cancel" {
        return Ok(ResizeAmount::Cancel);
    }
    if s == "max" {
        return Ok(ResizeAmount::Max);
    }
    let (modifier, rest) = if let Some(r) = s.strip_prefix('+') {
        (1i32, r)
    } else if let Some(r) = s.strip_prefix('-') {
        (-1i32, r)
    } else {
        (0i32, s)
    };
    let bytes = parse_size_with_suffix(rest)?;
    Ok(match modifier {
        1 => ResizeAmount::Add(bytes),
        -1 => ResizeAmount::Sub(bytes),
        _ => ResizeAmount::Set(bytes),
    })
}

fn parse_resize_args(s: &str) -> Result<ResizeArgs> {
    // A leading "<number>:" means a devid was specified.
    if let Some(colon) = s.find(':') {
        if let Ok(devid) = s[..colon].parse::<u64>() {
            let amount = parse_resize_amount(&s[colon + 1..])?;
            return Ok(ResizeArgs::new(amount).with_devid(devid));
        }
    }
    Ok(ResizeArgs::new(parse_resize_amount(s)?))
}

impl Runnable for FilesystemResizeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.offline {
            // TODO: offline resize requires opening the image as a btrfs
            // filesystem directly without a mount point.
            anyhow::bail!("--offline is not yet implemented");
        }

        if self.enqueue {
            // TODO: check for a running exclusive operation and wait.
            anyhow::bail!("--enqueue is not yet implemented");
        }

        let args = parse_resize_args(&self.size)
            .with_context(|| format!("invalid resize argument: '{}'", self.size))?;

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        resize(file.as_fd(), args)
            .with_context(|| format!("resize failed on '{}'", self.path.display()))?;

        Ok(())
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

const FS_NOCOW_FL: libc::c_long = 0x00800000;
const MIN_SWAP_SIZE: u64 = 40 * 1024;

fn system_page_size() -> Result<u64> {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    anyhow::ensure!(size > 0, "failed to get system page size");
    Ok(size as u64)
}

/// A UUID value parsed from a CLI argument.
///
/// Accepts `clear` (nil UUID), `random` (random v4 UUID), or any standard
/// UUID string (with or without hyphens).
#[derive(Debug, Clone, Copy)]
pub struct ParsedUuid(Uuid);

impl std::ops::Deref for ParsedUuid {
    type Target = Uuid;
    fn deref(&self) -> &Uuid {
        &self.0
    }
}

impl FromStr for ParsedUuid {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "clear" => Ok(Self(Uuid::nil())),
            "random" => Ok(Self(Uuid::new_v4())),
            "time" => Ok(Self(Uuid::now_v7())),
            _ => Uuid::parse_str(s)
                .map(Self)
                .map_err(|e| format!("invalid UUID: {e}")),
        }
    }
}

fn write_swap_header(file: &File, page_count: u32, uuid: &Uuid, page_size: u64) -> Result<()> {
    let mut header = vec![0u8; page_size as usize];
    header[0x400] = 0x01;
    header[0x404..0x408].copy_from_slice(&page_count.to_le_bytes());
    header[0x40c..0x41c].copy_from_slice(uuid.as_bytes());

    // The SWAPSPACE2 signature occupies the last 10 bytes of the first page.
    let sig_offset = page_size as usize - 10;
    header[sig_offset..].copy_from_slice(b"SWAPSPACE2");
    use std::os::unix::fs::FileExt;
    file.write_at(&header, 0)
        .context("failed to write swap header")?;
    Ok(())
}

impl Runnable for FilesystemMkswapfileCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let size = parse_size_with_suffix(&self.size)
            .with_context(|| format!("invalid size: '{}'", self.size))?;

        let page_size = system_page_size()?;

        anyhow::ensure!(
            size >= MIN_SWAP_SIZE,
            "swapfile needs to be at least 40 KiB, got {} bytes",
            size
        );

        let uuid = self.uuid.as_deref().copied().unwrap_or_else(Uuid::new_v4);

        let size = size - (size % page_size);
        let total_pages = size / page_size;

        anyhow::ensure!(total_pages > 10, "swapfile too small after page alignment");

        // The first page holds the header; the kernel counts the rest.
        let page_count = total_pages - 1;
        anyhow::ensure!(
            page_count <= u32::MAX as u64,
            "swapfile too large: page count exceeds u32"
        );

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.path)
            .with_context(|| format!("failed to create '{}'", self.path.display()))?;

        // Set the NOCOW attribute before allocating space.
        let ret = unsafe { libc::ioctl(file.as_raw_fd(), libc::FS_IOC_SETFLAGS, &FS_NOCOW_FL) };
        nix::errno::Errno::result(ret).context("failed to set NOCOW attribute")?;

        fallocate(&file, FallocateFlags::empty(), 0, size as libc::off_t)
            .context("failed to allocate space for swapfile")?;

        write_swap_header(&file, page_count as u32, &uuid, page_size)?;

        println!(
            "created swapfile '{}' size {} bytes",
            self.path.display(),
            human_bytes(size),
        );

        Ok(())
    }
}

impl Runnable for FilesystemCommitStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        anyhow::bail!("unimplemented")
    }
}

use crate::{CommandGroup, Runnable, util::SizeFormat};
use clap::{Args, Parser};

mod commit_stats;
mod defrag;
mod df;
mod du;
mod label;
mod mkswapfile;
mod resize;
mod show;
mod sync;
mod usage;

pub use self::{
    commit_stats::*, defrag::*, df::*, du::*, label::*, mkswapfile::*,
    resize::*, show::*, sync::*, usage::*,
};

/// Overall filesystem tasks and information.
///
/// Perform filesystem-level operations including checking available space,
/// disk usage analysis, defragmentation, resizing, labeling, and
/// synchronization. These commands provide views into filesystem state and
/// allow configuration of filesystem-wide settings.
#[derive(Parser, Debug)]
pub struct FilesystemCommand {
    #[clap(subcommand)]
    pub subcommand: FilesystemSubcommand,
}

impl CommandGroup for FilesystemCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            FilesystemSubcommand::Df(cmd) => cmd,
            FilesystemSubcommand::Du(cmd) => cmd,
            FilesystemSubcommand::Show(cmd) => cmd,
            FilesystemSubcommand::Sync(cmd) => cmd,
            FilesystemSubcommand::Defragment(cmd) => cmd,
            FilesystemSubcommand::Resize(cmd) => cmd,
            FilesystemSubcommand::Label(cmd) => cmd,
            FilesystemSubcommand::Usage(cmd) => cmd,
            FilesystemSubcommand::Mkswapfile(cmd) => cmd,
            FilesystemSubcommand::CommitStats(cmd) => cmd,
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
///
/// Control how sizes are displayed in output. By default, human-readable
/// format with base 1024 (KiB, MiB, GiB, TiB) is used. You can specify
/// exact units or enable base 1000 (kB, MB, GB, TB) with --si.
#[derive(Args, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct UnitMode {
    /// Show raw numbers in bytes
    #[clap(long, overrides_with_all = ["human_readable", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
    pub raw: bool,

    /// Show human-friendly numbers using base 1024 (default)
    #[clap(long, overrides_with_all = ["raw", "iec", "si", "kbytes", "mbytes", "gbytes", "tbytes"])]
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

impl UnitMode {
    /// Resolve the clap flags into a `SizeFormat`.
    #[must_use]
    pub fn resolve(&self) -> SizeFormat {
        let si = self.si;
        if self.raw {
            SizeFormat::Raw
        } else if self.kbytes {
            SizeFormat::Fixed(if si { 1000 } else { 1024 })
        } else if self.mbytes {
            SizeFormat::Fixed(if si { 1_000_000 } else { 1024 * 1024 })
        } else if self.gbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000
            } else {
                1024 * 1024 * 1024
            })
        } else if self.tbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000_000
            } else {
                1024u64.pow(4)
            })
        } else if si {
            SizeFormat::HumanSi
        } else {
            SizeFormat::HumanIec
        }
    }
}

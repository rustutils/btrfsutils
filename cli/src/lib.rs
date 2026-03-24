use anyhow::Result;
use clap::{Parser, ValueEnum};

pub mod balance;
pub mod check;
pub mod device;
pub mod filesystem;
pub mod inspect;
pub mod property;
pub mod qgroup;
pub mod quota;
pub mod receive;
pub mod replace;
pub mod rescue;
pub mod restore;
pub mod scrub;
pub mod send;
pub mod subvolume;
pub mod util;

use crate::{
    balance::BalanceCommand, check::CheckCommand, device::DeviceCommand,
    filesystem::FilesystemCommand, inspect::InspectCommand, property::PropertyCommand,
    qgroup::QgroupCommand, quota::QuotaCommand, receive::ReceiveCommand, replace::ReplaceCommand,
    rescue::RescueCommand, restore::RestoreCommand, scrub::ScrubCommand, send::SendCommand,
    subvolume::SubvolumeCommand,
};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    #[default]
    Text,
    Json,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Level {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

#[derive(Parser, Debug)]
#[clap(version, infer_subcommands = true)]
pub struct Arguments {
    #[clap(flatten)]
    pub global: GlobalOptions,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Parser, Debug)]
pub struct GlobalOptions {
    /// Increase verbosity of the subcommand
    #[clap(global = true, short, long)]
    pub verbose: bool,

    /// Print only errors
    #[clap(global = true, short, long)]
    pub quiet: bool,

    /// If supported, do not do any active/changing actions
    #[clap(global = true, long)]
    pub dry_run: bool,

    /// Set log level
    #[clap(global = true, short, long)]
    pub log: Option<Level>,

    /// If supported, print subcommand output in that format
    #[clap(global = true, long)]
    pub format: Option<Format>,
}

pub trait Runnable {
    fn run(&self, format: Format, dry_run: bool) -> Result<()>;
}

#[derive(Parser, Debug)]
pub enum Command {
    /// Balance data across devices, or change block groups using filters
    Balance(BalanceCommand),
    /// Check structural integrity of a filesystem (unmounted)
    Check(CheckCommand),
    /// Manage and query devices in the filesystem
    Device(DeviceCommand),
    /// Overall filesystem tasks and information
    Filesystem(FilesystemCommand),
    /// Query various internal information
    #[command(alias = "inspect-internal")]
    Inspect(InspectCommand),
    /// Modify properties of filesystem objects
    Property(PropertyCommand),
    /// Manage quota groups
    Qgroup(QgroupCommand),
    /// Manage filesystem quota settings
    Quota(QuotaCommand),
    /// Receive subvolumes from a stream
    Receive(ReceiveCommand),
    /// Replace a device in the filesystem
    Replace(ReplaceCommand),
    /// Toolbox for specific rescue operations
    Rescue(RescueCommand),
    /// Try to restore files from a damaged filesystem (unmounted)
    Restore(RestoreCommand),
    /// Verify checksums of data and metadata
    Scrub(ScrubCommand),
    /// Send the subvolume(s) to stdout
    Send(SendCommand),
    /// Manage subvolumes: create, delete, list, etc
    Subvolume(SubvolumeCommand),
}

impl Runnable for Command {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match self {
            Command::Balance(cmd) => cmd.run(format, dry_run),
            Command::Check(cmd) => cmd.run(format, dry_run),
            Command::Device(cmd) => cmd.run(format, dry_run),
            Command::Filesystem(cmd) => cmd.run(format, dry_run),
            Command::Inspect(cmd) => cmd.run(format, dry_run),
            Command::Property(cmd) => cmd.run(format, dry_run),
            Command::Qgroup(cmd) => cmd.run(format, dry_run),
            Command::Quota(cmd) => cmd.run(format, dry_run),
            Command::Receive(cmd) => cmd.run(format, dry_run),
            Command::Replace(cmd) => cmd.run(format, dry_run),
            Command::Rescue(cmd) => cmd.run(format, dry_run),
            Command::Restore(cmd) => cmd.run(format, dry_run),
            Command::Scrub(cmd) => cmd.run(format, dry_run),
            Command::Send(cmd) => cmd.run(format, dry_run),
            Command::Subvolume(cmd) => cmd.run(format, dry_run),
        }
    }
}

impl Arguments {
    pub fn run(&self) -> Result<()> {
        env_logger::init();
        self.command
            .run(self.global.format.unwrap_or_default(), self.global.dry_run)
    }
}

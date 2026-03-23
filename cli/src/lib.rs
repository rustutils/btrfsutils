use anyhow::Result;
use clap::{Parser, ValueEnum};

pub mod balance;
pub mod filesystem;
pub mod util;

use crate::{balance::BalanceCommand, filesystem::FilesystemCommand};

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
#[clap(version)]
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
    /// Manage and query devices in the filesystem
    Device,
    /// Overall filesystem tasks and information
    Filesystem(FilesystemCommand),
    /// Query various internal information
    InspectInternal,
    /// Modify properties of filesystem objects
    Property,
    /// Manage quota groups
    Qgroup,
    /// Manage filesystem quota settings
    Quota,
    /// Replace a device in the filesystem
    Replace,
    /// Toolbox for specific rescue operations
    Rescue,
    /// Verify checksums of data and metadata
    Scrub,
    /// Manage subvolumes: create, delete, list, etc
    Subvolume,

    Check,
    Receive,
    Restore,
    Send,
}

impl Runnable for Command {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match self {
            Command::Balance(cmd) => cmd.run(format, dry_run),
            Command::Device => todo!(),
            Command::Filesystem(cmd) => cmd.run(format, dry_run),
            Command::InspectInternal => todo!(),
            Command::Property => todo!(),
            Command::Qgroup => todo!(),
            Command::Quota => todo!(),
            Command::Replace => todo!(),
            Command::Rescue => todo!(),
            Command::Scrub => todo!(),
            Command::Subvolume => todo!(),
            Command::Check => todo!(),
            Command::Receive => todo!(),
            Command::Restore => todo!(),
            Command::Send => todo!(),
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

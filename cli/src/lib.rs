//! # btrfs-cli: the btrfs command-line tool
//!
//! This crate provides the `btrfs` command-line binary, an alternative
//! implementation of btrfs-progs written in Rust. It is built on top of `btrfs-uapi` for kernel communication,
//! `btrfs-disk` for direct on-disk structure parsing, and `btrfs-stream` for
//! send/receive stream processing.
//!
//! Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to
//! see what is available. Most commands require root privileges or
//! `CAP_SYS_ADMIN`.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::struct_excessive_bools)]

use anyhow::Result;
use clap::{ArgAction, Parser, ValueEnum};

mod balance;
mod check;
mod device;
mod filesystem;
mod inspect;
mod property;
mod qgroup;
mod quota;
mod receive;
mod replace;
mod rescue;
mod restore;
mod scrub;
mod send;
mod subvolume;
mod util;

pub use crate::{
    balance::*, check::*, device::*, filesystem::*, inspect::*, property::*,
    qgroup::*, quota::*, receive::*, replace::*, rescue::*, restore::*,
    scrub::*, send::*, subvolume::*,
};

/// Output format for commands that support structured output.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    #[default]
    Text,
    Json,
}

/// Log verbosity level, ordered from most to least verbose.
#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum,
)]
pub enum Level {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// User-space command-line tool for managing Btrfs filesystems.
///
/// btrfs is a modern copy-on-write filesystem for Linux that provides advanced features
/// including subvolumes, snapshots, RAID support, compression, quotas, and checksumming.
/// This tool allows you to create and manage filesystems, devices, subvolumes, snapshots,
/// quotas, and perform various maintenance operations.
///
/// Most operations require CAP_SYS_ADMIN (root privileges) or special permissions for
/// the specific filesystem.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
#[clap(version, infer_subcommands = true)]
pub struct Arguments {
    #[clap(flatten)]
    pub global: GlobalOptions,

    #[clap(subcommand)]
    pub command: Command,
}

const GLOBAL_OPTIONS: &str = "Global options";

/// Flags shared across all subcommands (verbosity, dry-run, output format).
#[derive(Parser, Debug)]
pub struct GlobalOptions {
    /// Increase verbosity (repeat for more: -v, -vv, -vvv)
    #[clap(global = true, short, long, action = ArgAction::Count, help_heading = GLOBAL_OPTIONS)]
    pub verbose: u8,

    /// Print only errors
    #[clap(global = true, short, long, help_heading = GLOBAL_OPTIONS)]
    pub quiet: bool,

    /// If supported, do not do any active/changing actions
    #[clap(global = true, long, help_heading = GLOBAL_OPTIONS)]
    pub dry_run: bool,

    /// Set log level
    #[clap(global = true, long, help_heading = GLOBAL_OPTIONS)]
    pub log: Option<Level>,

    /// If supported, print subcommand output in that format
    #[clap(global = true, long, help_heading = GLOBAL_OPTIONS)]
    pub format: Option<Format>,
}

/// A CLI subcommand that can be executed.
pub trait Runnable {
    /// Execute this command.
    ///
    /// # Errors
    ///
    /// Returns an error if the command fails.
    fn run(&self, format: Format, dry_run: bool) -> Result<()>;

    /// Whether this command supports the global --dry-run flag.
    ///
    /// Commands that do not support dry-run will cause an error if the user
    /// passes --dry-run. Override this to return `true` in commands that
    /// handle the flag.
    fn supports_dry_run(&self) -> bool {
        false
    }
}

#[derive(Parser, Debug)]
pub enum Command {
    Balance(BalanceCommand),
    Check(CheckCommand),
    Device(DeviceCommand),
    Filesystem(FilesystemCommand),
    #[command(alias = "inspect-internal")]
    Inspect(InspectCommand),
    #[cfg(feature = "mkfs")]
    Mkfs(btrfs_mkfs::args::Arguments),
    Property(PropertyCommand),
    Qgroup(QgroupCommand),
    Quota(QuotaCommand),
    Receive(ReceiveCommand),
    Replace(ReplaceCommand),
    Rescue(RescueCommand),
    Restore(RestoreCommand),
    Scrub(ScrubCommand),
    Send(SendCommand),
    Subvolume(SubvolumeCommand),
    #[cfg(feature = "tune")]
    Tune(btrfs_tune::args::Arguments),
}

impl Runnable for Command {
    fn supports_dry_run(&self) -> bool {
        match self {
            Command::Restore(cmd) => cmd.supports_dry_run(),
            Command::Subvolume(cmd) => cmd.supports_dry_run(),
            _ => false,
        }
    }

    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match self {
            Command::Balance(cmd) => cmd.run(format, dry_run),
            Command::Check(cmd) => cmd.run(format, dry_run),
            Command::Device(cmd) => cmd.run(format, dry_run),
            Command::Filesystem(cmd) => cmd.run(format, dry_run),
            Command::Inspect(cmd) => cmd.run(format, dry_run),
            #[cfg(feature = "mkfs")]
            Command::Mkfs(args) => btrfs_mkfs::run::run(args),
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
            #[cfg(feature = "tune")]
            Command::Tune(args) => btrfs_tune::run::run(args),
        }
    }
}

impl Arguments {
    /// Parse and run the CLI command.
    ///
    /// # Errors
    ///
    /// Returns an error if the command fails.
    pub fn run(&self) -> Result<()> {
        let level = if let Some(explicit) = self.global.log {
            match explicit {
                Level::Debug => log::LevelFilter::Debug,
                Level::Info => log::LevelFilter::Info,
                Level::Warn => log::LevelFilter::Warn,
                Level::Error => log::LevelFilter::Error,
            }
        } else if self.global.quiet {
            log::LevelFilter::Error
        } else {
            match self.global.verbose {
                0 => log::LevelFilter::Warn,
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            }
        };
        env_logger::Builder::new().filter_level(level).init();

        if self.global.dry_run && !self.command.supports_dry_run() {
            anyhow::bail!(
                "the --dry-run option is not supported by this command"
            );
        }

        self.command
            .run(self.global.format.unwrap_or_default(), self.global.dry_run)
    }
}

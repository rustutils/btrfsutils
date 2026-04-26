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
//!
//! # Stability
//!
//! This is a pre-1.0 release. Read-only commands are stable. The
//! mutating commands that go through the new `btrfs-transaction`
//! crate (offline `filesystem resize`, the `rescue` subcommands,
//! and the `tune` conversions when built with the `tune` feature)
//! are experimental and may have edge cases that testing doesn't
//! cover. Take a backup before running them on filesystems you
//! care about.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::struct_excessive_bools)]
// Test code uses literal byte buffers and small cast conversions that
// pedantic clippy flags but that are intentional in unit tests.
#![cfg_attr(
    test,
    allow(
        clippy::cast_lossless,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::identity_op,
        clippy::match_wildcard_for_single_variants,
        clippy::semicolon_if_nothing_returned,
        clippy::uninlined_format_args,
        clippy::unreadable_literal,
    )
)]

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
    Modern,
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <Self as clap::ValueEnum>::from_str(s, true).map_err(|e| e.clone())
    }
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
#[clap(version, infer_subcommands = true, arg_required_else_help = true)]
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

    /// If supported, print subcommand output in that format. [env: BTRFS_OUTPUT_FORMAT]
    #[clap(global = true, long, help_heading = GLOBAL_OPTIONS)]
    pub format: Option<Format>,
}

/// Runtime context passed to every command.
pub struct RunContext {
    /// Output format (text or json).
    pub format: Format,
    /// Whether the user requested a dry run.
    pub dry_run: bool,
    /// Whether the user requested quiet mode (suppress non-error output).
    pub quiet: bool,
}

/// A CLI subcommand that can be executed.
pub trait Runnable {
    /// Execute this command.
    ///
    /// # Errors
    ///
    /// Returns an error if the command fails.
    fn run(&self, ctx: &RunContext) -> Result<()>;

    /// Output formats this command supports.
    ///
    /// The default is text and modern. Commands that also support JSON
    /// should override this to include `Format::Json`.
    fn supported_formats(&self) -> &[Format] {
        &[Format::Text, Format::Modern]
    }

    /// Whether this command supports the global --dry-run flag.
    ///
    /// Commands that do not support dry-run will cause an error if the user
    /// passes --dry-run. Override this to return `true` in commands that
    /// handle the flag.
    fn supports_dry_run(&self) -> bool {
        false
    }
}

/// A command group that delegates to a leaf subcommand.
///
/// Implement this for parent commands (e.g. `BalanceCommand`,
/// `DeviceCommand`) that simply dispatch to their subcommand.
/// A blanket `Runnable` impl forwards all methods through `leaf()`.
pub trait CommandGroup {
    fn leaf(&self) -> &dyn Runnable;
}

impl<T: CommandGroup> Runnable for T {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        self.leaf().run(ctx)
    }

    fn supported_formats(&self) -> &[Format] {
        self.leaf().supported_formats()
    }

    fn supports_dry_run(&self) -> bool {
        self.leaf().supports_dry_run()
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

#[cfg(feature = "mkfs")]
impl Runnable for btrfs_mkfs::args::Arguments {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        btrfs_mkfs::run::run(self)
    }
}

#[cfg(feature = "tune")]
impl Runnable for btrfs_tune::args::Arguments {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        btrfs_tune::run::run(self)
    }
}

impl CommandGroup for Command {
    fn leaf(&self) -> &dyn Runnable {
        match self {
            Command::Balance(cmd) => cmd,
            Command::Check(cmd) => cmd,
            Command::Device(cmd) => cmd,
            Command::Filesystem(cmd) => cmd,
            Command::Inspect(cmd) => cmd,
            #[cfg(feature = "mkfs")]
            Command::Mkfs(cmd) => cmd,
            Command::Property(cmd) => cmd,
            Command::Qgroup(cmd) => cmd,
            Command::Quota(cmd) => cmd,
            Command::Receive(cmd) => cmd,
            Command::Replace(cmd) => cmd,
            Command::Rescue(cmd) => cmd,
            Command::Restore(cmd) => cmd,
            Command::Scrub(cmd) => cmd,
            Command::Send(cmd) => cmd,
            Command::Subvolume(cmd) => cmd,
            #[cfg(feature = "tune")]
            Command::Tune(cmd) => cmd,
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

        let format = self
            .global
            .format
            // Resolve from env manually rather than via clap's `env`
            // attribute. Using `env` on a global flag makes clap treat
            // the env var as a provided argument, which defeats
            // `arg_required_else_help` on parent commands.
            .or_else(|| {
                std::env::var("BTRFS_OUTPUT_FORMAT")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or_default();
        if !self.command.supported_formats().contains(&format) {
            anyhow::bail!(
                "the --format {format:?} option is not supported by this command",
            );
        }

        let ctx = RunContext {
            format,
            dry_run: self.global.dry_run,
            quiet: self.global.quiet,
        };
        self.command.run(&ctx)
    }
}

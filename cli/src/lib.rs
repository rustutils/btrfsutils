//! # btrfs-cli: a Rust reimplementation of the btrfs command-line tool
//!
//! This crate provides the `btrfs` command-line binary, a Rust reimplementation
//! of btrfs-progs. It is built on top of `btrfs-uapi` for kernel communication,
//! `btrfs-disk` for direct on-disk structure parsing, and `btrfs-stream` for
//! send/receive stream processing.
//!
//! Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to
//! see what is available. Most commands require root privileges or
//! `CAP_SYS_ADMIN`.

use anyhow::Result;
use clap::{Parser, ValueEnum};

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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    #[default]
    Text,
    Json,
}

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
    #[clap(global = true, long)]
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
    Balance(BalanceCommand),
    Check(CheckCommand),
    Device(DeviceCommand),
    Filesystem(FilesystemCommand),
    #[command(alias = "inspect-internal")]
    Inspect(InspectCommand),
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

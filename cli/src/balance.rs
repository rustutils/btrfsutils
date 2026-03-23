use super::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::balance::{
    BalanceArgs, BalanceCtl, BalanceFlags, BalanceState, balance, balance_ctl, balance_progress,
};
use clap::Parser;
use nix::errno::Errno;
use std::fs::File;
use std::os::unix::io::AsFd;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

/// Balance data across devices, or change block groups using filters
#[derive(Parser, Debug)]
pub struct BalanceCommand {
    #[clap(subcommand)]
    pub subcommand: BalanceSubcommand,
}

impl Runnable for BalanceCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            BalanceSubcommand::Start(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Pause(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Cancel(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Resume(cmd) => cmd.run(format, dry_run),
            BalanceSubcommand::Status(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum BalanceSubcommand {
    Start(BalanceStartCommand),
    Pause(BalancePauseCommand),
    Cancel(BalanceCancelCommand),
    Resume(BalanceResumeCommand),
    Status(BalanceStatusCommand),
}

type Filters = String;

/// Balance chunks across the devices
///
/// Balance and/or convert (change allocation profile of) chunks that
/// passed all filters in a comma-separated list of filters for a
/// particular chunk type.  If filter list is not given balance all
/// chunks of that type.  In case none of the -d, -m or -s options is
/// given balance all chunks in a filesystem. This is potentially
/// long operation and the user is warned before this start, with
/// a delay to stop it.
#[derive(Parser, Debug)]
pub struct BalanceStartCommand {
    /// Act on data chunks with optional filters
    #[clap(long, short)]
    pub data_filters: Option<Filters>,
    /// Act on metadata chunks with optional filters
    #[clap(long, short)]
    pub metadata_filters: Option<Filters>,
    /// Act on system chunks (requires force) with optional filters
    #[clap(long, short)]
    pub system_filters: Option<Filters>,

    /// Force a reduction of metadata integrity, or skip timeout when converting to RAID56 profiles
    #[clap(long, short)]
    pub force: bool,

    /// Do not print warning and do not delay start
    #[clap(long)]
    pub full_balance: bool,

    /// Run the balance as a background process
    #[clap(long, short, alias = "bg")]
    pub background: bool,

    /// Wait if there's another exclusive operation running, otherwise continue
    #[clap(long)]
    pub enqueue: bool,

    pub path: PathBuf,
}

/// Pause a running balance operation
#[derive(Parser, Debug)]
pub struct BalancePauseCommand {
    pub path: PathBuf,
}

/// Cancel a running balance operation
#[derive(Parser, Debug)]
pub struct BalanceCancelCommand {
    pub path: PathBuf,
}

/// Resume a paused balance operation
#[derive(Parser, Debug)]
pub struct BalanceResumeCommand {
    pub path: PathBuf,
}

/// Show status of running or paused balance operation.
#[derive(Parser, Debug)]
pub struct BalanceStatusCommand {
    pub path: PathBuf,
}

/// Open a path as a read-only file descriptor, suitable for passing to ioctls.
fn open_path(path: &PathBuf) -> Result<File> {
    File::open(path).with_context(|| format!("failed to open '{}'", path.display()))
}

impl Runnable for BalanceStartCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // -s requires --force
        if self.system_filters.is_some() && !self.force {
            anyhow::bail!(
                "Refusing to explicitly operate on system chunks.\n\
                 Pass --force if you really want to do that."
            );
        }

        // TODO: background mode (requires daemonizing the process)
        if self.background {
            anyhow::bail!("--background is not yet implemented");
        }

        let has_filters = self.data_filters.is_some()
            || self.metadata_filters.is_some()
            || self.system_filters.is_some();

        let mut flags = BalanceFlags::empty();

        // TODO: parse filter strings into BalanceArgs (e.g. "usage=50,profiles=raid1")
        let data_args: Option<BalanceArgs> = if self.data_filters.is_some() {
            flags |= BalanceFlags::DATA;
            Some(BalanceArgs::new())
        } else {
            None
        };

        // When metadata is balanced, system is always included with the same
        // args, matching the behaviour of the C tool.
        let meta_args: Option<BalanceArgs> = if self.metadata_filters.is_some() {
            flags |= BalanceFlags::METADATA | BalanceFlags::SYSTEM;
            Some(BalanceArgs::new())
        } else {
            None
        };

        // System args: explicitly requested, OR copied from meta if meta was
        // given but system was not explicitly specified (see C reference).
        let sys_args: Option<BalanceArgs> = if self.system_filters.is_some() {
            flags |= BalanceFlags::SYSTEM;
            Some(BalanceArgs::new())
        } else {
            // Already handled by the metadata branch above; pass None here so
            // balance() leaves the sys field zeroed (same as a default-constructed
            // BalanceArgs since flags on the copied args are what matters).
            meta_args.clone()
        };

        if !has_filters {
            // No type filters specified — relocate everything.
            flags |= BalanceFlags::DATA | BalanceFlags::METADATA | BalanceFlags::SYSTEM;
        }

        if self.force {
            flags |= BalanceFlags::FORCE;
        }

        // Warn the user about a full (unfiltered) balance and give them a
        // chance to abort, unless --full-balance was passed.
        if !has_filters && !self.full_balance {
            eprintln!("WARNING:\n");
            eprintln!("\tFull balance without filters requested. This operation is very");
            eprintln!("\tintense and takes potentially very long. It is recommended to");
            eprintln!("\tuse the balance filters to narrow down the scope of balance.");
            eprintln!("\tUse 'btrfs balance start --full-balance' to skip this warning.");
            eprintln!("\tThe operation will start in 10 seconds. Use Ctrl-C to stop it.");
            thread::sleep(Duration::from_secs(10));
            eprintln!("\nStarting balance without any filters.");
        }

        let file = open_path(&self.path)?;

        match balance(file.as_fd(), flags, data_args, meta_args, sys_args) {
            Ok(progress) => {
                println!(
                    "Done, had to relocate {} out of {} chunks",
                    progress.completed, progress.considered
                );
                Ok(())
            }
            Err(e) if e == Errno::ECANCELED => {
                // The kernel sets ECANCELED when the balance was paused or
                // cancelled mid-run; this is not an error from the user's
                // perspective.
                eprintln!("Balance was paused or cancelled by user.");
                Ok(())
            }
            Err(e) => {
                Err(e).with_context(|| format!("error during balancing '{}'", self.path.display()))
            }
        }
    }
}

impl Runnable for BalancePauseCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        match balance_ctl(file.as_fd(), BalanceCtl::Pause) {
            Ok(()) => Ok(()),
            Err(e) if e == Errno::ENOTCONN => {
                anyhow::bail!(
                    "balance pause on '{}' failed: Not running",
                    self.path.display()
                )
            }
            Err(e) => {
                Err(e).with_context(|| format!("balance pause on '{}' failed", self.path.display()))
            }
        }
    }
}

impl Runnable for BalanceCancelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        match balance_ctl(file.as_fd(), BalanceCtl::Cancel) {
            Ok(()) => Ok(()),
            Err(e) if e == Errno::ENOTCONN => {
                anyhow::bail!(
                    "balance cancel on '{}' failed: Not in progress",
                    self.path.display()
                )
            }
            Err(e) => Err(e)
                .with_context(|| format!("balance cancel on '{}' failed", self.path.display())),
        }
    }
}

impl Runnable for BalanceResumeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        // Resume is just a balance start with the RESUME flag and no type
        // filters; the kernel picks up where it left off.
        match balance(file.as_fd(), BalanceFlags::RESUME, None, None, None) {
            Ok(progress) => {
                println!(
                    "Done, had to relocate {} out of {} chunks",
                    progress.completed, progress.considered
                );
                Ok(())
            }
            Err(e) if e == Errno::ECANCELED => {
                eprintln!("Balance was paused or cancelled by user.");
                Ok(())
            }
            Err(e) if e == Errno::ENOTCONN => {
                anyhow::bail!(
                    "balance resume on '{}' failed: Not in progress",
                    self.path.display()
                )
            }
            Err(e) if e == Errno::EINPROGRESS => {
                anyhow::bail!(
                    "balance resume on '{}' failed: Already running",
                    self.path.display()
                )
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "error during balance resume on '{}'\n\
                     There may be more info in syslog - try dmesg | tail",
                    self.path.display()
                )
            }),
        }
    }
}

impl Runnable for BalanceStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        match balance_progress(file.as_fd()) {
            Ok((state, progress)) => {
                if state.contains(BalanceState::RUNNING) {
                    print!("Balance on '{}' is running", self.path.display());
                    if state.contains(BalanceState::CANCEL_REQ) {
                        println!(", cancel requested");
                    } else if state.contains(BalanceState::PAUSE_REQ) {
                        println!(", pause requested");
                    } else {
                        println!();
                    }
                } else {
                    println!("Balance on '{}' is paused", self.path.display());
                }

                let pct_left = if progress.expected > 0 {
                    100.0 * (1.0 - progress.completed as f64 / progress.expected as f64)
                } else {
                    0.0
                };

                println!(
                    "{} out of about {} chunks balanced ({} considered), {:3.0}% left",
                    progress.completed, progress.expected, progress.considered, pct_left
                );

                Ok(())
            }
            Err(e) if e == Errno::ENOTCONN => {
                println!("No balance found on '{}'", self.path.display());
                Ok(())
            }
            Err(e) => Err(e)
                .with_context(|| format!("balance status on '{}' failed", self.path.display())),
        }
    }
}

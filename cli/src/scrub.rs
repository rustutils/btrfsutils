use crate::{Format, Runnable, util::human_bytes};
use anyhow::Result;
use btrfs_uapi::scrub::ScrubProgress;
use clap::Parser;

mod cancel;
mod limit;
mod resume;
mod start;
mod status;

pub use self::{cancel::*, limit::*, resume::*, start::*, status::*};

/// Verify checksums of data and metadata.
///
/// Scrub reads all data and metadata on a filesystem and verifies checksums.
/// This detects hardware errors and bit rot. Scrub is typically a long-running
/// operation and can be paused, resumed, or cancelled. Progress and status can
/// be queried, and speed limits can be configured. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct ScrubCommand {
    #[clap(subcommand)]
    pub subcommand: ScrubSubcommand,
}

impl Runnable for ScrubCommand {
    fn run(&self, format: Format, dry_run: bool) -> Result<()> {
        match &self.subcommand {
            ScrubSubcommand::Start(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Cancel(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Resume(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Status(cmd) => cmd.run(format, dry_run),
            ScrubSubcommand::Limit(cmd) => cmd.run(format, dry_run),
        }
    }
}

#[derive(Parser, Debug)]
pub enum ScrubSubcommand {
    Start(ScrubStartCommand),
    Cancel(ScrubCancelCommand),
    Resume(ScrubResumeCommand),
    Status(ScrubStatusCommand),
    Limit(ScrubLimitCommand),
}

/// Format a bytes-per-second limit for display; `0` means unlimited.
fn format_limit(limit: u64) -> String {
    if limit == 0 {
        "unlimited".to_owned()
    } else {
        format!("{}/s", human_bytes(limit))
    }
}

/// Number of decimal digits in `n` (minimum 1).
fn digits(n: u64) -> usize {
    if n == 0 { 1 } else { n.ilog10() as usize + 1 }
}

/// Add progress counters from `src` into `dst`.
fn accumulate(dst: &mut ScrubProgress, src: &ScrubProgress) {
    dst.data_extents_scrubbed += src.data_extents_scrubbed;
    dst.tree_extents_scrubbed += src.tree_extents_scrubbed;
    dst.data_bytes_scrubbed += src.data_bytes_scrubbed;
    dst.tree_bytes_scrubbed += src.tree_bytes_scrubbed;
    dst.read_errors += src.read_errors;
    dst.csum_errors += src.csum_errors;
    dst.verify_errors += src.verify_errors;
    dst.super_errors += src.super_errors;
    dst.uncorrectable_errors += src.uncorrectable_errors;
    dst.corrected_errors += src.corrected_errors;
    dst.unverified_errors += src.unverified_errors;
    dst.no_csum += src.no_csum;
    dst.csum_discards += src.csum_discards;
    dst.malloc_errors += src.malloc_errors;
}

/// Print a single-device progress summary.
fn print_progress_summary(p: &ScrubProgress, devid: u64, path: &str) {
    println!(
        "  devid {devid} ({path}): scrubbed {}",
        human_bytes(p.bytes_scrubbed())
    );
    print_error_summary(p);
}

/// Print the error summary line.
fn print_error_summary(p: &ScrubProgress) {
    if p.malloc_errors > 0 {
        eprintln!(
            "WARNING: memory allocation errors during scrub — results may be inaccurate"
        );
    }
    print!("  Error summary:  ");
    if p.is_clean() {
        println!(" no errors found");
    } else {
        if p.read_errors > 0 {
            print!(" read={}", p.read_errors);
        }
        if p.super_errors > 0 {
            print!(" super={}", p.super_errors);
        }
        if p.verify_errors > 0 {
            print!(" verify={}", p.verify_errors);
        }
        if p.csum_errors > 0 {
            print!(" csum={}", p.csum_errors);
        }
        println!();
        println!("    Corrected:      {}", p.corrected_errors);
        println!("    Uncorrectable:  {}", p.uncorrectable_errors);
        println!("    Unverified:     {}", p.unverified_errors);
    }
}

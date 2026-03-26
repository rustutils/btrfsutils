use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::{filesystem::filesystem_info, sysfs::SysfsBtrfs};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show commit statistics for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemCommitStatsCommand {
    /// Print stats then reset the max_commit_ms counter (requires root)
    #[clap(long, short = 'z')]
    pub reset: bool,

    pub path: PathBuf,
}

impl Runnable for FilesystemCommitStatsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let info = filesystem_info(file.as_fd()).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;

        let sysfs = SysfsBtrfs::new(&info.uuid);

        let stats = sysfs
            .commit_stats()
            .context("failed to read commit_stats from sysfs")?;

        println!("UUID: {}", info.uuid.as_hyphenated());
        println!("Commit stats since mount:");
        println!("  {:<28}{:>8}", "Total commits:", stats.commits);
        println!(
            "  {:<28}{:>8}ms",
            "Last commit duration:", stats.last_commit_ms
        );
        println!(
            "  {:<28}{:>8}ms",
            "Max commit duration:", stats.max_commit_ms
        );
        println!(
            "  {:<28}{:>8}ms",
            "Total time spent in commit:", stats.total_commit_ms
        );

        if self.reset {
            sysfs
                .reset_commit_stats()
                .context("failed to reset commit_stats (requires root)")?;
            println!("NOTE: Max commit duration has been reset");
        }

        Ok(())
    }
}

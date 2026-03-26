use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::sysfs::SysfsBtrfs;
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show status information about quota on the filesystem
#[derive(Parser, Debug)]
pub struct QuotaStatusCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Only check if quotas are enabled, without printing full status
    #[clap(long)]
    pub is_enabled: bool,
}

fn describe_mode(mode: &str) -> &str {
    match mode {
        "qgroup" => "full accounting",
        "squota" => "simplified accounting",
        other => other,
    }
}

impl Runnable for QuotaStatusCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;
        let fd = file.as_fd();

        let fs =
            btrfs_uapi::filesystem::filesystem_info(fd).with_context(|| {
                format!(
                    "failed to get filesystem info for '{}'",
                    self.path.display()
                )
            })?;

        let status =
            SysfsBtrfs::new(&fs.uuid).quota_status().with_context(|| {
                format!(
                    "failed to read quota status for '{}'",
                    self.path.display()
                )
            })?;

        if self.is_enabled {
            if !status.enabled {
                // Exit with a non-zero code the way the C tool does, without
                // printing anything.
                std::process::exit(1);
            }
            return Ok(());
        }

        println!("Quotas on {}:", self.path.display());

        if !status.enabled {
            println!("  Enabled:                 no");
            return Ok(());
        }

        println!("  Enabled:                 yes");

        if let Some(ref mode) = status.mode {
            println!(
                "  Mode:                    {} ({})",
                mode,
                describe_mode(mode)
            );
        }

        if let Some(inconsistent) = status.inconsistent {
            println!(
                "  Inconsistent:            {}{}",
                if inconsistent { "yes" } else { "no" },
                if inconsistent { " (rescan needed)" } else { "" }
            );
        }

        if let Some(override_limits) = status.override_limits {
            println!(
                "  Override limits:         {}",
                if override_limits { "yes" } else { "no" }
            );
        }

        if let Some(threshold) = status.drop_subtree_threshold {
            println!("  Drop subtree threshold:  {}", threshold);
        }

        if let Some(total) = status.total_count {
            println!("  Total count:             {}", total);
        }

        if let Some(level0) = status.level0_count {
            println!("  Level 0:                 {}", level0);
        }

        Ok(())
    }
}

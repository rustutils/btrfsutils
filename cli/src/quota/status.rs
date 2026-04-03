use crate::{Format, RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::{quota::QuotaRescanStatus, sysfs::SysfsBtrfs};
use clap::Parser;
use cols::Cols;
use std::{os::unix::io::AsFd, path::PathBuf};
use uuid::Uuid;

/// Show status information about quota on the filesystem
///
/// Displays the current quota configuration and accounting state. The
/// fields shown when quotas are enabled:
///
/// Enabled: whether quota accounting is active on this filesystem.
///
/// Mode: the accounting mode. "qgroup" (full accounting) tracks every
/// extent backref and provides accurate shared/exclusive byte counts.
/// "squota" (simple quotas, kernel 6.7+) uses lighter-weight lifetime
/// tracking that avoids the overhead of full backref walking but does
/// not distinguish shared vs exclusive usage.
///
/// Inconsistent: if "yes", the qgroup numbers are stale and a rescan
/// is needed (btrfs quota rescan). This happens after unclean shutdowns
/// or when qgroups are first enabled on an existing filesystem.
///
/// Override limits: when enabled, qgroup limits are not enforced for
/// the current mount. Writes will succeed even if they exceed the
/// configured limits. Useful for emergency recovery.
///
/// Drop subtree threshold: controls how deep the kernel tracks qgroup
/// changes when deleting subvolumes. Levels below this threshold skip
/// detailed per-extent accounting during heavy delete workloads,
/// trading accuracy for performance. A value of 0 means full tracking.
///
/// Total count: the total number of qgroup entries in the quota tree.
///
/// Level 0: the number of level-0 qgroups. Each subvolume automatically
/// gets a level-0 qgroup (e.g. 0/256, 0/257). Higher-level qgroups
/// (1/0, 2/0, ...) are user-created containers for hierarchical limits.
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
    fn run(&self, ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
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

        let rescan = if status.enabled {
            Some(btrfs_uapi::quota::quota_rescan_status(fd))
        } else {
            None
        };

        match ctx.format {
            Format::Modern => {
                print_status_modern(&self.path, &fs.uuid, &status, rescan.as_ref());
            }
            Format::Text => {
                print_status_text(&self.path, &status);
            }
            Format::Json => unreachable!(),
        }

        Ok(())
    }
}

fn print_status_text(
    path: &std::path::Path,
    status: &btrfs_uapi::sysfs::QuotaStatus,
) {
    println!("Quotas on {}:", path.display());

    if !status.enabled {
        println!("  Enabled:                 no");
        return;
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
        println!("  Drop subtree threshold:  {threshold}");
    }

    if let Some(total) = status.total_count {
        println!("  Total count:             {total}");
    }

    if let Some(level0) = status.level0_count {
        println!("  Level 0:                 {level0}");
    }
}

#[derive(Cols)]
struct StatusRow {
    #[column(header = "PROPERTY")]
    label: String,
    #[column(header = "VALUE")]
    value: String,
}

fn print_status_modern(
    path: &std::path::Path,
    uuid: &Uuid,
    status: &btrfs_uapi::sysfs::QuotaStatus,
    rescan: Option<&nix::Result<QuotaRescanStatus>>,
) {
    println!("Quotas on {}:", path.display());

    let mut rows: Vec<StatusRow> = Vec::new();

    rows.push(StatusRow {
        label: "UUID".to_string(),
        value: uuid.as_hyphenated().to_string(),
    });

    rows.push(StatusRow {
        label: "Enabled".to_string(),
        value: if status.enabled { "yes" } else { "no" }.to_string(),
    });

    if !status.enabled {
        let mut out = std::io::stdout().lock();
        let _ = StatusRow::print_table(&rows, &mut out);
        return;
    }

    if let Some(ref mode) = status.mode {
        rows.push(StatusRow {
            label: "Mode".to_string(),
            value: format!("{mode} ({})", describe_mode(mode)),
        });
    }

    if let Some(inconsistent) = status.inconsistent {
        rows.push(StatusRow {
            label: "Inconsistent".to_string(),
            value: if inconsistent {
                "yes (rescan needed)"
            } else {
                "no"
            }
            .to_string(),
        });
    }

    if let Some(override_limits) = status.override_limits {
        rows.push(StatusRow {
            label: "Override limits".to_string(),
            value: if override_limits { "yes" } else { "no" }.to_string(),
        });
    }

    if let Some(threshold) = status.drop_subtree_threshold {
        rows.push(StatusRow {
            label: "Drop subtree threshold".to_string(),
            value: threshold.to_string(),
        });
    }

    if let Some(total) = status.total_count {
        rows.push(StatusRow {
            label: "Total count".to_string(),
            value: total.to_string(),
        });
    }

    if let Some(level0) = status.level0_count {
        rows.push(StatusRow {
            label: "Level 0".to_string(),
            value: level0.to_string(),
        });
    }

    if let Some(rescan_result) = rescan {
        rows.push(StatusRow {
            label: "Rescan".to_string(),
            value: match rescan_result {
                Ok(rs) if rs.running => {
                    format!("in progress (objectid {})", rs.progress)
                }
                Ok(_) => "not in progress".to_string(),
                Err(_) => "unknown (insufficient privileges)".to_string(),
            },
        });
    }

    let mut out = std::io::stdout().lock();
    let _ = StatusRow::print_table(&rows, &mut out);
}

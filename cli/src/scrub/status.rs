use crate::{
    Format, RunContext, Runnable,
    filesystem::UnitMode,
    util::{fmt_size, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    device::device_info_all, filesystem::filesystem_info, scrub::scrub_progress,
};
use clap::Parser;
use cols::Cols;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show the status of a running or finished scrub
#[derive(Parser, Debug)]
pub struct ScrubStatusCommand {
    /// Show stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Print full raw data instead of summary
    #[clap(short = 'R', long = "raw-data")]
    pub raw_data: bool,

    #[clap(flatten)]
    pub units: UnitMode,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubStatusCommand {
    #[allow(clippy::too_many_lines)]
    fn run(&self, ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        let fs = filesystem_info(fd).with_context(|| {
            format!(
                "failed to get filesystem info for '{}'",
                self.path.display()
            )
        })?;
        let devices = device_info_all(fd, &fs).with_context(|| {
            format!("failed to get device info for '{}'", self.path.display())
        })?;

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mut any_running = false;
        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();

        match ctx.format {
            Format::Modern => {
                let mut rows: Vec<ScrubRow> = Vec::new();
                for dev in &devices {
                    match scrub_progress(fd, dev.devid).with_context(|| {
                        format!(
                            "failed to get scrub progress for device {}",
                            dev.devid
                        )
                    })? {
                        None => {
                            rows.push(ScrubRow {
                                devid: dev.devid,
                                path: dev.path.clone(),
                                scrubbed: "-".to_string(),
                                errors: "-".to_string(),
                            });
                        }
                        Some(progress) => {
                            any_running = true;
                            super::accumulate(&mut fs_totals, &progress);
                            rows.push(ScrubRow {
                                devid: dev.devid,
                                path: dev.path.clone(),
                                scrubbed: format!(
                                    "{}/~{}",
                                    fmt_size(progress.bytes_scrubbed(), &mode),
                                    fmt_size(dev.bytes_used, &mode),
                                ),
                                errors: format_error_count(&progress),
                            });
                        }
                    }
                }

                if any_running {
                    let mut out = std::io::stdout().lock();
                    let _ = ScrubRow::print_table(&rows, &mut out);
                } else {
                    println!("\tno scrub in progress");
                }
            }
            Format::Text => {
                for dev in &devices {
                    match scrub_progress(fd, dev.devid).with_context(|| {
                        format!(
                            "failed to get scrub progress for device {}",
                            dev.devid
                        )
                    })? {
                        None => {
                            if self.device {
                                println!(
                                    "device {} ({}): no scrub in progress",
                                    dev.devid, dev.path
                                );
                            }
                        }
                        Some(progress) => {
                            any_running = true;
                            super::accumulate(&mut fs_totals, &progress);
                            if self.device {
                                super::print_device_progress(
                                    &progress,
                                    dev.devid,
                                    &dev.path,
                                    self.raw_data,
                                    &mode,
                                );
                            }
                        }
                    }
                }

                if !any_running {
                    println!("\tno scrub in progress");
                } else if !self.device {
                    if self.raw_data {
                        super::print_raw_progress(
                            &fs_totals,
                            0,
                            "filesystem totals",
                        );
                    } else {
                        println!(
                            "Bytes scrubbed:   {}",
                            fmt_size(fs_totals.bytes_scrubbed(), &mode)
                        );
                        super::print_error_summary(&fs_totals);
                    }
                }
            }
            Format::Json => unreachable!(),
        }

        Ok(())
    }
}

#[derive(Cols)]
struct ScrubRow {
    #[column(header = "DEVID", right)]
    devid: u64,
    #[column(header = "PATH")]
    path: String,
    #[column(header = "SCRUBBED", right)]
    scrubbed: String,
    #[column(header = "ERRORS")]
    errors: String,
}

pub(super) fn format_error_count(
    p: &btrfs_uapi::scrub::ScrubProgress,
) -> String {
    if p.is_clean() {
        "no errors".to_string()
    } else {
        let total =
            p.read_errors + p.csum_errors + p.verify_errors + p.super_errors;
        let uncorrectable = p.uncorrectable_errors;
        if uncorrectable > 0 {
            format!("{total} errors ({uncorrectable} uncorrectable)")
        } else {
            format!("{total} errors (all corrected)")
        }
    }
}

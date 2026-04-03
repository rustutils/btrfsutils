use crate::{
    Format, RunContext, Runnable,
    util::{SizeFormat, fmt_size, open_path, parse_size_with_suffix},
};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    device::device_info_all,
    filesystem::filesystem_info,
    scrub::{scrub_progress, scrub_start},
    sysfs::SysfsBtrfs,
};
use clap::Parser;
use cols::Cols;
use console::Term;
use std::{
    os::unix::io::{AsFd, AsRawFd},
    path::PathBuf,
    thread,
    time::Duration,
};

/// Start a new scrub on the filesystem or a device.
///
/// Scrubs all devices sequentially. This command blocks until the scrub
/// completes; use Ctrl-C to cancel.
#[derive(Parser, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct ScrubStartCommand {
    /// Do not background (default behavior, accepted for compatibility)
    #[clap(short = 'B')]
    pub no_background: bool,

    /// Stats per device
    #[clap(long, short)]
    pub device: bool,

    /// Read-only mode: check for errors but do not attempt repairs
    #[clap(long, short)]
    pub readonly: bool,

    /// Print full raw data instead of summary
    #[clap(short = 'R', long)]
    pub raw: bool,

    /// Force starting new scrub even if a scrub is already running
    #[clap(long, short)]
    pub force: bool,

    /// Set the throughput limit for each device (0 for unlimited), restored
    /// afterwards
    #[clap(long, value_name = "SIZE", value_parser = parse_size_with_suffix)]
    pub limit: Option<u64>,

    /// Set ioprio class (see ionice(1) manpage)
    #[clap(short = 'c', value_name = "CLASS")]
    pub ioprio_class: Option<i32>,

    /// Set ioprio classdata (see ionice(1) manpage)
    #[clap(short = 'n', value_name = "CDATA")]
    pub ioprio_classdata: Option<i32>,

    /// Path to a mounted btrfs filesystem or a device
    pub path: PathBuf,
}

impl Runnable for ScrubStartCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
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

        if !self.force {
            for dev in &devices {
                if scrub_progress(fd, dev.devid)
                    .with_context(|| {
                        format!(
                            "failed to check scrub status for device {}",
                            dev.devid
                        )
                    })?
                    .is_some()
                {
                    bail!(
                        "Scrub is already running.\n\
                         To cancel use 'btrfs scrub cancel {path}'.\n\
                         To see the status use 'btrfs scrub status {path}'",
                        path = self.path.display()
                    );
                }
            }
        }

        let sysfs = SysfsBtrfs::new(&fs.uuid);
        let old_limits = self.apply_limits(&sysfs, &devices)?;

        if self.ioprio_class.is_some() || self.ioprio_classdata.is_some() {
            super::set_ioprio(
                self.ioprio_class.unwrap_or(3), // default: idle
                self.ioprio_classdata.unwrap_or(0),
            );
        }

        println!("UUID: {}", fs.uuid.as_hyphenated());

        let mode = SizeFormat::HumanIec;

        match ctx.format {
            Format::Modern => {
                self.run_modern(fd, &devices, &mode, ctx.quiet, &self.path);
            }
            Format::Text => {
                self.run_text(fd, &devices, &mode);
            }
            Format::Json => unreachable!(),
        }

        self.restore_limits(&sysfs, &old_limits);

        Ok(())
    }
}

impl ScrubStartCommand {
    fn run_text(
        &self,
        fd: std::os::unix::io::BorrowedFd,
        devices: &[btrfs_uapi::device::DeviceInfo],
        mode: &SizeFormat,
    ) {
        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();

        for dev in devices {
            println!("scrubbing device {} ({})", dev.devid, dev.path);

            match scrub_start(fd, dev.devid, self.readonly) {
                Ok(progress) => {
                    super::accumulate(&mut fs_totals, &progress);
                    if self.device {
                        super::print_device_progress(
                            &progress, dev.devid, &dev.path, self.raw, mode,
                        );
                    }
                }
                Err(e) => {
                    eprintln!("error scrubbing device {}: {e}", dev.devid);
                }
            }
        }

        if !self.device {
            if self.raw {
                super::print_raw_progress(&fs_totals, 0, "filesystem totals");
            } else {
                super::print_error_summary(&fs_totals);
            }
        } else if devices.len() > 1 {
            println!("\nFilesystem totals:");
            if self.raw {
                super::print_raw_progress(&fs_totals, 0, "filesystem totals");
            } else {
                super::print_error_summary(&fs_totals);
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn run_modern(
        &self,
        fd: std::os::unix::io::BorrowedFd,
        devices: &[btrfs_uapi::device::DeviceInfo],
        mode: &SizeFormat,
        quiet: bool,
        mount_path: &std::path::Path,
    ) {
        let term = Term::stderr();
        let is_term = term.is_term();
        let poll_interval = if is_term {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(1)
        };

        let mut fs_totals = btrfs_uapi::scrub::ScrubProgress::default();
        let mut dev_results: Vec<(
            u64,
            String,
            btrfs_uapi::scrub::ScrubProgress,
        )> = Vec::new();

        for dev in devices {
            let readonly = self.readonly;
            let devid = dev.devid;
            let dev_used = dev.bytes_used;

            // Spawn the blocking scrub on a background thread.
            // SAFETY: the fd outlives the thread (we join before returning),
            // and the ioctl is safe to call from any thread.
            let raw_fd = fd.as_raw_fd();
            let handle = thread::spawn(move || {
                use std::os::unix::io::BorrowedFd;
                let fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
                scrub_start(fd, devid, readonly)
            });

            // Poll progress while the scrub thread is running.
            if !quiet {
                while !handle.is_finished() {
                    if let Ok(Some(progress)) = scrub_progress(fd, devid) {
                        let msg =
                            format_progress(devid, &progress, dev_used, mode);
                        if is_term {
                            let _ = term.clear_line();
                            let _ = term.write_str(&msg);
                        } else {
                            let _ = term.write_line(&msg);
                        }
                    }
                    thread::sleep(poll_interval);
                }

                if is_term {
                    let _ = term.clear_line();
                }
            }

            match handle.join().unwrap() {
                Ok(progress) => {
                    super::accumulate(&mut fs_totals, &progress);
                    dev_results.push((devid, dev.path.clone(), progress));
                }
                Err(e) => {
                    eprintln!("error scrubbing device {devid}: {e}");
                }
            }
        }

        if self.raw {
            // Raw mode: filesystem as root, devices with counter children.
            let mp = mount_path.display().to_string();
            let mut root = ScrubRawRow {
                name: "filesystem".to_string(),
                value: mp,
                children: dev_results
                    .iter()
                    .map(|(devid, path, p)| {
                        scrub_raw_row(&format!("devid {devid}"), path, p)
                    })
                    .collect(),
            };

            // For multi-device, add an aggregated totals row at the end.
            if dev_results.len() > 1 {
                root.children.push(scrub_raw_row("totals", "", &fs_totals));
            }

            let mut out = std::io::stdout().lock();
            let _ = ScrubRawRow::print_table(&[root], &mut out);
        } else {
            // Summary mode: compact tree with key stats.
            let mp = mount_path.display().to_string();
            let mut root =
                scrub_result_row("filesystem", &mp, &fs_totals, mode);
            root.children = dev_results
                .iter()
                .map(|(devid, path, p)| {
                    scrub_result_row(&format!("devid {devid}"), path, p, mode)
                })
                .collect();

            let mut out = std::io::stdout().lock();
            let _ = ScrubResultRow::print_table(&[root], &mut out);
        }

        if fs_totals.malloc_errors > 0 {
            eprintln!(
                "WARNING: {} memory allocation error(s) during scrub — results may be incomplete",
                fs_totals.malloc_errors
            );
        }
    }

    fn apply_limits(
        &self,
        sysfs: &SysfsBtrfs,
        devices: &[btrfs_uapi::device::DeviceInfo],
    ) -> Result<Vec<(u64, u64)>> {
        let mut old_limits = Vec::new();
        if let Some(limit) = self.limit {
            for dev in devices {
                let old = sysfs.scrub_speed_max_get(dev.devid).with_context(
                    || {
                        format!(
                            "failed to read scrub limit for devid {}",
                            dev.devid
                        )
                    },
                )?;
                old_limits.push((dev.devid, old));
                sysfs.scrub_speed_max_set(dev.devid, limit).with_context(
                    || {
                        format!(
                            "failed to set scrub limit for devid {}",
                            dev.devid
                        )
                    },
                )?;
            }
        }
        Ok(old_limits)
    }

    #[allow(clippy::unused_self)] // method kept on the command struct for consistency
    fn restore_limits(&self, sysfs: &SysfsBtrfs, old_limits: &[(u64, u64)]) {
        for &(devid, old_limit) in old_limits {
            if let Err(e) = sysfs.scrub_speed_max_set(devid, old_limit) {
                eprintln!(
                    "WARNING: failed to restore scrub limit for devid {devid}: {e}"
                );
            }
        }
    }
}

#[derive(Cols)]
struct ScrubResultRow {
    #[column(tree)]
    name: String,
    #[column(header = "PATH", wrap)]
    path: String,
    #[column(header = "DATA", right)]
    data: String,
    #[column(header = "META", right)]
    meta: String,
    #[column(header = "CORRECTED", right)]
    corrected: String,
    #[column(header = "UNCORRECTABLE", right)]
    uncorrectable: String,
    #[column(header = "UNVERIFIED", right)]
    unverified: String,
    #[column(children)]
    children: Vec<Self>,
}

fn scrub_result_row(
    name: &str,
    path: &str,
    p: &btrfs_uapi::scrub::ScrubProgress,
    mode: &SizeFormat,
) -> ScrubResultRow {
    ScrubResultRow {
        name: name.to_string(),
        path: path.to_string(),
        data: fmt_size(p.data_bytes_scrubbed, mode),
        meta: fmt_size(p.tree_bytes_scrubbed, mode),
        corrected: p.corrected_errors.to_string(),
        uncorrectable: p.uncorrectable_errors.to_string(),
        unverified: p.unverified_errors.to_string(),
        children: Vec::new(),
    }
}

#[derive(Cols)]
struct ScrubRawRow {
    #[column(tree)]
    name: String,
    #[column(header = "VALUE", right, wrap)]
    value: String,
    #[column(children)]
    children: Vec<Self>,
}

fn scrub_raw_row(
    name: &str,
    value: &str,
    p: &btrfs_uapi::scrub::ScrubProgress,
) -> ScrubRawRow {
    let counters = vec![
        ("data_extents_scrubbed", p.data_extents_scrubbed),
        ("tree_extents_scrubbed", p.tree_extents_scrubbed),
        ("data_bytes_scrubbed", p.data_bytes_scrubbed),
        ("tree_bytes_scrubbed", p.tree_bytes_scrubbed),
        ("read_errors", p.read_errors),
        ("csum_errors", p.csum_errors),
        ("verify_errors", p.verify_errors),
        ("no_csum", p.no_csum),
        ("csum_discards", p.csum_discards),
        ("super_errors", p.super_errors),
        ("malloc_errors", p.malloc_errors),
        ("uncorrectable_errors", p.uncorrectable_errors),
        ("corrected_errors", p.corrected_errors),
        ("unverified_errors", p.unverified_errors),
        ("last_physical", p.last_physical),
    ];
    ScrubRawRow {
        name: name.to_string(),
        value: value.to_string(),
        children: counters
            .into_iter()
            .map(|(k, v)| ScrubRawRow {
                name: k.to_string(),
                value: v.to_string(),
                children: Vec::new(),
            })
            .collect(),
    }
}

fn format_progress(
    devid: u64,
    progress: &btrfs_uapi::scrub::ScrubProgress,
    dev_used: u64,
    mode: &SizeFormat,
) -> String {
    let scrubbed = progress.bytes_scrubbed();
    let errors = super::status::format_error_count(progress);
    format!(
        "scrubbing devid {devid}: {}/~{} ({errors})",
        fmt_size(scrubbed, mode),
        fmt_size(dev_used, mode),
    )
}

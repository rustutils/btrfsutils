use crate::{Format, Runnable, util::check_device_for_overwrite};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    filesystem::fs_info,
    replace::{ReplaceSource, ReplaceState, replace_start, replace_status},
    sysfs::SysfsBtrfs,
};
use clap::Parser;
use std::{
    ffi::CString,
    fs::{self, File},
    os::unix::io::AsFd,
    path::PathBuf,
    thread,
    time::Duration,
};

/// Replace a device in the filesystem.
///
/// The source device can be specified either as a path (e.g. /dev/sdb) or as a
/// numeric device ID. The target device will be used to replace the source. The
/// filesystem must be mounted at mount_point.
#[derive(Parser, Debug)]
pub struct ReplaceStartCommand {
    /// Source device path or devid to replace
    pub source: String,

    /// Target device that will replace the source
    pub target: PathBuf,

    /// Mount point of the filesystem
    pub mount_point: PathBuf,

    /// Only read from srcdev if no other zero-defect mirror exists
    #[clap(short = 'r')]
    pub redundancy_only: bool,

    /// Force using and overwriting targetdev even if it contains a valid btrfs filesystem
    #[clap(short = 'f')]
    pub force: bool,

    /// Do not background the replace operation; wait for it to finish
    #[clap(short = 'B')]
    pub no_background: bool,

    /// Wait if there's another exclusive operation running, instead of returning an error
    #[clap(long)]
    pub enqueue: bool,

    /// Do not perform whole device TRIM on the target device
    #[clap(short = 'K', long)]
    pub nodiscard: bool,
}

impl Runnable for ReplaceStartCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // Validate the target device before opening the filesystem.
        check_device_for_overwrite(&self.target, self.force)?;

        let file = File::open(&self.mount_point)
            .with_context(|| format!("failed to open '{}'", self.mount_point.display()))?;
        let fd = file.as_fd();

        // If --enqueue is set, wait for any running exclusive operation to finish.
        if self.enqueue {
            let info = fs_info(fd).with_context(|| {
                format!(
                    "failed to get filesystem info for '{}'",
                    self.mount_point.display()
                )
            })?;
            let sysfs = SysfsBtrfs::new(&info.uuid);
            let op = sysfs.wait_for_exclusive_operation().with_context(|| {
                format!(
                    "failed to check exclusive operation on '{}'",
                    self.mount_point.display()
                )
            })?;
            if op != "none" {
                eprintln!("waited for exclusive operation '{op}' to finish");
            }
        }

        // Check if a replace is already running.
        let current = replace_status(fd).with_context(|| {
            format!(
                "failed to get replace status on '{}'",
                self.mount_point.display()
            )
        })?;
        if current.state == ReplaceState::Started {
            bail!(
                "a device replace operation is already in progress on '{}'",
                self.mount_point.display()
            );
        }

        // Resolve source: if it parses as a number, treat it as a devid;
        // otherwise treat it as a device path.
        let source = if let Ok(devid) = self.source.parse::<u64>() {
            ReplaceSource::DevId(devid)
        } else {
            ReplaceSource::Path(
                &CString::new(self.source.as_bytes())
                    .with_context(|| format!("invalid source device path '{}'", self.source))?,
            )
        };

        let tgtdev = CString::new(self.target.as_os_str().as_encoded_bytes())
            .with_context(|| format!("invalid target device path '{}'", self.target.display()))?;

        // Discard (TRIM) the target device unless --nodiscard is set.
        if !self.nodiscard {
            let tgtfile = fs::OpenOptions::new()
                .write(true)
                .open(&self.target)
                .with_context(|| {
                    format!(
                        "failed to open target device '{}' for discard",
                        self.target.display()
                    )
                })?;
            match btrfs_uapi::blkdev::discard_whole_device(tgtfile.as_fd()) {
                Ok(0) => {}
                Ok(_) => eprintln!("discarded target device '{}'", self.target.display()),
                Err(e) => {
                    eprintln!(
                        "warning: discard failed on '{}': {e}; continuing anyway",
                        self.target.display()
                    );
                }
            }
        }

        match replace_start(fd, source, &tgtdev, self.redundancy_only).with_context(|| {
            format!(
                "failed to start replace on '{}'",
                self.mount_point.display()
            )
        })? {
            Ok(()) => {}
            Err(e) => bail!("{e}"),
        }

        println!(
            "replace started: {} -> {}",
            self.source,
            self.target.display(),
        );

        if self.no_background {
            // Poll until the replace finishes.
            loop {
                thread::sleep(Duration::from_secs(1));
                let status = replace_status(fd).with_context(|| {
                    format!(
                        "failed to get replace status on '{}'",
                        self.mount_point.display()
                    )
                })?;

                let pct = status.progress_1000 as f64 / 10.0;
                eprint!(
                    "\r{pct:.1}% done, {} write errs, {} uncorr. read errs",
                    status.num_write_errors, status.num_uncorrectable_read_errors,
                );

                if status.state != ReplaceState::Started {
                    eprintln!();
                    match status.state {
                        ReplaceState::Finished => {
                            println!("replace finished successfully");
                        }
                        ReplaceState::Canceled => {
                            bail!("replace was cancelled");
                        }
                        _ => {
                            bail!("replace ended in unexpected state: {:?}", status.state);
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}


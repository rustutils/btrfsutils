use crate::{Format, Runnable};
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
    io::BufRead,
    os::unix::{fs::FileTypeExt, io::AsFd},
    path::{Path, PathBuf},
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
        check_target_device(&self.target, self.force)?;

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

/// Validate the target device before starting the replace operation.
///
/// Checks that the target is a block device, is not currently mounted, and
/// does not already contain a btrfs filesystem (unless --force is set).
fn check_target_device(target: &Path, force: bool) -> Result<()> {
    let meta = fs::metadata(target)
        .with_context(|| format!("cannot access target device '{}'", target.display()))?;

    if !meta.file_type().is_block_device() {
        bail!("'{}' is not a block device", target.display());
    }

    if is_device_mounted(target)? {
        bail!(
            "'{}' is mounted; refusing to use a mounted device as replace target",
            target.display()
        );
    }

    if !force && has_btrfs_superblock(target) {
        bail!(
            "'{}' already contains a btrfs filesystem; use -f to force",
            target.display()
        );
    }

    Ok(())
}

/// Check if a device path appears in /proc/mounts.
fn is_device_mounted(device: &Path) -> Result<bool> {
    let canonical = fs::canonicalize(device)
        .with_context(|| format!("cannot resolve path '{}'", device.display()))?;
    let canonical_str = canonical.to_string_lossy();

    let file = fs::File::open("/proc/mounts").context("failed to open /proc/mounts")?;
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if let Some(mount_dev) = line.split_whitespace().next() {
            if mount_dev == canonical_str.as_ref() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Try to read a btrfs superblock from the device. Returns true if a valid
/// btrfs magic signature is found.
fn has_btrfs_superblock(device: &Path) -> bool {
    let Ok(mut file) = File::open(device) else {
        return false;
    };
    match btrfs_disk::superblock::read_superblock(&mut file, 0) {
        Ok(sb) => sb.magic_is_valid(),
        Err(_) => false,
    }
}

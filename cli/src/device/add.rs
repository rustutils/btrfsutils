use crate::{Format, Runnable, util::check_device_for_overwrite};
use anyhow::{Context, Result};
use btrfs_uapi::{device::device_add, filesystem::filesystem_info, sysfs::SysfsBtrfs};
use clap::Parser;
use std::{ffi::CString, fs, os::unix::io::AsFd, path::PathBuf};

/// Add one or more devices to a mounted filesystem
///
/// The device must not be mounted and should not contain a filesystem or
/// other data. The operation requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct DeviceAddCommand {
    /// Force overwrite of an existing filesystem on the device
    #[clap(short = 'f', long)]
    pub force: bool,

    /// Do not perform whole device TRIM (discard) before adding
    #[clap(short = 'K', long)]
    pub nodiscard: bool,

    /// Wait if there's another exclusive operation running, instead of returning an error
    #[clap(long)]
    pub enqueue: bool,

    /// One or more block devices to add
    #[clap(required = true, num_args = 1..)]
    pub devices: Vec<PathBuf>,

    /// Mount point of the target filesystem
    pub target: PathBuf,
}

impl Runnable for DeviceAddCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = fs::File::open(&self.target)
            .with_context(|| format!("failed to open '{}'", self.target.display()))?;
        let fd = file.as_fd();

        // If --enqueue is set, wait for any running exclusive operation to finish.
        if self.enqueue {
            let info = filesystem_info(fd).with_context(|| {
                format!(
                    "failed to get filesystem info for '{}'",
                    self.target.display()
                )
            })?;
            let sysfs = SysfsBtrfs::new(&info.uuid);
            let op = sysfs.wait_for_exclusive_operation().with_context(|| {
                format!(
                    "failed to check exclusive operation on '{}'",
                    self.target.display()
                )
            })?;
            if op != "none" {
                eprintln!("waited for exclusive operation '{op}' to finish");
            }
        }

        let mut had_error = false;

        for device in &self.devices {
            // Validate the device: must be a block device, not mounted, no
            // existing btrfs filesystem (unless --force).
            if let Err(e) = check_device_for_overwrite(device, self.force) {
                eprintln!("error: {e:#}");
                had_error = true;
                continue;
            }

            // Discard (TRIM) the device unless --nodiscard is set.
            if !self.nodiscard {
                match fs::OpenOptions::new().write(true).open(device) {
                    Ok(tgtfile) => {
                        match btrfs_uapi::blkdev::discard_whole_device(tgtfile.as_fd()) {
                            Ok(0) => {}
                            Ok(_) => eprintln!("discarded device '{}'", device.display()),
                            Err(e) => {
                                eprintln!(
                                    "warning: discard failed on '{}': {e}; continuing anyway",
                                    device.display()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: could not open '{}' for discard: {e}; continuing anyway",
                            device.display()
                        );
                    }
                }
            }

            let path_str = device.to_str().ok_or_else(|| {
                anyhow::anyhow!("device path is not valid UTF-8: '{}'", device.display())
            })?;

            let cpath = CString::new(path_str).with_context(|| {
                format!("device path contains a null byte: '{}'", device.display())
            })?;

            match device_add(fd, &cpath) {
                Ok(()) => println!("added device '{}'", device.display()),
                Err(e) => {
                    eprintln!("error adding device '{}': {e}", device.display());
                    had_error = true;
                }
            }
        }

        if had_error {
            anyhow::bail!("one or more devices could not be added");
        }

        Ok(())
    }
}

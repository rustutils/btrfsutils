use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::device::device_add;
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Add one or more devices to a mounted filesystem
///
/// The device must not be mounted and should not contain a filesystem or
/// other data. The operation requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct DeviceAddCommand {
    /// One or more block devices to add, followed by the filesystem mount point
    ///
    /// Example: btrfs device add /dev/sdb /dev/sdc /mnt/data
    #[clap(required = true, num_args = 2..)]
    pub args: Vec<PathBuf>,
}

impl Runnable for DeviceAddCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // The last argument is the mount point; everything before it is a device.
        // split_last() returns (&last, &[..rest]), so mount is first.
        let (mount, devices) = self
            .args
            .split_last()
            .expect("clap ensures at least 2 args");

        let file =
            File::open(mount).with_context(|| format!("failed to open '{}'", mount.display()))?;
        let fd = file.as_fd();

        let mut had_error = false;

        for device in devices {
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

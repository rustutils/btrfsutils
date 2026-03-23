use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::device::{DeviceSpec, device_remove};
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Remove one or more devices from a mounted filesystem
///
/// Each device can be specified as a block device path, a numeric device ID,
/// the special token "missing" (to remove a device that is no longer present),
/// or "cancel" (to cancel an in-progress removal).
///
/// The operation requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct DeviceRemoveCommand {
    /// One or more devices to remove (path, devid, "missing", or "cancel"),
    /// followed by the filesystem mount point
    ///
    /// Example: btrfs device remove /dev/sdb 3 missing /mnt/data
    #[clap(required = true, num_args = 2..)]
    pub args: Vec<String>,
}

impl Runnable for DeviceRemoveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // The last argument is the mount point; everything before it is a device spec.
        // split_last() returns (&last, &[..rest]), so mount_str is first.
        let (mount_str, specs) = self
            .args
            .split_last()
            .expect("clap ensures at least 2 args");

        let mount = PathBuf::from(mount_str);
        let file =
            File::open(&mount).with_context(|| format!("failed to open '{}'", mount.display()))?;
        let fd = file.as_fd();

        let mut had_error = false;

        for spec_str in specs {
            match remove_one(fd, spec_str) {
                Ok(()) => println!("removed device '{spec_str}'"),
                Err(e) => {
                    eprintln!("error removing device '{spec_str}': {e}");
                    had_error = true;
                }
            }
        }

        if had_error {
            anyhow::bail!("one or more devices could not be removed");
        }

        Ok(())
    }
}

/// Attempt to remove a single device identified by `spec_str` from the
/// filesystem open on `fd`.
///
/// If `spec_str` parses as a `u64` it is treated as a device ID; otherwise it
/// is treated as a path (or the special strings `"missing"` / `"cancel"`).
fn remove_one(fd: std::os::unix::io::BorrowedFd, spec_str: &str) -> Result<()> {
    if let Ok(devid) = spec_str.parse::<u64>() {
        device_remove(fd, DeviceSpec::Id(devid))
            .with_context(|| format!("failed to remove devid {devid}"))?;
    } else {
        let cpath = CString::new(spec_str)
            .with_context(|| format!("device spec contains a null byte: '{spec_str}'"))?;
        device_remove(fd, DeviceSpec::Path(&cpath))
            .with_context(|| format!("failed to remove device '{spec_str}'"))?;
    }
    Ok(())
}

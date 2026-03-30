use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::device::device_ready;
use clap::Parser;
use nix::errno::Errno;
use std::{ffi::CString, path::PathBuf};

/// Check whether all devices of a multi-device filesystem are present
///
/// Opens /dev/btrfs-control and queries the kernel for the given device.
/// Exits with code 0 if all member devices are present and the filesystem
/// is ready to mount, or a non-zero code if the device set is incomplete.
#[derive(Parser, Debug)]
pub struct DeviceReadyCommand {
    /// A block device belonging to the multi-device filesystem to check
    pub device: PathBuf,
}

impl Runnable for DeviceReadyCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let path_str = self.device.to_str().ok_or_else(|| {
            anyhow::anyhow!(
                "device path is not valid UTF-8: '{}'",
                self.device.display()
            )
        })?;

        let cpath = CString::new(path_str).with_context(|| {
            format!(
                "device path contains a null byte: '{}'",
                self.device.display()
            )
        })?;

        match device_ready(&cpath) {
            Ok(()) => {
                println!("'{}' is ready for mount", self.device.display());
                Ok(())
            }
            Err(Errno::ENOENT | Errno::ENXIO) => {
                anyhow::bail!(
                    "'{}': not all devices are present, filesystem is not ready",
                    self.device.display()
                )
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "failed to check device readiness for '{}'",
                    self.device.display()
                )
            }),
        }
    }
}

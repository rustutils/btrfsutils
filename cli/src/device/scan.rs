use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::device::{device_forget, device_scan};
use clap::Parser;
use std::{ffi::CString, path::PathBuf};

/// Scan or unregister devices for multi-device btrfs filesystems
///
/// Without --forget, registers each given device with the kernel so that
/// multi-device filesystems can be assembled and mounted.
///
/// With --forget, unregisters the given devices (or all stale devices if none
/// are specified) so the kernel no longer tracks them.
#[derive(Parser, Debug)]
pub struct DeviceScanCommand {
    /// Unregister devices instead of registering them.
    ///
    /// If no devices are given, all devices that are not part of a currently
    /// mounted filesystem are unregistered.
    #[clap(long, short = 'u', alias = "forget")]
    pub forget: bool,

    /// Block devices to scan or unregister
    pub devices: Vec<PathBuf>,
}

impl Runnable for DeviceScanCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.forget {
            if self.devices.is_empty() {
                device_forget(None).context("failed to unregister stale devices from kernel")?;
                println!("unregistered all stale devices");
            } else {
                let mut had_error = false;
                for device in &self.devices {
                    match forget_one(device) {
                        Ok(()) => println!("unregistered '{}'", device.display()),
                        Err(e) => {
                            eprintln!("error unregistering '{}': {e}", device.display());
                            had_error = true;
                        }
                    }
                }
                if had_error {
                    anyhow::bail!("one or more devices could not be unregistered");
                }
            }
        } else {
            if self.devices.is_empty() {
                anyhow::bail!(
                    "scanning all block devices is not yet implemented; \
                     please specify one or more device paths explicitly"
                );
            }
            let mut had_error = false;
            for device in &self.devices {
                match scan_one(device) {
                    Ok(()) => println!("registered '{}'", device.display()),
                    Err(e) => {
                        eprintln!("error registering '{}': {e}", device.display());
                        had_error = true;
                    }
                }
            }
            if had_error {
                anyhow::bail!("one or more devices could not be registered");
            }
        }

        Ok(())
    }
}

fn scan_one(device: &PathBuf) -> Result<()> {
    let path_str = device
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: '{}'", device.display()))?;
    let cpath = CString::new(path_str)
        .with_context(|| format!("path contains a null byte: '{}'", device.display()))?;
    device_scan(&cpath).with_context(|| format!("failed to register '{}'", device.display()))?;
    Ok(())
}

fn forget_one(device: &PathBuf) -> Result<()> {
    let path_str = device
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: '{}'", device.display()))?;
    let cpath = CString::new(path_str)
        .with_context(|| format!("path contains a null byte: '{}'", device.display()))?;
    device_forget(Some(&cpath))
        .with_context(|| format!("failed to unregister '{}'", device.display()))?;
    Ok(())
}

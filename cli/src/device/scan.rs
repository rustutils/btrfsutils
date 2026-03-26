use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::device::{device_forget, device_scan};
use clap::Parser;
use std::{ffi::CString, fs, path::PathBuf};

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
                device_forget(None).context(
                    "failed to unregister stale devices from kernel",
                )?;
                println!("unregistered all stale devices");
            } else {
                let mut had_error = false;
                for device in &self.devices {
                    match forget_one(device) {
                        Ok(()) => {
                            println!("unregistered '{}'", device.display())
                        }
                        Err(e) => {
                            eprintln!(
                                "error unregistering '{}': {e}",
                                device.display()
                            );
                            had_error = true;
                        }
                    }
                }
                if had_error {
                    anyhow::bail!(
                        "one or more devices could not be unregistered"
                    );
                }
            }
        } else if self.devices.is_empty() {
            scan_all()?;
        } else {
            let mut had_error = false;
            for device in &self.devices {
                match scan_one(device) {
                    Ok(()) => println!("registered '{}'", device.display()),
                    Err(e) => {
                        eprintln!(
                            "error registering '{}': {e}",
                            device.display()
                        );
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

/// Scan all block devices on the system for btrfs filesystems.
///
/// Enumerates devices from /proc/partitions and attempts to register each one.
/// Non-btrfs devices silently fail (the kernel rejects them), so only devices
/// that the kernel actually recognizes as btrfs are reported.
fn scan_all() -> Result<()> {
    let devices = block_devices_from_proc_partitions()
        .context("failed to enumerate block devices from /proc/partitions")?;

    if devices.is_empty() {
        println!("no block devices found");
        return Ok(());
    }

    let mut registered = 0u32;
    for device in &devices {
        let path_str = match device.to_str() {
            Some(s) => s,
            None => continue,
        };
        let cpath = match CString::new(path_str) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if device_scan(&cpath).is_ok() {
            println!("registered '{}'", device.display());
            registered += 1;
        }
    }

    if registered == 0 {
        println!("no btrfs devices found");
    }

    Ok(())
}

/// Parse /proc/partitions and return /dev/ paths for all block devices.
///
/// /proc/partitions has a two-line header followed by lines of the form:
///   major minor #blocks name
fn block_devices_from_proc_partitions() -> Result<Vec<PathBuf>> {
    let contents = fs::read_to_string("/proc/partitions")
        .context("failed to read /proc/partitions")?;

    let mut devices = Vec::new();
    for line in contents.lines().skip(2) {
        let name = match line.split_whitespace().nth(3) {
            Some(n) => n,
            None => continue,
        };
        devices.push(PathBuf::from(format!("/dev/{name}")));
    }
    Ok(devices)
}

fn scan_one(device: &PathBuf) -> Result<()> {
    let path_str = device.to_str().ok_or_else(|| {
        anyhow::anyhow!("path is not valid UTF-8: '{}'", device.display())
    })?;
    let cpath = CString::new(path_str).with_context(|| {
        format!("path contains a null byte: '{}'", device.display())
    })?;
    device_scan(&cpath).with_context(|| {
        format!("failed to register '{}'", device.display())
    })?;
    Ok(())
}

fn forget_one(device: &PathBuf) -> Result<()> {
    let path_str = device.to_str().ok_or_else(|| {
        anyhow::anyhow!("path is not valid UTF-8: '{}'", device.display())
    })?;
    let cpath = CString::new(path_str).with_context(|| {
        format!("path contains a null byte: '{}'", device.display())
    })?;
    device_forget(Some(&cpath)).with_context(|| {
        format!("failed to unregister '{}'", device.display())
    })?;
    Ok(())
}

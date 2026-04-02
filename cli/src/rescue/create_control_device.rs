use crate::{RunContext, Runnable};
use anyhow::{Context, Result};
use clap::Parser;
use std::ffi::CString;

/// Create /dev/btrfs-control
///
/// The btrfs control device is a character device (major 10, minor 234) used
/// by the btrfs kernel module for management operations. This command creates
/// it if it is missing, for example after a minimal system installation or
/// when udev is not running.
#[derive(Parser, Debug)]
pub struct RescueCreateControlDeviceCommand {}

impl Runnable for RescueCreateControlDeviceCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        const PATH: &str = "/dev/btrfs-control";
        let dev = libc::makedev(10, 234);
        let path = CString::new(PATH).unwrap();
        // SAFETY: path is a valid NUL-terminated string from CString.
        let ret =
            unsafe { libc::mknod(path.as_ptr(), libc::S_IFCHR | 0o600, dev) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("could not create {PATH}"));
        }
        Ok(())
    }
}

use crate::{
    Format, Runnable,
    util::{ParsedUuid, parse_size_with_suffix},
};
use anyhow::{Context, Result};
use clap::Parser;
use nix::{
    fcntl::{FallocateFlags, fallocate},
    libc,
};
use std::{
    fs::{File, OpenOptions},
    os::unix::{
        fs::{FileExt, OpenOptionsExt},
        io::AsRawFd,
    },
    path::PathBuf,
};
use uuid::Uuid;

const FS_NOCOW_FL: libc::c_long = 0x0080_0000;
const MIN_SWAP_SIZE: u64 = 40 * 1024;

/// Create a swapfile on a btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemMkswapfileCommand {
    /// Size of the swapfile
    #[clap(long, short, default_value = "2G")]
    pub size: String,

    /// UUID to embed in the swap header (clear, random, time, or explicit UUID;
    /// default: random)
    #[clap(long = "uuid", short = 'U')]
    pub uuid: Option<ParsedUuid>,

    /// Path to the swapfile to create
    pub path: PathBuf,
}

fn system_page_size() -> Result<u64> {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    anyhow::ensure!(size > 0, "failed to get system page size");
    #[allow(clippy::cast_sign_loss)]
    // sysconf returns positive value after ensure check
    Ok(size as u64)
}

fn write_swap_header(
    file: &File,
    page_count: u32,
    uuid: &Uuid,
    page_size: u64,
) -> Result<()> {
    #[allow(clippy::cast_possible_truncation)] // page_size fits in usize
    let mut header = vec![0u8; page_size as usize];
    header[0x400] = 0x01;
    header[0x404..0x408].copy_from_slice(&page_count.to_le_bytes());
    header[0x40c..0x41c].copy_from_slice(uuid.as_bytes());
    #[allow(clippy::cast_possible_truncation)] // page_size fits in usize
    let sig_offset = page_size as usize - 10;
    header[sig_offset..].copy_from_slice(b"SWAPSPACE2");
    file.write_at(&header, 0)
        .context("failed to write swap header")?;
    Ok(())
}

impl Runnable for FilesystemMkswapfileCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let size = parse_size_with_suffix(&self.size)
            .with_context(|| format!("invalid size: '{}'", self.size))?;

        let page_size = system_page_size()?;

        anyhow::ensure!(
            size >= MIN_SWAP_SIZE,
            "swapfile needs to be at least 40 KiB, got {size} bytes"
        );

        let uuid = self.uuid.as_deref().copied().unwrap_or_else(Uuid::new_v4);

        let size = size - (size % page_size);
        let total_pages = size / page_size;

        anyhow::ensure!(
            total_pages > 10,
            "swapfile too small after page alignment"
        );

        let page_count = total_pages - 1;
        anyhow::ensure!(
            u32::try_from(page_count).is_ok(),
            "swapfile too large: page count exceeds u32"
        );

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.path)
            .with_context(|| {
                format!("failed to create '{}'", self.path.display())
            })?;

        let ret = unsafe {
            libc::ioctl(file.as_raw_fd(), libc::FS_IOC_SETFLAGS, &FS_NOCOW_FL)
        };
        nix::errno::Errno::result(ret)
            .context("failed to set NOCOW attribute")?;

        #[allow(clippy::cast_possible_wrap)] // size fits in off_t
        fallocate(&file, FallocateFlags::empty(), 0, size as libc::off_t)
            .context("failed to allocate space for swapfile")?;

        #[allow(clippy::cast_possible_truncation)]
        // validated above with try_from
        write_swap_header(&file, page_count as u32, &uuid, page_size)?;

        println!(
            "created swapfile '{}' size {} bytes",
            self.path.display(),
            crate::util::human_bytes(size),
        );

        Ok(())
    }
}

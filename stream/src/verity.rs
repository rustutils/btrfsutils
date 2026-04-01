//! # File integrity: fs-verity support
//!
//! Wraps the `FS_IOC_ENABLE_VERITY` ioctl, which enables fs-verity on a file.
//! This is a VFS ioctl (not btrfs-specific) defined in `linux/fsverity.h`.

use std::os::{fd::AsRawFd, unix::io::BorrowedFd};

// FS_IOC_ENABLE_VERITY = _IOW('f', 133, struct fsverity_enable_arg)
nix::ioctl_write_ptr!(fs_ioc_enable_verity, b'f', 133, FsverityEnableArg);

/// Arguments for the `FS_IOC_ENABLE_VERITY` ioctl.
///
/// Mirrors `struct fsverity_enable_arg` from `linux/fsverity.h`.
#[repr(C)]
pub struct FsverityEnableArg {
    pub version: u32,
    pub hash_algorithm: u32,
    pub block_size: u32,
    pub salt_size: u32,
    pub salt_ptr: u64,
    pub sig_size: u32,
    __reserved1: u32,
    pub sig_ptr: u64,
    __reserved2: [u64; 11],
}

/// Enable fs-verity on the file referred to by `fd`.
///
/// The file must be opened read-only and must not already have verity enabled.
/// `algorithm` is the hash algorithm (1 = SHA-256, 2 = SHA-512), `block_size`
/// must be a power of two between 1024 and 65536 (filesystem block size is
/// typical).
///
/// # Errors
///
/// Returns an error if the ioctl fails (e.g. verity already enabled, file
/// not read-only, or unsupported filesystem).
#[allow(clippy::cast_possible_truncation)] // salt/sig lengths fit in u32
pub fn enable_verity(
    fd: BorrowedFd,
    algorithm: u8,
    block_size: u32,
    salt: &[u8],
    sig: &[u8],
) -> nix::Result<()> {
    let arg = FsverityEnableArg {
        version: 1,
        hash_algorithm: u32::from(algorithm),
        block_size,
        salt_size: salt.len() as u32,
        salt_ptr: if salt.is_empty() {
            0
        } else {
            salt.as_ptr() as u64
        },
        sig_size: sig.len() as u32,
        __reserved1: 0,
        sig_ptr: if sig.is_empty() {
            0
        } else {
            sig.as_ptr() as u64
        },
        __reserved2: [0; 11],
    };
    unsafe { fs_ioc_enable_verity(fd.as_raw_fd(), &raw const arg) }?;
    Ok(())
}

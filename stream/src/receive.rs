use crate::stream::{StreamCommand, Timespec};
use anyhow::{Context, Result, anyhow, bail};
use btrfs_uapi::{
    inode::subvolid_resolve,
    send_receive::{
        clone_range, encoded_write, received_subvol_set,
        subvolume_search_by_received_uuid, subvolume_search_by_uuid,
    },
    subvolume::{
        SubvolumeFlags, snapshot_create, subvolume_create, subvolume_flags_get,
        subvolume_flags_set, subvolume_info,
    },
};
use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::{AsFd, AsRawFd},
        unix::fs::PermissionsExt,
    },
    path::{Path, PathBuf},
};

/// Find the mount root for a given path by walking up the directory tree
/// while the device ID (`st_dev`) remains the same. The mount root is the
/// highest directory on the same device.
///
/// Note: this will not detect bind mounts of a subdirectory (where `st_dev`
/// is the same all the way to `/`). This matches the C btrfs-progs behavior.
fn find_mount_root(path: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::MetadataExt;

    let path = path
        .canonicalize()
        .with_context(|| format!("cannot canonicalize '{}'", path.display()))?;
    let dev = path
        .metadata()
        .with_context(|| format!("cannot stat '{}'", path.display()))?
        .dev();

    let mut root = path.clone();
    while let Some(parent) = root.parent() {
        let parent_dev = parent
            .metadata()
            .with_context(|| format!("cannot stat '{}'", parent.display()))?
            .dev();
        if parent_dev != dev {
            break;
        }
        root = parent.to_path_buf();
    }

    Ok(root)
}

/// Applies a parsed btrfs send stream to a mounted btrfs filesystem.
///
/// `ReceiveContext` is the receive-side counterpart to the kernel's
/// `BTRFS_IOC_SEND`. It takes [`StreamCommand`] values produced by
/// [`StreamReader`][crate::StreamReader] and executes the corresponding
/// filesystem operations to recreate the sent subvolume on the destination.
///
/// The typical usage pattern is:
///
/// 1. Create a context with [`ReceiveContext::new`], pointing at the
///    destination directory (must be on a mounted btrfs filesystem).
/// 2. Feed each command from the stream into [`process_command`][Self::process_command].
/// 3. When the stream yields [`StreamCommand::End`], call
///    [`finish_subvol`][Self::finish_subvol] to finalize the received
///    subvolume (sets the received UUID and marks it read-only).
/// 4. For multi-stream input, repeat from step 2 with a new stream reader.
///
/// Supported operations:
///
/// v1 commands: subvolume and snapshot creation, file/directory/symlink/fifo/
/// socket/device node creation, rename, link, unlink, rmdir, write (with fd
/// caching for sequential writes to the same file), clone range (resolves
/// source subvolume via UUID tree lookup), xattr set/remove, truncate, chmod,
/// chown, utimes. UpdateExtent is a no-op (informational only).
///
/// v2 commands: encoded write (passes compressed data directly to the kernel
/// via `BTRFS_IOC_ENCODED_WRITE`, with automatic decompression fallback for
/// zlib, zstd, and lzo when the ioctl is unavailable or fails), fallocate
/// (preallocate and punch hole). Fileattr is intentionally a no-op, matching
/// the C reference.
///
/// v3 commands: enable verity (`FS_IOC_ENABLE_VERITY`).
///
/// Snapshot creation resolves the parent subvolume by searching the UUID tree
/// for the received UUID first, then falling back to the regular UUID. The
/// parent's ctransid is verified against both ctransid and stransid to handle
/// parents that were themselves received.
///
/// Requires `CAP_SYS_ADMIN` and a mounted, writable btrfs filesystem.
pub struct ReceiveContext {
    /// File descriptor to the mount root (for UUID tree searches and snapshots).
    mnt_fd: File,
    /// Absolute path of the filesystem mount root (for resolving subvolid paths).
    mount_root: PathBuf,
    /// Absolute path of the destination directory.
    dest_dir: PathBuf,
    /// Path of the current subvolume being received (relative name).
    cur_subvol: Option<String>,
    /// Absolute path of the current subvolume.
    cur_subvol_path: Option<PathBuf>,
    /// UUID from the stream's SUBVOL/SNAPSHOT command (for SET_RECEIVED_SUBVOL).
    received_uuid: Option<uuid::Uuid>,
    /// ctransid from the stream (for SET_RECEIVED_SUBVOL).
    stransid: u64,
    /// Cached write fd: keep one file open to avoid repeated open/close for
    /// sequential writes to the same file.
    write_fd: Option<File>,
    write_path: PathBuf,
}

impl ReceiveContext {
    /// Create a new receive context rooted at `dest_dir`.
    ///
    /// `dest_dir` must be a directory on a mounted btrfs filesystem. The mount
    /// root is auto-detected by walking up the directory tree while the device
    /// ID stays the same. An fd to the mount root is kept open for UUID tree
    /// lookups; subvolumes are created under `dest_dir`.
    pub fn new(dest_dir: &Path) -> Result<Self> {
        let mount_root = find_mount_root(dest_dir)?;
        let mnt_fd = File::open(&mount_root).with_context(|| {
            format!("cannot open mount root '{}'", mount_root.display())
        })?;

        Ok(Self {
            mnt_fd,
            mount_root,
            dest_dir: dest_dir.to_path_buf(),
            cur_subvol: None,
            cur_subvol_path: None,
            received_uuid: None,
            stransid: 0,
            write_fd: None,
            write_path: PathBuf::new(),
        })
    }

    /// Dispatch and execute a single stream command.
    ///
    /// The caller is responsible for handling [`StreamCommand::End`] before
    /// calling this method (it will panic on `End`). All other command types
    /// are dispatched to the appropriate handler. Paths in the command are
    /// resolved relative to the current subvolume within the destination
    /// directory.
    pub fn process_command(&mut self, cmd: &StreamCommand) -> Result<()> {
        match cmd {
            StreamCommand::Subvol {
                path,
                uuid,
                ctransid,
            } => self.process_subvol(path, uuid, *ctransid),
            StreamCommand::Snapshot {
                path,
                uuid,
                ctransid,
                clone_uuid,
                clone_ctransid,
            } => self.process_snapshot(
                path,
                uuid,
                *ctransid,
                clone_uuid,
                *clone_ctransid,
            ),
            StreamCommand::Mkfile { path } => self.process_mkfile(path),
            StreamCommand::Mkdir { path } => self.process_mkdir(path),
            StreamCommand::Mknod { path, mode, rdev } => {
                self.process_mknod(path, *mode, *rdev)
            }
            StreamCommand::Mkfifo { path } => self.process_mkfifo(path),
            StreamCommand::Mksock { path } => self.process_mksock(path),
            StreamCommand::Symlink { path, target } => {
                self.process_symlink(path, target)
            }
            StreamCommand::Rename { from, to } => self.process_rename(from, to),
            StreamCommand::Link { path, target } => {
                self.process_link(path, target)
            }
            StreamCommand::Unlink { path } => self.process_unlink(path),
            StreamCommand::Rmdir { path } => self.process_rmdir(path),
            StreamCommand::Write { path, offset, data } => {
                self.process_write(path, *offset, data)
            }
            StreamCommand::Clone {
                path,
                offset,
                len,
                clone_uuid,
                clone_ctransid,
                clone_path,
                clone_offset,
            } => self.process_clone(
                path,
                *offset,
                *len,
                clone_uuid,
                *clone_ctransid,
                clone_path,
                *clone_offset,
            ),
            StreamCommand::SetXattr { path, name, data } => {
                self.process_set_xattr(path, name, data)
            }
            StreamCommand::RemoveXattr { path, name } => {
                self.process_remove_xattr(path, name)
            }
            StreamCommand::Truncate { path, size } => {
                self.process_truncate(path, *size)
            }
            StreamCommand::Chmod { path, mode } => {
                self.process_chmod(path, *mode)
            }
            StreamCommand::Chown { path, uid, gid } => {
                self.process_chown(path, *uid, *gid)
            }
            StreamCommand::Utimes {
                path, atime, mtime, ..
            } => self.process_utimes(path, atime, mtime),
            StreamCommand::UpdateExtent { .. } => Ok(()),
            StreamCommand::EncodedWrite {
                path,
                offset,
                unencoded_file_len,
                unencoded_len,
                unencoded_offset,
                compression,
                encryption,
                data,
            } => self.process_encoded_write(
                path,
                *offset,
                *unencoded_file_len,
                *unencoded_len,
                *unencoded_offset,
                *compression,
                *encryption,
                data,
            ),
            StreamCommand::Fallocate {
                path,
                mode,
                offset,
                len,
            } => self.process_fallocate(path, *mode, *offset, *len),
            StreamCommand::Fileattr { .. } => {
                // Intentionally a no-op, matching C reference.  File
                // attributes (chattr flags) are filesystem-internal and
                // not reliably transferable across systems.
                Ok(())
            }
            StreamCommand::EnableVerity {
                path,
                algorithm,
                block_size,
                salt,
                sig,
            } => self.process_enable_verity(
                path,
                *algorithm,
                *block_size,
                salt,
                sig,
            ),
            StreamCommand::End => unreachable!("End is handled by the caller"),
        }
    }

    /// Finalize the current subvolume after all commands have been applied.
    ///
    /// This sets the received UUID and stransid on the subvolume via
    /// `BTRFS_IOC_SET_RECEIVED_SUBVOL`, then marks it read-only. Call this
    /// after processing a [`StreamCommand::End`] or at EOF if a subvolume
    /// was in progress. Safe to call when no subvolume is active (returns
    /// `Ok(())` immediately).
    pub fn finish_subvol(&mut self) -> Result<()> {
        self.close_write_fd();

        let subvol_path = match &self.cur_subvol_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        let uuid = match &self.received_uuid {
            Some(u) => *u,
            None => return Ok(()),
        };

        let subvol_file = File::open(&subvol_path).with_context(|| {
            format!("cannot open subvolume '{}'", subvol_path.display())
        })?;
        let fd = subvol_file.as_fd();

        received_subvol_set(fd, &uuid, self.stransid).with_context(|| {
            format!(
                "failed to set received subvol on '{}'",
                subvol_path.display()
            )
        })?;

        // Make the subvolume read-only.
        let flags = subvolume_flags_get(fd).with_context(|| {
            format!("failed to get flags for '{}'", subvol_path.display())
        })?;
        subvolume_flags_set(fd, flags | SubvolumeFlags::RDONLY).with_context(
            || {
                format!(
                    "failed to set read-only on '{}'",
                    subvol_path.display()
                )
            },
        )?;

        self.cur_subvol = None;
        self.cur_subvol_path = None;
        self.received_uuid = None;
        self.stransid = 0;

        Ok(())
    }

    /// Close the cached write file descriptor, if any.
    ///
    /// Write operations cache an open fd to avoid repeated open/close when
    /// the stream contains sequential writes to the same file. Call this
    /// before operations that require no open writable fds (e.g. enabling
    /// verity) or when switching subvolumes.
    pub fn close_write_fd(&mut self) {
        self.write_fd = None;
        self.write_path = PathBuf::new();
    }

    fn full_path(&self, relative: &str) -> Result<PathBuf> {
        let subvol_path = self
            .cur_subvol_path
            .as_ref()
            .ok_or_else(|| anyhow!("no current subvolume"))?;
        Ok(subvol_path.join(relative))
    }

    fn process_subvol(
        &mut self,
        path: &str,
        uuid: &uuid::Uuid,
        ctransid: u64,
    ) -> Result<()> {
        self.finish_subvol()?;

        let subvol_path = self.dest_dir.join(path);

        // Create subvolume using the parent directory fd.
        let parent_dir = File::open(&self.dest_dir).with_context(|| {
            format!("cannot open '{}'", self.dest_dir.display())
        })?;
        let c_name = CString::new(path).with_context(|| {
            format!("subvolume name contains null byte: {path}")
        })?;
        subvolume_create(parent_dir.as_fd(), &c_name, &[]).with_context(
            || {
                format!(
                    "failed to create subvolume '{}'",
                    subvol_path.display()
                )
            },
        )?;

        self.cur_subvol = Some(path.to_string());
        self.cur_subvol_path = Some(subvol_path);
        self.received_uuid = Some(*uuid);
        self.stransid = ctransid;

        Ok(())
    }

    fn process_snapshot(
        &mut self,
        path: &str,
        uuid: &uuid::Uuid,
        ctransid: u64,
        clone_uuid: &uuid::Uuid,
        clone_ctransid: u64,
    ) -> Result<()> {
        self.finish_subvol()?;

        let subvol_path = self.dest_dir.join(path);

        // Find the parent subvolume by its received UUID, then fall back to UUID.
        let parent_root_id = subvolume_search_by_received_uuid(
            self.mnt_fd.as_fd(),
            clone_uuid,
        )
        .or_else(|_| subvolume_search_by_uuid(self.mnt_fd.as_fd(), clone_uuid))
        .with_context(|| {
            format!(
                "cannot find parent subvolume with UUID {} for snapshot '{}'",
                clone_uuid.as_hyphenated(),
                path
            )
        })?;

        // Verify the parent's ctransid matches.
        let parent_path = subvolid_resolve(self.mnt_fd.as_fd(), parent_root_id)
            .with_context(|| {
                format!(
                    "cannot resolve path for parent subvolume {parent_root_id}"
                )
            })?;

        // Open the parent subvolume to verify ctransid and create the snapshot.
        // The path from subvolid_resolve is relative to the filesystem root,
        // so join with mount_root, not dest_dir.
        let parent_full = self.mount_root.join(&parent_path);
        let parent_file = File::open(&parent_full).with_context(|| {
            format!("cannot open parent subvolume '{}'", parent_full.display())
        })?;

        let parent_info =
            subvolume_info(parent_file.as_fd()).with_context(|| {
                format!(
                    "failed to get info for parent '{}'",
                    parent_full.display()
                )
            })?;

        // The parent's ctransid must match: check both ctransid and stransid
        // (stransid is set when the parent was itself received).
        if parent_info.ctransid != clone_ctransid
            && parent_info.stransid != clone_ctransid
        {
            bail!(
                "parent subvolume '{}' ctransid mismatch: stream expects {}, found ctransid={} stransid={}",
                parent_path,
                clone_ctransid,
                parent_info.ctransid,
                parent_info.stransid,
            );
        }

        // Create the snapshot.
        let dest_dir_file = File::open(&self.dest_dir).with_context(|| {
            format!("cannot open '{}'", self.dest_dir.display())
        })?;
        let c_name = CString::new(path).with_context(|| {
            format!("snapshot name contains null byte: {path}")
        })?;
        snapshot_create(
            dest_dir_file.as_fd(),
            parent_file.as_fd(),
            &c_name,
            false,
            &[],
        )
        .with_context(|| {
            format!("failed to create snapshot '{}'", subvol_path.display())
        })?;

        // Make the snapshot writable so we can apply the stream delta.
        let snap_file = File::open(&subvol_path).with_context(|| {
            format!("cannot open snapshot '{}'", subvol_path.display())
        })?;
        let snap_flags =
            subvolume_flags_get(snap_file.as_fd()).with_context(|| {
                format!("failed to get flags for '{}'", subvol_path.display())
            })?;
        if snap_flags.contains(SubvolumeFlags::RDONLY) {
            subvolume_flags_set(
                snap_file.as_fd(),
                snap_flags & !SubvolumeFlags::RDONLY,
            )
            .with_context(|| {
                format!(
                    "failed to make snapshot '{}' writable",
                    subvol_path.display()
                )
            })?;
        }

        self.cur_subvol = Some(path.to_string());
        self.cur_subvol_path = Some(subvol_path);
        self.received_uuid = Some(*uuid);
        self.stransid = ctransid;

        Ok(())
    }

    fn process_mkfile(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        File::create(&full).with_context(|| {
            format!("failed to create file '{}'", full.display())
        })?;
        Ok(())
    }

    fn process_mkdir(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        fs::create_dir(&full).with_context(|| {
            format!("failed to create directory '{}'", full.display())
        })?;
        Ok(())
    }

    fn process_mknod(
        &mut self,
        path: &str,
        mode: u64,
        rdev: u64,
    ) -> Result<()> {
        self.do_mknod(path, mode as nix::libc::mode_t, rdev as nix::libc::dev_t)
    }

    fn process_mkfifo(&mut self, path: &str) -> Result<()> {
        self.do_mknod(path, nix::libc::S_IFIFO | 0o600, 0)
    }

    fn process_mksock(&mut self, path: &str) -> Result<()> {
        self.do_mknod(path, nix::libc::S_IFSOCK | 0o600, 0)
    }

    fn do_mknod(
        &mut self,
        path: &str,
        mode: nix::libc::mode_t,
        rdev: nix::libc::dev_t,
    ) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe { nix::libc::mknod(c_path.as_ptr(), mode, rdev) };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("mknod failed for '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_symlink(&mut self, path: &str, target: &str) -> Result<()> {
        let full = self.full_path(path)?;
        std::os::unix::fs::symlink(target, &full).with_context(|| {
            format!("failed to create symlink '{}'", full.display())
        })?;
        Ok(())
    }

    fn process_rename(&mut self, from: &str, to: &str) -> Result<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        fs::rename(&full_from, &full_to).with_context(|| {
            format!(
                "failed to rename '{}' to '{}'",
                full_from.display(),
                full_to.display()
            )
        })?;
        Ok(())
    }

    fn process_link(&mut self, path: &str, target: &str) -> Result<()> {
        let full_path = self.full_path(path)?;
        let full_target = self.full_path(target)?;
        fs::hard_link(&full_target, &full_path).with_context(|| {
            format!(
                "failed to hard link '{}' -> '{}'",
                full_path.display(),
                full_target.display()
            )
        })?;
        Ok(())
    }

    fn process_unlink(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        // Close cached write fd if it points to this file.
        if self.write_path == full {
            self.close_write_fd();
        }
        fs::remove_file(&full).with_context(|| {
            format!("failed to unlink '{}'", full.display())
        })?;
        Ok(())
    }

    fn process_rmdir(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        fs::remove_dir(&full)
            .with_context(|| format!("failed to rmdir '{}'", full.display()))?;
        Ok(())
    }

    fn process_write(
        &mut self,
        path: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let full = self.full_path(path)?;

        // Reuse cached fd if writing to the same file.
        if self.write_path != full {
            self.close_write_fd();
            let file =
                OpenOptions::new().write(true).open(&full).with_context(
                    || format!("cannot open '{}' for writing", full.display()),
                )?;
            self.write_fd = Some(file);
            self.write_path = full.clone();
        }

        let fd = self.write_fd.as_ref().unwrap();
        let written = unsafe {
            nix::libc::pwrite(
                fd.as_raw_fd(),
                data.as_ptr() as *const nix::libc::c_void,
                data.len(),
                offset as nix::libc::off_t,
            )
        };
        if written < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("pwrite failed on '{}'", full.display())
            });
        }
        if (written as usize) != data.len() {
            bail!(
                "short pwrite on '{}': wrote {} of {} bytes",
                full.display(),
                written,
                data.len()
            );
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_clone(
        &mut self,
        path: &str,
        offset: u64,
        len: u64,
        clone_uuid: &uuid::Uuid,
        _clone_ctransid: u64,
        clone_path: &str,
        clone_offset: u64,
    ) -> Result<()> {
        let full = self.full_path(path)?;

        // Find the clone source subvolume.
        let clone_subvol_root =
            subvolume_search_by_received_uuid(self.mnt_fd.as_fd(), clone_uuid)
                .or_else(|_| {
                    subvolume_search_by_uuid(self.mnt_fd.as_fd(), clone_uuid)
                })
                .with_context(|| {
                    format!(
                        "cannot find clone source subvolume with UUID {}",
                        clone_uuid.as_hyphenated()
                    )
                })?;

        let subvol_path =
            subvolid_resolve(self.mnt_fd.as_fd(), clone_subvol_root)
                .with_context(|| {
                    format!("cannot resolve path for clone source subvolume {clone_subvol_root}")
                })?;

        // The path from subvolid_resolve is relative to the filesystem root,
        // so join with mount_root, not dest_dir.
        let clone_full = self.mount_root.join(&subvol_path).join(clone_path);
        let clone_file = File::open(&clone_full).with_context(|| {
            format!("cannot open clone source '{}'", clone_full.display())
        })?;

        // Close cached write fd if it's for this file — we need a fresh fd.
        if self.write_path == full {
            self.close_write_fd();
        }

        let dest_file = OpenOptions::new()
            .write(true)
            .open(&full)
            .with_context(|| {
                format!("cannot open '{}' for clone", full.display())
            })?;

        clone_range(
            dest_file.as_fd(),
            clone_file.as_fd(),
            clone_offset,
            len,
            offset,
        )
        .with_context(|| {
            format!("clone_range failed on '{}'", full.display())
        })?;

        Ok(())
    }

    fn process_set_xattr(
        &mut self,
        path: &str,
        name: &str,
        data: &[u8],
    ) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let c_name = CString::new(name)
            .with_context(|| format!("invalid xattr name: {name}"))?;
        let ret = unsafe {
            nix::libc::lsetxattr(
                c_path.as_ptr(),
                c_name.as_ptr(),
                data.as_ptr() as *const nix::libc::c_void,
                data.len(),
                0,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("lsetxattr failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_remove_xattr(&mut self, path: &str, name: &str) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let c_name = CString::new(name)
            .with_context(|| format!("invalid xattr name: {name}"))?;
        let ret = unsafe {
            nix::libc::lremovexattr(c_path.as_ptr(), c_name.as_ptr())
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("lremovexattr failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_truncate(&mut self, path: &str, size: u64) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe {
            nix::libc::truncate(c_path.as_ptr(), size as nix::libc::off_t)
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("truncate failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_chmod(&mut self, path: &str, mode: u64) -> Result<()> {
        let full = self.full_path(path)?;
        fs::set_permissions(&full, fs::Permissions::from_mode(mode as u32))
            .with_context(|| format!("chmod failed on '{}'", full.display()))?;
        Ok(())
    }

    fn process_chown(&mut self, path: &str, uid: u64, gid: u64) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe {
            nix::libc::lchown(
                c_path.as_ptr(),
                uid as nix::libc::uid_t,
                gid as nix::libc::gid_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("lchown failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_utimes(
        &mut self,
        path: &str,
        atime: &Timespec,
        mtime: &Timespec,
    ) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let times = [
            nix::libc::timespec {
                tv_sec: atime.sec as i64,
                tv_nsec: atime.nsec as nix::libc::c_long,
            },
            nix::libc::timespec {
                tv_sec: mtime.sec as i64,
                tv_nsec: mtime.nsec as nix::libc::c_long,
            },
        ];
        let ret = unsafe {
            nix::libc::utimensat(
                nix::libc::AT_FDCWD,
                c_path.as_ptr(),
                times.as_ptr(),
                nix::libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("utimensat failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_fallocate(
        &mut self,
        path: &str,
        mode: u32,
        offset: u64,
        len: u64,
    ) -> Result<()> {
        let full = self.full_path(path)?;
        self.close_write_fd();
        let file =
            OpenOptions::new()
                .write(true)
                .open(&full)
                .with_context(|| {
                    format!("cannot open '{}' for fallocate", full.display())
                })?;
        let ret = unsafe {
            nix::libc::fallocate(
                file.as_raw_fd(),
                mode as i32,
                offset as nix::libc::off_t,
                len as nix::libc::off_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("fallocate failed on '{}'", full.display())
            });
        }
        Ok(())
    }

    fn process_enable_verity(
        &mut self,
        path: &str,
        algorithm: u8,
        block_size: u32,
        salt: &[u8],
        sig: &[u8],
    ) -> Result<()> {
        let full = self.full_path(path)?;

        // Must close any cached write fd first: enabling verity requires no
        // open writable file descriptors.
        self.close_write_fd();

        // fs-verity requires the file to be opened read-only.
        let file = File::open(&full).with_context(|| {
            format!("cannot open '{}' for verity", full.display())
        })?;

        crate::verity::enable_verity(
            file.as_fd(),
            algorithm,
            block_size,
            salt,
            sig,
        )
        .with_context(|| {
            format!("FS_IOC_ENABLE_VERITY failed on '{}'", full.display())
        })?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_encoded_write(
        &mut self,
        path: &str,
        offset: u64,
        unencoded_file_len: u64,
        unencoded_len: u64,
        unencoded_offset: u64,
        compression: u32,
        encryption: u32,
        data: &[u8],
    ) -> Result<()> {
        if encryption != 0 {
            bail!(
                "encrypted encoded writes are not supported (encryption={encryption})"
            );
        }

        let full = self.full_path(path)?;

        // Reuse cached fd if writing to the same file.
        if self.write_path != full {
            self.close_write_fd();
            let file =
                OpenOptions::new().write(true).open(&full).with_context(
                    || format!("cannot open '{}' for writing", full.display()),
                )?;
            self.write_fd = Some(file);
            self.write_path = full.clone();
        }

        // Try the encoded write ioctl first — passes compressed data directly
        // to the filesystem without decompression.
        let fd = self.write_fd.as_ref().unwrap();
        match encoded_write(
            fd.as_fd(),
            data,
            offset,
            unencoded_file_len,
            unencoded_len,
            unencoded_offset,
            compression,
            encryption,
        ) {
            Ok(()) => return Ok(()),
            Err(nix::errno::Errno::ENOTTY)
            | Err(nix::errno::Errno::EINVAL)
            | Err(nix::errno::Errno::ENOSPC) => {
                // Fall through to decompression.
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("encoded write failed on '{}'", full.display())
                });
            }
        }

        // Decompression fallback: decompress and pwrite.
        let decompressed =
            decompress(data, unencoded_len as usize, compression)
                .with_context(|| {
                    format!("decompression failed for '{}'", full.display())
                })?;

        let write_data = &decompressed[unencoded_offset as usize
            ..unencoded_offset as usize + unencoded_file_len as usize];

        use std::os::unix::fs::FileExt;
        fd.write_all_at(write_data, offset).with_context(|| {
            format!("pwrite failed on '{}'", full.display())
        })?;

        Ok(())
    }
}

/// Decompress `data` into a buffer of `output_len` bytes using the specified
/// compression algorithm.
fn decompress(
    data: &[u8],
    output_len: usize,
    compression: u32,
) -> Result<Vec<u8>> {
    match compression {
        0 => {
            // No compression — data is already unencoded.
            Ok(data.to_vec())
        }
        1 => {
            // ZLIB
            use std::io::Read;
            let mut decoder = flate2::read::ZlibDecoder::new(data);
            let mut out = vec![0u8; output_len];
            decoder
                .read_exact(&mut out)
                .context("zlib decompression failed")?;
            Ok(out)
        }
        2 => {
            // ZSTD
            let out = zstd::bulk::decompress(data, output_len)
                .context("zstd decompression failed")?;
            Ok(out)
        }
        3..=7 => {
            // LZO with sector sizes 4K through 64K.
            // Sector size = 1 << (compression - 3 + 12).
            let sector_size = 1usize << (compression - 3 + 12);
            decompress_lzo(data, output_len, sector_size)
        }
        _ => bail!("unsupported compression type {compression}"),
    }
}

/// Decompress btrfs LZO format: data is compressed sector by sector. Each
/// sector is independently LZO1X compressed. The format is:
/// - 4 bytes LE: total compressed size (including this field)
/// - For each sector:
///   - 4 bytes LE: compressed segment length
///   - N bytes: LZO1X compressed data
///   - Padding to the next sector boundary (if the remaining space in
///     the current sector is less than 4 bytes for the next header)
fn decompress_lzo(
    data: &[u8],
    output_len: usize,
    sector_size: usize,
) -> Result<Vec<u8>> {
    if data.len() < 4 {
        bail!("LZO data too short for header");
    }
    let total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if total_len > data.len() {
        bail!(
            "LZO total length {total_len} exceeds data length {}",
            data.len()
        );
    }

    let mut out = Vec::with_capacity(output_len);
    let mut pos = 4; // skip the 4-byte total length header

    while pos < total_len && out.len() < output_len {
        // Skip to the next sector boundary if the remaining space in the
        // current sector is too small for a segment header (4 bytes).
        let sector_remaining = sector_size - (pos % sector_size);
        if sector_remaining < 4 {
            if total_len - pos <= sector_remaining {
                break;
            }
            pos += sector_remaining;
        }

        if pos + 4 > total_len {
            bail!("LZO segment header truncated at offset {pos}");
        }
        let seg_len =
            u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + seg_len > data.len() {
            bail!(
                "LZO segment data truncated at offset {pos}, need {seg_len} bytes"
            );
        }

        let remaining = (output_len - out.len()).min(sector_size);
        let mut segment_out = vec![0u8; remaining];
        lzokay::decompress::decompress(
            &data[pos..pos + seg_len],
            &mut segment_out,
        )
        .map_err(|e| {
            anyhow!("LZO decompression failed at offset {pos}: {e:?}")
        })?;
        out.extend_from_slice(&segment_out);

        pos += seg_len;
    }

    if out.len() < output_len {
        out.resize(output_len, 0);
    }

    Ok(out)
}

fn path_to_cstring(path: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).with_context(|| {
        format!("path contains null byte: '{}'", path.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // -- decompress: compression type 0 (none) --

    #[test]
    fn decompress_none_passthrough() {
        let data = b"hello world";
        let result = decompress(data, data.len(), 0).unwrap();
        assert_eq!(result, data);
    }

    // -- decompress: compression type 1 (zlib) --

    #[test]
    fn decompress_zlib() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let original = b"the quick brown fox jumps over the lazy dog";
        let mut encoder =
            ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = decompress(&compressed, original.len(), 1).unwrap();
        assert_eq!(result, original);
    }

    // -- decompress: compression type 2 (zstd) --

    #[test]
    fn decompress_zstd() {
        let original = b"repeating data repeating data repeating data";
        let compressed = zstd::bulk::compress(original, 3).unwrap();

        let result = decompress(&compressed, original.len(), 2).unwrap();
        assert_eq!(result, original);
    }

    // -- decompress: unsupported compression type --

    #[test]
    fn decompress_unsupported_type() {
        let err = decompress(b"data", 4, 99).unwrap_err();
        assert!(
            err.to_string().contains("unsupported compression type 99"),
            "unexpected error: {err}"
        );
    }

    // -- decompress_lzo: header too short --

    #[test]
    fn decompress_lzo_header_too_short() {
        let err = decompress_lzo(&[0, 1, 2], 100, 4096).unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "unexpected error: {err}"
        );
    }

    // -- decompress_lzo: total_len exceeds data --

    #[test]
    fn decompress_lzo_total_len_exceeds_data() {
        // total_len = 1000, but data is only 8 bytes
        let mut data = vec![0u8; 8];
        data[0..4].copy_from_slice(&1000u32.to_le_bytes());
        let err = decompress_lzo(&data, 100, 4096).unwrap_err();
        assert!(
            err.to_string().contains("exceeds data length"),
            "unexpected error: {err}"
        );
    }

    /// Build a btrfs-format LZO compressed buffer from raw segments.
    /// Each segment is LZO1X compressed data for one sector.
    fn build_lzo_buffer(segments: &[Vec<u8>], sector_size: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        // Placeholder for total_len header
        buf.extend_from_slice(&[0u8; 4]);

        for seg in segments {
            // 4-byte segment length
            buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            buf.extend_from_slice(seg);
            // Pad to next sector boundary if needed (relative to start of
            // segment data, which begins at offset 4 in the overall buffer).
            // The position within the buffer after writing this segment:
            let pos = buf.len();
            let sector_rem = sector_size - (pos % sector_size);
            if sector_rem < 4 && sector_rem < sector_size {
                // Pad so next segment header is aligned
                buf.resize(buf.len() + sector_rem, 0);
            }
        }

        // Write total_len (includes the 4-byte header itself)
        let total = buf.len() as u32;
        buf[0..4].copy_from_slice(&total.to_le_bytes());
        buf
    }

    // -- decompress_lzo: single segment --

    #[test]
    fn decompress_lzo_single_segment() {
        let original = b"hello lzo compression test data!";
        let compressed =
            lzo1x::compress(original, lzo1x::CompressLevel::default());

        let buf = build_lzo_buffer(&[compressed], 4096);
        let result = decompress_lzo(&buf, original.len(), 4096).unwrap();
        assert_eq!(&result[..original.len()], original.as_slice());
    }

    // -- decompress_lzo: output zero-fill --
    // When the decompressed data from all segments is shorter than output_len,
    // the remainder is zero-filled. We use a 4096-byte sector whose decompressed
    // content is exactly 4096 bytes, then request output_len = 8192 so the
    // second half is zeros.

    #[test]
    fn decompress_lzo_output_zero_fill() {
        // Create exactly one sector worth of data (4096 bytes)
        let original = vec![0xABu8; 4096];
        let compressed =
            lzo1x::compress(&original, lzo1x::CompressLevel::default());

        let output_len = 8192; // twice the data we have
        let buf = build_lzo_buffer(&[compressed], 4096);
        let result = decompress_lzo(&buf, output_len, 4096).unwrap();
        assert_eq!(&result[..4096], original.as_slice());
        // Remainder should be zero-filled
        assert!(
            result[4096..].iter().all(|&b| b == 0),
            "expected zero-fill after decompressed data"
        );
        assert_eq!(result.len(), output_len);
    }

    // -- decompress_lzo: segment data truncated --

    #[test]
    fn decompress_lzo_segment_truncated() {
        // total_len header says 20 bytes total, segment header says 100
        // bytes but only a few remain
        let mut data = vec![0u8; 20];
        let total_len: u32 = 20;
        data[0..4].copy_from_slice(&total_len.to_le_bytes());
        // Segment header at offset 4: claims 100 bytes of compressed data
        let seg_len: u32 = 100;
        data[4..8].copy_from_slice(&seg_len.to_le_bytes());
        // Only 12 bytes remain (offsets 8..20), far less than 100

        let err = decompress_lzo(&data, 4096, 4096).unwrap_err();
        assert!(
            err.to_string().contains("truncated"),
            "unexpected error: {err}"
        );
    }

    // -- decompress: LZO via compression types 3-7 --

    #[test]
    fn decompress_lzo_via_compression_type_3() {
        let original = b"lzo via decompress entry point";
        let compressed =
            lzo1x::compress(original, lzo1x::CompressLevel::default());
        let buf = build_lzo_buffer(&[compressed], 4096);

        // compression=3 means sector_size = 1 << (3-3+12) = 4096
        let result = decompress(&buf, original.len(), 3).unwrap();
        assert_eq!(&result[..original.len()], original.as_slice());
    }

    // -- path_to_cstring --

    #[test]
    fn path_to_cstring_valid() {
        let path = Path::new("/tmp/test-file");
        let cstr = path_to_cstring(path).unwrap();
        assert_eq!(cstr.as_bytes(), b"/tmp/test-file");
    }

    #[test]
    fn path_to_cstring_with_null_byte() {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt};
        let os_str = OsStr::from_bytes(b"/tmp/bad\x00path");
        let path = Path::new(os_str);
        let err = path_to_cstring(path).unwrap_err();
        assert!(
            err.to_string().contains("null byte"),
            "unexpected error: {err}"
        );
    }
}

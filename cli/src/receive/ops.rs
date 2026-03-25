use crate::stream::{StreamCommand, Timespec};
use anyhow::{Context, Result, bail};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub struct ReceiveContext {
    /// File descriptor to the mount root (for UUID tree searches and snapshots).
    mnt_fd: File,
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
    pub fn new(dest_dir: &Path) -> Result<Self> {
        let mnt_fd = File::open(dest_dir)
            .with_context(|| format!("cannot open destination '{}'", dest_dir.display()))?;

        Ok(Self {
            mnt_fd,
            dest_dir: dest_dir.to_path_buf(),
            cur_subvol: None,
            cur_subvol_path: None,
            received_uuid: None,
            stransid: 0,
            write_fd: None,
            write_path: PathBuf::new(),
        })
    }

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
            } => self.process_snapshot(path, uuid, *ctransid, clone_uuid, *clone_ctransid),
            StreamCommand::Mkfile { path } => self.process_mkfile(path),
            StreamCommand::Mkdir { path } => self.process_mkdir(path),
            StreamCommand::Mknod { path, mode, rdev } => self.process_mknod(path, *mode, *rdev),
            StreamCommand::Mkfifo { path } => self.process_mkfifo(path),
            StreamCommand::Mksock { path } => self.process_mksock(path),
            StreamCommand::Symlink { path, target } => self.process_symlink(path, target),
            StreamCommand::Rename { from, to } => self.process_rename(from, to),
            StreamCommand::Link { path, target } => self.process_link(path, target),
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
            StreamCommand::RemoveXattr { path, name } => self.process_remove_xattr(path, name),
            StreamCommand::Truncate { path, size } => self.process_truncate(path, *size),
            StreamCommand::Chmod { path, mode } => self.process_chmod(path, *mode),
            StreamCommand::Chown { path, uid, gid } => self.process_chown(path, *uid, *gid),
            StreamCommand::Utimes {
                path,
                atime,
                mtime,
                ..
            } => self.process_utimes(path, atime, mtime),
            StreamCommand::UpdateExtent { .. } => Ok(()),
            StreamCommand::End => unreachable!("End is handled by the caller"),
        }
    }

    /// Finalize the current subvolume: set received UUID and make read-only.
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

        let subvol_file = File::open(&subvol_path)
            .with_context(|| format!("cannot open subvolume '{}'", subvol_path.display()))?;
        let fd = subvol_file.as_fd();

        btrfs_uapi::receive::received_subvol_set(fd, &uuid, self.stransid)
            .with_context(|| {
                format!(
                    "failed to set received subvol on '{}'",
                    subvol_path.display()
                )
            })?;

        // Make the subvolume read-only.
        let flags =
            btrfs_uapi::subvolume::subvolume_flags_get(fd).with_context(|| {
                format!("failed to get flags for '{}'", subvol_path.display())
            })?;
        btrfs_uapi::subvolume::subvolume_flags_set(
            fd,
            flags | btrfs_uapi::subvolume::SubvolumeFlags::RDONLY,
        )
        .with_context(|| format!("failed to set read-only on '{}'", subvol_path.display()))?;

        self.cur_subvol = None;
        self.cur_subvol_path = None;
        self.received_uuid = None;
        self.stransid = 0;

        Ok(())
    }

    pub fn close_write_fd(&mut self) {
        self.write_fd = None;
        self.write_path = PathBuf::new();
    }

    fn full_path(&self, relative: &str) -> Result<PathBuf> {
        let subvol_path = self
            .cur_subvol_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no current subvolume"))?;
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
        let parent_dir = File::open(&self.dest_dir)
            .with_context(|| format!("cannot open '{}'", self.dest_dir.display()))?;
        let c_name = CString::new(path)
            .with_context(|| format!("subvolume name contains null byte: {path}"))?;
        btrfs_uapi::subvolume::subvolume_create(parent_dir.as_fd(), &c_name)
            .with_context(|| format!("failed to create subvolume '{}'", subvol_path.display()))?;

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
        let parent_root_id = btrfs_uapi::receive::subvolume_search_by_received_uuid(
            self.mnt_fd.as_fd(),
            clone_uuid,
        )
        .or_else(|_| {
            btrfs_uapi::receive::subvolume_search_by_uuid(self.mnt_fd.as_fd(), clone_uuid)
        })
        .with_context(|| {
            format!(
                "cannot find parent subvolume with UUID {} for snapshot '{}'",
                clone_uuid.as_hyphenated(),
                path
            )
        })?;

        // Verify the parent's ctransid matches.
        let parent_path =
            btrfs_uapi::inode::subvolid_resolve(self.mnt_fd.as_fd(), parent_root_id)
                .with_context(|| {
                    format!("cannot resolve path for parent subvolume {parent_root_id}")
                })?;

        // Open the parent subvolume to verify ctransid and create the snapshot.
        let parent_full = self.dest_dir.join(&parent_path);
        let parent_file = File::open(&parent_full).with_context(|| {
            format!("cannot open parent subvolume '{}'", parent_full.display())
        })?;

        let parent_info = btrfs_uapi::subvolume::subvolume_info(parent_file.as_fd())
            .with_context(|| {
                format!(
                    "failed to get info for parent '{}'",
                    parent_full.display()
                )
            })?;

        // The parent's ctransid must match: check both ctransid and stransid
        // (stransid is set when the parent was itself received).
        if parent_info.ctransid != clone_ctransid && parent_info.stransid != clone_ctransid {
            bail!(
                "parent subvolume '{}' ctransid mismatch: stream expects {}, found ctransid={} stransid={}",
                parent_path,
                clone_ctransid,
                parent_info.ctransid,
                parent_info.stransid,
            );
        }

        // Create the snapshot.
        let dest_dir_file = File::open(&self.dest_dir)
            .with_context(|| format!("cannot open '{}'", self.dest_dir.display()))?;
        let c_name = CString::new(path)
            .with_context(|| format!("snapshot name contains null byte: {path}"))?;
        btrfs_uapi::subvolume::snapshot_create(
            parent_file.as_fd(),
            dest_dir_file.as_fd(),
            &c_name,
            false,
        )
        .with_context(|| format!("failed to create snapshot '{}'", subvol_path.display()))?;

        // Make the snapshot writable so we can apply the stream delta.
        let snap_file = File::open(&subvol_path)
            .with_context(|| format!("cannot open snapshot '{}'", subvol_path.display()))?;
        let snap_flags = btrfs_uapi::subvolume::subvolume_flags_get(snap_file.as_fd())
            .with_context(|| {
                format!("failed to get flags for '{}'", subvol_path.display())
            })?;
        if snap_flags.contains(btrfs_uapi::subvolume::SubvolumeFlags::RDONLY) {
            btrfs_uapi::subvolume::subvolume_flags_set(
                snap_file.as_fd(),
                snap_flags & !btrfs_uapi::subvolume::SubvolumeFlags::RDONLY,
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
        File::create(&full)
            .with_context(|| format!("failed to create file '{}'", full.display()))?;
        Ok(())
    }

    fn process_mkdir(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        fs::create_dir(&full)
            .with_context(|| format!("failed to create directory '{}'", full.display()))?;
        Ok(())
    }

    fn process_mknod(&mut self, path: &str, mode: u64, rdev: u64) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe {
            nix::libc::mknod(
                c_path.as_ptr(),
                mode as nix::libc::mode_t,
                rdev as nix::libc::dev_t,
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("mknod failed for '{}'", full.display()));
        }
        Ok(())
    }

    fn process_mkfifo(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe { nix::libc::mkfifo(c_path.as_ptr(), 0o600) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("mkfifo failed for '{}'", full.display()));
        }
        Ok(())
    }

    fn process_mksock(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe {
            nix::libc::mknod(c_path.as_ptr(), nix::libc::S_IFSOCK | 0o600, 0)
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("mksock failed for '{}'", full.display()));
        }
        Ok(())
    }

    fn process_symlink(&mut self, path: &str, target: &str) -> Result<()> {
        let full = self.full_path(path)?;
        std::os::unix::fs::symlink(target, &full)
            .with_context(|| format!("failed to create symlink '{}'", full.display()))?;
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
        fs::remove_file(&full)
            .with_context(|| format!("failed to unlink '{}'", full.display()))?;
        Ok(())
    }

    fn process_rmdir(&mut self, path: &str) -> Result<()> {
        let full = self.full_path(path)?;
        fs::remove_dir(&full)
            .with_context(|| format!("failed to rmdir '{}'", full.display()))?;
        Ok(())
    }

    fn process_write(&mut self, path: &str, offset: u64, data: &[u8]) -> Result<()> {
        let full = self.full_path(path)?;

        // Reuse cached fd if writing to the same file.
        if self.write_path != full {
            self.close_write_fd();
            let file = OpenOptions::new()
                .write(true)
                .open(&full)
                .with_context(|| format!("cannot open '{}' for writing", full.display()))?;
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
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("pwrite failed on '{}'", full.display()));
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
        let clone_subvol_root = btrfs_uapi::receive::subvolume_search_by_received_uuid(
            self.mnt_fd.as_fd(),
            clone_uuid,
        )
        .or_else(|_| {
            btrfs_uapi::receive::subvolume_search_by_uuid(self.mnt_fd.as_fd(), clone_uuid)
        })
        .with_context(|| {
            format!(
                "cannot find clone source subvolume with UUID {}",
                clone_uuid.as_hyphenated()
            )
        })?;

        let subvol_path =
            btrfs_uapi::inode::subvolid_resolve(self.mnt_fd.as_fd(), clone_subvol_root)
                .with_context(|| {
                    format!("cannot resolve path for clone source subvolume {clone_subvol_root}")
                })?;

        let clone_full = self.dest_dir.join(&subvol_path).join(clone_path);
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
            .with_context(|| format!("cannot open '{}' for clone", full.display()))?;

        btrfs_uapi::receive::clone_range(
            dest_file.as_fd(),
            clone_file.as_fd(),
            clone_offset,
            len,
            offset,
        )
        .with_context(|| format!("clone_range failed on '{}'", full.display()))?;

        Ok(())
    }

    fn process_set_xattr(&mut self, path: &str, name: &str, data: &[u8]) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let c_name =
            CString::new(name).with_context(|| format!("invalid xattr name: {name}"))?;
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
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lsetxattr failed on '{}'", full.display()));
        }
        Ok(())
    }

    fn process_remove_xattr(&mut self, path: &str, name: &str) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let c_name =
            CString::new(name).with_context(|| format!("invalid xattr name: {name}"))?;
        let ret = unsafe { nix::libc::lremovexattr(c_path.as_ptr(), c_name.as_ptr()) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lremovexattr failed on '{}'", full.display()));
        }
        Ok(())
    }

    fn process_truncate(&mut self, path: &str, size: u64) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let ret = unsafe { nix::libc::truncate(c_path.as_ptr(), size as nix::libc::off_t) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("truncate failed on '{}'", full.display()));
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
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lchown failed on '{}'", full.display()));
        }
        Ok(())
    }

    fn process_utimes(&mut self, path: &str, atime: &Timespec, mtime: &Timespec) -> Result<()> {
        let full = self.full_path(path)?;
        let c_path = path_to_cstring(&full)?;
        let times = [
            nix::libc::timespec {
                tv_sec: atime.sec as nix::libc::time_t,
                tv_nsec: atime.nsec as nix::libc::c_long,
            },
            nix::libc::timespec {
                tv_sec: mtime.sec as nix::libc::time_t,
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
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("utimensat failed on '{}'", full.display()));
        }
        Ok(())
    }
}

fn path_to_cstring(path: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("path contains null byte: '{}'", path.display()))
}

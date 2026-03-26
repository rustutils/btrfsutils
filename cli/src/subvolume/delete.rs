use crate::{Format, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_uapi::subvolume::{
    subvolume_delete, subvolume_delete_by_id, subvolume_info, subvolume_list,
};
use btrfs_uapi::sync::{start_sync, wait_sync};
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Delete one or more subvolumes or snapshots.
///
/// Delete subvolumes from the filesystem, specified by path or id. The
/// corresponding directory is removed instantly but the data blocks are
/// removed later.
///
/// The deletion does not involve full commit by default due to performance
/// reasons (as a consequence, the subvolume may appear again after a crash).
/// Use one of the --commit options to wait until the operation is safely
/// stored on the media.
#[derive(Parser, Debug)]
pub struct SubvolumeDeleteCommand {
    /// Wait for transaction commit at the end of the operation
    #[clap(short = 'c', long, conflicts_with = "commit_each")]
    pub commit_after: bool,

    /// Wait for transaction commit after deleting each subvolume
    #[clap(short = 'C', long, conflicts_with = "commit_after")]
    pub commit_each: bool,

    /// Delete by subvolume ID instead of path. When used, exactly one
    /// positional argument is expected: the filesystem path (mount point).
    #[clap(short = 'i', long, conflicts_with = "recursive")]
    pub subvolid: Option<u64>,

    /// Delete accessible subvolumes beneath each subvolume recursively.
    /// This is not atomic and may need root to delete subvolumes not
    /// accessible by the user.
    #[clap(short = 'R', long, conflicts_with = "subvolid")]
    pub recursive: bool,

    /// Be verbose, print subvolume names as they are deleted
    #[clap(short = 'v', long)]
    pub verbose: bool,

    /// Subvolume paths to delete, or (with --subvolid) the filesystem path
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for SubvolumeDeleteCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.subvolid.is_some() && self.paths.len() != 1 {
            bail!("--subvolid requires exactly one path argument (the filesystem mount point)");
        }

        let mut had_error = false;
        // For --commit-after, we save an fd and sync once at the end.
        let mut commit_after_fd: Option<File> = None;

        if let Some(subvolid) = self.subvolid {
            let (ok, fd) = self.delete_by_id(subvolid, &self.paths[0]);
            had_error |= !ok;
            if self.commit_after {
                commit_after_fd = fd;
            }
        } else {
            for path in &self.paths {
                let (ok, fd) = self.delete_by_path(path);
                had_error |= !ok;
                if self.commit_after && fd.is_some() {
                    commit_after_fd = fd;
                }
            }
        }

        // --commit-after: sync once at the end.
        if let Some(ref file) = commit_after_fd {
            if let Err(e) = wait_for_commit(file.as_fd()) {
                eprintln!("error: failed to commit: {e:#}");
                had_error = true;
            }
        }

        if had_error {
            bail!("one or more subvolumes could not be deleted");
        }

        Ok(())
    }
}

impl SubvolumeDeleteCommand {
    /// Delete a subvolume by path. Returns (success, optional fd for commit-after).
    fn delete_by_path(&self, path: &PathBuf) -> (bool, Option<File>) {
        let result = (|| -> Result<File> {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("'{}' has no parent directory", path.display()))?;

            let name_os = path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("'{}' has no file name", path.display()))?;

            let name_str = name_os
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("'{}' is not valid UTF-8", path.display()))?;

            let cname = CString::new(name_str)
                .with_context(|| format!("subvolume name contains a null byte: '{name_str}'"))?;

            let parent_file = File::open(parent)
                .with_context(|| format!("failed to open '{}'", parent.display()))?;
            let fd = parent_file.as_fd();

            if self.recursive {
                self.delete_children(path)?;
            }

            if self.verbose {
                println!("Delete subvolume '{}'", path.display());
            }

            subvolume_delete(fd, &cname)
                .with_context(|| format!("failed to delete '{}'", path.display()))?;

            if !self.verbose {
                println!("Delete subvolume '{}'", path.display());
            }

            if self.commit_each {
                wait_for_commit(fd)
                    .with_context(|| format!("failed to commit after '{}'", path.display()))?;
            }

            Ok(parent_file)
        })();

        match result {
            Ok(file) => (true, Some(file)),
            Err(e) => {
                eprintln!("error: {e:#}");
                (false, None)
            }
        }
    }

    /// Delete a subvolume by numeric ID. Returns (success, optional fd for commit-after).
    fn delete_by_id(&self, subvolid: u64, fs_path: &PathBuf) -> (bool, Option<File>) {
        let result = (|| -> Result<File> {
            let file = File::open(fs_path)
                .with_context(|| format!("failed to open '{}'", fs_path.display()))?;
            let fd = file.as_fd();

            if self.verbose {
                println!("Delete subvolume (subvolid={subvolid})");
            }

            subvolume_delete_by_id(fd, subvolid).with_context(|| {
                format!(
                    "failed to delete subvolid={subvolid} on '{}'",
                    fs_path.display()
                )
            })?;

            if !self.verbose {
                println!("Delete subvolume (subvolid={subvolid})");
            }

            if self.commit_each {
                wait_for_commit(fd).with_context(|| {
                    format!("failed to commit on '{}'", fs_path.display())
                })?;
            }

            Ok(file)
        })();

        match result {
            Ok(file) => (true, Some(file)),
            Err(e) => {
                eprintln!("error: {e:#}");
                (false, None)
            }
        }
    }

    /// Recursively delete all child subvolumes beneath `path`, deepest first.
    fn delete_children(&self, path: &PathBuf) -> Result<()> {
        let file = File::open(path)
            .with_context(|| format!("failed to open '{}'", path.display()))?;
        let fd = file.as_fd();

        // Get the root ID of this subvolume.
        let info = subvolume_info(fd)
            .with_context(|| format!("failed to get subvolume info for '{}'", path.display()))?;
        let target_id = info.id;

        // List all subvolumes on this filesystem and find those nested under target_id.
        let all = subvolume_list(fd)
            .with_context(|| format!("failed to list subvolumes on '{}'", path.display()))?;

        // Build the set of subvolume IDs that are descendants of target_id.
        // We need post-order traversal (delete children before parents).
        let mut children: Vec<u64> = Vec::new();
        let mut frontier = vec![target_id];

        while let Some(parent) = frontier.pop() {
            for item in &all {
                if item.parent_id == parent && item.root_id != target_id {
                    children.push(item.root_id);
                    frontier.push(item.root_id);
                }
            }
        }

        // Reverse so deepest children are deleted first (post-order).
        children.reverse();

        for child_id in children {
            if self.verbose {
                // Try to find the name for verbose output.
                if let Some(item) = all.iter().find(|i| i.root_id == child_id) {
                    if !item.name.is_empty() {
                        println!("Delete subvolume '{}/{}'", path.display(), item.name);
                    } else {
                        println!("Delete subvolume (subvolid={child_id})");
                    }
                }
            }

            subvolume_delete_by_id(fd, child_id).with_context(|| {
                format!(
                    "failed to delete child subvolid={child_id} under '{}'",
                    path.display()
                )
            })?;

            if self.commit_each {
                wait_for_commit(fd).with_context(|| {
                    format!("failed to commit after child subvolid={child_id}")
                })?;
            }
        }

        Ok(())
    }
}

/// Initiate a sync and wait for it to complete (start_sync + wait_sync).
fn wait_for_commit(fd: std::os::unix::io::BorrowedFd) -> Result<()> {
    let transid = start_sync(fd).context("start_sync failed")?;
    wait_sync(fd, transid).context("wait_sync failed")?;
    Ok(())
}

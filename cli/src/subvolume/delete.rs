use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::subvolume_delete;
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Delete one or more subvolumes or snapshots.
///
/// Each path must point directly to a subvolume.
/// Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct SubvolumeDeleteCommand {
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for SubvolumeDeleteCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let mut had_error = false;

        for path in &self.paths {
            let parent = match path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("'{}' has no parent directory", path.display()))
            {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let name_os = match path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("'{}' has no file name", path.display()))
            {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let name_str = match name_os
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("'{}' is not valid UTF-8", path.display()))
            {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let cname = match CString::new(name_str)
                .with_context(|| format!("subvolume name contains a null byte: '{}'", name_str))
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let file = match File::open(parent)
                .with_context(|| format!("failed to open '{}'", parent.display()))
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            match subvolume_delete(file.as_fd(), &cname) {
                Ok(()) => println!("Delete subvolume '{}'", path.display()),
                Err(e) => {
                    eprintln!("error deleting '{}': {e}", path.display());
                    had_error = true;
                }
            }
        }

        if had_error {
            anyhow::bail!("one or more subvolumes could not be deleted");
        }

        Ok(())
    }
}

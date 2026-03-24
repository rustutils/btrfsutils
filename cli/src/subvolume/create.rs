use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::subvolume_create;
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Create a new subvolume at each given path.
///
/// The parent directory must already exist and be on a btrfs filesystem.
/// Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct SubvolumeCreateCommand {
    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for SubvolumeCreateCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        for path in &self.paths {
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
                .with_context(|| format!("subvolume name contains a null byte: '{}'", name_str))?;

            let file = File::open(parent)
                .with_context(|| format!("failed to open '{}'", parent.display()))?;

            subvolume_create(file.as_fd(), &cname)
                .with_context(|| format!("failed to create subvolume '{}'", path.display()))?;

            println!("Create subvolume '{}'", path.display());
        }

        Ok(())
    }
}

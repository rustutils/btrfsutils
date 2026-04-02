use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::filesystem::{label_get, label_set};
use clap::Parser;
use std::{
    ffi::CString,
    os::unix::{ffi::OsStrExt, io::AsFd},
    path::PathBuf,
};

/// Get or set the label of a btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemLabelCommand {
    /// The device or mount point to operate on
    pub path: PathBuf,

    /// The new label to set (if omitted, the current label is printed)
    pub new_label: Option<std::ffi::OsString>,
}

impl Runnable for FilesystemLabelCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;
        match &self.new_label {
            None => {
                let label = label_get(file.as_fd()).with_context(|| {
                    format!("failed to get label for '{}'", self.path.display())
                })?;
                println!("{}", label.to_bytes().escape_ascii());
            }
            Some(new_label) => {
                let cstring = CString::new(new_label.as_bytes())
                    .context("label must not contain null bytes")?;
                label_set(file.as_fd(), &cstring).with_context(|| {
                    format!("failed to set label for '{}'", self.path.display())
                })?;
            }
        }
        Ok(())
    }
}

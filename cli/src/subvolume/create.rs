use crate::{Format, Runnable, util::parse_qgroupid};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::subvolume_create;
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Create a new subvolume at each given path.
///
/// The parent directory must already exist and be on a btrfs filesystem
/// (unless -p is given). Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct SubvolumeCreateCommand {
    /// Add the newly created subvolume to a qgroup (can be given multiple times)
    #[clap(short = 'i', value_name = "QGROUPID", action = clap::ArgAction::Append)]
    pub qgroups: Vec<String>,

    /// Create any missing parent directories (like mkdir -p)
    #[clap(short = 'p', long = "parents")]
    pub parents: bool,

    #[clap(required = true)]
    pub paths: Vec<PathBuf>,
}

impl Runnable for SubvolumeCreateCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let qgroup_ids: Vec<u64> = self
            .qgroups
            .iter()
            .map(|s| parse_qgroupid(s))
            .collect::<Result<_>>()?;

        let mut had_error = false;

        for path in &self.paths {
            let parent = match path.parent().ok_or_else(|| {
                anyhow::anyhow!("'{}' has no parent directory", path.display())
            }) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let name_os = match path.file_name().ok_or_else(|| {
                anyhow::anyhow!("'{}' has no file name", path.display())
            }) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let name_str = match name_os.to_str().ok_or_else(|| {
                anyhow::anyhow!("'{}' is not valid UTF-8", path.display())
            }) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            let cname = match CString::new(name_str).with_context(|| {
                format!("subvolume name contains a null byte: '{name_str}'")
            }) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            if self.parents
                && let Err(e) =
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!(
                            "failed to create parent directories for '{}'",
                            parent.display()
                        )
                    })
            {
                eprintln!("error creating '{}': {e}", path.display());
                had_error = true;
                continue;
            }

            let file = match File::open(parent).with_context(|| {
                format!("failed to open '{}'", parent.display())
            }) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                    continue;
                }
            };

            match subvolume_create(file.as_fd(), &cname, &qgroup_ids) {
                Ok(()) => println!("Create subvolume '{}'", path.display()),
                Err(e) => {
                    eprintln!("error creating '{}': {e}", path.display());
                    had_error = true;
                }
            }
        }

        if had_error {
            anyhow::bail!("one or more subvolumes could not be created");
        }

        Ok(())
    }
}

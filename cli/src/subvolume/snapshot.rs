use crate::{Format, Runnable, util::parse_qgroupid};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::snapshot_create;
use clap::Parser;
use std::{ffi::CString, fs::File, os::unix::io::AsFd, path::PathBuf};

/// Create a snapshot of a subvolume
#[derive(Parser, Debug)]
pub struct SubvolumeSnapshotCommand {
    /// Make the snapshot read-only
    #[clap(short)]
    pub readonly: bool,

    /// Add the newly created snapshot to a qgroup (can be given multiple times)
    #[clap(short = 'i', value_name = "QGROUPID", action = clap::ArgAction::Append)]
    pub qgroups: Vec<String>,

    /// Path to the source subvolume
    pub source: PathBuf,

    /// Destination: either an existing directory (snapshot will be named after the source) or a full path for the new snapshot
    pub dest: PathBuf,
}

impl Runnable for SubvolumeSnapshotCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let qgroup_ids: Vec<u64> = self
            .qgroups
            .iter()
            .map(|s| parse_qgroupid(s))
            .collect::<Result<_>>()?;

        let (dest_parent, name_os) =
            if self.dest.is_dir() {
                let name_os = self.source.file_name().ok_or_else(|| {
                    anyhow::anyhow!("source has no file name")
                })?;
                (self.dest.as_path(), name_os)
            } else {
                let dest_parent = self.dest.parent().ok_or_else(|| {
                    anyhow::anyhow!("destination has no parent")
                })?;
                let name_os = self.dest.file_name().ok_or_else(|| {
                    anyhow::anyhow!("destination has no name")
                })?;
                (dest_parent, name_os)
            };

        let name_str = name_os.to_str().ok_or_else(|| {
            anyhow::anyhow!("snapshot name is not valid UTF-8")
        })?;

        let cname = CString::new(name_str).with_context(|| {
            format!("snapshot name contains a null byte: '{}'", name_str)
        })?;

        let source_file = File::open(&self.source).with_context(|| {
            format!("failed to open source '{}'", self.source.display())
        })?;

        let parent_file = File::open(dest_parent).with_context(|| {
            format!(
                "failed to open destination parent '{}'",
                dest_parent.display()
            )
        })?;

        snapshot_create(
            parent_file.as_fd(),
            source_file.as_fd(),
            &cname,
            self.readonly,
            &qgroup_ids,
        )
        .with_context(|| {
            format!(
                "failed to create snapshot of '{}' in '{}/{}'",
                self.source.display(),
                dest_parent.display(),
                name_str,
            )
        })?;

        if self.readonly {
            println!(
                "Create readonly snapshot of '{}' in '{}/{}'",
                self.source.display(),
                dest_parent.display(),
                name_str,
            );
        } else {
            println!(
                "Create snapshot of '{}' in '{}/{}'",
                self.source.display(),
                dest_parent.display(),
                name_str,
            );
        }

        Ok(())
    }
}

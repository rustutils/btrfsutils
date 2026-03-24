use crate::{
    Format, Runnable,
    util::{parse_qgroupid, parse_size_with_suffix},
};
use anyhow::{Context, Result};
use btrfs_uapi::qgroup::QgroupLimitFlags;
use clap::Parser;
use nix::errno::Errno;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Set limits on a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupLimitCommand {
    /// Size limit in bytes (use suffix K/M/G/T), or "none" to remove the limit
    pub size: String,

    /// Qgroup ID (e.g. "0/5") or path if qgroupid is omitted
    #[clap(value_name = "QGROUPID_OR_PATH")]
    pub target: String,

    /// Path to the filesystem (required when qgroupid is given)
    pub path: Option<PathBuf>,

    /// Limit amount of data after compression
    #[clap(short = 'c')]
    pub compress: bool,

    /// Limit space exclusively assigned to this qgroup
    #[clap(short = 'e')]
    pub exclusive: bool,
}

impl Runnable for QgroupLimitCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let (fs_path, qgroupid) = match &self.path {
            None => {
                // target is the path; apply limit to the subvolume that fd refers to (qgroupid 0)
                let path = PathBuf::from(&self.target);
                (path, 0u64)
            }
            Some(path) => {
                // target is the qgroup ID string
                let qgroupid = parse_qgroupid(&self.target)?;
                (path.clone(), qgroupid)
            }
        };

        let size = if self.size.eq_ignore_ascii_case("none") {
            u64::MAX
        } else {
            parse_size_with_suffix(&self.size)?
        };

        let mut flags = QgroupLimitFlags::empty();
        let mut max_rfer = u64::MAX;
        let mut max_excl = u64::MAX;

        if self.compress {
            flags |= QgroupLimitFlags::RFER_CMPR | QgroupLimitFlags::EXCL_CMPR;
        }

        if self.exclusive {
            flags |= QgroupLimitFlags::MAX_EXCL;
            max_excl = size;
        } else {
            flags |= QgroupLimitFlags::MAX_RFER;
            max_rfer = size;
        }

        let file = File::open(&fs_path)
            .with_context(|| format!("failed to open '{}'", fs_path.display()))?;

        match btrfs_uapi::qgroup::qgroup_limit(file.as_fd(), qgroupid, flags, max_rfer, max_excl) {
            Ok(()) => Ok(()),
            Err(Errno::ENOTCONN) => {
                anyhow::bail!("quota not enabled on '{}'", fs_path.display())
            }
            Err(e) => Err(e)
                .with_context(|| format!("failed to set qgroup limit on '{}'", fs_path.display())),
        }
    }
}

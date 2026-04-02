use crate::{
    RunContext, Runnable,
    util::{open_path, parse_qgroupid, parse_size_with_suffix},
};
use anyhow::{Context, Result};
use btrfs_uapi::quota::QgroupLimitFlags;
use clap::Parser;
use nix::errno::Errno;
use std::{os::unix::io::AsFd, path::PathBuf};

/// A size limit that is either a byte count or "none" to remove the limit.
#[derive(Debug, Clone)]
pub enum QgroupLimitSize {
    Bytes(u64),
    None,
}

impl std::str::FromStr for QgroupLimitSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case("none") {
            Ok(Self::None)
        } else {
            parse_size_with_suffix(s).map(Self::Bytes)
        }
    }
}

/// Set limits on a subvolume quota group
#[derive(Parser, Debug)]
pub struct QgroupLimitCommand {
    /// Size limit in bytes (use suffix K/M/G/T), or "none" to remove the limit
    pub size: QgroupLimitSize,

    /// Qgroup ID (e.g. "0/5") or path if qgroupid is omitted
    #[clap(value_name = "QGROUPID_OR_PATH")]
    pub target: String,

    /// Path to the filesystem (required when qgroupid is given)
    pub path: Option<PathBuf>,

    /// Limit amount of data after compression
    #[clap(short = 'c', long)]
    pub compress: bool,

    /// Limit space exclusively assigned to this qgroup
    #[clap(short = 'e', long)]
    pub exclusive: bool,
}

impl Runnable for QgroupLimitCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
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

        let size = match self.size {
            QgroupLimitSize::Bytes(n) => n,
            QgroupLimitSize::None => u64::MAX,
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

        let file = open_path(&fs_path)?;

        match btrfs_uapi::quota::qgroup_limit(
            file.as_fd(),
            qgroupid,
            flags,
            max_rfer,
            max_excl,
        ) {
            Ok(()) => Ok(()),
            Err(Errno::ENOTCONN) => {
                anyhow::bail!("quota not enabled on '{}'", fs_path.display())
            }
            Err(e) => Err(e).with_context(|| {
                format!("failed to set qgroup limit on '{}'", fs_path.display())
            }),
        }
    }
}

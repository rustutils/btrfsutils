use crate::{Format, Runnable, util::parse_qgroupid};
use anyhow::{Context, Result, bail};
use clap::Parser;
use nix::errno::Errno;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Remove the relation between child qgroup SRC and DST
#[derive(Parser, Debug)]
pub struct QgroupRemoveCommand {
    /// Source qgroup id (e.g. "0/5")
    pub src: String,

    /// Destination qgroup id (e.g. "1/0")
    pub dst: String,

    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Schedule quota rescan if needed (default)
    #[clap(long, conflicts_with = "no_rescan")]
    pub rescan: bool,

    /// Don't schedule quota rescan
    #[clap(long)]
    pub no_rescan: bool,
}

impl Runnable for QgroupRemoveCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let src = parse_qgroupid(&self.src)?;
        let dst = parse_qgroupid(&self.dst)?;

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let needs_rescan = match btrfs_uapi::qgroup::qgroup_remove(fd, src, dst) {
            Ok(needs_rescan) => needs_rescan,
            Err(Errno::ENOTCONN) => bail!("quota not enabled on '{}'", self.path.display()),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "failed to remove qgroup relation '{}' from '{}' on '{}'",
                        self.src,
                        self.dst,
                        self.path.display()
                    )
                });
            }
        };

        // Default behaviour (neither flag given) is to rescan.
        let do_rescan = !self.no_rescan;

        if needs_rescan {
            if do_rescan {
                btrfs_uapi::quota::quota_rescan(fd).with_context(|| {
                    format!(
                        "failed to schedule quota rescan on '{}'",
                        self.path.display()
                    )
                })?;
                println!("Quota data changed, rescan scheduled");
            } else {
                eprintln!(
                    "WARNING: quotas may be inconsistent, rescan needed on '{}'",
                    self.path.display()
                );
            }
        }

        Ok(())
    }
}

use crate::{Format, Runnable, util::parse_qgroupid};
use anyhow::{Context, Result, bail};
use btrfs_uapi::qgroup::qgroupid_level;
use clap::Parser;
use nix::errno::Errno;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Assign a qgroup as the child of another qgroup
#[derive(Parser, Debug)]
pub struct QgroupAssignCommand {
    /// Source qgroup id (e.g. "0/5")
    pub src: String,
    /// Destination qgroup id (e.g. "1/0")
    pub dst: String,
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,
    /// Schedule a quota rescan if needed (default)
    #[clap(long, overrides_with = "no_rescan")]
    pub rescan: bool,
    /// Do not schedule a quota rescan
    #[clap(long, overrides_with = "rescan")]
    pub no_rescan: bool,
}

impl Runnable for QgroupAssignCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.rescan && self.no_rescan {
            bail!("--rescan and --no-rescan are mutually exclusive");
        }

        let src = parse_qgroupid(&self.src)?;
        let dst = parse_qgroupid(&self.dst)?;

        if qgroupid_level(src) >= qgroupid_level(dst) {
            bail!(
                "source qgroup '{}' must be at a lower level than destination qgroup '{}'",
                self.src,
                self.dst
            );
        }

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;
        let fd = file.as_fd();

        let needs_rescan = match btrfs_uapi::qgroup::qgroup_assign(fd, src, dst) {
            Ok(needs_rescan) => needs_rescan,
            Err(Errno::ENOTCONN) => bail!("quota not enabled on '{}'", self.path.display()),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "failed to assign qgroup '{}' to '{}' on '{}'",
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

use crate::{RunContext, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{subvolume_info_by_id, subvolume_list};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf, thread, time::Duration};

/// Wait until given subvolume(s) are completely removed from the filesystem
///
/// Wait until given subvolume(s) are completely removed from the filesystem
/// after deletion. If no subvolume id is given, wait until all current
/// deletion requests are completed, but do not wait for subvolumes deleted
/// meanwhile. The status of subvolume ids is checked periodically.
#[derive(Parser, Debug)]
pub struct SubvolumeSyncCommand {
    /// Path to the btrfs filesystem mount point
    path: PathBuf,

    /// One or more subvolume IDs to wait for (waits for all pending if omitted)
    subvolids: Vec<u64>,

    /// Sleep N seconds between checks (default: 1)
    #[clap(short = 's', long, value_name = "SECONDS")]
    sleep: Option<u64>,
}

impl Runnable for SubvolumeSyncCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = open_path(&self.path)?;

        let interval = Duration::from_secs(self.sleep.unwrap_or(1));

        let mut ids: Vec<u64> = if self.subvolids.is_empty() {
            // No IDs given: find all deleted subvolumes (those with parent_id == 0
            // in subvolume_list, meaning no ROOT_BACKREF — pending deletion).
            let items = subvolume_list(file.as_fd())
                .with_context(|| "failed to list subvolumes")?;
            items
                .iter()
                .filter(|item| item.parent_id == 0)
                .map(|item| item.root_id)
                .collect()
        } else {
            self.subvolids.clone()
        };

        if ids.is_empty() {
            return Ok(());
        }

        let total = ids.len();
        let mut done = 0usize;

        loop {
            let mut all_gone = true;

            for id in &mut ids {
                if *id == 0 {
                    continue;
                }
                match subvolume_info_by_id(file.as_fd(), *id) {
                    Ok(_) => {
                        all_gone = false;
                    }
                    Err(nix::errno::Errno::ENOENT) => {
                        done += 1;
                        eprintln!("Subvolume id {id} is gone ({done}/{total})");
                        *id = 0;
                    }
                    Err(e) => {
                        anyhow::bail!("failed to query subvolume {id}: {e}");
                    }
                }
            }

            if all_gone {
                break;
            }

            thread::sleep(interval);
        }

        Ok(())
    }
}

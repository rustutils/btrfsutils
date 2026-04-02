use crate::{
    Format, Runnable,
    filesystem::UnitMode,
    util::{ParsedUuid, SizeFormat, fmt_size, format_time, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    quota::{self, QgroupInfo},
    send_receive::subvolume_search_by_uuid,
    subvolume::{subvolume_info, subvolume_info_by_id},
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show detailed information about a subvolume
///
/// Displays UUIDs, generation numbers, creation time, flags, and send/receive
/// transaction IDs for the subvolume that contains the given path.
///
/// The subvolume can be specified by path (default), or by root id or UUID
/// that are looked up relative to the given path.
#[derive(Parser, Debug)]
pub struct SubvolumeShowCommand {
    /// Look up subvolume by its root ID instead of path
    #[clap(short = 'r', long = "rootid")]
    pub rootid: Option<u64>,

    /// Look up subvolume by its UUID instead of path
    #[clap(short = 'u', long = "uuid", conflicts_with = "rootid")]
    pub uuid: Option<ParsedUuid>,

    #[clap(flatten)]
    pub units: UnitMode,

    /// Path to a subvolume or any file within it
    pub path: PathBuf,
}

impl Runnable for SubvolumeShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        let info = if let Some(rootid) = self.rootid {
            subvolume_info_by_id(file.as_fd(), rootid).with_context(|| {
                format!("failed to get subvolume info for rootid {rootid}")
            })?
        } else if let Some(ref uuid) = self.uuid {
            let inner = &**uuid;
            let rootid = subvolume_search_by_uuid(file.as_fd(), inner)
                .with_context(|| {
                    format!("failed to find subvolume with UUID {inner}")
                })?;
            subvolume_info_by_id(file.as_fd(), rootid).with_context(|| {
                format!("failed to get subvolume info for UUID {inner}")
            })?
        } else {
            subvolume_info(file.as_fd()).with_context(|| {
                format!(
                    "failed to get subvolume info for '{}'",
                    self.path.display()
                )
            })?
        };

        println!("{}", self.path.display());
        println!("\tName: \t\t\t{}", info.name);
        println!("\tUUID: \t\t\t{}", format_uuid(&info.uuid));
        println!("\tParent UUID: \t\t{}", format_uuid(&info.parent_uuid));
        println!("\tReceived UUID: \t\t{}", format_uuid(&info.received_uuid));
        println!("\tCreation time: \t\t{}", format_time(info.otime));
        println!("\tSubvolume ID: \t\t{}", info.id);
        println!("\tGeneration: \t\t{}", info.generation);
        println!("\tGen at creation: \t{}", info.otransid);
        println!("\tParent ID: \t\t{}", info.parent_id);
        println!("\tTop level ID: \t\t{}", info.parent_id);
        println!("\tFlags: \t\t\t{}", info.flags);
        println!("\tSend transid: \t\t{}", info.stransid);
        println!("\tSend time: \t\t{}", format_time(info.stime));
        println!("\tReceive transid: \t{}", info.rtransid);
        println!("\tReceive time: \t\t{}", format_time(info.rtime));

        // Quota data: look up this subvolume's qgroup (level 0, id = subvol id).
        if let Some(qg) = query_qgroup(file.as_fd(), info.id) {
            let mode = self.units.resolve();
            println!("\tQuota group:\t\t0/{}", info.id);
            println!(
                "\t  Limit referenced:\t{}",
                format_limit(qg.max_rfer, &mode)
            );
            println!(
                "\t  Limit exclusive:\t{}",
                format_limit(qg.max_excl, &mode)
            );
            println!("\t  Usage referenced:\t{}", fmt_size(qg.rfer, &mode));
            println!("\t  Usage exclusive:\t{}", fmt_size(qg.excl, &mode));
        }

        Ok(())
    }
}

fn format_uuid(uuid: &uuid::Uuid) -> String {
    if uuid.is_nil() {
        "-".to_string()
    } else {
        uuid.hyphenated().to_string()
    }
}

/// Query the qgroup entry for a specific subvolume ID.
///
/// Returns `None` if quotas are not enabled or the subvolume has no qgroup.
fn query_qgroup(
    fd: std::os::unix::io::BorrowedFd,
    subvol_id: u64,
) -> Option<QgroupInfo> {
    let list = quota::qgroup_list(fd).ok()?;
    list.qgroups.into_iter().find(|q| {
        quota::qgroupid_level(q.qgroupid) == 0
            && quota::qgroupid_subvolid(q.qgroupid) == subvol_id
    })
}

/// Format a qgroup limit value: "-" if no limit, formatted size otherwise.
fn format_limit(limit: u64, mode: &SizeFormat) -> String {
    if limit == 0 || limit == u64::MAX {
        "-".to_string()
    } else {
        fmt_size(limit, mode)
    }
}

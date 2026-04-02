use crate::{
    Format, RunContext, Runnable,
    filesystem::UnitMode,
    util::{
        ParsedUuid, SizeFormat, fmt_size, format_time, open_path, print_json,
    },
};
use anyhow::{Context, Result};
use btrfs_uapi::{
    quota::{self, QgroupInfo},
    send_receive::subvolume_search_by_uuid,
    subvolume::{SubvolumeInfo, subvolume_info, subvolume_info_by_id},
};
use clap::Parser;
use serde::Serialize;
use std::{os::unix::io::AsFd, path::PathBuf, time::SystemTime};

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

#[derive(Serialize)]
struct SubvolShowJson {
    name: String,
    uuid: String,
    parent_uuid: String,
    received_uuid: String,
    creation_time: String,
    subvolume_id: u64,
    generation: u64,
    gen_at_creation: u64,
    parent_id: u64,
    flags: String,
    send_transid: u64,
    send_time: String,
    receive_transid: u64,
    receive_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota: Option<QuotaJson>,
}

#[derive(Serialize)]
struct QuotaJson {
    qgroupid: String,
    limit_referenced: Option<u64>,
    limit_exclusive: Option<u64>,
    usage_referenced: u64,
    usage_exclusive: u64,
}

fn format_time_for_json(t: SystemTime) -> String {
    format_time(t)
}

fn subvol_to_json(
    info: &SubvolumeInfo,
    qg: Option<&QgroupInfo>,
) -> SubvolShowJson {
    SubvolShowJson {
        name: info.name.clone(),
        uuid: format_uuid(&info.uuid),
        parent_uuid: format_uuid(&info.parent_uuid),
        received_uuid: format_uuid(&info.received_uuid),
        creation_time: format_time_for_json(info.otime),
        subvolume_id: info.id,
        generation: info.generation,
        gen_at_creation: info.otransid,
        parent_id: info.parent_id,
        flags: info.flags.to_string(),
        send_transid: info.stransid,
        send_time: format_time_for_json(info.stime),
        receive_transid: info.rtransid,
        receive_time: format_time_for_json(info.rtime),
        quota: qg.map(|q| QuotaJson {
            qgroupid: format!("0/{}", info.id),
            limit_referenced: if q.max_rfer == 0 || q.max_rfer == u64::MAX {
                None
            } else {
                Some(q.max_rfer)
            },
            limit_exclusive: if q.max_excl == 0 || q.max_excl == u64::MAX {
                None
            } else {
                Some(q.max_excl)
            },
            usage_referenced: q.rfer,
            usage_exclusive: q.excl,
        }),
    }
}

impl Runnable for SubvolumeShowCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
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

        let qg = query_qgroup(file.as_fd(), info.id);

        match ctx.format {
            Format::Text => {
                println!("{}", self.path.display());
                println!("\tName: \t\t\t{}", info.name);
                println!("\tUUID: \t\t\t{}", format_uuid(&info.uuid));
                println!(
                    "\tParent UUID: \t\t{}",
                    format_uuid(&info.parent_uuid)
                );
                println!(
                    "\tReceived UUID: \t\t{}",
                    format_uuid(&info.received_uuid)
                );
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

                if let Some(ref qg) = qg {
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
                    println!(
                        "\t  Usage referenced:\t{}",
                        fmt_size(qg.rfer, &mode)
                    );
                    println!(
                        "\t  Usage exclusive:\t{}",
                        fmt_size(qg.excl, &mode)
                    );
                }
            }
            Format::Json => {
                let json = subvol_to_json(&info, qg.as_ref());
                print_json("subvolume-show", &json)?;
            }
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

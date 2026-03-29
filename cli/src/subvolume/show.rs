use crate::{
    Format, Runnable,
    util::{ParsedUuid, format_time, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::{
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

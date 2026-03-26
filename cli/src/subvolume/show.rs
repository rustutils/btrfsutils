use crate::{Format, Runnable, util::ParsedUuid};
use anyhow::{Context, Result};
use btrfs_uapi::{
    send_receive::subvolume_search_by_uuid,
    subvolume::{subvolume_info, subvolume_info_by_id},
};
use clap::Parser;
use std::{
    fs::File,
    mem,
    os::unix::io::AsFd,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

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
    #[clap(short = 'r', long = "rootid", value_name = "ID")]
    pub rootid: Option<u64>,

    /// Look up subvolume by its UUID instead of path
    #[clap(
        short = 'u',
        long = "uuid",
        value_name = "UUID",
        conflicts_with = "rootid"
    )]
    pub uuid: Option<ParsedUuid>,

    /// Path to a subvolume or any file within it
    pub path: PathBuf,
}

impl Runnable for SubvolumeShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

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

/// Format a [`SystemTime`] as a local-time datetime string in the same style
/// as the C btrfs-progs tool: `YYYY-MM-DD HH:MM:SS ±HHMM`.
///
/// Returns `"-"` when the time is [`UNIX_EPOCH`] (i.e. not set).
fn format_time(t: SystemTime) -> String {
    if t == UNIX_EPOCH {
        return "-".to_string();
    }

    let secs = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as nix::libc::time_t,
        Err(_) => return "-".to_string(),
    };

    // SAFETY: localtime_r is async-signal-safe and writes into the tm we
    // provide; we pass a valid pointer and the output is fully initialised
    // before we read it.
    let mut tm: nix::libc::tm = unsafe { mem::zeroed() };
    let result = unsafe { nix::libc::localtime_r(&secs, &mut tm) };
    if result.is_null() {
        return "-".to_string();
    }

    let year = tm.tm_year + 1900;
    let mon = tm.tm_mon + 1;
    let mday = tm.tm_mday;
    let hour = tm.tm_hour;
    let min = tm.tm_min;
    let sec = tm.tm_sec;

    // tm_gmtoff is the UTC offset in seconds (positive = east of UTC).
    let gmtoff = tm.tm_gmtoff; // seconds
    let off_sign = if gmtoff < 0 { '-' } else { '+' };
    let off_abs = gmtoff.unsigned_abs();
    let off_h = off_abs / 3600;
    let off_m = (off_abs % 3600) / 60;

    format!(
        "{year:04}-{mon:02}-{mday:02} {hour:02}:{min:02}:{sec:02} {off_sign}{off_h:02}{off_m:02}"
    )
}

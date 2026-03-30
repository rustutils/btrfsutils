use crate::{
    Format, Runnable,
    util::{SizeFormat, fmt_size, open_path},
};
use anyhow::{Context, Result};
use btrfs_uapi::quota::{
    QgroupInfo, QgroupLimitFlags, QgroupStatusFlags, qgroupid_level,
    qgroupid_subvolid,
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

const HEADING_COLUMN_SELECTION: &str = "Column selection";
const HEADING_FILTERING: &str = "Filtering";
const HEADING_SIZE_UNITS: &str = "Size units";

/// List subvolume quota groups
#[derive(Parser, Debug)]
pub struct QgroupShowCommand {
    /// Path to a mounted btrfs filesystem
    pub path: PathBuf,

    /// Print parent qgroup id
    #[clap(short = 'p', long, help_heading = HEADING_COLUMN_SELECTION)]
    pub print_parent: bool,

    /// Print child qgroup id
    #[clap(short = 'c', long, help_heading = HEADING_COLUMN_SELECTION)]
    pub print_child: bool,

    /// Print limit of referenced size
    #[clap(short = 'r', long, help_heading = HEADING_COLUMN_SELECTION)]
    pub print_rfer_limit: bool,

    /// Print limit of exclusive size
    #[clap(short = 'e', long, help_heading = HEADING_COLUMN_SELECTION)]
    pub print_excl_limit: bool,

    /// List all qgroups impacting path, including ancestral qgroups
    #[clap(short = 'F', long, help_heading = HEADING_FILTERING)]
    pub filter_all: bool,

    /// List all qgroups impacting path, excluding ancestral qgroups
    #[clap(short = 'f', long, help_heading = HEADING_FILTERING)]
    pub filter_direct: bool,

    /// Show raw numbers in bytes
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub raw: bool,

    /// Show human friendly numbers, base 1024 (default)
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub human_readable: bool,

    /// Use 1024 as a base (IEC units)
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub iec: bool,

    /// Use 1000 as a base (SI units)
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub si: bool,

    /// Show sizes in KiB
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub kbytes: bool,

    /// Show sizes in MiB
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub mbytes: bool,

    /// Show sizes in GiB
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub gbytes: bool,

    /// Show sizes in TiB
    #[clap(long, help_heading = HEADING_SIZE_UNITS)]
    pub tbytes: bool,

    /// Sort by a comma-separated list of fields (qgroupid, rfer, excl, max_rfer, max_excl)
    #[clap(long)]
    pub sort: Option<SortKeys>,

    /// Force a sync before getting quota information
    #[clap(long)]
    pub sync: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortField {
    Qgroupid,
    Rfer,
    Excl,
    MaxRfer,
    MaxExcl,
}

#[derive(Debug, Clone, Copy)]
struct SortKey {
    field: SortField,
    descending: bool,
}

impl std::str::FromStr for SortKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (descending, name) = match s.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let field = match name {
            "qgroupid" => SortField::Qgroupid,
            "rfer" => SortField::Rfer,
            "excl" => SortField::Excl,
            "max_rfer" => SortField::MaxRfer,
            "max_excl" => SortField::MaxExcl,
            _ => {
                return Err(format!(
                    "unknown sort field '{name}'; expected qgroupid, rfer, excl, max_rfer, or max_excl"
                ));
            }
        };
        Ok(SortKey { field, descending })
    }
}

/// Comma-separated list of sort keys (e.g. "rfer,-excl").
#[derive(Debug, Clone)]
pub struct SortKeys(Vec<SortKey>);

impl std::str::FromStr for SortKeys {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let keys: Vec<SortKey> = s
            .split(',')
            .map(|part| part.trim().parse())
            .collect::<Result<_, _>>()?;
        if keys.is_empty() {
            return Err("sort field list must not be empty".to_string());
        }
        Ok(SortKeys(keys))
    }
}

fn fmt_limit(
    bytes: u64,
    flags: QgroupLimitFlags,
    flag_bit: QgroupLimitFlags,
    mode: &SizeFormat,
) -> String {
    if bytes == u64::MAX || !flags.contains(flag_bit) {
        "none".to_string()
    } else {
        fmt_size(bytes, mode)
    }
}

fn format_qgroupid(qgroupid: u64) -> String {
    format!(
        "{}/{}",
        qgroupid_level(qgroupid),
        qgroupid_subvolid(qgroupid)
    )
}

impl Runnable for QgroupShowCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // filter_all / filter_direct: not implemented, ignored
        let _ = self.filter_all;
        let _ = self.filter_direct;

        let file = open_path(&self.path)?;
        let fd = file.as_fd();

        if self.sync {
            btrfs_uapi::filesystem::sync(fd).with_context(|| {
                format!("failed to sync '{}'", self.path.display())
            })?;
        }

        let list = btrfs_uapi::quota::qgroup_list(fd).with_context(|| {
            format!("failed to list qgroups on '{}'", self.path.display())
        })?;

        if list.qgroups.is_empty() {
            return Ok(());
        }

        if list.status_flags.contains(QgroupStatusFlags::INCONSISTENT) {
            eprintln!(
                "WARNING: qgroup data is inconsistent, use 'btrfs quota rescan' to fix"
            );
        }

        // Determine display mode
        let si = self.si;
        let mode = if self.raw {
            SizeFormat::Raw
        } else if self.kbytes {
            SizeFormat::Fixed(if si { 1000 } else { 1024 })
        } else if self.mbytes {
            SizeFormat::Fixed(if si { 1_000_000 } else { 1024 * 1024 })
        } else if self.gbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000
            } else {
                1024 * 1024 * 1024
            })
        } else if self.tbytes {
            SizeFormat::Fixed(if si {
                1_000_000_000_000
            } else {
                1024u64.pow(4)
            })
        } else if si {
            SizeFormat::HumanSi
        } else {
            SizeFormat::HumanIec
        };

        // Sort
        let mut qgroups: Vec<QgroupInfo> = list.qgroups.clone();

        match &self.sort {
            Some(SortKeys(keys)) => {
                qgroups.sort_by(|a, b| {
                    for key in keys {
                        let ord = match key.field {
                            SortField::Qgroupid => a.qgroupid.cmp(&b.qgroupid),
                            SortField::Rfer => a.rfer.cmp(&b.rfer),
                            SortField::Excl => a.excl.cmp(&b.excl),
                            SortField::MaxRfer => a.max_rfer.cmp(&b.max_rfer),
                            SortField::MaxExcl => a.max_excl.cmp(&b.max_excl),
                        };
                        let ord =
                            if key.descending { ord.reverse() } else { ord };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            None => {
                qgroups.sort_by_key(|q| q.qgroupid);
            }
        }

        // Build header
        let mut header =
            format!("{:<16} {:>12} {:>12}", "qgroupid", "rfer", "excl");
        if self.print_rfer_limit {
            header.push_str(&format!(" {:>12}", "max_rfer"));
        }
        if self.print_excl_limit {
            header.push_str(&format!(" {:>12}", "max_excl"));
        }
        if self.print_parent {
            header.push_str(&format!("  {:<20}", "parent"));
        }
        if self.print_child {
            header.push_str(&format!("  {:<20}", "child"));
        }
        println!("{header}");

        for q in &qgroups {
            let id_str = format_qgroupid(q.qgroupid);
            let rfer_str = fmt_size(q.rfer, &mode);
            let excl_str = fmt_size(q.excl, &mode);

            let mut line =
                format!("{id_str:<16} {rfer_str:>12} {excl_str:>12}");

            if self.print_rfer_limit {
                let s = fmt_limit(
                    q.max_rfer,
                    q.limit_flags,
                    QgroupLimitFlags::MAX_RFER,
                    &mode,
                );
                line.push_str(&format!(" {s:>12}"));
            }

            if self.print_excl_limit {
                let s = fmt_limit(
                    q.max_excl,
                    q.limit_flags,
                    QgroupLimitFlags::MAX_EXCL,
                    &mode,
                );
                line.push_str(&format!(" {s:>12}"));
            }

            if self.print_parent {
                let parents: Vec<String> =
                    q.parents.iter().map(|&id| format_qgroupid(id)).collect();
                line.push_str(&format!("  {:<20}", parents.join(",")));
            }

            if self.print_child {
                let children: Vec<String> =
                    q.children.iter().map(|&id| format_qgroupid(id)).collect();
                line.push_str(&format!("  {:<20}", children.join(",")));
            }

            println!("{line}");
        }

        Ok(())
    }
}

use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{
    SubvolumeFlags, SubvolumeListItem, subvolume_list,
};
use clap::Parser;
use std::{
    cmp::Ordering, fs::File, os::unix::io::AsFd, path::PathBuf, str::FromStr,
};

const HEADING_PATH_FILTERING: &str = "Path filtering";
const HEADING_FIELD_SELECTION: &str = "Field selection";
const HEADING_TYPE_FILTERING: &str = "Type filtering";
const HEADING_SORTING: &str = "Sorting";

/// List subvolumes and snapshots in the filesystem
///
/// The default output format matches btrfs-progs:
///   ID NNN gen NNN top level NNN path NAME
///
/// Optional flags enable additional columns or filter the results.
#[derive(Parser, Debug)]
pub struct SubvolumeListCommand {
    /// Print only subvolumes below the given path
    #[clap(short = 'o', long, help_heading = HEADING_PATH_FILTERING)]
    only_below: bool,

    /// Print all subvolumes in the filesystem, including deleted ones, and
    /// distinguish absolute and relative paths with respect to the given path
    #[clap(short = 'a', long, help_heading = HEADING_PATH_FILTERING)]
    all: bool,

    /// Print parent ID column (same as top level for non-snapshots)
    #[clap(short, long, help_heading = HEADING_FIELD_SELECTION)]
    parent: bool,

    /// Print ogeneration (generation at creation) column
    #[clap(short = 'c', long, help_heading = HEADING_FIELD_SELECTION)]
    ogeneration: bool,

    /// Print generation column (already shown by default; kept for
    /// compatibility with btrfs-progs CLI)
    #[clap(short, long, help_heading = HEADING_FIELD_SELECTION)]
    generation: bool,

    /// Print UUID column
    #[clap(short, long, help_heading = HEADING_FIELD_SELECTION)]
    uuid: bool,

    /// Print parent UUID column
    #[clap(short = 'Q', long, help_heading = HEADING_FIELD_SELECTION)]
    parent_uuid: bool,

    /// Print received UUID column
    #[clap(short = 'R', long, help_heading = HEADING_FIELD_SELECTION)]
    received_uuid: bool,

    /// List only snapshots (subvolumes with a non-nil parent UUID)
    #[clap(short = 's', long, help_heading = HEADING_TYPE_FILTERING)]
    snapshots_only: bool,

    /// List only read-only subvolumes
    #[clap(short = 'r', long, help_heading = HEADING_TYPE_FILTERING)]
    readonly: bool,

    /// List deleted subvolumes that are not yet cleaned
    #[clap(short = 'd', long, help_heading = HEADING_TYPE_FILTERING)]
    deleted: bool,

    /// Print the result as a table
    #[clap(short = 't', long, help_heading = "Other")]
    table: bool,

    /// Filter by generation: VALUE (exact), +VALUE (>= VALUE), -VALUE (<= VALUE)
    #[clap(short = 'G', long, value_name = "[+|-]VALUE", allow_hyphen_values = true, help_heading = HEADING_SORTING)]
    gen_filter: Option<GenFilter>,

    /// Filter by ogeneration: VALUE (exact), +VALUE (>= VALUE), -VALUE (<= VALUE)
    #[clap(short = 'C', long, value_name = "[+|-]VALUE", allow_hyphen_values = true, help_heading = HEADING_SORTING)]
    ogen_filter: Option<GenFilter>,

    /// Sort by comma-separated keys: gen, ogen, rootid, path
    ///
    /// Prefix with + (ascending, default) or - (descending).
    /// Example: --sort=gen,-ogen,path
    #[clap(
        long,
        value_name = "KEYS",
        value_delimiter = ',',
        allow_hyphen_values = true,
        help_heading = HEADING_SORTING,
    )]
    sort: Vec<SortKey>,

    /// Path to a mounted btrfs filesystem
    path: PathBuf,
}

impl Runnable for SubvolumeListCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("failed to open '{}'", self.path.display())
        })?;

        let mut items = subvolume_list(file.as_fd()).with_context(|| {
            format!("failed to list subvolumes for '{}'", self.path.display())
        })?;

        let top_id = btrfs_uapi::inode::lookup_path_rootid(file.as_fd())
            .with_context(|| "failed to get root id for path")?;

        // Apply filters.
        //
        // Deleted subvolumes have parent_id == 0 (no ROOT_BACKREF found).
        // By default they are hidden; -d shows only deleted; -a shows all.
        if self.deleted {
            items.retain(|item| item.parent_id == 0);
        } else if !self.all {
            items.retain(|item| item.parent_id != 0);
        }
        if self.readonly {
            items.retain(|item| item.flags.contains(SubvolumeFlags::RDONLY));
        }
        if self.snapshots_only {
            items.retain(|item| !item.parent_uuid.is_nil());
        }
        if let Some(ref f) = self.gen_filter {
            items.retain(|item| f.matches(item.generation));
        }
        if let Some(ref f) = self.ogen_filter {
            items.retain(|item| f.matches(item.otransid));
        }
        if self.only_below {
            // -o: only list subvolumes that are direct children of the
            // subvolume the fd is open on (i.e. whose parent_id matches the
            // fd's root ID).
            items.retain(|item| item.parent_id == top_id);
        }

        // -a: annotate paths of subvolumes outside the fd's subvolume with
        // a <FS_TREE> prefix, matching btrfs-progs behaviour.
        if self.all {
            for item in &mut items {
                if item.parent_id != 0 && item.parent_id != top_id {
                    if !item.name.is_empty() {
                        item.name = format!("<FS_TREE>/{}", item.name);
                    }
                }
            }
        }

        // Sort.
        if self.sort.is_empty() {
            items.sort_by_key(|item| item.root_id);
        } else {
            items.sort_by(|a, b| {
                for key in &self.sort {
                    let ord = key.compare(a, b);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }

        if self.table {
            self.print_table(&items);
        } else {
            self.print_default(&items);
        }

        Ok(())
    }
}

impl SubvolumeListCommand {
    fn print_default(&self, items: &[SubvolumeListItem]) {
        for item in items {
            let name = if item.name.is_empty() {
                "<unknown>"
            } else {
                &item.name
            };

            // Build the output line incrementally in the same field order as
            // btrfs-progs: ID, gen, [cgen,] top level, [parent,] path, [uuid,]
            // [parent_uuid,] [received_uuid]
            let mut line =
                format!("ID {} gen {}", item.root_id, item.generation);

            if self.ogeneration {
                line.push_str(&format!(" cgen {}", item.otransid));
            }

            line.push_str(&format!(" top level {}", item.parent_id));

            if self.parent {
                line.push_str(&format!(" parent {}", item.parent_id));
            }

            line.push_str(&format!(" path {name}"));

            if self.uuid {
                line.push_str(&format!(" uuid {}", fmt_uuid(&item.uuid)));
            }

            if self.parent_uuid {
                line.push_str(&format!(
                    " parent_uuid {}",
                    fmt_uuid(&item.parent_uuid)
                ));
            }

            if self.received_uuid {
                line.push_str(&format!(
                    " received_uuid {}",
                    fmt_uuid(&item.received_uuid)
                ));
            }

            println!("{line}");
        }
    }

    fn print_table(&self, items: &[SubvolumeListItem]) {
        // Collect column headers and data in order.
        let mut headers: Vec<&str> = vec!["ID", "gen"];
        if self.ogeneration {
            headers.push("cgen");
        }
        headers.push("top level");
        if self.parent {
            headers.push("parent");
        }
        headers.push("path");
        if self.uuid {
            headers.push("uuid");
        }
        if self.parent_uuid {
            headers.push("parent_uuid");
        }
        if self.received_uuid {
            headers.push("received_uuid");
        }

        // Print header row.
        println!("{}", headers.join("\t"));

        // Print separator.
        let sep: Vec<String> =
            headers.iter().map(|h| "-".repeat(h.len())).collect();
        println!("{}", sep.join("\t"));

        // Print rows.
        for item in items {
            let name = if item.name.is_empty() {
                "<unknown>"
            } else {
                &item.name
            };

            let mut cols: Vec<String> =
                vec![item.root_id.to_string(), item.generation.to_string()];
            if self.ogeneration {
                cols.push(item.otransid.to_string());
            }
            cols.push(item.parent_id.to_string());
            if self.parent {
                cols.push(item.parent_id.to_string());
            }
            cols.push(name.to_string());
            if self.uuid {
                cols.push(fmt_uuid(&item.uuid));
            }
            if self.parent_uuid {
                cols.push(fmt_uuid(&item.parent_uuid));
            }
            if self.received_uuid {
                cols.push(fmt_uuid(&item.received_uuid));
            }

            println!("{}", cols.join("\t"));
        }
    }
}

fn fmt_uuid(u: &uuid::Uuid) -> String {
    if u.is_nil() {
        "-".to_string()
    } else {
        u.hyphenated().to_string()
    }
}

/// A generation filter: exact match, >= (plus), or <= (minus).
#[derive(Debug, Clone)]
pub enum GenFilter {
    Exact(u64),
    AtLeast(u64),
    AtMost(u64),
}

impl GenFilter {
    fn matches(&self, value: u64) -> bool {
        match self {
            GenFilter::Exact(v) => value == *v,
            GenFilter::AtLeast(v) => value >= *v,
            GenFilter::AtMost(v) => value <= *v,
        }
    }
}

impl FromStr for GenFilter {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix('+') {
            let v: u64 = rest
                .parse()
                .map_err(|_| format!("invalid number: '{rest}'"))?;
            Ok(GenFilter::AtLeast(v))
        } else if let Some(rest) = s.strip_prefix('-') {
            let v: u64 = rest
                .parse()
                .map_err(|_| format!("invalid number: '{rest}'"))?;
            Ok(GenFilter::AtMost(v))
        } else {
            let v: u64 =
                s.parse().map_err(|_| format!("invalid number: '{s}'"))?;
            Ok(GenFilter::Exact(v))
        }
    }
}

/// A sort key with direction.
#[derive(Debug, Clone)]
pub struct SortKey {
    field: SortField,
    descending: bool,
}

#[derive(Debug, Clone)]
enum SortField {
    Gen,
    Ogen,
    Rootid,
    Path,
}

impl SortKey {
    fn compare(
        &self,
        a: &SubvolumeListItem,
        b: &SubvolumeListItem,
    ) -> Ordering {
        let ord = match self.field {
            SortField::Gen => a.generation.cmp(&b.generation),
            SortField::Ogen => a.otransid.cmp(&b.otransid),
            SortField::Rootid => a.root_id.cmp(&b.root_id),
            SortField::Path => a.name.cmp(&b.name),
        };
        if self.descending { ord.reverse() } else { ord }
    }
}

impl FromStr for SortKey {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (descending, field_str) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix('+') {
            (false, rest)
        } else {
            (false, s)
        };

        let field = match field_str {
            "gen" => SortField::Gen,
            "ogen" => SortField::Ogen,
            "rootid" => SortField::Rootid,
            "path" => SortField::Path,
            _ => {
                return Err(format!(
                    "unknown sort key: '{field_str}' (expected gen, ogen, rootid, or path)"
                ));
            }
        };

        Ok(SortKey { field, descending })
    }
}

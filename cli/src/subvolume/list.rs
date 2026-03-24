use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{SubvolumeFlags, subvolume_list};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// List subvolumes and snapshots in the filesystem
///
/// The default output format matches btrfs-progs:
///   ID <id> gen <gen> top level <parent_id> path <name>
///
/// Optional flags enable additional columns or filter the results.
#[derive(Parser, Debug)]
pub struct SubvolumeListCommand {
    /// Print parent ID column (same as top level for non-snapshots)
    #[clap(short)]
    parent: bool,

    /// Print generation column (already shown by default; kept for
    /// compatibility with btrfs-progs CLI)
    #[clap(short)]
    generation: bool,

    /// Print ogeneration (generation at creation) column
    #[clap(short = 'c')]
    ogeneration: bool,

    /// Print UUID column
    #[clap(short)]
    uuid: bool,

    /// Print parent UUID column
    #[clap(short = 'Q')]
    parent_uuid: bool,

    /// Print received UUID column
    #[clap(short = 'R')]
    received_uuid: bool,

    /// List only read-only subvolumes
    #[clap(short = 'r')]
    readonly: bool,

    /// List only snapshots (subvolumes with a non-nil parent UUID)
    #[clap(short = 's')]
    snapshots_only: bool,

    /// Path to a mounted btrfs filesystem
    path: PathBuf,
}

impl Runnable for SubvolumeListCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let mut items = subvolume_list(file.as_fd())
            .with_context(|| format!("failed to list subvolumes for '{}'", self.path.display()))?;

        // Apply filters.
        if self.readonly {
            items.retain(|item| item.flags.contains(SubvolumeFlags::RDONLY));
        }
        if self.snapshots_only {
            items.retain(|item| !item.parent_uuid.is_nil());
        }

        items.sort_by_key(|item| item.root_id);

        for item in &items {
            let name = if item.name.is_empty() {
                "<unknown>".to_string()
            } else {
                item.name.clone()
            };

            // Build the output line incrementally in the same field order as
            // btrfs-progs: ID, gen, [cgen,] top level, [parent,] path, [uuid,]
            // [parent_uuid,] [received_uuid]
            let mut line = format!("ID {} gen {}", item.root_id, item.generation);

            if self.ogeneration {
                line.push_str(&format!(" cgen {}", item.otransid));
            }

            line.push_str(&format!(" top level {}", item.parent_id));

            if self.parent {
                line.push_str(&format!(" parent {}", item.parent_id));
            }

            line.push_str(&format!(" path {}", name));

            if self.uuid {
                line.push_str(&format!(" uuid {}", fmt_uuid(&item.uuid)));
            }

            if self.parent_uuid {
                line.push_str(&format!(" parent_uuid {}", fmt_uuid(&item.parent_uuid)));
            }

            if self.received_uuid {
                line.push_str(&format!(" received_uuid {}", fmt_uuid(&item.received_uuid)));
            }

            println!("{line}");
        }

        Ok(())
    }
}

fn fmt_uuid(u: &uuid::Uuid) -> String {
    if u.is_nil() {
        "-".to_string()
    } else {
        u.hyphenated().to_string()
    }
}

use super::UnitMode;
use crate::{
    Format, RunContext, Runnable,
    util::{fmt_size, open_path, print_json},
};
use anyhow::{Context, Result};
use btrfs_uapi::space::space_info;
use clap::Parser;
use serde::Serialize;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Show space usage information for a mounted filesystem
#[derive(Parser, Debug)]
pub struct FilesystemDfCommand {
    #[clap(flatten)]
    pub units: UnitMode,

    pub path: PathBuf,
}

#[derive(Serialize)]
struct SpaceEntryJson {
    #[serde(rename = "type")]
    bg_type: String,
    profile: String,
    total: u64,
    used: u64,
}

impl Runnable for FilesystemDfCommand {
    fn supported_formats(&self) -> &[Format] {
        &[Format::Text, Format::Json, Format::Modern]
    }

    fn run(&self, ctx: &RunContext) -> Result<()> {
        let mode = self.units.resolve();
        let file = open_path(&self.path)?;
        let entries = space_info(file.as_fd()).with_context(|| {
            format!("failed to get space info for '{}'", self.path.display())
        })?;

        match ctx.format {
            Format::Modern | Format::Text => {
                for entry in &entries {
                    println!(
                        "{}, {}: total={}, used={}",
                        entry.flags.type_name(),
                        entry.flags.profile_name(),
                        fmt_size(entry.total_bytes, &mode),
                        fmt_size(entry.used_bytes, &mode),
                    );
                }
            }
            Format::Json => {
                let json: Vec<SpaceEntryJson> = entries
                    .iter()
                    .map(|e| SpaceEntryJson {
                        bg_type: e.flags.type_name().to_string(),
                        profile: e.flags.profile_name().to_string(),
                        total: e.total_bytes,
                        used: e.used_bytes,
                    })
                    .collect();
                print_json("filesystem-df", &json)?;
            }
        }

        Ok(())
    }
}

use crate::{Format, Runnable};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{SubvolumeFlags, subvolume_flags_get, subvolume_flags_set};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Show the flags of a subvolume
#[derive(Parser, Debug)]
pub struct SubvolumeGetFlagsCommand {
    /// Path to a subvolume
    pub path: PathBuf,
}

impl Runnable for SubvolumeGetFlagsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        let flags = subvolume_flags_get(file.as_fd())
            .with_context(|| format!("failed to get flags for '{}'", self.path.display()))?;

        println!("{}", flags);

        Ok(())
    }
}

/// Set the flags of a subvolume
#[derive(Parser, Debug)]
pub struct SubvolumeSetFlagsCommand {
    /// Flags to set ("readonly" or "-" to clear)
    #[clap(value_parser = parse_flags)]
    pub flags: SubvolumeFlags,

    /// Path to a subvolume
    pub path: PathBuf,
}

impl Runnable for SubvolumeSetFlagsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        subvolume_flags_set(file.as_fd(), self.flags)
            .with_context(|| format!("failed to set flags on '{}'", self.path.display()))?;

        println!("Set flags to {} on '{}'", self.flags, self.path.display());

        Ok(())
    }
}

fn parse_flags(s: &str) -> std::result::Result<SubvolumeFlags, String> {
    match s {
        "readonly" => Ok(SubvolumeFlags::RDONLY),
        "-" | "" | "none" => Ok(SubvolumeFlags::empty()),
        _ => Err(format!("unknown flag '{}'; expected 'readonly' or '-'", s)),
    }
}

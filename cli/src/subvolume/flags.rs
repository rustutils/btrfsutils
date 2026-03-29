use crate::{Format, Runnable, util::open_path};
use anyhow::{Context, Result};
use btrfs_uapi::subvolume::{
    SubvolumeFlags, subvolume_flags_get, subvolume_flags_set,
};
use clap::Parser;
use std::{os::unix::io::AsFd, path::PathBuf};

/// Wrapper around SubvolumeFlags that implements FromStr for clap parsing.
#[derive(Debug, Clone, Copy)]
pub struct ParsedSubvolumeFlags(SubvolumeFlags);

impl std::str::FromStr for ParsedSubvolumeFlags {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "readonly" => Ok(Self(SubvolumeFlags::RDONLY)),
            "-" | "" | "none" => Ok(Self(SubvolumeFlags::empty())),
            _ => Err(format!("unknown flag '{s}'; expected 'readonly' or '-'")),
        }
    }
}

/// Show the flags of a subvolume
#[derive(Parser, Debug)]
pub struct SubvolumeGetFlagsCommand {
    /// Path to a subvolume
    pub path: PathBuf,
}

impl Runnable for SubvolumeGetFlagsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        let flags = subvolume_flags_get(file.as_fd()).with_context(|| {
            format!("failed to get flags for '{}'", self.path.display())
        })?;

        println!("{}", flags);

        Ok(())
    }
}

/// Set the flags of a subvolume
#[derive(Parser, Debug)]
pub struct SubvolumeSetFlagsCommand {
    /// Flags to set ("readonly" or "-" to clear)
    pub flags: ParsedSubvolumeFlags,

    /// Path to a subvolume
    pub path: PathBuf,
}

impl Runnable for SubvolumeSetFlagsCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;

        subvolume_flags_set(file.as_fd(), self.flags.0).with_context(|| {
            format!("failed to set flags on '{}'", self.path.display())
        })?;

        println!("Set flags to {} on '{}'", self.flags.0, self.path.display());

        Ok(())
    }
}

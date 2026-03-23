use crate::{Format, Runnable, util::parse_size_with_suffix};
use anyhow::{Context, Result};
use btrfs_uapi::resize::{ResizeAmount, ResizeArgs, resize};
use clap::Parser;
use std::{fs::File, os::unix::io::AsFd, path::PathBuf};

/// Resize a mounted btrfs filesystem
#[derive(Parser, Debug)]
pub struct FilesystemResizeCommand {
    /// Wait if there is another exclusive operation running, otherwise error
    #[clap(long)]
    pub enqueue: bool,

    /// Resize a filesystem stored in a file image (unmounted)
    #[clap(long)]
    pub offline: bool,

    /// New size for the filesystem, e.g. "1G", "+512M", "-1G", "max", "cancel",
    /// or "devid:<id>:<size>" to target a specific device
    pub size: String,

    pub path: PathBuf,
}

fn parse_resize_amount(s: &str) -> Result<ResizeAmount> {
    if s == "cancel" {
        return Ok(ResizeAmount::Cancel);
    }
    if s == "max" {
        return Ok(ResizeAmount::Max);
    }
    let (modifier, rest) = if let Some(r) = s.strip_prefix('+') {
        (1i32, r)
    } else if let Some(r) = s.strip_prefix('-') {
        (-1i32, r)
    } else {
        (0i32, s)
    };
    let bytes = parse_size_with_suffix(rest)?;
    Ok(match modifier {
        1 => ResizeAmount::Add(bytes),
        -1 => ResizeAmount::Sub(bytes),
        _ => ResizeAmount::Set(bytes),
    })
}

fn parse_resize_args(s: &str) -> Result<ResizeArgs> {
    if let Some(colon) = s.find(':') {
        if let Ok(devid) = s[..colon].parse::<u64>() {
            let amount = parse_resize_amount(&s[colon + 1..])?;
            return Ok(ResizeArgs::new(amount).with_devid(devid));
        }
    }
    Ok(ResizeArgs::new(parse_resize_amount(s)?))
}

impl Runnable for FilesystemResizeCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        if self.offline {
            anyhow::bail!("--offline is not yet implemented");
        }

        if self.enqueue {
            anyhow::bail!("--enqueue is not yet implemented");
        }

        let args = parse_resize_args(&self.size)
            .with_context(|| format!("invalid resize argument: '{}'", self.size))?;

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path.display()))?;

        resize(file.as_fd(), args)
            .with_context(|| format!("resize failed on '{}'", self.path.display()))?;

        Ok(())
    }
}

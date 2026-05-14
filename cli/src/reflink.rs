use crate::{CommandGroup, RunContext, Runnable, util::parse_size_with_suffix};
use anyhow::{Context, Result};
use btrfs_uapi::reflink;
use clap::Parser;
use std::{fs::OpenOptions, os::fd::AsFd, path::PathBuf};

/// Toolbox for reflink operations: lightweight file copies that
/// share data extents instead of copying bytes.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
#[clap(arg_required_else_help = true)]
pub struct ReflinkCommand {
    #[clap(subcommand)]
    pub subcommand: ReflinkSubcommand,
}

impl CommandGroup for ReflinkCommand {
    fn leaf(&self) -> &dyn Runnable {
        match &self.subcommand {
            ReflinkSubcommand::Clone(cmd) => cmd,
        }
    }
}

#[derive(Parser, Debug)]
pub enum ReflinkSubcommand {
    Clone(ReflinkCloneCommand),
}

/// Lightweight file copy: data extents are shared between source
/// and target instead of physically copied. Subsequent modifications
/// are copy-on-write, so reading from the clone is fast and storage
/// is only consumed once until the files diverge.
///
/// With no -r flags, the entire source file is cloned to the target
/// (which is created or truncated as needed). With one or more
/// -r RANGESPEC flags, only those byte ranges are cloned into the
/// target at the specified destination offsets and the existing
/// target contents outside those ranges are preserved.
///
/// RANGESPEC has three parts: SRCOFF:LENGTH:DESTOFF, where SRCOFF
/// is the byte offset in the source file, LENGTH is the number of
/// bytes (0 = up to end-of-source), and DESTOFF is the byte offset
/// in the target file. All three values accept the size suffix
/// k/m/g/t/p/e (case-insensitive). Offsets and length must be
/// block-aligned (typically 4 KiB) except when the source range
/// reaches end-of-file.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct ReflinkCloneCommand {
    /// Reflink only this range: SRCOFF:LENGTH:DESTOFF. May be
    /// specified more than once; ranges are processed in order.
    #[clap(short = 'r', long = "range", value_name = "RANGESPEC")]
    ranges: Vec<String>,

    /// Source file (read).
    source: PathBuf,

    /// Target file (created if missing, truncated when no -r is given).
    target: PathBuf,
}

impl Runnable for ReflinkCloneCommand {
    fn run(&self, ctx: &RunContext) -> Result<()> {
        let ranges: Vec<RangeSpec> = self
            .ranges
            .iter()
            .map(|s| RangeSpec::parse(s))
            .collect::<Result<_>>()?;

        let source = OpenOptions::new()
            .read(true)
            .open(&self.source)
            .with_context(|| {
                format!("opening source {}", self.source.display())
            })?;
        let target = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Truncate only when cloning the whole file; explicit
            // ranges target an existing layout and would lose data
            // if we truncated underneath them.
            .truncate(ranges.is_empty())
            .open(&self.target)
            .with_context(|| {
                format!("opening target {}", self.target.display())
            })?;

        if !ctx.quiet {
            println!("Source: {}", self.source.display());
            println!("Target: {}", self.target.display());
        }

        if ranges.is_empty() {
            // length=0 → "to end of source file" per the kernel ABI.
            reflink::clone_range(source.as_fd(), 0, 0, target.as_fd(), 0)
                .context("cloning entire source file")?;
        } else {
            for r in &ranges {
                if !ctx.quiet {
                    println!(
                        "Range: {}:{}:{}",
                        r.src_offset, r.length, r.dest_offset
                    );
                }
                reflink::clone_range(
                    source.as_fd(),
                    r.src_offset,
                    r.length,
                    target.as_fd(),
                    r.dest_offset,
                )
                .with_context(|| {
                    format!(
                        "cloning {}:{}:{}",
                        r.src_offset, r.length, r.dest_offset
                    )
                })?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeSpec {
    src_offset: u64,
    length: u64,
    dest_offset: u64,
}

impl RangeSpec {
    fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            anyhow::bail!(
                "range spec {s:?} must have three colon-separated parts: SRCOFF:LENGTH:DESTOFF"
            );
        }
        let src_offset = parse_size_with_suffix(parts[0])
            .with_context(|| format!("parsing SRCOFF {:?}", parts[0]))?;
        let length = parse_size_with_suffix(parts[1])
            .with_context(|| format!("parsing LENGTH {:?}", parts[1]))?;
        let dest_offset = parse_size_with_suffix(parts[2])
            .with_context(|| format!("parsing DESTOFF {:?}", parts[2]))?;
        Ok(Self {
            src_offset,
            length,
            dest_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_spec_parses_decimal_triple() {
        let r = RangeSpec::parse("0:4096:8192").unwrap();
        assert_eq!(
            r,
            RangeSpec {
                src_offset: 0,
                length: 4096,
                dest_offset: 8192,
            }
        );
    }

    #[test]
    fn range_spec_accepts_size_suffixes() {
        let r = RangeSpec::parse("1k:4M:2G").unwrap();
        assert_eq!(
            r,
            RangeSpec {
                src_offset: 1024,
                length: 4 * 1024 * 1024,
                dest_offset: 2 * 1024 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn range_spec_length_zero_is_eof_sentinel() {
        let r = RangeSpec::parse("0:0:0").unwrap();
        assert_eq!(r.length, 0);
    }

    #[test]
    fn range_spec_rejects_wrong_arity() {
        assert!(RangeSpec::parse("0:4096").is_err());
        assert!(RangeSpec::parse("0:4096:8192:extra").is_err());
        assert!(RangeSpec::parse("").is_err());
    }

    #[test]
    fn range_spec_rejects_unknown_suffix() {
        assert!(RangeSpec::parse("1x:4096:0").is_err());
    }
}

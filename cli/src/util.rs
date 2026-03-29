use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local};
use std::{
    fs::{self, File},
    io::BufRead,
    os::unix::fs::FileTypeExt,
    path::Path,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

/// Open a path and return the `File`, with a contextual error message on failure.
pub fn open_path(path: &Path) -> Result<File> {
    File::open(path)
        .with_context(|| format!("failed to open '{}'", path.display()))
}

/// Return `true` if `device` appears as a source in `/proc/mounts`.
///
/// Compares canonical paths so symlinks in `/dev/disk/by-*` are handled
/// correctly. Returns `false` if `/proc/mounts` cannot be read or if the
/// canonical path of `device` cannot be resolved.
pub fn is_mounted(device: &Path) -> bool {
    let Ok(canon) = fs::canonicalize(device) else {
        return false;
    };
    let Ok(f) = File::open("/proc/mounts") else {
        return false;
    };
    let reader = std::io::BufReader::new(f);
    for line in reader.lines().map_while(|l| l.ok()) {
        let mut fields = line.split_whitespace();
        if let Some(src) = fields.next() {
            if let Ok(src_canon) = fs::canonicalize(src) {
                if src_canon == canon {
                    return true;
                }
            }
        }
    }
    false
}

/// Resolved size display mode.
pub enum SizeFormat {
    /// Print raw byte count with no suffix.
    Raw,
    /// Human-readable with base 1024 (KiB, MiB, GiB, …).
    HumanIec,
    /// Human-readable with base 1000 (kB, MB, GB, …).
    HumanSi,
    /// Divide by a fixed power and print the integer result.
    Fixed(u64),
}

/// Format a byte count according to the given [`SizeFormat`].
pub fn fmt_size(bytes: u64, mode: &SizeFormat) -> String {
    match mode {
        SizeFormat::Raw => bytes.to_string(),
        SizeFormat::HumanIec => human_bytes(bytes),
        SizeFormat::HumanSi => human_bytes_si(bytes),
        SizeFormat::Fixed(divisor) => format!("{}", bytes / divisor),
    }
}

/// Format a byte count as a human-readable string using binary prefixes.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.2}{}", UNITS[unit])
    }
}

/// Format a byte count as a human-readable string using SI (base-1000) prefixes.
pub fn human_bytes_si(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "kB", "MB", "GB", "TB", "PB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit + 1 < UNITS.len() {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.2}{}", UNITS[unit])
    }
}

/// Format a [`SystemTime`] as a local-time datetime string in the same style
/// as the C btrfs-progs tool: `YYYY-MM-DD HH:MM:SS ±HHMM`.
///
/// Returns `"-"` when the time is [`UNIX_EPOCH`] (i.e. not set).
pub fn format_time(t: SystemTime) -> String {
    if t == UNIX_EPOCH {
        return "-".to_string();
    }
    match DateTime::<Local>::from(t)
        .format("%Y-%m-%d %H:%M:%S %z")
        .to_string()
    {
        s if s.is_empty() => "-".to_string(),
        s => s,
    }
}

/// Format a [`SystemTime`] for replace-status output: `%e.%b %T`
/// (e.g. ` 5.Mar 14:30:00`).
pub fn format_time_short(t: &SystemTime) -> String {
    DateTime::<Local>::from(*t).format("%e.%b %T").to_string()
}

/// Format a unix timestamp (sec, nsec) as `sec.nsec (YYYY-MM-DD HH:MM:SS)`.
pub fn format_timespec(sec: u64, nsec: u32) -> String {
    let sec_i64 = sec as i64;
    match DateTime::from_timestamp(sec_i64, nsec) {
        Some(utc) => {
            let local = utc.with_timezone(&Local);
            format!("{}.{} ({})", sec, nsec, local.format("%Y-%m-%d %H:%M:%S"))
        }
        None => format!("{sec}.{nsec}"),
    }
}

/// Parse a size string with an optional binary suffix (K, M, G, T, P, E).
pub fn parse_size_with_suffix(s: &str) -> Result<u64> {
    let (num_str, suffix) = match s.find(|c: char| c.is_alphabetic()) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let n: u64 = num_str
        .parse()
        .with_context(|| format!("invalid size number: '{num_str}'"))?;
    let multiplier: u64 = match suffix.to_uppercase().as_str() {
        "" => 1,
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        "T" => 1024u64.pow(4),
        "P" => 1024u64.pow(5),
        "E" => 1024u64.pow(6),
        _ => anyhow::bail!("unknown size suffix: '{suffix}'"),
    };
    n.checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("size overflow: '{s}'"))
}

/// A UUID value parsed from a CLI argument.
///
/// Accepts `clear` (nil UUID), `random` (random v4 UUID), `time` (v7
/// time-ordered UUID), or any standard UUID string (with or without hyphens).
#[derive(Debug, Clone, Copy)]
pub struct ParsedUuid(Uuid);

impl std::ops::Deref for ParsedUuid {
    type Target = Uuid;
    fn deref(&self) -> &Uuid {
        &self.0
    }
}

/// Parse a qgroup ID string of the form `"<level>/<subvolid>"` into a packed u64.
///
/// The packed form is `(level as u64) << 48 | subvolid`.
/// Example: `"0/5"` → `5`, `"1/256"` → `0x0001_0000_0000_0100`.
pub fn parse_qgroupid(s: &str) -> anyhow::Result<u64> {
    let (level_str, id_str) = s.split_once('/').ok_or_else(|| {
        anyhow::anyhow!("invalid qgroup ID '{}': expected <level>/<id>", s)
    })?;
    let level: u64 = level_str.parse().map_err(|_| {
        anyhow::anyhow!("invalid qgroup level '{}' in '{}'", level_str, s)
    })?;
    let subvolid: u64 = id_str.parse().map_err(|_| {
        anyhow::anyhow!("invalid qgroup subvolid '{}' in '{}'", id_str, s)
    })?;
    Ok((level << 48) | subvolid)
}

impl FromStr for ParsedUuid {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "clear" => Ok(Self(Uuid::nil())),
            "random" => Ok(Self(Uuid::new_v4())),
            "time" => Ok(Self(Uuid::now_v7())),
            _ => Uuid::parse_str(s)
                .map(Self)
                .map_err(|e| format!("invalid UUID: {e}")),
        }
    }
}

/// Check that a device is suitable for use as a btrfs target (add or replace).
///
/// Verifies that the path is a block device, is not currently mounted, and
/// does not already contain a btrfs filesystem (unless `force` is true).
pub fn check_device_for_overwrite(device: &Path, force: bool) -> Result<()> {
    let meta = fs::metadata(device).with_context(|| {
        format!("cannot access device '{}'", device.display())
    })?;

    if !meta.file_type().is_block_device() {
        bail!("'{}' is not a block device", device.display());
    }

    if is_device_mounted(device)? {
        bail!(
            "'{}' is mounted; refusing to use a mounted device",
            device.display()
        );
    }

    if !force && has_btrfs_superblock(device) {
        bail!(
            "'{}' already contains a btrfs filesystem; use -f to force",
            device.display()
        );
    }

    Ok(())
}

/// Check if a device path appears in /proc/mounts.
pub fn is_device_mounted(device: &Path) -> Result<bool> {
    let canonical = fs::canonicalize(device).with_context(|| {
        format!("cannot resolve path '{}'", device.display())
    })?;
    let canonical_str = canonical.to_string_lossy();

    let file =
        File::open("/proc/mounts").context("failed to open /proc/mounts")?;
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if let Some(mount_dev) = line.split_whitespace().next()
            && mount_dev == canonical_str.as_ref()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Try to read a btrfs superblock from the device. Returns true if a valid
/// btrfs magic signature is found.
pub fn has_btrfs_superblock(device: &Path) -> bool {
    let Ok(mut file) = File::open(device) else {
        return false;
    };
    match btrfs_disk::superblock::read_superblock(&mut file, 0) {
        Ok(sb) => sb.magic_is_valid(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- human_bytes ---

    #[test]
    fn human_bytes_zero() {
        assert_eq!(human_bytes(0), "0B");
    }

    #[test]
    fn human_bytes_small() {
        assert_eq!(human_bytes(1), "1B");
        assert_eq!(human_bytes(1023), "1023B");
    }

    #[test]
    fn human_bytes_exact_powers() {
        assert_eq!(human_bytes(1024), "1.00KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.00MiB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.00GiB");
        assert_eq!(human_bytes(1024u64.pow(4)), "1.00TiB");
        assert_eq!(human_bytes(1024u64.pow(5)), "1.00PiB");
    }

    #[test]
    fn human_bytes_fractional() {
        // 1.5 GiB = 1024^3 + 512*1024^2
        assert_eq!(
            human_bytes(1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "1.50GiB"
        );
    }

    #[test]
    fn human_bytes_u64_max() {
        // Should not panic; lands in PiB range
        let s = human_bytes(u64::MAX);
        assert!(s.ends_with("PiB"), "expected PiB suffix, got: {s}");
    }

    // --- parse_size_with_suffix ---

    #[test]
    fn parse_size_bare_number() {
        assert_eq!(parse_size_with_suffix("0").unwrap(), 0);
        assert_eq!(parse_size_with_suffix("42").unwrap(), 42);
    }

    #[test]
    fn parse_size_all_suffixes() {
        assert_eq!(parse_size_with_suffix("1K").unwrap(), 1024);
        assert_eq!(parse_size_with_suffix("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size_with_suffix("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_with_suffix("1T").unwrap(), 1024u64.pow(4));
        assert_eq!(parse_size_with_suffix("1P").unwrap(), 1024u64.pow(5));
        assert_eq!(parse_size_with_suffix("1E").unwrap(), 1024u64.pow(6));
    }

    #[test]
    fn parse_size_case_insensitive() {
        assert_eq!(parse_size_with_suffix("4k").unwrap(), 4 * 1024);
        assert_eq!(
            parse_size_with_suffix("2g").unwrap(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_size_overflow() {
        assert!(parse_size_with_suffix("16385P").is_err());
    }

    #[test]
    fn parse_size_bad_number() {
        assert!(parse_size_with_suffix("abcM").is_err());
        assert!(parse_size_with_suffix("").is_err());
    }

    #[test]
    fn parse_size_unknown_suffix() {
        assert!(parse_size_with_suffix("10X").is_err());
    }

    // --- parse_qgroupid ---

    #[test]
    fn parse_qgroupid_level0() {
        assert_eq!(parse_qgroupid("0/5").unwrap(), 5);
        assert_eq!(parse_qgroupid("0/256").unwrap(), 256);
    }

    #[test]
    fn parse_qgroupid_higher_level() {
        assert_eq!(parse_qgroupid("1/256").unwrap(), (1u64 << 48) | 256);
        assert_eq!(parse_qgroupid("2/0").unwrap(), 2u64 << 48);
    }

    #[test]
    fn parse_qgroupid_missing_slash() {
        assert!(parse_qgroupid("5").is_err());
    }

    #[test]
    fn parse_qgroupid_bad_level() {
        assert!(parse_qgroupid("abc/5").is_err());
    }

    #[test]
    fn parse_qgroupid_bad_subvolid() {
        assert!(parse_qgroupid("0/abc").is_err());
    }

    // --- ParsedUuid ---

    #[test]
    fn parsed_uuid_clear() {
        let u: ParsedUuid = "clear".parse().unwrap();
        assert!(u.is_nil());
    }

    #[test]
    fn parsed_uuid_random() {
        let u: ParsedUuid = "random".parse().unwrap();
        assert!(!u.is_nil());
    }

    #[test]
    fn parsed_uuid_time() {
        let u: ParsedUuid = "time".parse().unwrap();
        assert!(!u.is_nil());
    }

    #[test]
    fn parsed_uuid_explicit() {
        let u: ParsedUuid =
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn parsed_uuid_no_hyphens() {
        let u: ParsedUuid = "550e8400e29b41d4a716446655440000".parse().unwrap();
        assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn parsed_uuid_invalid() {
        assert!("not-a-uuid".parse::<ParsedUuid>().is_err());
        assert!("".parse::<ParsedUuid>().is_err());
    }
}

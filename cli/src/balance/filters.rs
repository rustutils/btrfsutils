//! Parse balance filter strings like `usage=50,profiles=raid1|single,limit=10`.

use anyhow::{Result, bail};
use btrfs_uapi::{balance::BalanceArgs, space::BlockGroupFlags};

/// Parse a comma-separated filter string into a `BalanceArgs`.
///
/// Supported filters:
///   profiles=name|name|...   — RAID profile bitmask (single, raid0, raid1, raid10,
///                               raid5, raid6, raid1c3, raid1c4, dup)
///   usage=N                  — chunks below N% usage (0-100)
///   usage=min..max           — chunks with usage in range
///   devid=N                  — chunks on device N
///   drange=start..end        — physical byte range on device
///   vrange=start..end        — virtual (logical) byte range
///   convert=profile          — convert to target profile
///   soft                     — with convert, skip already-converted chunks
///   limit=N                  — process at most N chunks
///   limit=min..max           — process between min and max chunks
///   stripes=min..max         — filter by stripe count range
pub fn parse_filters(filter_str: &str) -> Result<BalanceArgs> {
    let mut args = BalanceArgs::new();

    if filter_str.is_empty() {
        return Ok(args);
    }

    for part in filter_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((key, value)) = part.split_once('=') {
            args = parse_one_filter(args, key.trim(), value.trim())?;
        } else if part == "soft" {
            args = args.soft();
        } else {
            bail!("unrecognized balance filter: '{part}'");
        }
    }

    Ok(args)
}

fn parse_one_filter(args: BalanceArgs, key: &str, value: &str) -> Result<BalanceArgs> {
    if value.is_empty() {
        bail!("the {key} filter requires an argument");
    }

    match key {
        "profiles" => {
            let bits = parse_profiles(value)?;
            Ok(args.profiles(bits))
        }
        "usage" => {
            if let Ok(n) = value.parse::<u64>() {
                if n > 100 {
                    bail!("invalid usage argument: {value} (must be 0-100)");
                }
                Ok(args.usage(n))
            } else if let Some((min, max)) = parse_range_u32(value)? {
                // Only validate max when explicitly provided (open-ended
                // ranges like "10.." use u32::MAX as sentinel).
                if max != u32::MAX && max > 100 {
                    bail!("invalid usage argument: {value} (max must be <= 100)");
                }
                Ok(args.usage_range(min, max))
            } else {
                bail!("invalid usage argument: {value}")
            }
        }
        "devid" => {
            let devid: u64 = value
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid devid: {value}"))?;
            if devid == 0 {
                bail!("invalid devid: {value}");
            }
            Ok(args.devid(devid))
        }
        "drange" => {
            let (start, end) = parse_range_u64(value)?;
            Ok(args.drange(start, end))
        }
        "vrange" => {
            let (start, end) = parse_range_u64(value)?;
            Ok(args.vrange(start, end))
        }
        "convert" => {
            let profile = parse_one_profile(value)?;
            Ok(args.convert(profile))
        }
        "limit" => {
            if let Ok(n) = value.parse::<u64>() {
                Ok(args.limit(n))
            } else if let Some((min, max)) = parse_range_u32(value)? {
                Ok(args.limit_range(min, max))
            } else {
                bail!("invalid limit argument: {value}")
            }
        }
        "stripes" => {
            if let Some((min, max)) = parse_range_u32(value)? {
                Ok(args.stripes_range(min, max))
            } else {
                bail!("invalid stripes argument: {value}")
            }
        }
        _ => bail!("unrecognized balance filter: '{key}'"),
    }
}

/// Parse `|`-separated profile names into a bitmask.
fn parse_profiles(s: &str) -> Result<u64> {
    let mut flags = 0u64;
    for name in s.split('|') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        flags |= parse_one_profile(name)?;
    }
    Ok(flags)
}

/// Parse a single profile name into its bitmask value.
fn parse_one_profile(name: &str) -> Result<u64> {
    let bits = match name.to_ascii_lowercase().as_str() {
        "single" => BlockGroupFlags::SINGLE.bits(),
        "raid0" => BlockGroupFlags::RAID0.bits(),
        "raid1" => BlockGroupFlags::RAID1.bits(),
        "raid1c3" => BlockGroupFlags::RAID1C3.bits(),
        "raid1c4" => BlockGroupFlags::RAID1C4.bits(),
        "raid5" => BlockGroupFlags::RAID5.bits(),
        "raid6" => BlockGroupFlags::RAID6.bits(),
        "raid10" => BlockGroupFlags::RAID10.bits(),
        "dup" => BlockGroupFlags::DUP.bits(),
        _ => bail!("unknown profile: '{name}'"),
    };
    Ok(bits)
}

/// Parse a `min..max` range of u32 values. Either side may be omitted:
/// `..max`, `min..`, `min..max`.
fn parse_range_u32(s: &str) -> Result<Option<(u32, u32)>> {
    let Some((left, right)) = s.split_once("..") else {
        return Ok(None);
    };
    let min: u32 = if left.is_empty() {
        0
    } else {
        left.parse()
            .map_err(|_| anyhow::anyhow!("invalid range min: {left}"))?
    };
    let max: u32 = if right.is_empty() {
        u32::MAX
    } else {
        right
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid range max: {right}"))?
    };
    Ok(Some((min, max)))
}

/// Parse a `start..end` range of u64 values. Both sides required.
fn parse_range_u64(s: &str) -> Result<(u64, u64)> {
    let (left, right) = s
        .split_once("..")
        .ok_or_else(|| anyhow::anyhow!("expected range 'start..end', got: {s}"))?;
    let start: u64 = if left.is_empty() {
        0
    } else {
        left.parse()
            .map_err(|_| anyhow::anyhow!("invalid range start: {left}"))?
    };
    let end: u64 = if right.is_empty() {
        u64::MAX
    } else {
        right
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid range end: {right}"))?
    };
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter() {
        let args = parse_filters("").unwrap();
        // Should produce a default BalanceArgs with no flags set.
        let _ = args; // just verify it doesn't error
    }

    #[test]
    fn usage_single_value() {
        let _args = parse_filters("usage=50").unwrap();
    }

    #[test]
    fn usage_range() {
        let _args = parse_filters("usage=10..50").unwrap();
    }

    #[test]
    fn usage_over_100() {
        assert!(parse_filters("usage=101").is_err());
    }

    #[test]
    fn usage_range_over_100() {
        assert!(parse_filters("usage=0..101").is_err());
    }

    #[test]
    fn devid() {
        let _args = parse_filters("devid=1").unwrap();
    }

    #[test]
    fn devid_zero() {
        assert!(parse_filters("devid=0").is_err());
    }

    #[test]
    fn profiles_single() {
        let _args = parse_filters("profiles=single").unwrap();
    }

    #[test]
    fn profiles_multiple() {
        let _args = parse_filters("profiles=raid1|single|dup").unwrap();
    }

    #[test]
    fn profiles_unknown() {
        assert!(parse_filters("profiles=raid99").is_err());
    }

    #[test]
    fn convert_profile() {
        let _args = parse_filters("convert=raid1").unwrap();
    }

    #[test]
    fn soft_flag() {
        let _args = parse_filters("soft").unwrap();
    }

    #[test]
    fn limit_single() {
        let _args = parse_filters("limit=10").unwrap();
    }

    #[test]
    fn limit_range() {
        let _args = parse_filters("limit=5..20").unwrap();
    }

    #[test]
    fn stripes_range() {
        let _args = parse_filters("stripes=1..4").unwrap();
    }

    #[test]
    fn drange() {
        let _args = parse_filters("drange=0..1073741824").unwrap();
    }

    #[test]
    fn vrange() {
        let _args = parse_filters("vrange=0..1073741824").unwrap();
    }

    #[test]
    fn multiple_filters() {
        let _args = parse_filters("usage=50,devid=1,limit=10").unwrap();
    }

    #[test]
    fn convert_with_soft() {
        let _args = parse_filters("convert=raid1,soft").unwrap();
    }

    #[test]
    fn unknown_filter() {
        assert!(parse_filters("bogus=42").is_err());
    }

    #[test]
    fn missing_value() {
        assert!(parse_filters("usage=").is_err());
    }

    #[test]
    fn profiles_case_insensitive() {
        let _args = parse_filters("profiles=RAID1|Single").unwrap();
    }

    #[test]
    fn open_ended_ranges() {
        let _args = parse_filters("usage=..50").unwrap();
        let _args = parse_filters("usage=10..").unwrap();
        let _args = parse_filters("limit=..100").unwrap();
    }
}

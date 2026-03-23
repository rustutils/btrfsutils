use anyhow::{Context, Result};
use std::str::FromStr;
use uuid::Uuid;

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

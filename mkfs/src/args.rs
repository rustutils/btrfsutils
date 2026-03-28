use clap::Parser;
use std::path::PathBuf;
use uuid::Uuid;

/// Create a btrfs filesystem.
///
/// mkfs.btrfs is used to create a btrfs filesystem on a single or multiple
/// devices. The device is typically a block device but can be a file-backed
/// image as well. Multiple devices are grouped by UUID of the filesystem.
///
/// The default block group profiles for data and metadata depend on the number
/// of devices. For a single device the defaults are SINGLE for data and DUP
/// for metadata. For multiple devices the defaults are SINGLE for data and
/// RAID1 for metadata.
#[derive(Parser, Debug)]
#[command(name = "mkfs.btrfs", version)]
pub struct Arguments {
    /// Data block group profile.
    ///
    /// Valid values are raid0, raid1, raid1c3, raid1c4, raid5, raid6,
    /// raid10, single, or dup (case insensitive).
    #[arg(short = 'd', long = "data", value_name = "PROFILE")]
    pub data_profile: Option<Profile>,

    /// Metadata block group profile.
    ///
    /// Valid values are raid0, raid1, raid1c3, raid1c4, raid5, raid6,
    /// raid10, single, or dup (case insensitive). Default is DUP for a
    /// single device, RAID1 for multiple devices.
    #[arg(short = 'm', long = "metadata", value_name = "PROFILE")]
    pub metadata_profile: Option<Profile>,

    /// Mix data and metadata in the same block groups.
    ///
    /// Recommended for filesystems smaller than 1 GiB. The nodesize and
    /// sectorsize must be equal, and the data and metadata profiles must
    /// match.
    #[arg(short = 'M', long)]
    pub mixed: bool,

    /// Filesystem label (maximum 255 bytes).
    #[arg(short = 'L', long = "label", value_name = "LABEL")]
    pub label: Option<String>,

    /// Size of btree nodes.
    ///
    /// Default is 16 KiB or the page size, whichever is larger. Must be a
    /// multiple of the sectorsize and a power of 2, up to 64 KiB.
    #[arg(short = 'n', long, value_name = "SIZE")]
    pub nodesize: Option<SizeArg>,

    /// Data block allocation unit.
    ///
    /// Default is 4 KiB. Using a value different from the system page size
    /// may result in an unmountable filesystem.
    #[arg(short = 's', long, value_name = "SIZE")]
    pub sectorsize: Option<SizeArg>,

    /// Set filesystem size per device.
    ///
    /// If not set, the entire device size is used. The total filesystem
    /// size is the sum of all device sizes.
    #[arg(short = 'b', long = "byte-count", value_name = "SIZE")]
    pub byte_count: Option<SizeArg>,

    /// Checksum algorithm.
    ///
    /// Valid values are crc32c (default), xxhash, sha256, or blake2.
    #[arg(long = "checksum", alias = "csum", value_name = "TYPE")]
    pub checksum: Option<ChecksumArg>,

    /// Comma-separated list of filesystem features.
    ///
    /// Prefix a feature with ^ to disable it. Use 'list-all' to list all
    /// available features.
    #[arg(
        short = 'O',
        long = "features",
        alias = "runtime-features",
        short_alias = 'R',
        value_name = "LIST",
        value_delimiter = ','
    )]
    pub features: Vec<FeatureArg>,

    /// Specify the filesystem UUID.
    #[arg(short = 'U', long = "uuid", value_name = "UUID")]
    pub filesystem_uuid: Option<Uuid>,

    /// Specify the device UUID (sub-uuid).
    ///
    /// Only meaningful for single-device filesystems.
    #[arg(long = "device-uuid", value_name = "UUID")]
    pub device_uuid: Option<Uuid>,

    /// Force overwrite of an existing filesystem.
    #[arg(short = 'f', long)]
    pub force: bool,

    /// Do not perform whole-device TRIM.
    #[arg(short = 'K', long)]
    pub nodiscard: bool,

    /// Copy files from a directory into the filesystem image.
    #[arg(short = 'r', long = "rootdir", value_name = "DIR")]
    pub rootdir: Option<PathBuf>,

    /// Create a subdirectory as a subvolume (requires --rootdir).
    ///
    /// TYPE is one of: rw (default), ro, default, default-ro.
    /// Can be specified multiple times.
    #[arg(short = 'u', long = "subvol", value_name = "TYPE:SUBDIR")]
    pub subvol: Vec<SubvolArg>,

    /// Specify inode flags for a path (requires --rootdir).
    ///
    /// FLAGS is a comma-separated list of: nodatacow, nodatasum. Can be
    /// specified multiple times.
    #[arg(long = "inode-flags", value_name = "FLAGS:PATH")]
    pub inode_flags: Vec<InodeFlagsArg>,

    /// Compress files when populating from --rootdir.
    ///
    /// ALGO is one of: no (default), zstd, lzo, zlib. An optional
    /// compression level can be appended after a colon.
    #[arg(long = "compress", value_name = "ALGO[:LEVEL]")]
    pub compress: Option<CompressArg>,

    /// Clone file extents from --rootdir instead of copying bytes.
    #[arg(long)]
    pub reflink: bool,

    /// Shrink the filesystem to minimal size after populating from --rootdir.
    #[arg(long)]
    pub shrink: bool,

    /// Quiet mode: only print errors and warnings.
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Increase verbosity level.
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Block devices or image files to format.
    #[arg(required = true)]
    pub devices: Vec<PathBuf>,
}

/// Size argument with suffix support (e.g. "16k", "4m", "1g").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeArg(pub u64);

impl std::str::FromStr for SizeArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (num_str, suffix) = match s.find(|c: char| c.is_alphabetic()) {
            Some(i) => (&s[..i], &s[i..]),
            None => (s, ""),
        };
        let base: u64 =
            num_str.parse().map_err(|e| format!("invalid size: {e}"))?;
        let multiplier = match suffix.to_lowercase().as_str() {
            "" => 1u64,
            "k" | "kib" => 1 << 10,
            "m" | "mib" => 1 << 20,
            "g" | "gib" => 1 << 30,
            "t" | "tib" => 1 << 40,
            "p" | "pib" => 1 << 50,
            "e" | "eib" => 1 << 60,
            _ => return Err(format!("unknown size suffix: {suffix}")),
        };
        base.checked_mul(multiplier)
            .map(SizeArg)
            .ok_or_else(|| format!("size overflow: {s}"))
    }
}

/// Block group RAID profile.
/// Block group RAID profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Single,
    Dup,
    Raid0,
    Raid1,
    Raid1c3,
    Raid1c4,
    Raid5,
    Raid6,
    Raid10,
}

impl std::str::FromStr for Profile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "single" => Ok(Profile::Single),
            "dup" => Ok(Profile::Dup),
            "raid0" => Ok(Profile::Raid0),
            "raid1" => Ok(Profile::Raid1),
            "raid1c3" => Ok(Profile::Raid1c3),
            "raid1c4" => Ok(Profile::Raid1c4),
            "raid5" => Ok(Profile::Raid5),
            "raid6" => Ok(Profile::Raid6),
            "raid10" => Ok(Profile::Raid10),
            _ => Err(format!("unknown profile: {s}")),
        }
    }
}

impl std::fmt::Display for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Profile::Single => write!(f, "single"),
            Profile::Dup => write!(f, "DUP"),
            Profile::Raid0 => write!(f, "RAID0"),
            Profile::Raid1 => write!(f, "RAID1"),
            Profile::Raid1c3 => write!(f, "RAID1C3"),
            Profile::Raid1c4 => write!(f, "RAID1C4"),
            Profile::Raid5 => write!(f, "RAID5"),
            Profile::Raid6 => write!(f, "RAID6"),
            Profile::Raid10 => write!(f, "RAID10"),
        }
    }
}

/// Checksum algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumArg {
    Crc32c,
    Xxhash,
    Sha256,
    Blake2,
}

impl std::str::FromStr for ChecksumArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "crc32c" => Ok(ChecksumArg::Crc32c),
            "xxhash" | "xxhash64" => Ok(ChecksumArg::Xxhash),
            "sha256" => Ok(ChecksumArg::Sha256),
            "blake2" | "blake2b" => Ok(ChecksumArg::Blake2),
            _ => Err(format!("unknown checksum type: {s}")),
        }
    }
}

impl std::fmt::Display for ChecksumArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksumArg::Crc32c => write!(f, "crc32c"),
            ChecksumArg::Xxhash => write!(f, "xxhash"),
            ChecksumArg::Sha256 => write!(f, "sha256"),
            ChecksumArg::Blake2 => write!(f, "blake2"),
        }
    }
}

/// Filesystem feature that can be enabled or disabled at mkfs time.
///
/// Prefix with ^ to disable (e.g. "^no-holes").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureArg {
    pub feature: Feature,
    pub enabled: bool,
}

impl std::str::FromStr for FeatureArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (enabled, name) = if let Some(rest) = s.strip_prefix('^') {
            (false, rest)
        } else {
            (true, s)
        };
        let feature = name.parse::<Feature>()?;
        Ok(FeatureArg { feature, enabled })
    }
}

/// Known filesystem features.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    MixedBg,
    Extref,
    Raid56,
    SkinnyMetadata,
    NoHoles,
    Zoned,
    Quota,
    FreeSpaceTree,
    BlockGroupTree,
    RaidStripeTree,
    Squota,
    ListAll,
}

impl std::str::FromStr for Feature {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('_', "-").as_str() {
            "mixed-bg" => Ok(Feature::MixedBg),
            "extref" => Ok(Feature::Extref),
            "raid56" => Ok(Feature::Raid56),
            "skinny-metadata" => Ok(Feature::SkinnyMetadata),
            "no-holes" => Ok(Feature::NoHoles),
            "zoned" => Ok(Feature::Zoned),
            "quota" => Ok(Feature::Quota),
            "free-space-tree" | "fst" => Ok(Feature::FreeSpaceTree),
            "block-group-tree" | "bgt" => Ok(Feature::BlockGroupTree),
            "raid-stripe-tree" => Ok(Feature::RaidStripeTree),
            "squota" => Ok(Feature::Squota),
            "list-all" => Ok(Feature::ListAll),
            _ => Err(format!("unknown feature: {s}")),
        }
    }
}

impl std::fmt::Display for Feature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Feature::MixedBg => write!(f, "mixed-bg"),
            Feature::Extref => write!(f, "extref"),
            Feature::Raid56 => write!(f, "raid56"),
            Feature::SkinnyMetadata => write!(f, "skinny-metadata"),
            Feature::NoHoles => write!(f, "no-holes"),
            Feature::Zoned => write!(f, "zoned"),
            Feature::Quota => write!(f, "quota"),
            Feature::FreeSpaceTree => write!(f, "free-space-tree"),
            Feature::BlockGroupTree => write!(f, "block-group-tree"),
            Feature::RaidStripeTree => write!(f, "raid-stripe-tree"),
            Feature::Squota => write!(f, "squota"),
            Feature::ListAll => write!(f, "list-all"),
        }
    }
}

/// Subvolume specification for --rootdir: `[TYPE:]SUBDIR`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubvolArg {
    pub subvol_type: SubvolType,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubvolType {
    #[default]
    Rw,
    Ro,
    Default,
    DefaultRo,
}

impl std::str::FromStr for SubvolArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // If the path starts with "./" it is literal (no type prefix).
        if s.starts_with("./") {
            return Ok(SubvolArg {
                subvol_type: SubvolType::Rw,
                path: PathBuf::from(s),
            });
        }
        if let Some((prefix, rest)) = s.split_once(':') {
            let subvol_type = match prefix {
                "rw" => SubvolType::Rw,
                "ro" => SubvolType::Ro,
                "default" => SubvolType::Default,
                "default-ro" => SubvolType::DefaultRo,
                _ => {
                    // Not a known type prefix — treat the whole string as a path.
                    return Ok(SubvolArg {
                        subvol_type: SubvolType::Rw,
                        path: PathBuf::from(s),
                    });
                }
            };
            if rest.is_empty() {
                return Err("subvolume path cannot be empty".to_string());
            }
            Ok(SubvolArg {
                subvol_type,
                path: PathBuf::from(rest),
            })
        } else {
            Ok(SubvolArg {
                subvol_type: SubvolType::Rw,
                path: PathBuf::from(s),
            })
        }
    }
}

/// Inode flags specification for --rootdir: `FLAGS:PATH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InodeFlagsArg {
    pub nodatacow: bool,
    pub nodatasum: bool,
    pub path: PathBuf,
}

impl std::str::FromStr for InodeFlagsArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (flags_str, path) = s
            .split_once(':')
            .ok_or_else(|| "expected FLAGS:PATH format".to_string())?;
        if path.is_empty() {
            return Err("path cannot be empty".to_string());
        }
        let mut nodatacow = false;
        let mut nodatasum = false;
        for flag in flags_str.split(',') {
            match flag.trim() {
                "nodatacow" => nodatacow = true,
                "nodatasum" => nodatasum = true,
                other => return Err(format!("unknown inode flag: {other}")),
            }
        }
        Ok(InodeFlagsArg {
            nodatacow,
            nodatasum,
            path: PathBuf::from(path),
        })
    }
}

/// Compression specification: `ALGO[:LEVEL]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressArg {
    pub algorithm: CompressAlgorithm,
    pub level: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressAlgorithm {
    No,
    Zstd,
    Lzo,
    Zlib,
}

impl std::str::FromStr for CompressArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (algo_str, level) = if let Some((a, l)) = s.split_once(':') {
            let level: u32 =
                l.parse().map_err(|e| format!("invalid level: {e}"))?;
            (a, Some(level))
        } else {
            (s, None)
        };
        let algorithm = match algo_str.to_lowercase().as_str() {
            "no" | "none" => CompressAlgorithm::No,
            "zstd" => CompressAlgorithm::Zstd,
            "lzo" => CompressAlgorithm::Lzo,
            "zlib" => CompressAlgorithm::Zlib,
            _ => {
                return Err(format!(
                    "unknown compression algorithm: {algo_str}"
                ));
            }
        };
        if level.is_some() && algorithm == CompressAlgorithm::No {
            return Err(
                "compression level not valid with 'no' algorithm".to_string()
            );
        }
        if let Some(l) = level {
            match algorithm {
                CompressAlgorithm::Zstd if l > 15 => {
                    return Err(format!("zstd level must be 1..15, got {l}"));
                }
                CompressAlgorithm::Zlib if l > 9 => {
                    return Err(format!("zlib level must be 1..9, got {l}"));
                }
                CompressAlgorithm::Lzo if level.is_some() => {
                    return Err(
                        "lzo does not support compression levels".to_string()
                    );
                }
                _ => {}
            }
        }
        Ok(CompressArg { algorithm, level })
    }
}

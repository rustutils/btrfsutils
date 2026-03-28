use clap::Parser;
use std::path::PathBuf;

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
    pub nodesize: Option<String>,

    /// Data block allocation unit.
    ///
    /// Default is 4 KiB. Using a value different from the system page size
    /// may result in an unmountable filesystem.
    #[arg(short = 's', long, value_name = "SIZE")]
    pub sectorsize: Option<String>,

    /// Set filesystem size per device.
    ///
    /// If not set, the entire device size is used. The total filesystem
    /// size is the sum of all device sizes.
    #[arg(short = 'b', long = "byte-count", value_name = "SIZE")]
    pub byte_count: Option<String>,

    /// Checksum algorithm.
    ///
    /// Valid values are crc32c (default), xxhash, sha256, or blake2.
    #[arg(long = "checksum", alias = "csum", value_name = "TYPE")]
    pub checksum: Option<CsumArg>,

    /// Comma-separated list of filesystem features.
    ///
    /// Prefix a feature with ^ to disable it. Use 'list-all' to list all
    /// available features.
    #[arg(short = 'O', long = "features", value_name = "LIST")]
    pub features: Option<String>,

    /// Deprecated alias for --features.
    #[arg(
        short = 'R',
        long = "runtime-features",
        value_name = "LIST",
        hide = true
    )]
    pub runtime_features: Option<String>,

    /// Specify the filesystem UUID.
    #[arg(short = 'U', long = "uuid", value_name = "UUID")]
    pub fs_uuid: Option<String>,

    /// Specify the device UUID (sub-uuid).
    ///
    /// Only meaningful for single-device filesystems.
    #[arg(long = "device-uuid", value_name = "UUID")]
    pub device_uuid: Option<String>,

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
    pub subvol: Vec<String>,

    /// Specify inode flags for a path (requires --rootdir).
    ///
    /// FLAGS is one of: nodatacow, nodatasum. Can be specified multiple
    /// times.
    #[arg(long = "inode-flags", value_name = "FLAGS:PATH")]
    pub inode_flags: Vec<String>,

    /// Compress files when populating from --rootdir.
    ///
    /// ALGO is one of: no (default), zstd, lzo, zlib. An optional
    /// compression level can be appended after a colon.
    #[arg(long = "compress", value_name = "ALGO[:LEVEL]")]
    pub compress: Option<String>,

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
pub enum CsumArg {
    Crc32c,
    Xxhash,
    Sha256,
    Blake2,
}

impl std::str::FromStr for CsumArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "crc32c" => Ok(CsumArg::Crc32c),
            "xxhash" | "xxhash64" => Ok(CsumArg::Xxhash),
            "sha256" => Ok(CsumArg::Sha256),
            "blake2" | "blake2b" => Ok(CsumArg::Blake2),
            _ => Err(format!("unknown checksum type: {s}")),
        }
    }
}

impl std::fmt::Display for CsumArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsumArg::Crc32c => write!(f, "crc32c"),
            CsumArg::Xxhash => write!(f, "xxhash"),
            CsumArg::Sha256 => write!(f, "sha256"),
            CsumArg::Blake2 => write!(f, "blake2"),
        }
    }
}

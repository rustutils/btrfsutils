use std::fs;
use std::io;
use std::path::PathBuf;

use uuid::Uuid;

/// Returns the sysfs directory path for the btrfs filesystem with the given
/// UUID: `/sys/fs/btrfs/<uuid>`.
pub fn sysfs_btrfs_path(uuid: &Uuid) -> PathBuf {
    PathBuf::from(format!("/sys/fs/btrfs/{}", uuid.as_hyphenated()))
}

/// Returns the path to a named file within the sysfs directory for the
/// filesystem with the given UUID: `/sys/fs/btrfs/<uuid>/<name>`.
pub fn sysfs_btrfs_path_file(uuid: &Uuid, name: &str) -> PathBuf {
    sysfs_btrfs_path(uuid).join(name)
}

/// Commit statistics for a mounted btrfs filesystem, read from
/// `/sys/fs/btrfs/<uuid>/commit_stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitStats {
    /// Total number of commits since mount.
    pub commits: u64,
    /// Duration of the current (in-progress) commit in milliseconds.
    pub cur_commit_ms: u64,
    /// Duration of the last completed commit in milliseconds.
    pub last_commit_ms: u64,
    /// Maximum commit duration since mount (or last reset) in milliseconds.
    pub max_commit_ms: u64,
    /// Total time spent in commits since mount in milliseconds.
    pub total_commit_ms: u64,
}

/// Provides typed access to the sysfs files exposed for a single mounted btrfs
/// filesystem under `/sys/fs/btrfs/<uuid>/`.
pub struct SysfsBtrfs {
    base: PathBuf,
}

impl SysfsBtrfs {
    /// Create a new `SysfsBtrfs` for the filesystem with the given UUID.
    pub fn new(uuid: &Uuid) -> Self {
        Self {
            base: sysfs_btrfs_path(uuid),
        }
    }

    fn read_file(&self, name: &str) -> io::Result<String> {
        let s = fs::read_to_string(self.base.join(name))?;
        Ok(s.trim_end().to_owned())
    }

    fn read_u64(&self, name: &str) -> io::Result<u64> {
        let s = self.read_file(name)?;
        s.parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_bool(&self, name: &str) -> io::Result<bool> {
        Ok(self.read_u64(name)? != 0)
    }

    /// Background reclaim threshold as a percentage (0–100).
    /// `/sys/fs/btrfs/<uuid>/bg_reclaim_threshold`
    pub fn bg_reclaim_threshold(&self) -> io::Result<u64> {
        self.read_u64("bg_reclaim_threshold")
    }

    /// Checksum algorithm in use, e.g. `"crc32c (crc32c-lib)"`.
    /// `/sys/fs/btrfs/<uuid>/checksum`
    pub fn checksum(&self) -> io::Result<String> {
        self.read_file("checksum")
    }

    /// Minimum clone/reflink alignment in bytes.
    /// `/sys/fs/btrfs/<uuid>/clone_alignment`
    pub fn clone_alignment(&self) -> io::Result<u64> {
        self.read_u64("clone_alignment")
    }

    /// Commit statistics since mount (or last reset).
    /// `/sys/fs/btrfs/<uuid>/commit_stats`
    pub fn commit_stats(&self) -> io::Result<CommitStats> {
        let contents = self.read_file("commit_stats")?;
        let mut commits = None;
        let mut cur_commit_ms = None;
        let mut last_commit_ms = None;
        let mut max_commit_ms = None;
        let mut total_commit_ms = None;

        for line in contents.lines() {
            let mut parts = line.splitn(2, ' ');
            let key = parts.next().unwrap_or("").trim();
            let val: u64 = parts
                .next()
                .unwrap_or("")
                .trim()
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            match key {
                "commits" => commits = Some(val),
                "cur_commit_ms" => cur_commit_ms = Some(val),
                "last_commit_ms" => last_commit_ms = Some(val),
                "max_commit_ms" => max_commit_ms = Some(val),
                "total_commit_ms" => total_commit_ms = Some(val),
                _ => {}
            }
        }

        let missing = |name| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("commit_stats: missing field '{name}'"),
            )
        };

        Ok(CommitStats {
            commits: commits.ok_or_else(|| missing("commits"))?,
            cur_commit_ms: cur_commit_ms.ok_or_else(|| missing("cur_commit_ms"))?,
            last_commit_ms: last_commit_ms.ok_or_else(|| missing("last_commit_ms"))?,
            max_commit_ms: max_commit_ms.ok_or_else(|| missing("max_commit_ms"))?,
            total_commit_ms: total_commit_ms.ok_or_else(|| missing("total_commit_ms"))?,
        })
    }

    /// Reset the `max_commit_ms` counter by writing `0` to the commit_stats
    /// file. Requires root.
    /// `/sys/fs/btrfs/<uuid>/commit_stats`
    pub fn reset_commit_stats(&self) -> io::Result<()> {
        fs::write(self.base.join("commit_stats"), b"0")
    }

    /// Name of the exclusive operation currently running, e.g. `"none"`,
    /// `"balance"`, `"device add"`.
    /// `/sys/fs/btrfs/<uuid>/exclusive_operation`
    pub fn exclusive_operation(&self) -> io::Result<String> {
        self.read_file("exclusive_operation")
    }

    /// Names of the filesystem features that are enabled. Each feature
    /// corresponds to a file in the `features/` subdirectory.
    /// `/sys/fs/btrfs/<uuid>/features/`
    pub fn features(&self) -> io::Result<Vec<String>> {
        let mut features = Vec::new();
        for entry in fs::read_dir(self.base.join("features"))? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                features.push(name.to_owned());
            }
        }
        features.sort();
        Ok(features)
    }

    /// Current filesystem generation number.
    /// `/sys/fs/btrfs/<uuid>/generation`
    pub fn generation(&self) -> io::Result<u64> {
        self.read_u64("generation")
    }

    /// Filesystem label. Empty string if no label is set.
    /// `/sys/fs/btrfs/<uuid>/label`
    pub fn label(&self) -> io::Result<String> {
        self.read_file("label")
    }

    /// Metadata UUID. May differ from the filesystem UUID if the metadata UUID
    /// feature is in use.
    /// `/sys/fs/btrfs/<uuid>/metadata_uuid`
    pub fn metadata_uuid(&self) -> io::Result<Uuid> {
        let s = self.read_file("metadata_uuid")?;
        Uuid::parse_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// B-tree node size in bytes.
    /// `/sys/fs/btrfs/<uuid>/nodesize`
    pub fn nodesize(&self) -> io::Result<u64> {
        self.read_u64("nodesize")
    }

    /// Whether the quota override is active.
    /// `/sys/fs/btrfs/<uuid>/quota_override`
    pub fn quota_override(&self) -> io::Result<bool> {
        self.read_bool("quota_override")
    }

    /// Read policy for RAID profiles, e.g. `"[pid]"` or `"[roundrobin]"`.
    /// `/sys/fs/btrfs/<uuid>/read_policy`
    pub fn read_policy(&self) -> io::Result<String> {
        self.read_file("read_policy")
    }

    /// Sector size in bytes.
    /// `/sys/fs/btrfs/<uuid>/sectorsize`
    pub fn sectorsize(&self) -> io::Result<u64> {
        self.read_u64("sectorsize")
    }

    /// Whether a temporary fsid is in use (seeding device feature).
    /// `/sys/fs/btrfs/<uuid>/temp_fsid`
    pub fn temp_fsid(&self) -> io::Result<bool> {
        self.read_bool("temp_fsid")
    }
}

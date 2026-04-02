//! # Sysfs interface: reading filesystem and device state from `/sys/fs/btrfs/`
//!
//! The kernel exposes per-filesystem information under
//! `/sys/fs/btrfs/<uuid>/`, where `<uuid>` is the filesystem UUID as returned
//! by [`filesystem_info`][`crate::filesystem::filesystem_info`]. This includes commit statistics,
//! feature flags, quota state, and per-device scrub limits.
//!
//! The primary entry point is [`SysfsBtrfs`], which is constructed from a
//! filesystem UUID and provides typed accessors for each sysfs file:
//!
//! ```no_run
//! # use btrfs_uapi::{filesystem::filesystem_info, sysfs::SysfsBtrfs};
//! # use std::{fs::File, os::unix::io::AsFd};
//! # let file = File::open("/mnt/btrfs").unwrap();
//! # let fd = file.as_fd();
//! let info = filesystem_info(fd).unwrap();
//! let sysfs = SysfsBtrfs::new(&info.uuid);
//! println!("label: {}", sysfs.label().unwrap());
//! println!("quota status: {:?}", sysfs.quota_status().unwrap());
//! ```
//!
//! All accessors return [`std::io::Result`] and will return an error with kind
//! [`std::io::ErrorKind::NotFound`] if the filesystem is not currently mounted.

use std::{ffi::OsStr, fs, io, path::PathBuf};
use uuid::Uuid;

/// Returns the sysfs directory path for the btrfs filesystem with the given
/// UUID: `/sys/fs/btrfs/<uuid>`.
#[must_use]
pub fn sysfs_btrfs_path(uuid: &Uuid) -> PathBuf {
    PathBuf::from(format!("/sys/fs/btrfs/{}", uuid.as_hyphenated()))
}

/// Returns the path to a named file within the sysfs directory for the
/// filesystem with the given UUID: `/sys/fs/btrfs/<uuid>/<name>`.
#[must_use]
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
    #[must_use]
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

    /// Background reclaim threshold as a percentage (0-100).
    /// `/sys/fs/btrfs/<uuid>/bg_reclaim_threshold`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn bg_reclaim_threshold(&self) -> io::Result<u64> {
        self.read_u64("bg_reclaim_threshold")
    }

    /// Checksum algorithm in use, e.g. `"crc32c (crc32c-lib)"`.
    /// `/sys/fs/btrfs/<uuid>/checksum`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read.
    pub fn checksum(&self) -> io::Result<String> {
        self.read_file("checksum")
    }

    /// Minimum clone/reflink alignment in bytes.
    /// `/sys/fs/btrfs/<uuid>/clone_alignment`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn clone_alignment(&self) -> io::Result<u64> {
        self.read_u64("clone_alignment")
    }

    /// Commit statistics since mount (or last reset).
    /// `/sys/fs/btrfs/<uuid>/commit_stats`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
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
            let val: u64 =
                parts.next().unwrap_or("").trim().parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                })?;
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
            cur_commit_ms: cur_commit_ms
                .ok_or_else(|| missing("cur_commit_ms"))?,
            last_commit_ms: last_commit_ms
                .ok_or_else(|| missing("last_commit_ms"))?,
            max_commit_ms: max_commit_ms
                .ok_or_else(|| missing("max_commit_ms"))?,
            total_commit_ms: total_commit_ms
                .ok_or_else(|| missing("total_commit_ms"))?,
        })
    }

    /// Reset the `max_commit_ms` counter by writing `0` to the `commit_stats`
    /// file. Requires root.
    /// `/sys/fs/btrfs/<uuid>/commit_stats`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the write fails.
    pub fn reset_commit_stats(&self) -> io::Result<()> {
        fs::write(self.base.join("commit_stats"), b"0")
    }

    /// Name of the exclusive operation currently running, e.g. `"none"`,
    /// `"balance"`, `"device add"`.
    /// `/sys/fs/btrfs/<uuid>/exclusive_operation`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read.
    pub fn exclusive_operation(&self) -> io::Result<String> {
        self.read_file("exclusive_operation")
    }

    /// Wait until no exclusive operation is running on the filesystem.
    ///
    /// Polls the `exclusive_operation` sysfs file at one-second intervals.
    /// Returns immediately if no exclusive operation is in progress, or after
    /// the running operation completes. Returns the name of the operation
    /// that was waited on, or `"none"` if nothing was running.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the sysfs file fails.
    pub fn wait_for_exclusive_operation(&self) -> io::Result<String> {
        let mut op = self.exclusive_operation()?;
        if op == "none" {
            return Ok(op);
        }
        let waited_for = op.clone();
        while op != "none" {
            std::thread::sleep(std::time::Duration::from_secs(1));
            op = self.exclusive_operation()?;
        }
        Ok(waited_for)
    }

    /// Names of the filesystem features that are enabled. Each feature
    /// corresponds to a file in the `features/` subdirectory.
    /// `/sys/fs/btrfs/<uuid>/features/`
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the features directory fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn generation(&self) -> io::Result<u64> {
        self.read_u64("generation")
    }

    /// Filesystem label. Empty string if no label is set.
    /// `/sys/fs/btrfs/<uuid>/label`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read.
    pub fn label(&self) -> io::Result<String> {
        self.read_file("label")
    }

    /// Metadata UUID. May differ from the filesystem UUID if the metadata UUID
    /// feature is in use.
    /// `/sys/fs/btrfs/<uuid>/metadata_uuid`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn metadata_uuid(&self) -> io::Result<Uuid> {
        let s = self.read_file("metadata_uuid")?;
        Uuid::parse_str(&s)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// B-tree node size in bytes.
    /// `/sys/fs/btrfs/<uuid>/nodesize`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn nodesize(&self) -> io::Result<u64> {
        self.read_u64("nodesize")
    }

    /// Whether the quota override is active.
    /// `/sys/fs/btrfs/<uuid>/quota_override`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn quota_override(&self) -> io::Result<bool> {
        self.read_bool("quota_override")
    }

    /// Read policy for RAID profiles, e.g. `"[pid]"` or `"[roundrobin]"`.
    /// `/sys/fs/btrfs/<uuid>/read_policy`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read.
    pub fn read_policy(&self) -> io::Result<String> {
        self.read_file("read_policy")
    }

    /// Sector size in bytes.
    /// `/sys/fs/btrfs/<uuid>/sectorsize`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn sectorsize(&self) -> io::Result<u64> {
        self.read_u64("sectorsize")
    }

    /// Whether a temporary fsid is in use (seeding device feature).
    /// `/sys/fs/btrfs/<uuid>/temp_fsid`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn temp_fsid(&self) -> io::Result<bool> {
        self.read_bool("temp_fsid")
    }

    /// Read the per-device scrub throughput limit for the given device, in
    /// bytes per second. A value of `0` means no limit is set (unlimited).
    /// `/sys/fs/btrfs/<uuid>/devinfo/<devid>/scrub_speed_max`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs file cannot be read or parsed.
    pub fn scrub_speed_max_get(&self, devid: u64) -> io::Result<u64> {
        let path = format!("devinfo/{devid}/scrub_speed_max");
        match self.read_u64(&path) {
            Ok(v) => Ok(v),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Set the per-device scrub throughput limit for the given device, in
    /// bytes per second. Pass `0` to remove the limit (unlimited).
    /// Requires root.
    /// `/sys/fs/btrfs/<uuid>/devinfo/<devid>/scrub_speed_max`
    ///
    /// # Errors
    ///
    /// Returns `Err` if the write fails.
    pub fn scrub_speed_max_set(
        &self,
        devid: u64,
        limit: u64,
    ) -> io::Result<()> {
        let path = self.base.join(format!("devinfo/{devid}/scrub_speed_max"));
        fs::write(path, format!("{limit}\n"))
    }

    /// Maximum send stream protocol version supported by the kernel.
    ///
    /// Returns `1` if the sysfs file does not exist (older kernels without
    /// versioned send stream support).
    /// `/sys/fs/btrfs/features/send_stream_version`
    #[must_use]
    pub fn send_stream_version(&self) -> u32 {
        // This is a global feature file, not per-filesystem.
        let path =
            std::path::Path::new("/sys/fs/btrfs/features/send_stream_version");
        match fs::read_to_string(path) {
            Ok(s) => s.trim().parse::<u32>().unwrap_or(1),
            Err(_) => 1,
        }
    }

    /// Quota status for this filesystem, read from
    /// `/sys/fs/btrfs/<uuid>/qgroups/`.
    ///
    /// Returns `Ok(QuotaStatus { enabled: false, .. })` when quota is not
    /// enabled (the `qgroups/` directory does not exist). Returns an
    /// [`io::Error`] with kind `NotFound` if the sysfs entry for this UUID
    /// does not exist (i.e. the filesystem is not currently mounted).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the sysfs files cannot be read or parsed.
    pub fn quota_status(&self) -> io::Result<QuotaStatus> {
        let qgroups = self.base.join("qgroups");

        if !qgroups.exists() {
            return Ok(QuotaStatus {
                enabled: false,
                mode: None,
                inconsistent: None,
                override_limits: None,
                drop_subtree_threshold: None,
                total_count: None,
                level0_count: None,
            });
        }

        let mode = {
            let s = fs::read_to_string(qgroups.join("mode"))?;
            s.trim_end().to_owned()
        };
        let inconsistent = fs::read_to_string(qgroups.join("inconsistent"))?
            .trim()
            .parse::<u64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            != 0;
        let override_limits = self.read_bool("quota_override")?;
        let drop_subtree_threshold =
            fs::read_to_string(qgroups.join("drop_subtree_threshold"))?
                .trim()
                .parse::<u64>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut total_count: u64 = 0;
        let mut level0_count: u64 = 0;
        for entry in fs::read_dir(&qgroups)? {
            let entry = entry?;
            let raw_name = entry.file_name();
            let name = raw_name.to_string_lossy();
            if let Some((level, _id)) =
                parse_qgroup_entry_name(OsStr::new(name.as_ref()))
            {
                total_count += 1;
                if level == 0 {
                    level0_count += 1;
                }
            }
        }

        Ok(QuotaStatus {
            enabled: true,
            mode: Some(mode),
            inconsistent: Some(inconsistent),
            override_limits: Some(override_limits),
            drop_subtree_threshold: Some(drop_subtree_threshold),
            total_count: Some(total_count),
            level0_count: Some(level0_count),
        })
    }
}

#[cfg(test)]
impl SysfsBtrfs {
    fn with_base(base: PathBuf) -> Self {
        Self { base }
    }
}

/// Parse a qgroups sysfs directory entry name of the form `<level>_<id>`.
///
/// Returns `Some((level, id))` for valid entries, `None` for anything else
/// (e.g. `mode`, `inconsistent`, and other non-qgroup files in the directory).
fn parse_qgroup_entry_name(name: &OsStr) -> Option<(u64, u64)> {
    let s = name.to_str()?;
    let (level_str, id_str) = s.split_once('_')?;
    let level: u64 = level_str.parse().ok()?;
    let id: u64 = id_str.parse().ok()?;
    Some((level, id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup() -> (TempDir, SysfsBtrfs) {
        let dir = TempDir::new().unwrap();
        let sysfs = SysfsBtrfs::with_base(dir.path().to_path_buf());
        (dir, sysfs)
    }

    #[test]
    fn read_u64_values() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("nodesize"), "16384\n").unwrap();
        fs::write(dir.path().join("sectorsize"), "4096\n").unwrap();
        fs::write(dir.path().join("clone_alignment"), "4096\n").unwrap();
        fs::write(dir.path().join("generation"), "42\n").unwrap();
        fs::write(dir.path().join("bg_reclaim_threshold"), "75\n").unwrap();

        assert_eq!(sysfs.nodesize().unwrap(), 16384);
        assert_eq!(sysfs.sectorsize().unwrap(), 4096);
        assert_eq!(sysfs.clone_alignment().unwrap(), 4096);
        assert_eq!(sysfs.generation().unwrap(), 42);
        assert_eq!(sysfs.bg_reclaim_threshold().unwrap(), 75);
    }

    #[test]
    fn read_u64_invalid() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("nodesize"), "not_a_number\n").unwrap();
        assert!(sysfs.nodesize().is_err());
    }

    #[test]
    fn read_u64_missing_file() {
        let (_dir, sysfs) = setup();
        let err = sysfs.nodesize().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn read_string_values() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("label"), "my-filesystem\n").unwrap();
        fs::write(dir.path().join("checksum"), "crc32c (crc32c-lib)\n")
            .unwrap();
        fs::write(dir.path().join("read_policy"), "[pid]\n").unwrap();
        fs::write(dir.path().join("exclusive_operation"), "none\n").unwrap();

        assert_eq!(sysfs.label().unwrap(), "my-filesystem");
        assert_eq!(sysfs.checksum().unwrap(), "crc32c (crc32c-lib)");
        assert_eq!(sysfs.read_policy().unwrap(), "[pid]");
        assert_eq!(sysfs.exclusive_operation().unwrap(), "none");
    }

    #[test]
    fn read_empty_label() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("label"), "\n").unwrap();
        assert_eq!(sysfs.label().unwrap(), "");
    }

    #[test]
    fn read_bool_values() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("quota_override"), "0\n").unwrap();
        assert!(!sysfs.quota_override().unwrap());

        fs::write(dir.path().join("quota_override"), "1\n").unwrap();
        assert!(sysfs.quota_override().unwrap());

        fs::write(dir.path().join("temp_fsid"), "0\n").unwrap();
        assert!(!sysfs.temp_fsid().unwrap());

        fs::write(dir.path().join("temp_fsid"), "1\n").unwrap();
        assert!(sysfs.temp_fsid().unwrap());
    }

    #[test]
    fn metadata_uuid() {
        let (dir, sysfs) = setup();
        fs::write(
            dir.path().join("metadata_uuid"),
            "deadbeef-dead-beef-dead-beefdeadbeef\n",
        )
        .unwrap();
        let uuid = sysfs.metadata_uuid().unwrap();
        assert_eq!(uuid.to_string(), "deadbeef-dead-beef-dead-beefdeadbeef");
    }

    #[test]
    fn metadata_uuid_invalid() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("metadata_uuid"), "not-a-uuid\n").unwrap();
        assert!(sysfs.metadata_uuid().is_err());
    }

    #[test]
    fn commit_stats_valid() {
        let (dir, sysfs) = setup();
        fs::write(
            dir.path().join("commit_stats"),
            "commits 100\n\
             cur_commit_ms 5\n\
             last_commit_ms 12\n\
             max_commit_ms 50\n\
             total_commit_ms 2000\n",
        )
        .unwrap();

        let stats = sysfs.commit_stats().unwrap();
        assert_eq!(
            stats,
            CommitStats {
                commits: 100,
                cur_commit_ms: 5,
                last_commit_ms: 12,
                max_commit_ms: 50,
                total_commit_ms: 2000,
            }
        );
    }

    #[test]
    fn commit_stats_missing_field() {
        let (dir, sysfs) = setup();
        fs::write(
            dir.path().join("commit_stats"),
            "commits 100\ncur_commit_ms 5\n",
        )
        .unwrap();
        let err = sysfs.commit_stats().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn commit_stats_extra_fields_ignored() {
        let (dir, sysfs) = setup();
        fs::write(
            dir.path().join("commit_stats"),
            "commits 1\n\
             cur_commit_ms 2\n\
             last_commit_ms 3\n\
             max_commit_ms 4\n\
             total_commit_ms 5\n\
             unknown_field 99\n",
        )
        .unwrap();
        let stats = sysfs.commit_stats().unwrap();
        assert_eq!(stats.commits, 1);
    }

    #[test]
    fn features_directory() {
        let (dir, sysfs) = setup();
        let feat_dir = dir.path().join("features");
        fs::create_dir(&feat_dir).unwrap();
        fs::write(feat_dir.join("skinny_metadata"), "").unwrap();
        fs::write(feat_dir.join("extended_iref"), "").unwrap();
        fs::write(feat_dir.join("no_holes"), "").unwrap();

        let features = sysfs.features().unwrap();
        // Should be sorted alphabetically.
        assert_eq!(
            features,
            vec!["extended_iref", "no_holes", "skinny_metadata"]
        );
    }

    #[test]
    fn features_empty() {
        let (dir, sysfs) = setup();
        fs::create_dir(dir.path().join("features")).unwrap();
        assert!(sysfs.features().unwrap().is_empty());
    }

    #[test]
    fn scrub_speed_max_get() {
        let (dir, sysfs) = setup();
        let devinfo = dir.path().join("devinfo/1");
        fs::create_dir_all(&devinfo).unwrap();
        fs::write(devinfo.join("scrub_speed_max"), "104857600\n").unwrap();

        assert_eq!(sysfs.scrub_speed_max_get(1).unwrap(), 104_857_600);
    }

    #[test]
    fn scrub_speed_max_get_missing_returns_zero() {
        let (_dir, sysfs) = setup();
        // No devinfo directory exists — should return 0, not error.
        assert_eq!(sysfs.scrub_speed_max_get(99).unwrap(), 0);
    }

    #[test]
    fn scrub_speed_max_set() {
        let (dir, sysfs) = setup();
        let devinfo = dir.path().join("devinfo/1");
        fs::create_dir_all(&devinfo).unwrap();

        sysfs.scrub_speed_max_set(1, 500_000_000).unwrap();
        let contents =
            fs::read_to_string(devinfo.join("scrub_speed_max")).unwrap();
        assert_eq!(contents, "500000000\n");
    }

    #[test]
    fn reset_commit_stats() {
        let (dir, sysfs) = setup();
        fs::write(dir.path().join("commit_stats"), "old data").unwrap();

        sysfs.reset_commit_stats().unwrap();
        let contents =
            fs::read_to_string(dir.path().join("commit_stats")).unwrap();
        assert_eq!(contents, "0");
    }

    #[test]
    fn quota_status_disabled() {
        let (_dir, sysfs) = setup();
        // No qgroups directory → disabled.
        let status = sysfs.quota_status().unwrap();
        assert!(!status.enabled);
        assert!(status.mode.is_none());
    }

    #[test]
    fn quota_status_enabled() {
        let (dir, sysfs) = setup();
        let qg = dir.path().join("qgroups");
        fs::create_dir(&qg).unwrap();
        fs::write(qg.join("mode"), "qgroup\n").unwrap();
        fs::write(qg.join("inconsistent"), "0\n").unwrap();
        fs::write(qg.join("drop_subtree_threshold"), "8\n").unwrap();
        fs::write(dir.path().join("quota_override"), "0\n").unwrap();
        // Level-0 qgroup entries.
        fs::write(qg.join("0_5"), "").unwrap();
        fs::write(qg.join("0_256"), "").unwrap();
        // Level-1 qgroup.
        fs::write(qg.join("1_50"), "").unwrap();

        let status = sysfs.quota_status().unwrap();
        assert!(status.enabled);
        assert_eq!(status.mode.as_deref(), Some("qgroup"));
        assert_eq!(status.inconsistent, Some(false));
        assert_eq!(status.override_limits, Some(false));
        assert_eq!(status.drop_subtree_threshold, Some(8));
        assert_eq!(status.total_count, Some(3));
        assert_eq!(status.level0_count, Some(2));
    }

    #[test]
    fn parse_qgroup_entry_name_valid() {
        assert_eq!(
            parse_qgroup_entry_name(OsStr::new("0_256")),
            Some((0, 256))
        );
        assert_eq!(parse_qgroup_entry_name(OsStr::new("1_50")), Some((1, 50)));
    }

    #[test]
    fn parse_qgroup_entry_name_invalid() {
        assert_eq!(parse_qgroup_entry_name(OsStr::new("mode")), None);
        assert_eq!(parse_qgroup_entry_name(OsStr::new("inconsistent")), None);
        assert_eq!(parse_qgroup_entry_name(OsStr::new("abc_def")), None);
        assert_eq!(parse_qgroup_entry_name(OsStr::new("")), None);
    }
}

/// Quota status for a mounted btrfs filesystem, read from sysfs under
/// `/sys/fs/btrfs/<uuid>/qgroups/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaStatus {
    /// Whether quota accounting is currently enabled.
    pub enabled: bool,
    /// Accounting mode: `"qgroup"` (full backref accounting) or `"squota"`
    /// (simplified lifetime accounting). `None` when quotas are disabled.
    pub mode: Option<String>,
    /// Whether the quota tree is inconsistent; a rescan is needed to restore
    /// accurate numbers. `None` when quotas are disabled.
    pub inconsistent: Option<bool>,
    /// Whether the quota override flag is active (limits are bypassed for
    /// the current mount). `None` when quotas are disabled.
    pub override_limits: Option<bool>,
    /// Drop-subtree threshold: qgroup hierarchy levels below this value skip
    /// detailed tracking during heavy write workloads. `None` when disabled.
    pub drop_subtree_threshold: Option<u64>,
    /// Total number of qgroups tracked by the kernel. `None` when disabled.
    pub total_count: Option<u64>,
    /// Number of level-0 qgroups (one per subvolume). `None` when disabled.
    pub level0_count: Option<u64>,
}

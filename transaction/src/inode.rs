//! # Inode item construction
//!
//! `InodeArgs` is the full-fields counterpart to `btrfs_disk::items::InodeItemArgs`.
//! It carries every field of `btrfs_inode_item` (including `flags`, `rdev`,
//! `sequence`, and four distinct timestamps) so callers like mkfs can
//! preserve host-filesystem metadata exactly.
//!
//! [`Transaction::create_inode`](crate::Transaction::create_inode) takes an
//! `InodeArgs` and inserts the resulting `INODE_ITEM` at the standard
//! `(inode, INODE_ITEM, 0)` key.

use btrfs_disk::items::{InodeFlags, Timespec};
use bytes::BufMut;

/// All fields of an on-disk `btrfs_inode_item`, ready to serialize.
///
/// Construct via field-init (every field is `pub`) or with [`Self::new`]
/// for sensible defaults plus a custom `mode`. `with_uniform_time` is a
/// convenience for tests where atime / ctime / mtime / otime are all
/// the same instant.
#[derive(Debug, Clone)]
pub struct InodeArgs {
    /// NFS-compatible generation number. For new inodes equals the
    /// transaction id at creation; preserved across COWs.
    pub generation: u64,
    /// Last-modifying transaction id.
    pub transid: u64,
    /// Logical file size in bytes (`stat::st_size`).
    pub size: u64,
    /// On-disk bytes used (sum of `EXTENT_DATA.num_bytes` for regular
    /// extents plus inline payload length; not affected by `disk_num_bytes`
    /// for compressed extents).
    pub nbytes: u64,
    /// Hard-link count.
    pub nlink: u32,
    /// Owner user id.
    pub uid: u32,
    /// Owner group id.
    pub gid: u32,
    /// POSIX mode (file type + permissions, e.g. `0o100644`).
    pub mode: u32,
    /// Device number for character/block-device inodes (`stat::st_rdev`).
    /// Zero for everything else.
    pub rdev: u64,
    /// Inode flags (`NODATASUM`, `NODATACOW`, `IMMUTABLE`, etc.).
    pub flags: InodeFlags,
    /// NFS-compatible change sequence number.
    pub sequence: u64,
    /// Last access time.
    pub atime: Timespec,
    /// Last metadata change time.
    pub ctime: Timespec,
    /// Last modification time.
    pub mtime: Timespec,
    /// Creation time.
    pub otime: Timespec,
}

impl InodeArgs {
    /// Construct an `InodeArgs` with `generation = transid` and zero
    /// for every other field except `mode` and `nlink = 1`.
    #[must_use]
    pub fn new(transid: u64, mode: u32) -> Self {
        let zero = Timespec { sec: 0, nsec: 0 };
        Self {
            generation: transid,
            transid,
            size: 0,
            nbytes: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            mode,
            rdev: 0,
            flags: InodeFlags::empty(),
            sequence: 0,
            atime: zero,
            ctime: zero,
            mtime: zero,
            otime: zero,
        }
    }

    /// Set all four timestamps (atime, ctime, mtime, otime) to `time`.
    #[must_use]
    pub fn with_uniform_time(mut self, time: Timespec) -> Self {
        self.atime = time;
        self.ctime = time;
        self.mtime = time;
        self.otime = time;
        self
    }

    /// On-disk size of an `INODE_ITEM` (`btrfs_inode_item`): 160 bytes.
    pub const SIZE: usize = 160;

    /// Serialize to the 160-byte on-disk `btrfs_inode_item` representation.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.put_u64_le(self.generation);
        buf.put_u64_le(self.transid);
        buf.put_u64_le(self.size);
        buf.put_u64_le(self.nbytes);
        buf.put_u64_le(0); // block_group
        buf.put_u32_le(self.nlink);
        buf.put_u32_le(self.uid);
        buf.put_u32_le(self.gid);
        buf.put_u32_le(self.mode);
        buf.put_u64_le(self.rdev);
        buf.put_u64_le(self.flags.bits());
        buf.put_u64_le(self.sequence);
        buf.put_bytes(0, 32); // reserved[4]
        for ts in [self.atime, self.ctime, self.mtime, self.otime] {
            buf.put_u64_le(ts.sec);
            buf.put_u32_le(ts.nsec);
        }
        debug_assert_eq!(buf.len(), Self::SIZE);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::items::InodeItem;

    #[test]
    fn round_trip_through_parse() {
        let args = InodeArgs {
            generation: 42,
            transid: 42,
            size: 1234,
            nbytes: 4096,
            nlink: 2,
            uid: 1000,
            gid: 100,
            mode: 0o100644,
            rdev: 0,
            flags: InodeFlags::NODATASUM | InodeFlags::IMMUTABLE,
            sequence: 7,
            atime: Timespec {
                sec: 1_700_000_000,
                nsec: 100,
            },
            ctime: Timespec {
                sec: 1_700_000_001,
                nsec: 200,
            },
            mtime: Timespec {
                sec: 1_700_000_002,
                nsec: 300,
            },
            otime: Timespec {
                sec: 1_700_000_003,
                nsec: 400,
            },
        };

        let bytes = args.to_bytes();
        let parsed = InodeItem::parse(&bytes).expect("parse");

        assert_eq!(parsed.generation, args.generation);
        assert_eq!(parsed.transid, args.transid);
        assert_eq!(parsed.size, args.size);
        assert_eq!(parsed.nbytes, args.nbytes);
        assert_eq!(parsed.nlink, args.nlink);
        assert_eq!(parsed.uid, args.uid);
        assert_eq!(parsed.gid, args.gid);
        assert_eq!(parsed.mode, args.mode);
        assert_eq!(parsed.rdev, args.rdev);
        assert_eq!(parsed.flags.bits(), args.flags.bits());
        assert_eq!(parsed.sequence, args.sequence);
        assert_eq!(parsed.atime.sec, args.atime.sec);
        assert_eq!(parsed.atime.nsec, args.atime.nsec);
        assert_eq!(parsed.ctime.sec, args.ctime.sec);
        assert_eq!(parsed.mtime.sec, args.mtime.sec);
        assert_eq!(parsed.otime.sec, args.otime.sec);
    }

    #[test]
    fn new_defaults_are_zero() {
        let a = InodeArgs::new(5, 0o040755);
        assert_eq!(a.generation, 5);
        assert_eq!(a.transid, 5);
        assert_eq!(a.mode, 0o040755);
        assert_eq!(a.nlink, 1);
        assert_eq!(a.size, 0);
        assert_eq!(a.nbytes, 0);
        assert_eq!(a.uid, 0);
        assert_eq!(a.gid, 0);
        assert_eq!(a.rdev, 0);
        assert_eq!(a.flags.bits(), 0);
    }

    #[test]
    fn with_uniform_time_sets_all_four() {
        let ts = Timespec {
            sec: 1_700_000_000,
            nsec: 99,
        };
        let a = InodeArgs::new(1, 0o100644).with_uniform_time(ts);
        assert_eq!(a.atime.sec, ts.sec);
        assert_eq!(a.ctime.sec, ts.sec);
        assert_eq!(a.mtime.sec, ts.sec);
        assert_eq!(a.otime.sec, ts.sec);
    }
}

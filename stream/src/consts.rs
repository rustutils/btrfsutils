//! Wire-format constants for the btrfs send stream protocol.
//!
//! Shared between the parser ([`crate::stream`]) and the encoder
//! ([`crate::send`]) so both speak the same protocol numbers without
//! risk of drift.

/// Magic byte sequence at the start of every send stream
/// (`"btrfs-stream\0"`).
pub(crate) const SEND_STREAM_MAGIC: &[u8] = b"btrfs-stream\0";
pub(crate) const SEND_STREAM_MAGIC_LEN: usize = 13;
/// Magic + version u32 = 17 bytes.
pub(crate) const STREAM_HEADER_LEN: usize = SEND_STREAM_MAGIC_LEN + 4;

/// Per-command header: `len: u32 | cmd: u16 | crc: u32` = 10 bytes.
pub(crate) const CMD_HEADER_LEN: usize = 10;
// TLV header: 2 bytes type + 2 bytes length = 4 bytes (inline at use site).

// ── Command types ─────────────────────────────────────────────────

pub(crate) const BTRFS_SEND_C_SUBVOL: u16 = 1;
pub(crate) const BTRFS_SEND_C_SNAPSHOT: u16 = 2;
pub(crate) const BTRFS_SEND_C_MKFILE: u16 = 3;
pub(crate) const BTRFS_SEND_C_MKDIR: u16 = 4;
pub(crate) const BTRFS_SEND_C_MKNOD: u16 = 5;
pub(crate) const BTRFS_SEND_C_MKFIFO: u16 = 6;
pub(crate) const BTRFS_SEND_C_MKSOCK: u16 = 7;
pub(crate) const BTRFS_SEND_C_SYMLINK: u16 = 8;
pub(crate) const BTRFS_SEND_C_RENAME: u16 = 9;
pub(crate) const BTRFS_SEND_C_LINK: u16 = 10;
pub(crate) const BTRFS_SEND_C_UNLINK: u16 = 11;
pub(crate) const BTRFS_SEND_C_RMDIR: u16 = 12;
pub(crate) const BTRFS_SEND_C_SET_XATTR: u16 = 13;
pub(crate) const BTRFS_SEND_C_REMOVE_XATTR: u16 = 14;
pub(crate) const BTRFS_SEND_C_WRITE: u16 = 15;
pub(crate) const BTRFS_SEND_C_CLONE: u16 = 16;
pub(crate) const BTRFS_SEND_C_TRUNCATE: u16 = 17;
pub(crate) const BTRFS_SEND_C_CHMOD: u16 = 18;
pub(crate) const BTRFS_SEND_C_CHOWN: u16 = 19;
pub(crate) const BTRFS_SEND_C_UTIMES: u16 = 20;
pub(crate) const BTRFS_SEND_C_END: u16 = 21;
pub(crate) const BTRFS_SEND_C_UPDATE_EXTENT: u16 = 22;
// v2 commands.
pub(crate) const BTRFS_SEND_C_FALLOCATE: u16 = 23;
pub(crate) const BTRFS_SEND_C_FILEATTR: u16 = 24;
pub(crate) const BTRFS_SEND_C_ENCODED_WRITE: u16 = 25;
// v3 commands.
pub(crate) const BTRFS_SEND_C_ENABLE_VERITY: u16 = 26;

// ── Attribute types ───────────────────────────────────────────────

pub(crate) const BTRFS_SEND_A_UUID: u16 = 1;
pub(crate) const BTRFS_SEND_A_CTRANSID: u16 = 2;
#[allow(dead_code)]
pub(crate) const BTRFS_SEND_A_INO: u16 = 3;
pub(crate) const BTRFS_SEND_A_SIZE: u16 = 4;
pub(crate) const BTRFS_SEND_A_MODE: u16 = 5;
pub(crate) const BTRFS_SEND_A_UID: u16 = 6;
pub(crate) const BTRFS_SEND_A_GID: u16 = 7;
pub(crate) const BTRFS_SEND_A_RDEV: u16 = 8;
pub(crate) const BTRFS_SEND_A_CTIME: u16 = 9;
pub(crate) const BTRFS_SEND_A_MTIME: u16 = 10;
pub(crate) const BTRFS_SEND_A_ATIME: u16 = 11;
#[allow(dead_code)]
pub(crate) const BTRFS_SEND_A_OTIME: u16 = 12;
pub(crate) const BTRFS_SEND_A_XATTR_NAME: u16 = 13;
pub(crate) const BTRFS_SEND_A_XATTR_DATA: u16 = 14;
pub(crate) const BTRFS_SEND_A_PATH: u16 = 15;
pub(crate) const BTRFS_SEND_A_PATH_TO: u16 = 16;
pub(crate) const BTRFS_SEND_A_PATH_LINK: u16 = 17;
pub(crate) const BTRFS_SEND_A_FILE_OFFSET: u16 = 18;
pub(crate) const BTRFS_SEND_A_DATA: u16 = 19;
pub(crate) const BTRFS_SEND_A_CLONE_UUID: u16 = 20;
pub(crate) const BTRFS_SEND_A_CLONE_CTRANSID: u16 = 21;
pub(crate) const BTRFS_SEND_A_CLONE_PATH: u16 = 22;
pub(crate) const BTRFS_SEND_A_CLONE_OFFSET: u16 = 23;
pub(crate) const BTRFS_SEND_A_CLONE_LEN: u16 = 24;
// v2 attribute types.
pub(crate) const BTRFS_SEND_A_FALLOCATE_MODE: u16 = 25;
pub(crate) const BTRFS_SEND_A_FILEATTR: u16 = 26;
pub(crate) const BTRFS_SEND_A_UNENCODED_FILE_LEN: u16 = 27;
pub(crate) const BTRFS_SEND_A_UNENCODED_LEN: u16 = 28;
pub(crate) const BTRFS_SEND_A_UNENCODED_OFFSET: u16 = 29;
pub(crate) const BTRFS_SEND_A_COMPRESSION: u16 = 30;
pub(crate) const BTRFS_SEND_A_ENCRYPTION: u16 = 31;
// v3 attribute types.
pub(crate) const BTRFS_SEND_A_VERITY_ALGORITHM: u16 = 32;
pub(crate) const BTRFS_SEND_A_VERITY_BLOCK_SIZE: u16 = 33;
pub(crate) const BTRFS_SEND_A_VERITY_SALT_DATA: u16 = 34;
pub(crate) const BTRFS_SEND_A_VERITY_SIG_DATA: u16 = 35;

//! Binary send stream parser for the btrfs send/receive protocol.
//!
//! Parses the TLV-encoded command stream produced by `btrfs send` (or the
//! kernel's `BTRFS_IOC_SEND` ioctl). The stream consists of a header followed
//! by a sequence of commands, each with a CRC32C checksum and a set of
//! typed TLV attributes.

use std::io::Read;
use uuid::Uuid;

/// Errors that can occur while parsing a btrfs send stream.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The stream header does not start with the btrfs send magic.
    #[error("invalid send stream: bad magic")]
    BadMagic,
    /// The stream version is not supported (must be 1-3).
    #[error("unsupported send stream version {0} (supported: 1-3)")]
    UnsupportedVersion(u32),
    /// CRC32C checksum mismatch on a command.
    #[error(
        "CRC mismatch for command {cmd}: expected {expected:#010x}, got {computed:#010x}"
    )]
    CrcMismatch {
        cmd: u16,
        expected: u32,
        computed: u32,
    },
    /// The stream header could not be read completely.
    #[error("truncated send stream header: {0}")]
    TruncatedHeader(std::io::Error),
    /// A command payload could not be read completely.
    #[error("truncated send stream payload: {0}")]
    TruncatedPayload(std::io::Error),
    /// A TLV attribute structure is incomplete.
    #[error("truncated TLV: {detail}")]
    TruncatedTlv { detail: String },
    /// A TLV attribute type is out of range.
    #[error("invalid TLV attribute type {0}")]
    InvalidTlvType(u16),
    /// A required TLV attribute is missing from a command.
    #[error("missing required attribute: {0}")]
    MissingAttribute(&'static str),
    /// A TLV attribute value is malformed (wrong size, bad encoding, etc.).
    #[error("attribute {name}: {detail}")]
    InvalidAttribute { name: &'static str, detail: String },
    /// An unknown command type was encountered.
    #[error("unknown send stream command type {0}")]
    UnknownCommand(u16),
    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, StreamError>;

const SEND_STREAM_MAGIC: &[u8] = b"btrfs-stream\0";
const SEND_STREAM_MAGIC_LEN: usize = 13;
const STREAM_HEADER_LEN: usize = SEND_STREAM_MAGIC_LEN + 4; // magic + version u32

const CMD_HEADER_LEN: usize = 10; // len(u32) + cmd(u16) + crc(u32)
// TLV header: 2 bytes type + 2 bytes length = 4 bytes.

// Maximum number of TLV attribute types we track.
const MAX_ATTRS: usize = 36;

// Command types.
const BTRFS_SEND_C_SUBVOL: u16 = 1;
const BTRFS_SEND_C_SNAPSHOT: u16 = 2;
const BTRFS_SEND_C_MKFILE: u16 = 3;
const BTRFS_SEND_C_MKDIR: u16 = 4;
const BTRFS_SEND_C_MKNOD: u16 = 5;
const BTRFS_SEND_C_MKFIFO: u16 = 6;
const BTRFS_SEND_C_MKSOCK: u16 = 7;
const BTRFS_SEND_C_SYMLINK: u16 = 8;
const BTRFS_SEND_C_RENAME: u16 = 9;
const BTRFS_SEND_C_LINK: u16 = 10;
const BTRFS_SEND_C_UNLINK: u16 = 11;
const BTRFS_SEND_C_RMDIR: u16 = 12;
const BTRFS_SEND_C_SET_XATTR: u16 = 13;
const BTRFS_SEND_C_REMOVE_XATTR: u16 = 14;
const BTRFS_SEND_C_WRITE: u16 = 15;
const BTRFS_SEND_C_CLONE: u16 = 16;
const BTRFS_SEND_C_TRUNCATE: u16 = 17;
const BTRFS_SEND_C_CHMOD: u16 = 18;
const BTRFS_SEND_C_CHOWN: u16 = 19;
const BTRFS_SEND_C_UTIMES: u16 = 20;
const BTRFS_SEND_C_END: u16 = 21;
const BTRFS_SEND_C_UPDATE_EXTENT: u16 = 22;
// v2 commands.
const BTRFS_SEND_C_FALLOCATE: u16 = 23;
const BTRFS_SEND_C_FILEATTR: u16 = 24;
const BTRFS_SEND_C_ENCODED_WRITE: u16 = 25;
// v3 commands.
const BTRFS_SEND_C_ENABLE_VERITY: u16 = 26;

// Attribute types.
const BTRFS_SEND_A_UUID: u16 = 1;
const BTRFS_SEND_A_CTRANSID: u16 = 2;
#[allow(dead_code)]
const BTRFS_SEND_A_INO: u16 = 3;
const BTRFS_SEND_A_SIZE: u16 = 4;
const BTRFS_SEND_A_MODE: u16 = 5;
const BTRFS_SEND_A_UID: u16 = 6;
const BTRFS_SEND_A_GID: u16 = 7;
const BTRFS_SEND_A_RDEV: u16 = 8;
const BTRFS_SEND_A_CTIME: u16 = 9;
const BTRFS_SEND_A_MTIME: u16 = 10;
const BTRFS_SEND_A_ATIME: u16 = 11;
#[allow(dead_code)]
const BTRFS_SEND_A_OTIME: u16 = 12;
const BTRFS_SEND_A_XATTR_NAME: u16 = 13;
const BTRFS_SEND_A_XATTR_DATA: u16 = 14;
const BTRFS_SEND_A_PATH: u16 = 15;
const BTRFS_SEND_A_PATH_TO: u16 = 16;
const BTRFS_SEND_A_PATH_LINK: u16 = 17;
const BTRFS_SEND_A_FILE_OFFSET: u16 = 18;
const BTRFS_SEND_A_DATA: u16 = 19;
const BTRFS_SEND_A_CLONE_UUID: u16 = 20;
const BTRFS_SEND_A_CLONE_CTRANSID: u16 = 21;
const BTRFS_SEND_A_CLONE_PATH: u16 = 22;
const BTRFS_SEND_A_CLONE_OFFSET: u16 = 23;
const BTRFS_SEND_A_CLONE_LEN: u16 = 24;
// v2 attribute types.
const BTRFS_SEND_A_FALLOCATE_MODE: u16 = 25;
const BTRFS_SEND_A_FILEATTR: u16 = 26;
const BTRFS_SEND_A_UNENCODED_FILE_LEN: u16 = 27;
const BTRFS_SEND_A_UNENCODED_LEN: u16 = 28;
const BTRFS_SEND_A_UNENCODED_OFFSET: u16 = 29;
const BTRFS_SEND_A_COMPRESSION: u16 = 30;
const BTRFS_SEND_A_ENCRYPTION: u16 = 31;
// v3 attribute types.
const BTRFS_SEND_A_VERITY_ALGORITHM: u16 = 32;
const BTRFS_SEND_A_VERITY_BLOCK_SIZE: u16 = 33;
const BTRFS_SEND_A_VERITY_SALT_DATA: u16 = 34;
const BTRFS_SEND_A_VERITY_SIG_DATA: u16 = 35;

/// A timestamp from the send stream (sec + nsec).
#[derive(Debug, Clone, Copy)]
pub struct Timespec {
    pub sec: u64,
    pub nsec: u32,
}

/// A parsed command from the send stream.
#[derive(Debug)]
pub enum StreamCommand {
    Subvol {
        path: String,
        uuid: Uuid,
        ctransid: u64,
    },
    Snapshot {
        path: String,
        uuid: Uuid,
        ctransid: u64,
        clone_uuid: Uuid,
        clone_ctransid: u64,
    },
    Mkfile {
        path: String,
    },
    Mkdir {
        path: String,
    },
    Mknod {
        path: String,
        mode: u64,
        rdev: u64,
    },
    Mkfifo {
        path: String,
    },
    Mksock {
        path: String,
    },
    Symlink {
        path: String,
        target: String,
    },
    Rename {
        from: String,
        to: String,
    },
    Link {
        path: String,
        target: String,
    },
    Unlink {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Write {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    Clone {
        path: String,
        offset: u64,
        len: u64,
        clone_uuid: Uuid,
        clone_ctransid: u64,
        clone_path: String,
        clone_offset: u64,
    },
    SetXattr {
        path: String,
        name: String,
        data: Vec<u8>,
    },
    RemoveXattr {
        path: String,
        name: String,
    },
    Truncate {
        path: String,
        size: u64,
    },
    Chmod {
        path: String,
        mode: u64,
    },
    Chown {
        path: String,
        uid: u64,
        gid: u64,
    },
    Utimes {
        path: String,
        atime: Timespec,
        mtime: Timespec,
        ctime: Timespec,
    },
    UpdateExtent {
        path: String,
        offset: u64,
        len: u64,
    },
    /// v2: write pre-compressed data that can be passed through to the
    /// filesystem without decompression via `BTRFS_IOC_ENCODED_WRITE`.
    EncodedWrite {
        path: String,
        offset: u64,
        /// Unencoded (decompressed) file length to write.
        unencoded_file_len: u64,
        /// Total unencoded length (may be larger due to sector alignment).
        unencoded_len: u64,
        /// Offset within the unencoded data where the file data starts.
        unencoded_offset: u64,
        /// Compression algorithm (0=none, 1=zlib, 2=zstd, 3-7=lzo with varying sector sizes).
        compression: u32,
        /// Encryption algorithm (currently always 0).
        encryption: u32,
        data: Vec<u8>,
    },
    /// v2: preallocate space or punch a hole.
    Fallocate {
        path: String,
        /// `FALLOC_FL_*` flags (0=allocate, `1=KEEP_SIZE`, `3=PUNCH_HOLE|KEEP_SIZE`).
        mode: u32,
        offset: u64,
        len: u64,
    },
    /// v2: set inode file attributes (chattr flags).
    Fileattr {
        path: String,
        attr: u64,
    },
    /// v3: enable fs-verity on a file.
    EnableVerity {
        path: String,
        algorithm: u8,
        block_size: u32,
        salt: Vec<u8>,
        sig: Vec<u8>,
    },
    End,
}

/// Reads and parses a btrfs send stream.
#[derive(Debug)]
pub struct StreamReader<R: Read> {
    reader: R,
    version: u32,
    buf: Vec<u8>,
}

impl<R: Read> StreamReader<R> {
    /// Read and validate the stream header, returning a new reader.
    pub fn new(mut reader: R) -> Result<Self> {
        let mut header = [0u8; STREAM_HEADER_LEN];
        reader
            .read_exact(&mut header)
            .map_err(StreamError::TruncatedHeader)?;

        if &header[..SEND_STREAM_MAGIC_LEN] != SEND_STREAM_MAGIC {
            return Err(StreamError::BadMagic);
        }

        let version = u32::from_le_bytes(
            header[SEND_STREAM_MAGIC_LEN..STREAM_HEADER_LEN]
                .try_into()
                .unwrap(),
        );

        if version == 0 || version > 3 {
            return Err(StreamError::UnsupportedVersion(version));
        }

        Ok(Self {
            reader,
            version,
            buf: Vec::with_capacity(64 * 1024),
        })
    }

    /// Return the stream protocol version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Consume the underlying reader back out.
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// Read the next command from the stream.
    ///
    /// Returns `Ok(None)` on clean EOF (no more data), `Ok(Some(End))` when
    /// the stream contains an explicit end-of-stream marker.
    pub fn next_command(&mut self) -> Result<Option<StreamCommand>> {
        // Read command header.
        let mut cmd_hdr = [0u8; CMD_HEADER_LEN];
        if !read_exact_or_eof(&mut self.reader, &mut cmd_hdr)? {
            return Ok(None); // clean EOF
        }

        let payload_len =
            u32::from_le_bytes(cmd_hdr[0..4].try_into().unwrap()) as usize;
        let cmd = u16::from_le_bytes(cmd_hdr[4..6].try_into().unwrap());
        let expected_crc =
            u32::from_le_bytes(cmd_hdr[6..10].try_into().unwrap());

        // Read payload.
        self.buf.resize(payload_len, 0);
        self.reader
            .read_exact(&mut self.buf)
            .map_err(StreamError::TruncatedPayload)?;

        // Validate CRC32C: compute over header (with crc field zeroed) + payload.
        // The btrfs send stream uses a raw CRC-32C (init=0, xorout=0), not the
        // standard ISO 3309 convention (init=0xFFFFFFFF, xorout=0xFFFFFFFF).
        // The crc32c crate only exposes the standard version, so we recover the
        // raw value: raw_crc32c(0, data) == !crc32c_append(!0, data).
        let mut crc_buf = Vec::with_capacity(CMD_HEADER_LEN + payload_len);
        crc_buf.extend_from_slice(&cmd_hdr[0..6]); // len + cmd
        crc_buf.extend_from_slice(&[0u8; 4]); // zeroed crc field
        crc_buf.extend_from_slice(&self.buf);
        let computed_crc = !crc32c::crc32c_append(!0, &crc_buf);
        if computed_crc != expected_crc {
            return Err(StreamError::CrcMismatch {
                cmd,
                expected: expected_crc,
                computed: computed_crc,
            });
        }

        // Parse TLV attributes from payload.
        let attrs = parse_tlv_attrs(&self.buf, self.version)?;

        // Dispatch by command type.
        match cmd {
            BTRFS_SEND_C_SUBVOL => Ok(Some(StreamCommand::Subvol {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                uuid: attr_uuid(&self.buf, &attrs, BTRFS_SEND_A_UUID, "uuid")?,
                ctransid: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CTRANSID,
                    "ctransid",
                )?,
            })),
            BTRFS_SEND_C_SNAPSHOT => Ok(Some(StreamCommand::Snapshot {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                uuid: attr_uuid(&self.buf, &attrs, BTRFS_SEND_A_UUID, "uuid")?,
                ctransid: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CTRANSID,
                    "ctransid",
                )?,
                clone_uuid: attr_uuid(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_UUID,
                    "clone_uuid",
                )?,
                clone_ctransid: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_CTRANSID,
                    "clone_ctransid",
                )?,
            })),
            BTRFS_SEND_C_MKFILE => Ok(Some(StreamCommand::Mkfile {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_MKDIR => Ok(Some(StreamCommand::Mkdir {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_MKNOD => Ok(Some(StreamCommand::Mknod {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                mode: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_MODE, "mode")?,
                rdev: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_RDEV, "rdev")?,
            })),
            BTRFS_SEND_C_MKFIFO => Ok(Some(StreamCommand::Mkfifo {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_MKSOCK => Ok(Some(StreamCommand::Mksock {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_SYMLINK => Ok(Some(StreamCommand::Symlink {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                target: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH_LINK,
                    "link_target",
                )?,
            })),
            BTRFS_SEND_C_RENAME => Ok(Some(StreamCommand::Rename {
                from: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                to: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH_TO,
                    "path_to",
                )?,
            })),
            BTRFS_SEND_C_LINK => Ok(Some(StreamCommand::Link {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                target: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH_LINK,
                    "link_target",
                )?,
            })),
            BTRFS_SEND_C_UNLINK => Ok(Some(StreamCommand::Unlink {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_RMDIR => Ok(Some(StreamCommand::Rmdir {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
            })),
            BTRFS_SEND_C_SET_XATTR => Ok(Some(StreamCommand::SetXattr {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                name: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_XATTR_NAME,
                    "xattr_name",
                )?,
                data: attr_data(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_XATTR_DATA,
                    "xattr_data",
                )?,
            })),
            BTRFS_SEND_C_REMOVE_XATTR => Ok(Some(StreamCommand::RemoveXattr {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                name: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_XATTR_NAME,
                    "xattr_name",
                )?,
            })),
            BTRFS_SEND_C_WRITE => Ok(Some(StreamCommand::Write {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                offset: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_FILE_OFFSET,
                    "file_offset",
                )?,
                data: attr_data(&self.buf, &attrs, BTRFS_SEND_A_DATA, "data")?,
            })),
            BTRFS_SEND_C_CLONE => Ok(Some(StreamCommand::Clone {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                offset: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_FILE_OFFSET,
                    "file_offset",
                )?,
                len: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_LEN,
                    "clone_len",
                )?,
                clone_uuid: attr_uuid(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_UUID,
                    "clone_uuid",
                )?,
                clone_ctransid: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_CTRANSID,
                    "clone_ctransid",
                )?,
                clone_path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_PATH,
                    "clone_path",
                )?,
                clone_offset: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CLONE_OFFSET,
                    "clone_offset",
                )?,
            })),
            BTRFS_SEND_C_TRUNCATE => Ok(Some(StreamCommand::Truncate {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                size: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_SIZE, "size")?,
            })),
            BTRFS_SEND_C_CHMOD => Ok(Some(StreamCommand::Chmod {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                mode: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_MODE, "mode")?,
            })),
            BTRFS_SEND_C_CHOWN => Ok(Some(StreamCommand::Chown {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                uid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_UID, "uid")?,
                gid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_GID, "gid")?,
            })),
            BTRFS_SEND_C_UTIMES => Ok(Some(StreamCommand::Utimes {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                atime: attr_timespec(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_ATIME,
                    "atime",
                )?,
                mtime: attr_timespec(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_MTIME,
                    "mtime",
                )?,
                ctime: attr_timespec(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_CTIME,
                    "ctime",
                )?,
            })),
            BTRFS_SEND_C_UPDATE_EXTENT => {
                Ok(Some(StreamCommand::UpdateExtent {
                    path: attr_string(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_PATH,
                        "path",
                    )?,
                    offset: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_FILE_OFFSET,
                        "file_offset",
                    )?,
                    len: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_SIZE,
                        "size",
                    )?,
                }))
            }
            BTRFS_SEND_C_FALLOCATE => Ok(Some(StreamCommand::Fallocate {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                mode: attr_u32(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_FALLOCATE_MODE,
                    "fallocate_mode",
                )?,
                offset: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_FILE_OFFSET,
                    "file_offset",
                )?,
                len: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_SIZE, "size")?,
            })),
            BTRFS_SEND_C_FILEATTR => Ok(Some(StreamCommand::Fileattr {
                path: attr_string(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_PATH,
                    "path",
                )?,
                attr: attr_u64(
                    &self.buf,
                    &attrs,
                    BTRFS_SEND_A_FILEATTR,
                    "fileattr",
                )?,
            })),
            BTRFS_SEND_C_ENCODED_WRITE => {
                Ok(Some(StreamCommand::EncodedWrite {
                    path: attr_string(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_PATH,
                        "path",
                    )?,
                    offset: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_FILE_OFFSET,
                        "file_offset",
                    )?,
                    unencoded_file_len: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_UNENCODED_FILE_LEN,
                        "unencoded_file_len",
                    )?,
                    unencoded_len: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_UNENCODED_LEN,
                        "unencoded_len",
                    )?,
                    unencoded_offset: attr_u64(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_UNENCODED_OFFSET,
                        "unencoded_offset",
                    )?,
                    // Compression and encryption default to 0 if absent.
                    compression: attr_opt_u32(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_COMPRESSION,
                        0,
                    ),
                    encryption: attr_opt_u32(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_ENCRYPTION,
                        0,
                    ),
                    data: attr_data(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_DATA,
                        "data",
                    )?,
                }))
            }
            BTRFS_SEND_C_ENABLE_VERITY => {
                Ok(Some(StreamCommand::EnableVerity {
                    path: attr_string(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_PATH,
                        "path",
                    )?,
                    algorithm: attr_u8(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_VERITY_ALGORITHM,
                        "verity_algorithm",
                    )?,
                    block_size: attr_u32(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_VERITY_BLOCK_SIZE,
                        "verity_block_size",
                    )?,
                    salt: attr_data(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_VERITY_SALT_DATA,
                        "verity_salt",
                    )?,
                    sig: attr_data(
                        &self.buf,
                        &attrs,
                        BTRFS_SEND_A_VERITY_SIG_DATA,
                        "verity_sig",
                    )?,
                }))
            }
            BTRFS_SEND_C_END => Ok(Some(StreamCommand::End)),
            _ => Err(StreamError::UnknownCommand(cmd)),
        }
    }
}

/// Read exactly `buf.len()` bytes, returning false on clean EOF (zero bytes read).
fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    let mut pos = 0;
    while pos < buf.len() {
        match reader.read(&mut buf[pos..]) {
            Ok(0) => {
                if pos == 0 {
                    return Ok(false);
                }
                return Err(StreamError::TruncatedPayload(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("unexpected EOF after {pos} bytes"),
                    ),
                ));
            }
            Ok(n) => pos += n,
            Err(e) => return Err(StreamError::TruncatedPayload(e)),
        }
    }
    Ok(true)
}

/// Attribute storage: for each attribute type index, Option<(offset, len)> into
/// the payload buffer.
type AttrTable = [Option<(usize, usize)>; MAX_ATTRS];

/// Parse TLV attributes from a command payload buffer into a lookup table.
fn parse_tlv_attrs(payload: &[u8], version: u32) -> Result<AttrTable> {
    let mut attrs: AttrTable = [None; MAX_ATTRS];
    let mut pos = 0;

    while pos < payload.len() {
        if pos + 2 > payload.len() {
            return Err(StreamError::TruncatedTlv {
                detail: "not enough bytes for type field".into(),
            });
        }
        let tlv_type =
            u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap());
        pos += 2;

        if tlv_type == 0 || tlv_type as usize > MAX_ATTRS {
            return Err(StreamError::InvalidTlvType(tlv_type));
        }

        // In v2+, the DATA attribute has no length field — it extends to the
        // end of the command payload.
        let tlv_len = if version >= 2 && tlv_type == BTRFS_SEND_A_DATA {
            payload.len() - pos
        } else {
            if pos + 2 > payload.len() {
                return Err(StreamError::TruncatedTlv {
                    detail: "not enough bytes for length field".into(),
                });
            }
            let len =
                u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap())
                    as usize;
            pos += 2;
            len
        };

        if pos + tlv_len > payload.len() {
            return Err(StreamError::TruncatedTlv {
                detail: format!(
                    "attribute type {tlv_type} needs {tlv_len} bytes but only {} remain",
                    payload.len() - pos
                ),
            });
        }

        attrs[(tlv_type - 1) as usize] = Some((pos, tlv_len));
        pos += tlv_len;
    }

    Ok(attrs)
}

fn get_attr<'a>(
    buf: &'a [u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<&'a [u8]> {
    let (offset, len) = attrs[(attr_type - 1) as usize]
        .ok_or(StreamError::MissingAttribute(name))?;
    Ok(&buf[offset..offset + len])
}

fn attr_u64(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<u64> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 8 {
        return Err(StreamError::InvalidAttribute {
            name,
            detail: format!("too short for u64: {} bytes", data.len()),
        });
    }
    Ok(u64::from_le_bytes(data[0..8].try_into().unwrap()))
}

fn attr_string(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<String> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    // Strings in the stream are null-terminated; strip the trailing NUL.
    let s = if data.last() == Some(&0) {
        &data[..data.len() - 1]
    } else {
        data
    };
    String::from_utf8(s.to_vec()).map_err(|_| StreamError::InvalidAttribute {
        name,
        detail: "not valid UTF-8".into(),
    })
}

fn attr_uuid(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<Uuid> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 16 {
        return Err(StreamError::InvalidAttribute {
            name,
            detail: format!("too short for UUID: {} bytes", data.len()),
        });
    }
    Ok(Uuid::from_bytes(data[0..16].try_into().unwrap()))
}

fn attr_timespec(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<Timespec> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 12 {
        return Err(StreamError::InvalidAttribute {
            name,
            detail: format!("too short for timespec: {} bytes", data.len()),
        });
    }
    Ok(Timespec {
        sec: u64::from_le_bytes(data[0..8].try_into().unwrap()),
        nsec: u32::from_le_bytes(data[8..12].try_into().unwrap()),
    })
}

fn attr_u32(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<u32> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 4 {
        return Err(StreamError::InvalidAttribute {
            name,
            detail: format!("too short for u32: {} bytes", data.len()),
        });
    }
    Ok(u32::from_le_bytes(data[0..4].try_into().unwrap()))
}

fn attr_u8(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<u8> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.is_empty() {
        return Err(StreamError::InvalidAttribute {
            name,
            detail: "is empty".into(),
        });
    }
    Ok(data[0])
}

/// Like `get_attr` but returns `None` instead of an error when the attribute
/// is absent. Used for optional attributes in v2 commands.
fn get_attr_opt<'a>(
    buf: &'a [u8],
    attrs: &AttrTable,
    attr_type: u16,
) -> Option<&'a [u8]> {
    let (offset, len) = attrs[(attr_type - 1) as usize]?;
    Some(&buf[offset..offset + len])
}

fn attr_opt_u32(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    default: u32,
) -> u32 {
    match get_attr_opt(buf, attrs, attr_type) {
        Some(data) if data.len() >= 4 => {
            u32::from_le_bytes(data[0..4].try_into().unwrap())
        }
        _ => default,
    }
}

fn attr_data(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &'static str,
) -> Result<Vec<u8>> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    Ok(data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a valid v1 stream header.
    fn v1_header() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(SEND_STREAM_MAGIC);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf
    }

    /// Build a v1 command with proper CRC. `cmd` is the command type code,
    /// `payload` is the raw TLV payload bytes.
    fn build_command(cmd: u16, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        // Build header with zeroed CRC for checksum computation.
        // Uses raw CRC-32C (init=0, xorout=0) matching the btrfs send format.
        let mut crc_input = Vec::new();
        crc_input.extend_from_slice(&len.to_le_bytes());
        crc_input.extend_from_slice(&cmd.to_le_bytes());
        crc_input.extend_from_slice(&[0u8; 4]); // zeroed crc
        crc_input.extend_from_slice(payload);
        let crc = !crc32c::crc32c_append(!0, &crc_input);

        let mut out = Vec::new();
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&cmd.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Build a TLV attribute with a u16 type, u16 length, and raw data.
    fn tlv(attr_type: u16, data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&attr_type.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u16).to_le_bytes());
        buf.extend_from_slice(data);
        buf
    }

    /// Build a TLV string attribute (null-terminated).
    fn tlv_string(attr_type: u16, s: &str) -> Vec<u8> {
        let mut data = s.as_bytes().to_vec();
        data.push(0); // null terminator
        tlv(attr_type, &data)
    }

    /// Build a TLV u64 attribute (little-endian).
    fn tlv_u64(attr_type: u16, val: u64) -> Vec<u8> {
        tlv(attr_type, &val.to_le_bytes())
    }

    /// Build a TLV UUID attribute.
    fn tlv_uuid(attr_type: u16, uuid: Uuid) -> Vec<u8> {
        tlv(attr_type, uuid.as_bytes())
    }

    /// Build a TLV timespec attribute (8 bytes sec + 4 bytes nsec, LE).
    fn tlv_timespec(attr_type: u16, sec: u64, nsec: u32) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&sec.to_le_bytes());
        data.extend_from_slice(&nsec.to_le_bytes());
        tlv(attr_type, &data)
    }

    // --- Header parsing ---

    #[test]
    fn valid_v1_header() {
        let mut stream = v1_header();
        // Append an END command so we can actually read something.
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));
        let reader = StreamReader::new(Cursor::new(stream)).unwrap();
        assert_eq!(reader.version(), 1);
    }

    #[test]
    fn valid_v2_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SEND_STREAM_MAGIC);
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));
        let reader = StreamReader::new(Cursor::new(buf)).unwrap();
        assert_eq!(reader.version(), 2);
    }

    #[test]
    fn bad_magic() {
        let buf = b"not-a-stream\0\x01\x00\x00\x00";
        let err = StreamReader::new(Cursor::new(buf.to_vec())).unwrap_err();
        assert!(
            format!("{err}").contains("bad magic"),
            "expected bad magic error, got: {err}"
        );
    }

    #[test]
    fn unsupported_version_zero() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SEND_STREAM_MAGIC);
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err = StreamReader::new(Cursor::new(buf)).unwrap_err();
        assert!(format!("{err}").contains("unsupported"), "got: {err}");
    }

    #[test]
    fn valid_v3_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SEND_STREAM_MAGIC);
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));
        let reader = StreamReader::new(Cursor::new(buf)).unwrap();
        assert_eq!(reader.version(), 3);
    }

    #[test]
    fn unsupported_version_four() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SEND_STREAM_MAGIC);
        buf.extend_from_slice(&4u32.to_le_bytes());
        let err = StreamReader::new(Cursor::new(buf)).unwrap_err();
        assert!(format!("{err}").contains("unsupported"), "got: {err}");
    }

    #[test]
    fn truncated_header() {
        let buf = b"btrfs-str"; // too short
        let err = StreamReader::new(Cursor::new(buf.to_vec())).unwrap_err();
        assert!(matches!(err, StreamError::TruncatedHeader(_)), "got: {err}");
    }

    // --- END command ---

    #[test]
    fn end_command() {
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let cmd = reader.next_command().unwrap().unwrap();
        assert!(matches!(cmd, StreamCommand::End));
    }

    #[test]
    fn eof_after_end() {
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        reader.next_command().unwrap(); // END
        assert!(reader.next_command().unwrap().is_none()); // clean EOF
    }

    // --- CRC validation ---

    #[test]
    fn corrupted_crc() {
        let mut stream = v1_header();
        let mut cmd = build_command(BTRFS_SEND_C_END, &[]);
        cmd[8] ^= 0xFF; // flip a byte in the CRC field
        stream.extend_from_slice(&cmd);
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(format!("{err}").contains("CRC mismatch"), "got: {err}");
    }

    #[test]
    fn corrupted_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "test"));
        let mut stream = v1_header();
        let mut cmd = build_command(BTRFS_SEND_C_MKFILE, &payload);
        // Corrupt a payload byte (after the 10-byte header).
        if cmd.len() > 11 {
            cmd[11] ^= 0xFF;
        }
        stream.extend_from_slice(&cmd);
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(format!("{err}").contains("CRC mismatch"), "got: {err}");
    }

    // --- Simple commands ---

    #[test]
    fn mkfile_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "hello.txt");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKFILE, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Mkfile { path } => assert_eq!(path, "hello.txt"),
            other => panic!("expected Mkfile, got {other:?}"),
        }
    }

    #[test]
    fn mkdir_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "subdir");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKDIR, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Mkdir { path } => assert_eq!(path, "subdir"),
            other => panic!("expected Mkdir, got {other:?}"),
        }
    }

    #[test]
    fn unlink_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "gone.txt");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_UNLINK, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Unlink { path } => assert_eq!(path, "gone.txt"),
            other => panic!("expected Unlink, got {other:?}"),
        }
    }

    #[test]
    fn rmdir_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "old_dir");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_RMDIR, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Rmdir { path } => assert_eq!(path, "old_dir"),
            other => panic!("expected Rmdir, got {other:?}"),
        }
    }

    // --- Commands with multiple attributes ---

    #[test]
    fn rename_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "old_name"));
        payload
            .extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH_TO, "new_name"));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_RENAME, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Rename { from, to } => {
                assert_eq!(from, "old_name");
                assert_eq!(to, "new_name");
            }
            other => panic!("expected Rename, got {other:?}"),
        }
    }

    #[test]
    fn symlink_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "link"));
        payload
            .extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH_LINK, "/target"));
        let mut stream = v1_header();
        stream
            .extend_from_slice(&build_command(BTRFS_SEND_C_SYMLINK, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Symlink { path, target } => {
                assert_eq!(path, "link");
                assert_eq!(target, "/target");
            }
            other => panic!("expected Symlink, got {other:?}"),
        }
    }

    #[test]
    fn chmod_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "file.sh"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_MODE, 0o755));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_CHMOD, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Chmod { path, mode } => {
                assert_eq!(path, "file.sh");
                assert_eq!(mode, 0o755);
            }
            other => panic!("expected Chmod, got {other:?}"),
        }
    }

    #[test]
    fn chown_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "owned"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_UID, 1000));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_GID, 1000));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_CHOWN, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Chown { path, uid, gid } => {
                assert_eq!(path, "owned");
                assert_eq!(uid, 1000);
                assert_eq!(gid, 1000);
            }
            other => panic!("expected Chown, got {other:?}"),
        }
    }

    #[test]
    fn truncate_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "shrink.bin"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_SIZE, 4096));
        let mut stream = v1_header();
        stream
            .extend_from_slice(&build_command(BTRFS_SEND_C_TRUNCATE, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Truncate { path, size } => {
                assert_eq!(path, "shrink.bin");
                assert_eq!(size, 4096);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn write_command() {
        let file_data = b"hello world";
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "data.bin"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_FILE_OFFSET, 512));
        payload.extend_from_slice(&tlv(BTRFS_SEND_A_DATA, file_data));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_WRITE, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Write { path, offset, data } => {
                assert_eq!(path, "data.bin");
                assert_eq!(offset, 512);
                assert_eq!(data, file_data);
            }
            other => panic!("expected Write, got {other:?}"),
        }
    }

    #[test]
    fn set_xattr_command() {
        let xattr_value = b"\x01\x02\x03";
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "file"));
        payload.extend_from_slice(&tlv_string(
            BTRFS_SEND_A_XATTR_NAME,
            "user.test",
        ));
        payload.extend_from_slice(&tlv(BTRFS_SEND_A_XATTR_DATA, xattr_value));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(
            BTRFS_SEND_C_SET_XATTR,
            &payload,
        ));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::SetXattr { path, name, data } => {
                assert_eq!(path, "file");
                assert_eq!(name, "user.test");
                assert_eq!(data, xattr_value);
            }
            other => panic!("expected SetXattr, got {other:?}"),
        }
    }

    #[test]
    fn utimes_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "timed"));
        payload.extend_from_slice(&tlv_timespec(BTRFS_SEND_A_ATIME, 1000, 111));
        payload.extend_from_slice(&tlv_timespec(BTRFS_SEND_A_MTIME, 2000, 222));
        payload.extend_from_slice(&tlv_timespec(BTRFS_SEND_A_CTIME, 3000, 333));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_UTIMES, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Utimes {
                path,
                atime,
                mtime,
                ctime,
            } => {
                assert_eq!(path, "timed");
                assert_eq!(atime.sec, 1000);
                assert_eq!(atime.nsec, 111);
                assert_eq!(mtime.sec, 2000);
                assert_eq!(mtime.nsec, 222);
                assert_eq!(ctime.sec, 3000);
                assert_eq!(ctime.nsec, 333);
            }
            other => panic!("expected Utimes, got {other:?}"),
        }
    }

    // --- Subvol and Snapshot ---

    #[test]
    fn subvol_command() {
        let uuid =
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "my_subvol"));
        payload.extend_from_slice(&tlv_uuid(BTRFS_SEND_A_UUID, uuid));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CTRANSID, 42));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_SUBVOL, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Subvol {
                path,
                uuid: u,
                ctransid,
            } => {
                assert_eq!(path, "my_subvol");
                assert_eq!(u, uuid);
                assert_eq!(ctransid, 42);
            }
            other => panic!("expected Subvol, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_command() {
        let uuid =
            Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let clone_uuid =
            Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "my_snap"));
        payload.extend_from_slice(&tlv_uuid(BTRFS_SEND_A_UUID, uuid));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CTRANSID, 100));
        payload
            .extend_from_slice(&tlv_uuid(BTRFS_SEND_A_CLONE_UUID, clone_uuid));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CLONE_CTRANSID, 99));
        let mut stream = v1_header();
        stream
            .extend_from_slice(&build_command(BTRFS_SEND_C_SNAPSHOT, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Snapshot {
                path,
                uuid: u,
                ctransid,
                clone_uuid: cu,
                clone_ctransid,
            } => {
                assert_eq!(path, "my_snap");
                assert_eq!(u, uuid);
                assert_eq!(ctransid, 100);
                assert_eq!(cu, clone_uuid);
                assert_eq!(clone_ctransid, 99);
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    // --- Clone command ---

    #[test]
    fn clone_command() {
        let clone_uuid =
            Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "dest"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_FILE_OFFSET, 0));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CLONE_LEN, 4096));
        payload
            .extend_from_slice(&tlv_uuid(BTRFS_SEND_A_CLONE_UUID, clone_uuid));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CLONE_CTRANSID, 7));
        payload
            .extend_from_slice(&tlv_string(BTRFS_SEND_A_CLONE_PATH, "source"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CLONE_OFFSET, 8192));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_CLONE, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Clone {
                path,
                offset,
                len,
                clone_uuid: cu,
                clone_ctransid,
                clone_path,
                clone_offset,
            } => {
                assert_eq!(path, "dest");
                assert_eq!(offset, 0);
                assert_eq!(len, 4096);
                assert_eq!(cu, clone_uuid);
                assert_eq!(clone_ctransid, 7);
                assert_eq!(clone_path, "source");
                assert_eq!(clone_offset, 8192);
            }
            other => panic!("expected Clone, got {other:?}"),
        }
    }

    // --- Multi-command stream ---

    #[test]
    fn multiple_commands_in_sequence() {
        let uuid = Uuid::nil();
        let mut stream = v1_header();

        // SUBVOL
        let mut p = Vec::new();
        p.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "vol"));
        p.extend_from_slice(&tlv_uuid(BTRFS_SEND_A_UUID, uuid));
        p.extend_from_slice(&tlv_u64(BTRFS_SEND_A_CTRANSID, 1));
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_SUBVOL, &p));

        // MKFILE
        stream.extend_from_slice(&build_command(
            BTRFS_SEND_C_MKFILE,
            &tlv_string(BTRFS_SEND_A_PATH, "file.txt"),
        ));

        // END
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_END, &[]));

        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();

        assert!(matches!(
            reader.next_command().unwrap().unwrap(),
            StreamCommand::Subvol { .. }
        ));
        assert!(matches!(
            reader.next_command().unwrap().unwrap(),
            StreamCommand::Mkfile { .. }
        ));
        assert!(matches!(
            reader.next_command().unwrap().unwrap(),
            StreamCommand::End
        ));
        assert!(reader.next_command().unwrap().is_none());
    }

    // --- Error cases ---

    #[test]
    fn unknown_command_type() {
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(99, &[]));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(
            format!("{err}").contains("unknown send stream command type 99"),
            "got: {err}"
        );
    }

    #[test]
    fn missing_required_attribute() {
        // MKFILE with no PATH attribute.
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKFILE, &[]));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(
            format!("{err}").contains("missing required attribute"),
            "got: {err}"
        );
    }

    #[test]
    fn truncated_payload() {
        let mut stream = v1_header();
        // Write a command header claiming 100 bytes of payload, but don't
        // provide the payload data.
        let len = 100u32;
        let cmd = BTRFS_SEND_C_END;
        let mut crc_input = Vec::new();
        crc_input.extend_from_slice(&len.to_le_bytes());
        crc_input.extend_from_slice(&cmd.to_le_bytes());
        crc_input.extend_from_slice(&[0u8; 4]);
        let crc = crc32c::crc32c(&crc_input);
        stream.extend_from_slice(&len.to_le_bytes());
        stream.extend_from_slice(&cmd.to_le_bytes());
        stream.extend_from_slice(&crc.to_le_bytes());
        // No payload bytes follow.
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(format!("{err}").contains("truncated"), "got: {err}");
    }

    #[test]
    fn into_inner_returns_reader() {
        let stream = v1_header();
        let cursor = Cursor::new(stream);
        let reader = StreamReader::new(cursor).unwrap();
        let _inner: Cursor<Vec<u8>> = reader.into_inner();
    }

    // --- UpdateExtent ---

    #[test]
    fn update_extent_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "extent.dat"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_FILE_OFFSET, 65536));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_SIZE, 131072));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(
            BTRFS_SEND_C_UPDATE_EXTENT,
            &payload,
        ));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::UpdateExtent { path, offset, len } => {
                assert_eq!(path, "extent.dat");
                assert_eq!(offset, 65536);
                assert_eq!(len, 131072);
            }
            other => panic!("expected UpdateExtent, got {other:?}"),
        }
    }

    // --- Mknod, Mkfifo, Mksock ---

    #[test]
    fn mknod_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "dev_node"));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_MODE, 0o660));
        payload.extend_from_slice(&tlv_u64(BTRFS_SEND_A_RDEV, 0x0801)); // 8:1
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKNOD, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Mknod { path, mode, rdev } => {
                assert_eq!(path, "dev_node");
                assert_eq!(mode, 0o660);
                assert_eq!(rdev, 0x0801);
            }
            other => panic!("expected Mknod, got {other:?}"),
        }
    }

    #[test]
    fn mkfifo_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "my_fifo");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKFIFO, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Mkfifo { path } => assert_eq!(path, "my_fifo"),
            other => panic!("expected Mkfifo, got {other:?}"),
        }
    }

    #[test]
    fn mksock_command() {
        let payload = tlv_string(BTRFS_SEND_A_PATH, "my_sock");
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_MKSOCK, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Mksock { path } => assert_eq!(path, "my_sock"),
            other => panic!("expected Mksock, got {other:?}"),
        }
    }

    // --- Link and RemoveXattr ---

    #[test]
    fn link_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "hardlink"));
        payload
            .extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH_LINK, "original"));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(BTRFS_SEND_C_LINK, &payload));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::Link { path, target } => {
                assert_eq!(path, "hardlink");
                assert_eq!(target, "original");
            }
            other => panic!("expected Link, got {other:?}"),
        }
    }

    #[test]
    fn remove_xattr_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tlv_string(BTRFS_SEND_A_PATH, "file"));
        payload.extend_from_slice(&tlv_string(
            BTRFS_SEND_A_XATTR_NAME,
            "user.old",
        ));
        let mut stream = v1_header();
        stream.extend_from_slice(&build_command(
            BTRFS_SEND_C_REMOVE_XATTR,
            &payload,
        ));
        let mut reader = StreamReader::new(Cursor::new(stream)).unwrap();
        match reader.next_command().unwrap().unwrap() {
            StreamCommand::RemoveXattr { path, name } => {
                assert_eq!(path, "file");
                assert_eq!(name, "user.old");
            }
            other => panic!("expected RemoveXattr, got {other:?}"),
        }
    }
}

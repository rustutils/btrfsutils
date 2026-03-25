//! Binary send stream parser for the btrfs send/receive protocol.
//!
//! Parses the TLV-encoded command stream produced by `btrfs send` (or the
//! kernel's `BTRFS_IOC_SEND` ioctl). The stream consists of a header followed
//! by a sequence of commands, each with a CRC32C checksum and a set of
//! typed TLV attributes.

use anyhow::{Context, Result, bail};
use std::io::Read;
use uuid::Uuid;

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
    End,
}

/// Reads and parses a btrfs send stream.
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
            .context("failed to read stream header")?;

        if &header[..SEND_STREAM_MAGIC_LEN] != SEND_STREAM_MAGIC {
            bail!("invalid send stream: bad magic");
        }

        let version = u32::from_le_bytes(
            header[SEND_STREAM_MAGIC_LEN..STREAM_HEADER_LEN]
                .try_into()
                .unwrap(),
        );

        if version == 0 || version > 2 {
            bail!("unsupported send stream version {version} (supported: 1-2)");
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
        match read_exact_or_eof(&mut self.reader, &mut cmd_hdr)? {
            false => return Ok(None), // clean EOF
            true => {}
        }

        let payload_len = u32::from_le_bytes(cmd_hdr[0..4].try_into().unwrap()) as usize;
        let cmd = u16::from_le_bytes(cmd_hdr[4..6].try_into().unwrap());
        let expected_crc = u32::from_le_bytes(cmd_hdr[6..10].try_into().unwrap());

        // Read payload.
        self.buf.resize(payload_len, 0);
        self.reader
            .read_exact(&mut self.buf)
            .context("truncated send stream: short payload")?;

        // Validate CRC32C: compute over header (with crc field zeroed) + payload.
        let mut crc_buf = Vec::with_capacity(CMD_HEADER_LEN + payload_len);
        crc_buf.extend_from_slice(&cmd_hdr[0..6]); // len + cmd
        crc_buf.extend_from_slice(&[0u8; 4]); // zeroed crc field
        crc_buf.extend_from_slice(&self.buf);
        let computed_crc = crc32c::crc32c(&crc_buf);
        if computed_crc != expected_crc {
            bail!(
                "CRC mismatch for command {cmd}: expected {expected_crc:#010x}, got {computed_crc:#010x}"
            );
        }

        // Parse TLV attributes from payload.
        let attrs = parse_tlv_attrs(&self.buf, self.version)?;

        // Dispatch by command type.
        match cmd {
            BTRFS_SEND_C_SUBVOL => Ok(Some(StreamCommand::Subvol {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                uuid: attr_uuid(&self.buf, &attrs, BTRFS_SEND_A_UUID, "uuid")?,
                ctransid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_CTRANSID, "ctransid")?,
            })),
            BTRFS_SEND_C_SNAPSHOT => Ok(Some(StreamCommand::Snapshot {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                uuid: attr_uuid(&self.buf, &attrs, BTRFS_SEND_A_UUID, "uuid")?,
                ctransid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_CTRANSID, "ctransid")?,
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
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_MKDIR => Ok(Some(StreamCommand::Mkdir {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_MKNOD => Ok(Some(StreamCommand::Mknod {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                mode: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_MODE, "mode")?,
                rdev: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_RDEV, "rdev")?,
            })),
            BTRFS_SEND_C_MKFIFO => Ok(Some(StreamCommand::Mkfifo {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_MKSOCK => Ok(Some(StreamCommand::Mksock {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_SYMLINK => Ok(Some(StreamCommand::Symlink {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                target: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH_LINK, "link_target")?,
            })),
            BTRFS_SEND_C_RENAME => Ok(Some(StreamCommand::Rename {
                from: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                to: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH_TO, "path_to")?,
            })),
            BTRFS_SEND_C_LINK => Ok(Some(StreamCommand::Link {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                target: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH_LINK, "link_target")?,
            })),
            BTRFS_SEND_C_UNLINK => Ok(Some(StreamCommand::Unlink {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_RMDIR => Ok(Some(StreamCommand::Rmdir {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
            })),
            BTRFS_SEND_C_SET_XATTR => Ok(Some(StreamCommand::SetXattr {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                name: attr_string(&self.buf, &attrs, BTRFS_SEND_A_XATTR_NAME, "xattr_name")?,
                data: attr_data(&self.buf, &attrs, BTRFS_SEND_A_XATTR_DATA, "xattr_data")?,
            })),
            BTRFS_SEND_C_REMOVE_XATTR => Ok(Some(StreamCommand::RemoveXattr {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                name: attr_string(&self.buf, &attrs, BTRFS_SEND_A_XATTR_NAME, "xattr_name")?,
            })),
            BTRFS_SEND_C_WRITE => Ok(Some(StreamCommand::Write {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                offset: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_FILE_OFFSET, "file_offset")?,
                data: attr_data(&self.buf, &attrs, BTRFS_SEND_A_DATA, "data")?,
            })),
            BTRFS_SEND_C_CLONE => Ok(Some(StreamCommand::Clone {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                offset: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_FILE_OFFSET, "file_offset")?,
                len: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_CLONE_LEN, "clone_len")?,
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
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                size: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_SIZE, "size")?,
            })),
            BTRFS_SEND_C_CHMOD => Ok(Some(StreamCommand::Chmod {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                mode: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_MODE, "mode")?,
            })),
            BTRFS_SEND_C_CHOWN => Ok(Some(StreamCommand::Chown {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                uid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_UID, "uid")?,
                gid: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_GID, "gid")?,
            })),
            BTRFS_SEND_C_UTIMES => Ok(Some(StreamCommand::Utimes {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                atime: attr_timespec(&self.buf, &attrs, BTRFS_SEND_A_ATIME, "atime")?,
                mtime: attr_timespec(&self.buf, &attrs, BTRFS_SEND_A_MTIME, "mtime")?,
                ctime: attr_timespec(&self.buf, &attrs, BTRFS_SEND_A_CTIME, "ctime")?,
            })),
            BTRFS_SEND_C_UPDATE_EXTENT => Ok(Some(StreamCommand::UpdateExtent {
                path: attr_string(&self.buf, &attrs, BTRFS_SEND_A_PATH, "path")?,
                offset: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_FILE_OFFSET, "file_offset")?,
                len: attr_u64(&self.buf, &attrs, BTRFS_SEND_A_SIZE, "size")?,
            })),
            BTRFS_SEND_C_END => Ok(Some(StreamCommand::End)),
            _ => bail!("unknown send stream command type {cmd}"),
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
                bail!("truncated send stream: unexpected EOF after {pos} bytes");
            }
            Ok(n) => pos += n,
            Err(e) => return Err(e).context("failed to read send stream"),
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
            bail!("truncated TLV: not enough bytes for type field");
        }
        let tlv_type = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap());
        pos += 2;

        if tlv_type == 0 || tlv_type as usize > MAX_ATTRS {
            bail!("invalid TLV attribute type {tlv_type}");
        }

        // In v2+, the DATA attribute has no length field — it extends to the
        // end of the command payload.
        let tlv_len = if version >= 2 && tlv_type == BTRFS_SEND_A_DATA {
            payload.len() - pos
        } else {
            if pos + 2 > payload.len() {
                bail!("truncated TLV: not enough bytes for length field");
            }
            let len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            len
        };

        if pos + tlv_len > payload.len() {
            bail!(
                "truncated TLV: attribute type {tlv_type} needs {tlv_len} bytes but only {} remain",
                payload.len() - pos
            );
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
    name: &str,
) -> Result<&'a [u8]> {
    let (offset, len) = attrs[(attr_type - 1) as usize]
        .ok_or_else(|| anyhow::anyhow!("missing required attribute: {name}"))?;
    Ok(&buf[offset..offset + len])
}

fn attr_u64(buf: &[u8], attrs: &AttrTable, attr_type: u16, name: &str) -> Result<u64> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 8 {
        bail!("attribute {name} too short for u64: {} bytes", data.len());
    }
    Ok(u64::from_le_bytes(data[0..8].try_into().unwrap()))
}

fn attr_string(buf: &[u8], attrs: &AttrTable, attr_type: u16, name: &str) -> Result<String> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    // Strings in the stream are null-terminated; strip the trailing NUL.
    let s = if data.last() == Some(&0) {
        &data[..data.len() - 1]
    } else {
        data
    };
    String::from_utf8(s.to_vec())
        .with_context(|| format!("attribute {name} is not valid UTF-8"))
}

fn attr_uuid(buf: &[u8], attrs: &AttrTable, attr_type: u16, name: &str) -> Result<Uuid> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 16 {
        bail!("attribute {name} too short for UUID: {} bytes", data.len());
    }
    Ok(Uuid::from_bytes(data[0..16].try_into().unwrap()))
}

fn attr_timespec(
    buf: &[u8],
    attrs: &AttrTable,
    attr_type: u16,
    name: &str,
) -> Result<Timespec> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    if data.len() < 12 {
        bail!(
            "attribute {name} too short for timespec: {} bytes",
            data.len()
        );
    }
    Ok(Timespec {
        sec: u64::from_le_bytes(data[0..8].try_into().unwrap()),
        nsec: u32::from_le_bytes(data[8..12].try_into().unwrap()),
    })
}

fn attr_data(buf: &[u8], attrs: &AttrTable, attr_type: u16, name: &str) -> Result<Vec<u8>> {
    let data = get_attr(buf, attrs, attr_type, name)?;
    Ok(data.to_vec())
}

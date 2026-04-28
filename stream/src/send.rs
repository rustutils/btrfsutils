//! Binary send stream encoder, mirror image of [`crate::StreamReader`].
//!
//! Writes TLV-framed [`StreamCommand`] values to any `impl Write`,
//! producing the same wire format the kernel emits via
//! `BTRFS_IOC_SEND` and that `btrfs receive` consumes. Roundtrips
//! through [`crate::StreamReader`] cleanly — that's the primary
//! correctness target (byte-for-byte parity with kernel send is
//! impossible because command ordering inside a transaction has
//! flexibility).
//!
//! The protocol versions:
//!
//! - **v1**: base set of commands; all encoded data is uncompressed.
//! - **v2**: adds `EncodedWrite`, `Fallocate`, `Fileattr`. Commands
//!   are still framed identically; the writer accepts any version
//!   and trusts the caller not to mix v2 commands into a v1 stream.
//! - **v3**: adds `EnableVerity`.
//!
//! [`StreamWriter::write_command`] does not enforce that a command
//! belongs to the negotiated version — that's a correctness concern
//! at a higher layer (the walker or the CLI). This keeps the encoder
//! simple and lets callers stream arbitrary command sequences for
//! testing.

use crate::{
    consts::{
        BTRFS_SEND_A_ATIME, BTRFS_SEND_A_CLONE_CTRANSID,
        BTRFS_SEND_A_CLONE_LEN, BTRFS_SEND_A_CLONE_OFFSET,
        BTRFS_SEND_A_CLONE_PATH, BTRFS_SEND_A_CLONE_UUID,
        BTRFS_SEND_A_COMPRESSION, BTRFS_SEND_A_CTIME, BTRFS_SEND_A_CTRANSID,
        BTRFS_SEND_A_DATA, BTRFS_SEND_A_ENCRYPTION,
        BTRFS_SEND_A_FALLOCATE_MODE, BTRFS_SEND_A_FILE_OFFSET,
        BTRFS_SEND_A_FILEATTR, BTRFS_SEND_A_GID, BTRFS_SEND_A_MODE,
        BTRFS_SEND_A_MTIME, BTRFS_SEND_A_PATH, BTRFS_SEND_A_PATH_LINK,
        BTRFS_SEND_A_PATH_TO, BTRFS_SEND_A_RDEV, BTRFS_SEND_A_SIZE,
        BTRFS_SEND_A_UID, BTRFS_SEND_A_UNENCODED_FILE_LEN,
        BTRFS_SEND_A_UNENCODED_LEN, BTRFS_SEND_A_UNENCODED_OFFSET,
        BTRFS_SEND_A_UUID, BTRFS_SEND_A_VERITY_ALGORITHM,
        BTRFS_SEND_A_VERITY_BLOCK_SIZE, BTRFS_SEND_A_VERITY_SALT_DATA,
        BTRFS_SEND_A_VERITY_SIG_DATA, BTRFS_SEND_A_XATTR_DATA,
        BTRFS_SEND_A_XATTR_NAME, BTRFS_SEND_C_CHMOD, BTRFS_SEND_C_CHOWN,
        BTRFS_SEND_C_CLONE, BTRFS_SEND_C_ENABLE_VERITY,
        BTRFS_SEND_C_ENCODED_WRITE, BTRFS_SEND_C_END, BTRFS_SEND_C_FALLOCATE,
        BTRFS_SEND_C_FILEATTR, BTRFS_SEND_C_LINK, BTRFS_SEND_C_MKDIR,
        BTRFS_SEND_C_MKFIFO, BTRFS_SEND_C_MKFILE, BTRFS_SEND_C_MKNOD,
        BTRFS_SEND_C_MKSOCK, BTRFS_SEND_C_REMOVE_XATTR, BTRFS_SEND_C_RENAME,
        BTRFS_SEND_C_RMDIR, BTRFS_SEND_C_SET_XATTR, BTRFS_SEND_C_SNAPSHOT,
        BTRFS_SEND_C_SUBVOL, BTRFS_SEND_C_SYMLINK, BTRFS_SEND_C_TRUNCATE,
        BTRFS_SEND_C_UNLINK, BTRFS_SEND_C_UPDATE_EXTENT, BTRFS_SEND_C_UTIMES,
        BTRFS_SEND_C_WRITE, CMD_HEADER_LEN, SEND_STREAM_MAGIC,
        SEND_STREAM_MAGIC_LEN, STREAM_HEADER_LEN,
    },
    stream::{StreamCommand, Timespec},
};
use std::io::{self, Write};
use uuid::Uuid;

/// TLV-framed encoder for the btrfs send stream. Construct with
/// [`StreamWriter::new`] (writes the stream header), then call
/// [`StreamWriter::write_command`] for each command. Drop or call
/// [`StreamWriter::finish`] to release the inner writer.
///
/// Symmetric with [`crate::StreamReader`] — round trips of every
/// [`StreamCommand`] variant are unit-tested.
#[derive(Debug)]
pub struct StreamWriter<W: Write> {
    inner: W,
    version: u32,
}

impl<W: Write> StreamWriter<W> {
    /// Wrap `inner` and write the 17-byte stream header
    /// (magic + version). `version` must be 1, 2, or 3.
    ///
    /// # Errors
    ///
    /// Returns an error on `version == 0 || version > 3`, or if the
    /// underlying writer fails.
    pub fn new(mut inner: W, version: u32) -> io::Result<Self> {
        if version == 0 || version > 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unsupported send stream version {version} (supported: 1-3)"
                ),
            ));
        }
        let mut header = [0u8; STREAM_HEADER_LEN];
        header[..SEND_STREAM_MAGIC_LEN].copy_from_slice(SEND_STREAM_MAGIC);
        header[SEND_STREAM_MAGIC_LEN..STREAM_HEADER_LEN]
            .copy_from_slice(&version.to_le_bytes());
        inner.write_all(&header)?;
        Ok(Self { inner, version })
    }

    /// Negotiated stream protocol version.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Flush and unwrap the inner writer. The caller is responsible
    /// for sending an explicit [`StreamCommand::End`] terminator
    /// beforehand if the consumer expects one (kernel-emitted
    /// streams always terminate with `End`).
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails.
    pub fn finish(mut self) -> io::Result<W> {
        self.inner.flush()?;
        Ok(self.inner)
    }

    /// Encode `cmd` as a framed TLV command and write it to the
    /// underlying writer. The frame's CRC32C (raw, init=0) is
    /// computed over the header (with the CRC field zeroed) and
    /// payload, matching the parser's verification.
    ///
    /// # Errors
    ///
    /// Returns an error if any individual attribute exceeds the
    /// 16-bit length field's range, the total payload exceeds the
    /// 32-bit length field's range, or the underlying writer fails.
    pub fn write_command(&mut self, cmd: &StreamCommand) -> io::Result<()> {
        let mut payload = Vec::new();
        let cmd_id = encode_command(cmd, &mut payload, self.version)?;
        self.write_framed(cmd_id, &payload)
    }

    fn write_framed(&mut self, cmd_id: u16, payload: &[u8]) -> io::Result<()> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "command {cmd_id} payload exceeds u32 length field: {} bytes",
                    payload.len(),
                ),
            )
        })?;

        let mut header = [0u8; CMD_HEADER_LEN];
        header[0..4].copy_from_slice(&payload_len.to_le_bytes());
        header[4..6].copy_from_slice(&cmd_id.to_le_bytes());
        // CRC field stays zero for the computation; we patch it in
        // afterwards.

        // raw_crc32c(seed=0, data) == !crc32c::crc32c_append(!0, data).
        // Computed incrementally to avoid copying the whole frame.
        let crc = crc32c::crc32c_append(!0, &header[0..6]);
        let crc = crc32c::crc32c_append(crc, &[0u8; 4]);
        let crc = !crc32c::crc32c_append(crc, payload);
        header[6..10].copy_from_slice(&crc.to_le_bytes());

        self.inner.write_all(&header)?;
        self.inner.write_all(payload)?;
        Ok(())
    }
}

// ── Per-command encoders ──────────────────────────────────────────

/// Encode `cmd`'s attribute payload into `out` and return the wire
/// command type id.
///
/// `version` controls one wire-format quirk: in v2+, the
/// `BTRFS_SEND_A_DATA` attribute has no length field — it extends
/// to the end of the command payload. The encoder mirrors that
/// special case for `Write` / `EncodedWrite` (the only commands
/// that carry DATA), and lets the larger v2 payload size be
/// addressed by the outer u32 length field.
#[allow(clippy::too_many_lines)]
fn encode_command(
    cmd: &StreamCommand,
    out: &mut Vec<u8>,
    version: u32,
) -> io::Result<u16> {
    match cmd {
        StreamCommand::Subvol {
            path,
            uuid,
            ctransid,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_uuid(out, BTRFS_SEND_A_UUID, uuid);
            put_attr_u64(out, BTRFS_SEND_A_CTRANSID, *ctransid);
            Ok(BTRFS_SEND_C_SUBVOL)
        }
        StreamCommand::Snapshot {
            path,
            uuid,
            ctransid,
            clone_uuid,
            clone_ctransid,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_uuid(out, BTRFS_SEND_A_UUID, uuid);
            put_attr_u64(out, BTRFS_SEND_A_CTRANSID, *ctransid);
            put_attr_uuid(out, BTRFS_SEND_A_CLONE_UUID, clone_uuid);
            put_attr_u64(out, BTRFS_SEND_A_CLONE_CTRANSID, *clone_ctransid);
            Ok(BTRFS_SEND_C_SNAPSHOT)
        }
        StreamCommand::Mkfile { path } => {
            path_only(out, path, BTRFS_SEND_C_MKFILE)
        }
        StreamCommand::Mkdir { path } => {
            path_only(out, path, BTRFS_SEND_C_MKDIR)
        }
        StreamCommand::Mknod { path, mode, rdev } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_MODE, *mode);
            put_attr_u64(out, BTRFS_SEND_A_RDEV, *rdev);
            Ok(BTRFS_SEND_C_MKNOD)
        }
        StreamCommand::Mkfifo { path } => {
            path_only(out, path, BTRFS_SEND_C_MKFIFO)
        }
        StreamCommand::Mksock { path } => {
            path_only(out, path, BTRFS_SEND_C_MKSOCK)
        }
        StreamCommand::Symlink { path, target } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_str(out, BTRFS_SEND_A_PATH_LINK, target)?;
            Ok(BTRFS_SEND_C_SYMLINK)
        }
        StreamCommand::Rename { from, to } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, from)?;
            put_attr_str(out, BTRFS_SEND_A_PATH_TO, to)?;
            Ok(BTRFS_SEND_C_RENAME)
        }
        StreamCommand::Link { path, target } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_str(out, BTRFS_SEND_A_PATH_LINK, target)?;
            Ok(BTRFS_SEND_C_LINK)
        }
        StreamCommand::Unlink { path } => {
            path_only(out, path, BTRFS_SEND_C_UNLINK)
        }
        StreamCommand::Rmdir { path } => {
            path_only(out, path, BTRFS_SEND_C_RMDIR)
        }
        StreamCommand::Write { path, offset, data } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_FILE_OFFSET, *offset);
            // DATA must be the last attribute in v2+ since it has
            // no length field there; emitting it last is also fine
            // for v1.
            put_attr_data(out, data, version)?;
            Ok(BTRFS_SEND_C_WRITE)
        }
        StreamCommand::Clone {
            path,
            offset,
            len,
            clone_uuid,
            clone_ctransid,
            clone_path,
            clone_offset,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_FILE_OFFSET, *offset);
            put_attr_u64(out, BTRFS_SEND_A_CLONE_LEN, *len);
            put_attr_uuid(out, BTRFS_SEND_A_CLONE_UUID, clone_uuid);
            put_attr_u64(out, BTRFS_SEND_A_CLONE_CTRANSID, *clone_ctransid);
            put_attr_str(out, BTRFS_SEND_A_CLONE_PATH, clone_path)?;
            put_attr_u64(out, BTRFS_SEND_A_CLONE_OFFSET, *clone_offset);
            Ok(BTRFS_SEND_C_CLONE)
        }
        StreamCommand::SetXattr { path, name, data } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_str(out, BTRFS_SEND_A_XATTR_NAME, name)?;
            put_attr_bytes(out, BTRFS_SEND_A_XATTR_DATA, data)?;
            Ok(BTRFS_SEND_C_SET_XATTR)
        }
        StreamCommand::RemoveXattr { path, name } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_str(out, BTRFS_SEND_A_XATTR_NAME, name)?;
            Ok(BTRFS_SEND_C_REMOVE_XATTR)
        }
        StreamCommand::Truncate { path, size } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_SIZE, *size);
            Ok(BTRFS_SEND_C_TRUNCATE)
        }
        StreamCommand::Chmod { path, mode } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_MODE, *mode);
            Ok(BTRFS_SEND_C_CHMOD)
        }
        StreamCommand::Chown { path, uid, gid } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_UID, *uid);
            put_attr_u64(out, BTRFS_SEND_A_GID, *gid);
            Ok(BTRFS_SEND_C_CHOWN)
        }
        StreamCommand::Utimes {
            path,
            atime,
            mtime,
            ctime,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_timespec(out, BTRFS_SEND_A_ATIME, atime);
            put_attr_timespec(out, BTRFS_SEND_A_MTIME, mtime);
            put_attr_timespec(out, BTRFS_SEND_A_CTIME, ctime);
            Ok(BTRFS_SEND_C_UTIMES)
        }
        StreamCommand::UpdateExtent { path, offset, len } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_FILE_OFFSET, *offset);
            put_attr_u64(out, BTRFS_SEND_A_SIZE, *len);
            Ok(BTRFS_SEND_C_UPDATE_EXTENT)
        }
        StreamCommand::EncodedWrite {
            path,
            offset,
            unencoded_file_len,
            unencoded_len,
            unencoded_offset,
            compression,
            encryption,
            data,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_FILE_OFFSET, *offset);
            put_attr_u64(
                out,
                BTRFS_SEND_A_UNENCODED_FILE_LEN,
                *unencoded_file_len,
            );
            put_attr_u64(out, BTRFS_SEND_A_UNENCODED_LEN, *unencoded_len);
            put_attr_u64(out, BTRFS_SEND_A_UNENCODED_OFFSET, *unencoded_offset);
            put_attr_u32(out, BTRFS_SEND_A_COMPRESSION, *compression);
            put_attr_u32(out, BTRFS_SEND_A_ENCRYPTION, *encryption);
            put_attr_data(out, data, version)?;
            Ok(BTRFS_SEND_C_ENCODED_WRITE)
        }
        StreamCommand::Fallocate {
            path,
            mode,
            offset,
            len,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u32(out, BTRFS_SEND_A_FALLOCATE_MODE, *mode);
            put_attr_u64(out, BTRFS_SEND_A_FILE_OFFSET, *offset);
            put_attr_u64(out, BTRFS_SEND_A_SIZE, *len);
            Ok(BTRFS_SEND_C_FALLOCATE)
        }
        StreamCommand::Fileattr { path, attr } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u64(out, BTRFS_SEND_A_FILEATTR, *attr);
            Ok(BTRFS_SEND_C_FILEATTR)
        }
        StreamCommand::EnableVerity {
            path,
            algorithm,
            block_size,
            salt,
            sig,
        } => {
            put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
            put_attr_u8(out, BTRFS_SEND_A_VERITY_ALGORITHM, *algorithm);
            put_attr_u32(out, BTRFS_SEND_A_VERITY_BLOCK_SIZE, *block_size);
            put_attr_bytes(out, BTRFS_SEND_A_VERITY_SALT_DATA, salt)?;
            put_attr_bytes(out, BTRFS_SEND_A_VERITY_SIG_DATA, sig)?;
            Ok(BTRFS_SEND_C_ENABLE_VERITY)
        }
        StreamCommand::End => Ok(BTRFS_SEND_C_END),
    }
}

fn path_only(out: &mut Vec<u8>, path: &str, cmd_id: u16) -> io::Result<u16> {
    put_attr_str(out, BTRFS_SEND_A_PATH, path)?;
    Ok(cmd_id)
}

// ── TLV writers ───────────────────────────────────────────────────

/// Write a TLV header (`type: u16 | len: u16`) followed by the
/// payload bytes. Errors when `data.len() > u16::MAX` since the
/// TLV length field can't represent it.
fn put_attr_bytes(out: &mut Vec<u8>, attr: u16, data: &[u8]) -> io::Result<()> {
    let len = u16::try_from(data.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "attribute {attr} payload exceeds u16 length field: {} bytes",
                data.len(),
            ),
        )
    })?;
    out.extend_from_slice(&attr.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(data);
    Ok(())
}

fn put_attr_str(out: &mut Vec<u8>, attr: u16, s: &str) -> io::Result<()> {
    put_attr_bytes(out, attr, s.as_bytes())
}

/// Encode `BTRFS_SEND_A_DATA` for `Write` / `EncodedWrite`. In v1
/// streams this is a regular TLV attribute; in v2+ the length
/// field is omitted and the data extends to the end of the
/// command payload, which lets writes exceed the 64 KiB v1 cap.
/// MUST be called last for the command — anything emitted after it
/// would be parsed as part of the data on v2+.
fn put_attr_data(
    out: &mut Vec<u8>,
    data: &[u8],
    version: u32,
) -> io::Result<()> {
    if version >= 2 {
        out.extend_from_slice(&BTRFS_SEND_A_DATA.to_le_bytes());
        out.extend_from_slice(data);
        Ok(())
    } else {
        put_attr_bytes(out, BTRFS_SEND_A_DATA, data)
    }
}

fn put_attr_u64(out: &mut Vec<u8>, attr: u16, v: u64) {
    put_attr_bytes(out, attr, &v.to_le_bytes())
        .expect("8-byte payload always fits");
}

fn put_attr_u32(out: &mut Vec<u8>, attr: u16, v: u32) {
    put_attr_bytes(out, attr, &v.to_le_bytes())
        .expect("4-byte payload always fits");
}

fn put_attr_u8(out: &mut Vec<u8>, attr: u16, v: u8) {
    put_attr_bytes(out, attr, &[v]).expect("1-byte payload always fits");
}

fn put_attr_uuid(out: &mut Vec<u8>, attr: u16, uuid: &Uuid) {
    put_attr_bytes(out, attr, uuid.as_bytes())
        .expect("16-byte UUID always fits");
}

fn put_attr_timespec(out: &mut Vec<u8>, attr: u16, t: &Timespec) {
    let mut buf = [0u8; 12];
    buf[0..8].copy_from_slice(&t.sec.to_le_bytes());
    buf[8..12].copy_from_slice(&t.nsec.to_le_bytes());
    put_attr_bytes(out, attr, &buf).expect("12-byte timespec always fits");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamReader;

    /// Exercise every [`StreamCommand`] variant by writing it to a
    /// buffer, parsing back, and asserting the parsed command
    /// matches the original. The roundtrip proves both that the
    /// encoder and parser agree on the wire format and that the
    /// CRC32C produced by the encoder validates.
    fn roundtrip(version: u32, cmds: &[StreamCommand]) {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = StreamWriter::new(&mut buf, version).unwrap();
        for cmd in cmds {
            writer.write_command(cmd).unwrap();
        }
        writer.finish().unwrap();

        let mut reader = StreamReader::new(buf.as_slice()).unwrap();
        assert_eq!(reader.version(), version);
        for expected in cmds {
            let got = reader.next_command().expect("parse").expect("not eof");
            assert_eq!(format!("{got:?}"), format!("{expected:?}"));
        }
        assert!(reader.next_command().expect("eof check").is_none());
    }

    #[test]
    fn header_only_roundtrips_each_version() {
        for v in [1, 2, 3] {
            let mut buf = Vec::new();
            let writer = StreamWriter::new(&mut buf, v).unwrap();
            writer.finish().unwrap();
            let reader = StreamReader::new(buf.as_slice()).unwrap();
            assert_eq!(reader.version(), v);
        }
    }

    #[test]
    fn invalid_versions_rejected() {
        let mut buf = Vec::new();
        assert!(StreamWriter::new(&mut buf, 0).is_err());
        let mut buf = Vec::new();
        assert!(StreamWriter::new(&mut buf, 4).is_err());
    }

    #[test]
    fn subvol_and_snapshot_roundtrip() {
        let uuid = Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);
        let clone_uuid =
            Uuid::from_u128(0xdead_beef_dead_beef_dead_beef_dead_beef);
        roundtrip(
            1,
            &[
                StreamCommand::Subvol {
                    path: "snap1".into(),
                    uuid,
                    ctransid: 42,
                },
                StreamCommand::Snapshot {
                    path: "snap2".into(),
                    uuid,
                    ctransid: 100,
                    clone_uuid,
                    clone_ctransid: 50,
                },
                StreamCommand::End,
            ],
        );
    }

    #[test]
    fn filesystem_object_creation_roundtrips() {
        roundtrip(
            1,
            &[
                StreamCommand::Mkfile {
                    path: "f.txt".into(),
                },
                StreamCommand::Mkdir { path: "d".into() },
                StreamCommand::Mknod {
                    path: "n".into(),
                    mode: 0o600,
                    rdev: 0x102,
                },
                StreamCommand::Mkfifo { path: "p".into() },
                StreamCommand::Mksock { path: "s".into() },
                StreamCommand::Symlink {
                    path: "l".into(),
                    target: "f.txt".into(),
                },
            ],
        );
    }

    #[test]
    fn rename_link_unlink_rmdir_roundtrip() {
        roundtrip(
            1,
            &[
                StreamCommand::Rename {
                    from: "a".into(),
                    to: "b".into(),
                },
                StreamCommand::Link {
                    path: "alias".into(),
                    target: "real".into(),
                },
                StreamCommand::Unlink { path: "old".into() },
                StreamCommand::Rmdir {
                    path: "empty".into(),
                },
            ],
        );
    }

    #[test]
    fn write_clone_truncate_roundtrip() {
        let clone_uuid =
            Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        roundtrip(
            1,
            &[
                StreamCommand::Write {
                    path: "f".into(),
                    offset: 4096,
                    data: vec![0x42; 1024],
                },
                StreamCommand::Clone {
                    path: "dst".into(),
                    offset: 8192,
                    len: 4096,
                    clone_uuid,
                    clone_ctransid: 7,
                    clone_path: "src".into(),
                    clone_offset: 0,
                },
                StreamCommand::Truncate {
                    path: "f".into(),
                    size: 65536,
                },
            ],
        );
    }

    #[test]
    fn xattr_perms_times_roundtrip() {
        roundtrip(
            1,
            &[
                StreamCommand::SetXattr {
                    path: "f".into(),
                    name: "user.greeting".into(),
                    data: b"hello".to_vec(),
                },
                StreamCommand::RemoveXattr {
                    path: "f".into(),
                    name: "user.gone".into(),
                },
                StreamCommand::Chmod {
                    path: "f".into(),
                    mode: 0o644,
                },
                StreamCommand::Chown {
                    path: "f".into(),
                    uid: 1000,
                    gid: 1001,
                },
                StreamCommand::Utimes {
                    path: "f".into(),
                    atime: Timespec { sec: 100, nsec: 1 },
                    mtime: Timespec { sec: 200, nsec: 2 },
                    ctime: Timespec { sec: 300, nsec: 3 },
                },
                StreamCommand::UpdateExtent {
                    path: "f".into(),
                    offset: 0,
                    len: 4096,
                },
            ],
        );
    }

    #[test]
    fn v2_commands_roundtrip() {
        roundtrip(
            2,
            &[
                StreamCommand::EncodedWrite {
                    path: "f".into(),
                    offset: 0,
                    unencoded_file_len: 4096,
                    unencoded_len: 4096,
                    unencoded_offset: 0,
                    compression: 2, // zstd
                    encryption: 0,
                    data: vec![0xab; 256],
                },
                StreamCommand::Fallocate {
                    path: "f".into(),
                    mode: 3, // PUNCH_HOLE | KEEP_SIZE
                    offset: 4096,
                    len: 4096,
                },
                StreamCommand::Fileattr {
                    path: "f".into(),
                    attr: 0x40, // FS_NOCOW_FL
                },
            ],
        );
    }

    #[test]
    fn v3_enable_verity_roundtrips() {
        roundtrip(
            3,
            &[StreamCommand::EnableVerity {
                path: "f".into(),
                algorithm: 1,
                block_size: 4096,
                salt: vec![1, 2, 3, 4],
                sig: vec![],
            }],
        );
    }

    #[test]
    fn end_command_roundtrips() {
        roundtrip(1, &[StreamCommand::End]);
    }

    #[test]
    fn corrupted_payload_fails_crc_check() {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::new(&mut buf, 1).unwrap();
        writer
            .write_command(&StreamCommand::Mkfile { path: "f".into() })
            .unwrap();
        writer.finish().unwrap();

        // Flip a bit in the first command's payload (after the
        // 17-byte stream header + 10-byte command header).
        let payload_start = STREAM_HEADER_LEN + CMD_HEADER_LEN;
        buf[payload_start + 4] ^= 0x01;

        let mut reader = StreamReader::new(buf.as_slice()).unwrap();
        let err = reader.next_command().unwrap_err();
        assert!(
            matches!(err, crate::StreamError::CrcMismatch { .. }),
            "expected CrcMismatch, got {err:?}",
        );
    }
}

use anyhow::Result;
use btrfs_stream::{StreamCommand, StreamReader, Timespec};
use std::io::Read;

fn fmt_timespec(ts: &Timespec) -> String {
    format!("{}.{}", ts.sec, ts.nsec)
}

fn fmt_uuid(uuid: &uuid::Uuid) -> String {
    uuid.as_hyphenated().to_string()
}

pub fn dump_stream<R: Read>(input: R) -> Result<()> {
    let mut reader = StreamReader::new(input)?;

    loop {
        match reader.next_command()? {
            None => break,
            Some(cmd) => match &cmd {
                StreamCommand::Subvol {
                    path,
                    uuid,
                    ctransid,
                } => {
                    println!(
                        "subvol          ./{path}    uuid={} transid={ctransid}",
                        fmt_uuid(uuid)
                    );
                }
                StreamCommand::Snapshot {
                    path,
                    uuid,
                    ctransid,
                    clone_uuid,
                    clone_ctransid,
                } => {
                    println!(
                        "snapshot        ./{path}    uuid={} transid={ctransid} parent_uuid={} parent_transid={clone_ctransid}",
                        fmt_uuid(uuid),
                        fmt_uuid(clone_uuid)
                    );
                }
                StreamCommand::Mkfile { path } => {
                    println!("mkfile          ./{path}");
                }
                StreamCommand::Mkdir { path } => {
                    println!("mkdir           ./{path}");
                }
                StreamCommand::Mknod { path, mode, rdev } => {
                    println!("mknod           ./{path}    mode={mode} rdev={rdev:#x}");
                }
                StreamCommand::Mkfifo { path } => {
                    println!("mkfifo          ./{path}");
                }
                StreamCommand::Mksock { path } => {
                    println!("mksock          ./{path}");
                }
                StreamCommand::Symlink { path, target } => {
                    println!("symlink         ./{path}    dest=./{target}");
                }
                StreamCommand::Rename { from, to } => {
                    println!("rename          ./{from}    dest=./{to}");
                }
                StreamCommand::Link { path, target } => {
                    println!("link            ./{path}    dest=./{target}");
                }
                StreamCommand::Unlink { path } => {
                    println!("unlink          ./{path}");
                }
                StreamCommand::Rmdir { path } => {
                    println!("rmdir           ./{path}");
                }
                StreamCommand::Write { path, offset, data } => {
                    println!(
                        "write           ./{path}    offset={offset} len={}",
                        data.len()
                    );
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
                    println!(
                        "clone           ./{path}    offset={offset} len={len} from={} offset={clone_offset} transid={clone_ctransid} uuid={}",
                        clone_path,
                        fmt_uuid(clone_uuid)
                    );
                }
                StreamCommand::SetXattr { path, name, data } => {
                    println!("set_xattr       ./{path}    name={name} len={}", data.len());
                }
                StreamCommand::RemoveXattr { path, name } => {
                    println!("remove_xattr    ./{path}    name={name}");
                }
                StreamCommand::Truncate { path, size } => {
                    println!("truncate        ./{path}    size={size}");
                }
                StreamCommand::Chmod { path, mode } => {
                    println!("chmod           ./{path}    mode={mode:o}");
                }
                StreamCommand::Chown { path, uid, gid } => {
                    println!("chown           ./{path}    uid={uid} gid={gid}");
                }
                StreamCommand::Utimes {
                    path,
                    atime,
                    mtime,
                    ctime,
                } => {
                    println!(
                        "utimes          ./{path}    atime={} mtime={} ctime={}",
                        fmt_timespec(atime),
                        fmt_timespec(mtime),
                        fmt_timespec(ctime)
                    );
                }
                StreamCommand::UpdateExtent { path, offset, len } => {
                    println!("update_extent   ./{path}    offset={offset} len={len}");
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
                    println!(
                        "encoded_write   ./{path}    offset={offset} len={} unencoded_file_len={unencoded_file_len} unencoded_len={unencoded_len} unencoded_offset={unencoded_offset} compression={compression} encryption={encryption}",
                        data.len()
                    );
                }
                StreamCommand::Fallocate {
                    path,
                    mode,
                    offset,
                    len,
                } => {
                    println!("fallocate       ./{path}    mode={mode} offset={offset} len={len}");
                }
                StreamCommand::Fileattr { path, attr } => {
                    println!("fileattr        ./{path}    fileattr=0x{attr:x}");
                }
                StreamCommand::EnableVerity {
                    path,
                    algorithm,
                    block_size,
                    salt,
                    sig,
                } => {
                    println!(
                        "enable_verity   ./{path}    algorithm={algorithm} block_size={block_size} salt_len={} sig_len={}",
                        salt.len(),
                        sig.len()
                    );
                }
                StreamCommand::End => {
                    println!("end");
                }
            },
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn fmt_timespec_zero() {
        let ts = Timespec { sec: 0, nsec: 0 };
        assert_eq!(fmt_timespec(&ts), "0.0");
    }

    #[test]
    fn fmt_timespec_nonzero() {
        let ts = Timespec {
            sec: 1234567890,
            nsec: 123456789,
        };
        assert_eq!(fmt_timespec(&ts), "1234567890.123456789");
    }

    #[test]
    fn fmt_uuid_nil() {
        let uuid = Uuid::nil();
        assert_eq!(fmt_uuid(&uuid), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn fmt_uuid_specific() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(fmt_uuid(&uuid), "550e8400-e29b-41d4-a716-446655440000");
    }
}

use crate::stream::{StreamCommand, StreamReader, Timespec};
use anyhow::Result;
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
            Some(cmd) => {
                match &cmd {
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
                        println!(
                            "set_xattr       ./{path}    name={name} len={}",
                            data.len()
                        );
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
                    StreamCommand::End => {
                        println!("end");
                    }
                }
            }
        }
    }

    Ok(())
}

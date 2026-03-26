use crate::{Format, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_uapi::{
    send_receive::SendFlags,
    subvolume::{SubvolumeFlags, subvolume_flags_get, subvolume_info},
    sysfs::SysfsBtrfs,
};
use clap::Parser;
use std::{
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsFd, AsRawFd, OwnedFd},
        unix::io::FromRawFd,
    },
    path::PathBuf,
    thread,
};

/// Send the subvolume(s) to stdout.
///
/// Generate a stream representation of one or more subvolumes that can be
/// transmitted over the network or stored for later restoration. Streams
/// are incremental and can be based on a parent subvolume to only send
/// changes. The stream output is in btrfs send format and can be received
/// with the receive command. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
pub struct SendCommand {
    /// Subvolume(s) to send
    #[clap(required = true)]
    subvolumes: Vec<PathBuf>,

    /// Omit end-cmd marker between subvolumes
    #[clap(short = 'e')]
    omit_end_cmd: bool,

    /// Send an incremental stream from parent to the subvolume
    #[clap(short = 'p', long)]
    parent: Option<PathBuf>,

    /// Use this snapshot as a clone source (may be given multiple times)
    #[clap(short = 'c', long = "clone-src")]
    clone_src: Vec<PathBuf>,

    /// Write output to a file instead of stdout
    #[clap(short = 'f', long)]
    outfile: Option<PathBuf>,

    /// Send in NO_FILE_DATA mode
    #[clap(long)]
    no_data: bool,

    /// Use send protocol version N (0 = highest supported by kernel)
    #[clap(long)]
    proto: Option<u32>,

    /// Send compressed data directly without decompressing
    #[clap(long)]
    compressed_data: bool,
}

/// Buffer size for protocol v1 (matches BTRFS_SEND_BUF_SIZE_V1 = 64 KiB).
const SEND_BUF_SIZE_V1: usize = 64 * 1024;
/// Buffer size for protocol v2+ (16 KiB + 128 KiB compressed = 144 KiB).
const SEND_BUF_SIZE_V2: usize = 16 * 1024 + 128 * 1024;

fn open_subvol_ro(path: &PathBuf) -> Result<File> {
    File::open(path).with_context(|| format!("cannot open '{}'", path.display()))
}

fn check_subvol_readonly(file: &File, path: &PathBuf) -> Result<()> {
    let flags = subvolume_flags_get(file.as_fd())
        .with_context(|| format!("failed to get flags for '{}'", path.display()))?;
    if !flags.contains(SubvolumeFlags::RDONLY) {
        bail!("subvolume '{}' is not read-only", path.display());
    }
    Ok(())
}

fn get_root_id(file: &File, path: &PathBuf) -> Result<u64> {
    let info = subvolume_info(file.as_fd())
        .with_context(|| format!("failed to get subvolume info for '{}'", path.display()))?;
    Ok(info.id)
}

/// Find the best parent among clone sources for incremental send.
///
/// Looks for a clone source that shares the same parent UUID as the target
/// subvolume and picks the one with the closest ctransid.
fn find_good_parent(
    subvol_info: &btrfs_uapi::subvolume::SubvolumeInfo,
    clone_source_paths: &[PathBuf],
) -> Result<Option<u64>> {
    if subvol_info.parent_uuid.is_nil() {
        return Ok(None);
    }

    let mut best_root_id = None;
    let mut best_diff = u64::MAX;

    for cs_path in clone_source_paths {
        let cs_file = open_subvol_ro(cs_path)?;
        let cs_info = subvolume_info(cs_file.as_fd()).with_context(|| {
            format!(
                "failed to get info for clone source '{}'",
                cs_path.display()
            )
        })?;

        // Check if this clone source shares the same parent or IS the parent.
        if cs_info.parent_uuid != subvol_info.parent_uuid && cs_info.uuid != subvol_info.parent_uuid
        {
            continue;
        }

        let diff = subvol_info.ctransid.abs_diff(cs_info.ctransid);
        if diff < best_diff {
            best_diff = diff;
            best_root_id = Some(cs_info.id);
        }
    }

    Ok(best_root_id)
}

/// Create a pipe and return (read_end, write_end) as OwnedFds.
fn make_pipe() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0i32; 2];
    let ret = unsafe { nix::libc::pipe(fds.as_mut_ptr()) };
    if ret < 0 {
        return Err(io::Error::last_os_error()).context("failed to create pipe");
    }
    // SAFETY: pipe() just returned two valid fds.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let write_end = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    Ok((read_end, write_end))
}

/// Spawn a thread that reads from `read_fd` and writes everything to `out`.
fn spawn_reader_thread(
    read_fd: OwnedFd,
    mut out: Box<dyn Write + Send>,
    buf_size: usize,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let mut file = File::from(read_fd);
        let mut buf = vec![0u8; buf_size];
        loop {
            let n = file
                .read(&mut buf)
                .context("failed to read send stream from kernel")?;
            if n == 0 {
                return Ok(());
            }
            out.write_all(&buf[..n])
                .context("failed to write send stream to output")?;
        }
    })
}

/// Open or create the output writer for the reader thread.
fn open_output(outfile: &Option<PathBuf>) -> Result<Box<dyn Write + Send>> {
    match outfile {
        Some(path) => {
            let file = File::options()
                .write(true)
                .append(true)
                .open(path)
                .with_context(|| format!("cannot open '{}' for writing", path.display()))?;
            Ok(Box::new(file))
        }
        None => Ok(Box::new(io::stdout())),
    }
}

impl Runnable for SendCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        // Validate output destination.
        if let Some(path) = &self.outfile {
            // Try opening existing file first, then create. Truncate since
            // this is the start of a new send.
            File::options()
                .write(true)
                .truncate(true)
                .open(path)
                .or_else(|_| {
                    File::options()
                        .write(true)
                        .truncate(true)
                        .create(true)
                        .open(path)
                })
                .with_context(|| format!("cannot create '{}'", path.display()))?;
        } else {
            let stdout = io::stdout();
            if unsafe { nix::libc::isatty(stdout.as_fd().as_raw_fd()) } == 1 {
                bail!("not dumping send stream into a terminal, redirect it into a file");
            }
        }

        // Validate all subvolumes are read-only.
        for subvol_path in &self.subvolumes {
            let file = open_subvol_ro(subvol_path)?;
            check_subvol_readonly(&file, subvol_path)?;
        }

        // Validate parent is read-only and get its root ID.
        let mut parent_root_id: u64 = 0;
        if let Some(parent_path) = &self.parent {
            let file = open_subvol_ro(parent_path)?;
            check_subvol_readonly(&file, parent_path)?;
            parent_root_id = get_root_id(&file, parent_path)?;
        }

        // Collect clone source root IDs and validate they are read-only.
        let mut clone_sources: Vec<u64> = Vec::new();
        for cs_path in &self.clone_src {
            let file = open_subvol_ro(cs_path)?;
            check_subvol_readonly(&file, cs_path)?;
            clone_sources.push(get_root_id(&file, cs_path)?);
        }

        // If a parent was given, add it to clone sources (matches C behavior).
        if self.parent.is_some() && !clone_sources.contains(&parent_root_id) {
            clone_sources.push(parent_root_id);
        }

        let full_send = self.parent.is_none() && self.clone_src.is_empty();

        // Determine protocol version.
        let first_file = open_subvol_ro(&self.subvolumes[0])?;
        let fs = btrfs_uapi::filesystem::fs_info(first_file.as_fd())
            .context("failed to get filesystem info")?;
        let sysfs = SysfsBtrfs::new(&fs.uuid);
        let proto_supported = sysfs.send_stream_version();

        let mut proto = self.proto.unwrap_or(1);
        if proto == 0 {
            proto = proto_supported;
        }

        if proto > proto_supported && proto_supported == 1 {
            bail!(
                "requested protocol version {} but kernel supports only {}",
                proto,
                proto_supported
            );
        }

        // Build send flags.
        let mut flags = SendFlags::empty();
        if self.no_data {
            flags |= SendFlags::NO_FILE_DATA;
        }
        if self.compressed_data {
            if proto == 1 && self.proto.is_none() {
                proto = 2;
            }
            if proto < 2 {
                bail!("--compressed-data requires protocol version >= 2 (requested {proto})");
            }
            if proto_supported < 2 {
                bail!("kernel does not support --compressed-data");
            }
            flags |= SendFlags::COMPRESSED;
        }
        if proto_supported > 1 {
            flags |= SendFlags::VERSION;
        }

        let buf_size = if proto > 1 {
            SEND_BUF_SIZE_V2
        } else {
            SEND_BUF_SIZE_V1
        };

        // Send each subvolume.
        let count = self.subvolumes.len();
        for (i, subvol_path) in self.subvolumes.iter().enumerate() {
            let is_first = i == 0;
            let is_last = i == count - 1;

            eprintln!("At subvol {}", subvol_path.display());

            let subvol_file = open_subvol_ro(subvol_path)?;

            // For incremental send without an explicit parent, find the best
            // parent among clone sources.
            let mut this_parent = parent_root_id;
            if !full_send && self.parent.is_none() {
                let info = subvolume_info(subvol_file.as_fd()).with_context(|| {
                    format!("failed to get info for '{}'", subvol_path.display())
                })?;
                match find_good_parent(&info, &self.clone_src)? {
                    Some(id) => this_parent = id,
                    None => bail!(
                        "cannot find a suitable parent for '{}' among clone sources",
                        subvol_path.display()
                    ),
                }
            }

            // Build per-subvolume flags.
            let mut subvol_flags = flags;
            if self.omit_end_cmd {
                if !is_first {
                    subvol_flags |= SendFlags::OMIT_STREAM_HEADER;
                }
                if !is_last {
                    subvol_flags |= SendFlags::OMIT_END_CMD;
                }
            }

            // Create pipe and spawn reader thread.
            let (pipe_read, pipe_write) = make_pipe()?;
            let out = open_output(&self.outfile)?;
            let reader = spawn_reader_thread(pipe_read, out, buf_size);

            let send_result = btrfs_uapi::send_receive::send(
                subvol_file.as_fd(),
                pipe_write.as_raw_fd(),
                this_parent,
                &mut clone_sources,
                subvol_flags,
                proto,
            );

            // Close write end so the reader thread sees EOF.
            drop(pipe_write);

            if let Err(e) = send_result {
                let _ = reader.join();
                if e == nix::errno::Errno::EINVAL && self.omit_end_cmd {
                    bail!(
                        "send ioctl failed: {e}\n\
                         Try upgrading your kernel or don't use -e."
                    );
                }
                return Err(e)
                    .with_context(|| format!("send failed for '{}'", subvol_path.display()));
            }

            match reader.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e).context("send stream reader failed"),
                Err(_) => bail!("send stream reader thread panicked"),
            }

            // After sending, add to clone sources for subsequent subvolumes.
            if !full_send && self.parent.is_none() {
                let root_id = get_root_id(&subvol_file, subvol_path)?;
                if !clone_sources.contains(&root_id) {
                    clone_sources.push(root_id);
                }
            }
        }

        Ok(())
    }
}

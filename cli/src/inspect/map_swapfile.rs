use crate::{RunContext, Runnable};
use anyhow::{Context, Result, bail};
use btrfs_disk::items::{ChunkItem, FileExtentBody, FileExtentType};
use btrfs_uapi::{
    raw::{
        BTRFS_BLOCK_GROUP_PROFILE_MASK, BTRFS_CHUNK_ITEM_KEY,
        BTRFS_CHUNK_TREE_OBJECTID, BTRFS_EXTENT_DATA_KEY,
        BTRFS_FIRST_CHUNK_TREE_OBJECTID,
    },
    tree_search::{Key, SearchFilter, tree_search},
};
use clap::Parser;
use std::{
    fs::File,
    os::unix::io::{AsFd, AsRawFd},
    path::PathBuf,
};

/// Print physical offset of first block and resume offset if file is
/// suitable as swapfile.
///
/// All conditions of swapfile extents are verified if they could pass
/// kernel tests. Use the value of resume offset for
/// /sys/power/resume_offset, this depends on the page size that is
/// detected on this system.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown)]
pub struct MapSwapfileCommand {
    /// Print only the value of resume_offset
    #[arg(short = 'r', long)]
    resume_offset: bool,

    /// Path to a file on the btrfs filesystem
    path: PathBuf,
}

impl Runnable for MapSwapfileCommand {
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        let file = File::open(&self.path).with_context(|| {
            format!("cannot open '{}'", self.path.display())
        })?;

        validate_file(&file, &self.path)?;

        let fd = file.as_fd();
        let chunks = read_chunk_tree(fd)?;

        let tree_id = btrfs_uapi::inode::lookup_path_rootid(fd)
            .context("cannot lookup parent subvolume")?;

        let stat = nix::sys::stat::fstat(&file).context("cannot fstat file")?;

        let physical_start =
            map_physical_start(fd, tree_id, stat.st_ino, &chunks)?;

        #[allow(clippy::cast_sign_loss)] // sysconf returns positive page size
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
        if self.resume_offset {
            println!("{}", physical_start / page_size);
        } else {
            println!("Physical start: {physical_start:12}");
            println!("Resume offset:  {:12}", physical_start / page_size);
        }

        Ok(())
    }
}

const FS_NOCOW_FL: libc::c_long = 0x0080_0000;
const FS_COMPR_FL: libc::c_long = 0x0000_0004;

/// Validate that the file is on btrfs, is a regular file, is NOCOW,
/// and is not compressed.
fn validate_file(file: &File, path: &std::path::Path) -> Result<()> {
    let stfs = nix::sys::statfs::fstatfs(file)
        .with_context(|| format!("cannot statfs '{}'", path.display()))?;
    if stfs.filesystem_type() != nix::sys::statfs::BTRFS_SUPER_MAGIC {
        bail!("not a file on btrfs");
    }

    let stat = nix::sys::stat::fstat(file)
        .with_context(|| format!("cannot fstat '{}'", path.display()))?;
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        bail!("not a regular file");
    }

    let mut flags: libc::c_long = 0;
    let ret = unsafe {
        libc::ioctl(file.as_raw_fd(), libc::FS_IOC_GETFLAGS, &mut flags)
    };
    if ret == -1 {
        bail!(
            "cannot verify file flags: {}",
            std::io::Error::last_os_error()
        );
    }
    if flags & FS_NOCOW_FL == 0 {
        bail!("file is not NOCOW");
    }
    if flags & FS_COMPR_FL != 0 {
        bail!("file has COMPR attribute");
    }

    Ok(())
}

/// A parsed chunk from the chunk tree with stripe info.
struct Chunk {
    offset: u64,
    length: u64,
    stripe_len: u64,
    type_flags: u64,
    num_stripes: usize,
    stripes: Vec<(u64, u64)>,
}

/// Read all chunks from the chunk tree via tree search.
fn read_chunk_tree(fd: std::os::unix::io::BorrowedFd) -> Result<Vec<Chunk>> {
    let mut chunks = Vec::new();

    tree_search(
        fd,
        SearchFilter::for_objectid_range(
            u64::from(BTRFS_CHUNK_TREE_OBJECTID),
            BTRFS_CHUNK_ITEM_KEY,
            u64::from(BTRFS_FIRST_CHUNK_TREE_OBJECTID),
            u64::from(BTRFS_FIRST_CHUNK_TREE_OBJECTID),
        ),
        |hdr, data| {
            let Some(ci) = ChunkItem::parse(data) else {
                return Ok(());
            };
            chunks.push(Chunk {
                offset: hdr.offset,
                length: ci.length,
                stripe_len: ci.stripe_len,
                type_flags: ci.chunk_type.bits(),
                num_stripes: ci.num_stripes as usize,
                stripes: ci
                    .stripes
                    .iter()
                    .map(|s| (s.devid, s.offset))
                    .collect(),
            });
            Ok(())
        },
    )
    .context("failed to read chunk tree")?;

    Ok(chunks)
}

/// Find the chunk containing `logical` via binary search.
fn find_chunk(chunks: &[Chunk], logical: u64) -> Option<&Chunk> {
    chunks
        .binary_search_by(|c| {
            if logical < c.offset {
                std::cmp::Ordering::Greater
            } else if logical >= c.offset + c.length {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()
        .map(|i| &chunks[i])
}

/// A file extent parsed from the extent data tree search.
struct FileExtent {
    logical_offset: u64,
    num_stripes: usize,
    stripe_len: u64,
    stripe_devid: u64,
    stripe_physical: u64,
    chunk_offset: u64,
}

/// Walk the extent data for a file and compute the physical start offset.
fn map_physical_start(
    fd: std::os::unix::io::BorrowedFd,
    tree_id: u64,
    ino: u64,
    chunks: &[Chunk],
) -> Result<u64> {
    // Collect extents first, then validate (tree_search callback is nix::Result).
    let mut extents: Vec<FileExtent> = Vec::new();
    let mut error: Option<String> = None;

    tree_search(
        fd,
        SearchFilter {
            tree_id,
            start: Key {
                objectid: ino,
                item_type: BTRFS_EXTENT_DATA_KEY,
                offset: 0,
            },
            end: Key {
                objectid: ino,
                item_type: BTRFS_EXTENT_DATA_KEY,
                offset: u64::MAX,
            },
            min_transid: 0,
            max_transid: u64::MAX,
        },
        |_hdr, data| {
            if error.is_some() {
                return Ok(());
            }
            let Some(fe) = btrfs_disk::items::FileExtentItem::parse(data)
            else {
                return Ok(());
            };

            if fe.extent_type != FileExtentType::Regular
                && fe.extent_type != FileExtentType::Prealloc
            {
                error = Some(if fe.extent_type == FileExtentType::Inline {
                    "file with inline extent".to_string()
                } else {
                    "unknown extent type".to_string()
                });
                return Ok(());
            }

            let FileExtentBody::Regular { disk_bytenr, .. } = &fe.body else {
                return Ok(());
            };
            let logical_offset = *disk_bytenr;
            if logical_offset == 0 {
                error = Some("file with holes".to_string());
                return Ok(());
            }

            if !matches!(
                fe.compression,
                btrfs_disk::items::CompressionType::None
            ) {
                error = Some("compressed extent".to_string());
                return Ok(());
            }

            let Some(chunk) = find_chunk(chunks, logical_offset) else {
                error = Some(format!(
                    "cannot find chunk containing {logical_offset}"
                ));
                return Ok(());
            };

            if chunk.type_flags & u64::from(BTRFS_BLOCK_GROUP_PROFILE_MASK) != 0
            {
                error = Some(format!(
                    "unsupported block group profile: {:#x}",
                    chunk.type_flags
                        & u64::from(BTRFS_BLOCK_GROUP_PROFILE_MASK)
                ));
                return Ok(());
            }

            extents.push(FileExtent {
                logical_offset,
                num_stripes: chunk.num_stripes,
                stripe_len: chunk.stripe_len,
                stripe_devid: chunk.stripes[0].0,
                stripe_physical: chunk.stripes[0].1,
                chunk_offset: chunk.offset,
            });

            Ok(())
        },
    )
    .context("failed to search extent data")?;

    if let Some(err) = error {
        bail!("{err}");
    }
    if extents.is_empty() {
        bail!("file has no extents");
    }

    // Validate all extents are on the same device.
    let first_devid = extents[0].stripe_devid;
    for ext in &extents[1..] {
        if ext.stripe_devid != first_devid {
            bail!("file stored on multiple devices");
        }
    }

    // Compute physical offset from the first extent.
    let ext = &extents[0];
    // For single profile (validated above), num_stripes == 1 and stripe_index
    // is always 0. The general formula from the C reference simplifies to:
    let offset = ext.logical_offset - ext.chunk_offset;
    let stripe_nr = offset / ext.stripe_len;
    let stripe_offset = offset - stripe_nr * ext.stripe_len;
    let physical_start = ext.stripe_physical
        + (stripe_nr / ext.num_stripes as u64) * ext.stripe_len
        + stripe_offset;

    Ok(physical_start)
}

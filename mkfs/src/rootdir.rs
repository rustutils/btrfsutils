//! # Rootdir population: walk a directory tree and apply via the transaction crate
//!
//! Implements `--rootdir` for mkfs. The walker traverses a host source
//! directory depth-first and, for every entry, calls into the transaction
//! crate to emit the matching `INODE_ITEM` / `DIR_ITEM` / `DIR_INDEX` /
//! `INODE_REF` / `XATTR_ITEM` / inline-or-regular `EXTENT_DATA` records
//! against an open filesystem. Subvolumes (`--subvol`), reflink
//! (`--reflink` via `FICLONERANGE`), shrink (`--shrink`), and per-path
//! inode flags (`--inode-flags`) are all handled here.

use crate::args::{CompressAlgorithm, InodeFlagsArg, SubvolArg, SubvolType};
use anyhow::{Context, Result};
use btrfs_disk::{raw, util::raw_crc32c};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

/// Clone a range from `src_fd` at `src_offset` to `dst_fd` at `dst_offset`.
///
/// Uses the `FICLONERANGE` ioctl to share extents at the filesystem level
/// instead of copying bytes. Both file descriptors must be on the same
/// filesystem (or a filesystem that supports cross-file reflink).
#[allow(clippy::cast_possible_wrap)] // ioctl request codes and fd fit in c_int/c_long
fn ficlonerange(
    src_fd: std::os::unix::io::RawFd,
    src_offset: u64,
    src_length: u64,
    dst_fd: std::os::unix::io::RawFd,
    dst_offset: u64,
) -> std::io::Result<()> {
    // FICLONERANGE = _IOW(0x94, 13, struct file_clone_range) = 0x4020940D
    #[allow(overflowing_literals)] // musl uses c_int for Ioctl, value fits as i32
    const FICLONERANGE: libc::Ioctl = 0x4020_940D as libc::Ioctl;

    #[repr(C)]
    struct FileCloneRange {
        src_fd: i64,
        src_offset: u64,
        src_length: u64,
        dest_offset: u64,
    }
    let fcr = FileCloneRange {
        src_fd: i64::from(src_fd),
        src_offset,
        src_length,
        dest_offset: dst_offset,
    };
    // SAFETY: ioctl with a valid pointer to a stack-allocated struct.
    let ret = unsafe { libc::ioctl(dst_fd, FICLONERANGE, &raw const fcr) };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Compression configuration passed through from CLI args.
#[derive(Debug, Clone, Copy)]
pub struct CompressConfig {
    pub algorithm: CompressAlgorithm,
    pub level: Option<u32>,
}

impl Default for CompressConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressAlgorithm::No,
            level: None,
        }
    }
}

/// Btrfs name hash: `crc32c((u32)~1, name)`.
///
/// Used for `DIR_ITEM` key offsets. The seed `~1` in C unsigned
/// 32-bit is `0xFFFFFFFE` (bitwise NOT of 1).
#[must_use]
pub fn btrfs_name_hash(name: &[u8]) -> u64 {
    u64::from(raw_crc32c(!1u32, name))
}

/// Convert a POSIX file mode (from stat) to a btrfs file type constant.
#[allow(clippy::cast_possible_truncation)] // file type constants fit in u8
fn mode_to_btrfs_type(mode: u32) -> u8 {
    let fmt = mode & libc::S_IFMT;
    match fmt {
        x if x == libc::S_IFREG => raw::BTRFS_FT_REG_FILE as u8,
        x if x == libc::S_IFDIR => raw::BTRFS_FT_DIR as u8,
        x if x == libc::S_IFCHR => raw::BTRFS_FT_CHRDEV as u8,
        x if x == libc::S_IFBLK => raw::BTRFS_FT_BLKDEV as u8,
        x if x == libc::S_IFIFO => raw::BTRFS_FT_FIFO as u8,
        x if x == libc::S_IFSOCK => raw::BTRFS_FT_SOCK as u8,
        x if x == libc::S_IFLNK => raw::BTRFS_FT_SYMLINK as u8,
        _ => raw::BTRFS_FT_UNKNOWN as u8,
    }
}

struct DeferredSubvol {
    host_path: PathBuf,
    subvol_id: u64,
    subvol_type: SubvolType,
    parent_root_id: u64,
    parent_dirid: u64,
    dir_index: u64,
    name: Vec<u8>,
}

fn direct_subvol_boundaries(
    all_subvols: &HashMap<PathBuf, (u64, SubvolType)>,
    parent_subvol_path: Option<&Path>,
) -> HashMap<PathBuf, (u64, SubvolType)> {
    let mut result = HashMap::new();
    for (path, &(id, typ)) in all_subvols {
        let is_child = match parent_subvol_path {
            Some(parent) => path.starts_with(parent) && path != parent,
            None => true,
        };
        if !is_child {
            continue;
        }
        // Check that no other subvolume is an intermediate ancestor.
        let is_direct = !all_subvols.keys().any(|other| {
            other != path
                && path.starts_with(other)
                && match parent_subvol_path {
                    Some(parent) => {
                        other.starts_with(parent) && other != parent
                    }
                    None => true,
                }
        });
        if is_direct {
            let rel = match parent_subvol_path {
                Some(parent) => {
                    path.strip_prefix(parent).unwrap_or(path).to_path_buf()
                }
                None => path.clone(),
            };
            result.insert(rel, (id, typ));
        }
    }
    result
}

/// Align `val` up to the next multiple of `align`.
#[must_use]
pub fn align_up(val: u64, align: u64) -> u64 {
    val.div_ceil(align) * align
}

fn is_special_file(mode: u32) -> bool {
    let fmt = mode & libc::S_IFMT;
    fmt == libc::S_IFCHR || fmt == libc::S_IFBLK
}

/// Read directory entries, sorted by name for deterministic output.
fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("cannot read directory '{}'", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();
    Ok(entries)
}

/// Read extended attributes from a path.
#[allow(clippy::cast_sign_loss)] // llistxattr/lgetxattr return non-negative on success
#[allow(clippy::ptr_cast_constness)]
fn read_xattrs(path: &Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut result = Vec::new();

    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .context("path contains null byte")?;

    // Get list size.
    let list_size =
        unsafe { libc::llistxattr(c_path.as_ptr(), std::ptr::null_mut(), 0) };
    if list_size < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOTSUP)
            || err.raw_os_error() == Some(libc::ENODATA)
        {
            return Ok(result);
        }
        return Err(err).context("llistxattr failed");
    }
    if list_size == 0 {
        return Ok(result);
    }

    let mut list_buf = vec![0u8; list_size as usize];
    let ret = unsafe {
        libc::llistxattr(
            c_path.as_ptr(),
            list_buf.as_mut_ptr().cast::<libc::c_char>(),
            list_buf.len(),
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error())
            .context("llistxattr failed");
    }

    // Parse null-separated names.
    for name in list_buf[..ret as usize].split(|&b| b == 0) {
        if name.is_empty() {
            continue;
        }

        let c_name =
            std::ffi::CString::new(name).context("xattr name contains null")?;

        // Get value size.
        let val_size = unsafe {
            libc::lgetxattr(
                c_path.as_ptr(),
                c_name.as_ptr(),
                std::ptr::null_mut(),
                0,
            )
        };
        if val_size < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOTSUP)
                || err.raw_os_error() == Some(libc::ENODATA)
            {
                continue;
            }
            return Err(err).context("lgetxattr failed");
        }

        let mut val_buf = vec![0u8; val_size as usize];
        let ret = unsafe {
            libc::lgetxattr(
                c_path.as_ptr(),
                c_name.as_ptr(),
                val_buf.as_mut_ptr().cast::<libc::c_void>(),
                val_buf.len(),
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error())
                .context("lgetxattr failed");
        }
        val_buf.truncate(ret as usize);

        result.push((name.to_vec(), val_buf));
    }

    Ok(result)
}

/// `--reflink` path for one file: reserve a data extent in the
/// destination, FICLONERANGE the source bytes into each stripe's
/// backing device, then insert `EXTENT_DATA` + `EXTENT_CSUM`
/// records and bump `INODE.nbytes`.
///
/// Compression is unsupported on this path (the recorded extent
/// `compression` byte would have to match the on-disk bytes, but
/// FICLONERANGE clones source bytes verbatim — the result would be
/// inconsistent). RAID5 / RAID6 are unsupported: the new bytes
/// would land in the data column but parity isn't recomputed by
/// FICLONERANGE.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // chunked loop with reserve / clone / csum / extent / nbytes steps
#[allow(clippy::cast_possible_truncation)] // aligned_size fits in usize for any practical chunk
fn reflink_file_to_transaction<R>(
    fs: &mut btrfs_transaction::Filesystem<R>,
    trans: &mut btrfs_transaction::Transaction<R>,
    src_path: &Path,
    src_size: u64,
    tree_id: u64,
    inode: u64,
    nodatasum: bool,
    device_handles: &BTreeMap<u64, std::fs::File>,
) -> Result<()>
where
    R: std::io::Read + std::io::Write + std::io::Seek,
{
    use btrfs_disk::{
        chunk::WritePlan,
        items::{CompressionType, FileExtentItem},
    };
    use std::os::unix::io::AsRawFd;

    const MAX_EXTENT_SIZE: u64 = 1024 * 1024;
    let sectorsize = u64::from(fs.sectorsize);

    let src_file = fs::File::open(src_path).with_context(|| {
        format!("--reflink: cannot open source '{}'", src_path.display())
    })?;

    let mut file_offset = 0u64;
    let mut bytes_left = src_size;
    while bytes_left > 0 {
        let chunk_len = bytes_left.min(MAX_EXTENT_SIZE);
        let aligned_size = align_up(chunk_len, sectorsize);

        let logical = trans
            .reserve_data_extent(fs, aligned_size, tree_id, inode, file_offset)
            .map_err(|e| {
                anyhow::anyhow!(
                    "reserve_data_extent for '{}': {e}",
                    src_path.display()
                )
            })?;

        let plan = fs
            .reader()
            .chunk_cache()
            .plan_write(logical, aligned_size as usize)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "--reflink: no chunk plan for logical {logical:#x}"
                )
            })?;
        let placements = match plan {
            WritePlan::Plain(p) => p,
            WritePlan::Parity(_) => {
                anyhow::bail!(
                    "--reflink is not supported on RAID5/RAID6 chunks"
                );
            }
        };

        for placement in &placements {
            let dev_file =
                device_handles.get(&placement.devid).ok_or_else(|| {
                    anyhow::anyhow!(
                        "--reflink: no open handle for devid {}",
                        placement.devid
                    )
                })?;
            let src_off = file_offset + placement.buf_offset as u64;
            ficlonerange(
                src_file.as_raw_fd(),
                src_off,
                placement.len as u64,
                dev_file.as_raw_fd(),
                placement.physical,
            )
            .with_context(|| {
                format!(
                    "FICLONERANGE failed for '{}' to device {}; \
                     source and image must be on the same filesystem",
                    src_path.display(),
                    placement.devid
                )
            })?;
        }

        if !nodatasum {
            let on_disk =
                fs.reader_mut().read_data(logical, aligned_size as usize)?;
            trans.insert_csums(fs, logical, &on_disk).map_err(|e| {
                anyhow::anyhow!(
                    "insert_csums after reflink of '{}': {e}",
                    src_path.display()
                )
            })?;
        }

        // Reflink stores raw bytes (no compression).
        let extent_data = FileExtentItem::to_bytes_regular(
            trans.transid,
            aligned_size,
            CompressionType::None,
            false,
            logical,
            aligned_size,
            0,
            aligned_size,
        );
        trans
            .insert_file_extent(fs, tree_id, inode, file_offset, &extent_data)
            .map_err(|e| {
                anyhow::anyhow!(
                    "insert_file_extent (reflink) for '{}': {e}",
                    src_path.display()
                )
            })?;

        let nbytes_delta = i64::try_from(aligned_size)
            .map_err(|_| anyhow::anyhow!("--reflink: aligned_size overflow"))?;
        trans
            .update_inode_nbytes(fs, tree_id, inode, nbytes_delta)
            .map_err(|e| {
                anyhow::anyhow!(
                    "update_inode_nbytes after reflink of '{}': {e}",
                    src_path.display()
                )
            })?;

        file_offset += aligned_size;
        bytes_left = bytes_left.saturating_sub(chunk_len);
    }

    Ok(())
}

/// Walk `rootdir` depth-first and apply each entry to an open
/// filesystem via the transaction crate.
///
/// Emits items via [`Transaction::create_inode`] / [`link_dir_entry`]
/// / [`set_xattr`] / [`write_file_data`] / [`link_subvol_entry`] /
/// [`insert_root_ref`], so the filesystem can be reopened, populated,
/// and committed using only the transaction crate's B-tree
/// search/insert/commit pipeline — no hand-built tree blocks.
///
/// `subvol_args` declares which subdirectories of `rootdir` should
/// become separate subvolumes (with optional `ro` / `default`
/// flags). Subvolume IDs are assigned starting at
/// `BTRFS_FIRST_FREE_OBJECTID` (256). Nested subvolumes are
/// supported.
///
/// Callers must:
/// 1. Build the empty filesystem first (via [`mkfs::make_btrfs`]).
/// 2. Open it with [`Filesystem::open`] / `open_multi`.
/// 3. `Transaction::start`, then call this function, then `commit`.
///
/// # Errors
///
/// Returns an error if any host-side stat/open/read fails or any
/// transaction-crate call fails.
///
/// [`Transaction::create_inode`]: btrfs_transaction::Transaction::create_inode
/// [`link_dir_entry`]: btrfs_transaction::Transaction::link_dir_entry
/// [`set_xattr`]: btrfs_transaction::Transaction::set_xattr
/// [`write_file_data`]: btrfs_transaction::Transaction::write_file_data
/// [`link_subvol_entry`]: btrfs_transaction::Transaction::link_subvol_entry
/// [`insert_root_ref`]: btrfs_transaction::Transaction::insert_root_ref
/// [`Filesystem::open`]: btrfs_transaction::Filesystem::open
/// [`mkfs::make_btrfs`]: crate::mkfs::make_btrfs
#[allow(clippy::too_many_arguments)]
pub fn walk_to_transaction<R>(
    rootdir: &Path,
    fs: &mut btrfs_transaction::Filesystem<R>,
    trans: &mut btrfs_transaction::Transaction<R>,
    now_sec: u64,
    compress: CompressConfig,
    subvol_args: &[SubvolArg],
    inode_flags: &[InodeFlagsArg],
    reflink_handles: Option<&BTreeMap<u64, std::fs::File>>,
) -> Result<()>
where
    R: std::io::Read + std::io::Write + std::io::Seek,
{
    use uuid::Uuid;

    // Assign subvol IDs starting at BTRFS_FIRST_FREE_OBJECTID (256),
    // matching the legacy walker's policy. Each --subvol arg gets a
    // unique tree id; the path-to-id mapping drives boundary
    // detection in the per-tree walker.
    let mut next_subvol_id = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);
    let mut subvol_id_map: HashMap<PathBuf, (u64, SubvolType)> = HashMap::new();
    for arg in subvol_args {
        subvol_id_map
            .insert(arg.path.clone(), (next_subvol_id, arg.subvol_type));
        next_subvol_id += 1;
    }

    // Build the path → (nodatacow, nodatasum) lookup the legacy
    // walker maintains for `--inode-flags` matches. Paths in the
    // map are relative to `rootdir`, matching how the user passes
    // them on the command line.
    let inode_flags_map: HashMap<PathBuf, (bool, bool)> = inode_flags
        .iter()
        .map(|f| (f.path.clone(), (f.nodatacow, f.nodatasum)))
        .collect();

    // Walk the main FS tree first.
    let main_boundaries = direct_subvol_boundaries(&subvol_id_map, None);
    let main_root = u64::from(raw::BTRFS_FS_TREE_OBJECTID);
    let mut deferred = walk_one_tree_to_transaction(
        rootdir,
        rootdir,
        main_root,
        fs,
        trans,
        now_sec,
        compress,
        &main_boundaries,
        &inode_flags_map,
        reflink_handles,
    )?;

    // Process deferred subvolumes; nested subvols append to the
    // queue as they're discovered during each subvol's walk.
    while let Some(def) = deferred.pop() {
        // 1. Allocate the subvol tree, populate inode 256 + ".."
        //    INODE_REF + ROOT_ITEM patch.
        crate::post_bootstrap::create_subvolume_shape(
            fs,
            trans,
            def.subvol_id,
            now_sec,
            Uuid::new_v4(),
        )?;

        // 2. Insert ROOT_REF (parent → child) + ROOT_BACKREF
        //    (child → parent) in the root tree (tree id 1).
        trans
            .insert_root_ref(
                fs,
                def.parent_root_id,
                def.subvol_id,
                def.parent_dirid,
                def.dir_index,
                &def.name,
            )
            .with_context(|| {
                format!(
                    "insert_root_ref for subvol '{}'",
                    String::from_utf8_lossy(&def.name)
                )
            })?;

        // 3. Walk the subvol's contents into its own tree.
        //
        //    `direct_subvol_boundaries` compares its `parent_subvol_path`
        //    against `subvol_id_map` keys, which are relative to
        //    `rootdir` (the user wrote `--subvol rw:rwsub/inner`, not
        //    an absolute path). `def.host_path` was captured during
        //    the walk as an absolute path, so strip the rootdir prefix
        //    before passing it. Without this, nested subvolumes silently
        //    degrade to plain directories.
        let def_rel = def
            .host_path
            .strip_prefix(rootdir)
            .map_or_else(|_| def.host_path.clone(), Path::to_path_buf);
        let sub_boundaries =
            direct_subvol_boundaries(&subvol_id_map, Some(&def_rel));
        let nested = walk_one_tree_to_transaction(
            rootdir,
            &def.host_path,
            def.subvol_id,
            fs,
            trans,
            now_sec,
            compress,
            &sub_boundaries,
            &inode_flags_map,
            reflink_handles,
        )?;
        deferred.extend(nested);

        // 4. Apply ro / default flags after population so any
        //    intra-subvol writes go in unrestricted.
        if matches!(def.subvol_type, SubvolType::Ro | SubvolType::DefaultRo) {
            trans
                .set_root_readonly(fs, def.subvol_id)
                .with_context(|| {
                    format!(
                        "set_root_readonly for subvol '{}'",
                        String::from_utf8_lossy(&def.name)
                    )
                })?;
        }
        if matches!(
            def.subvol_type,
            SubvolType::Default | SubvolType::DefaultRo
        ) {
            trans
                .set_default_subvol(fs, def.subvol_id)
                .with_context(|| {
                    format!(
                        "set_default_subvol for subvol '{}'",
                        String::from_utf8_lossy(&def.name)
                    )
                })?;
        }
    }
    Ok(())
}

/// Walk one tree (the main FS tree, or a single subvolume) and emit
/// its items via the transaction pipeline. Returns subvolume
/// boundaries discovered inside this tree as `DeferredSubvol`
/// entries for the caller to process.
#[allow(clippy::cast_sign_loss)] // stat timestamps are non-negative in practice
#[allow(clippy::cast_possible_truncation)] // *time_nsec fits in u32
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn walk_one_tree_to_transaction<R>(
    rootdir: &Path,
    tree_root: &Path,
    root_objectid: u64,
    fs: &mut btrfs_transaction::Filesystem<R>,
    trans: &mut btrfs_transaction::Transaction<R>,
    now_sec: u64,
    compress: CompressConfig,
    subvol_boundaries: &HashMap<PathBuf, (u64, SubvolType)>,
    inode_flags_map: &HashMap<PathBuf, (bool, bool)>,
    reflink_handles: Option<&BTreeMap<u64, std::fs::File>>,
) -> Result<Vec<DeferredSubvol>>
where
    R: std::io::Read + std::io::Write + std::io::Seek,
{
    use btrfs_disk::items::{InodeFlags, Timespec};
    use btrfs_transaction::inode::InodeArgs;

    let root_ino: u64 = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID); // 256
    let mut next_ino: u64 = root_ino + 1; // 257

    // (host_dev, host_ino) -> btrfs_ino, for hardlink coalescing
    // within this tree only (the legacy walker scopes hardlink maps
    // per-subvol; cross-subvol hardlinks are out of scope).
    let mut hardlink_map: HashMap<(u64, u64), u64> = HashMap::new();
    let mut nlink_count: HashMap<u64, u32> = HashMap::new();
    let mut dir_index_map: HashMap<u64, u64> = HashMap::new();
    dir_index_map.insert(root_ino, 2);
    let mut deferred_subvols: Vec<DeferredSubvol> = Vec::new();
    let now_ts = Timespec {
        sec: now_sec,
        nsec: 0,
    };

    // Apply xattrs to the tree's root directory (inode 256).
    for (xname, xvalue) in read_xattrs(tree_root)? {
        trans
            .set_xattr(fs, root_objectid, root_ino, &xname, &xvalue)
            .map_err(|e| {
                anyhow::anyhow!(
                    "set_xattr on root dir of tree {root_objectid}: {e}"
                )
            })?;
    }

    let mut stack: Vec<(PathBuf, u64)> = read_dir_sorted(tree_root)?
        .into_iter()
        .map(|p| (p, root_ino))
        .collect();

    while let Some((host_path, parent_ino)) = stack.pop() {
        let meta = fs::symlink_metadata(&host_path).with_context(|| {
            format!("cannot stat '{}'", host_path.display())
        })?;

        let name_os = host_path
            .file_name()
            .expect("entry has no filename")
            .to_owned();
        let name_bytes = name_os.as_encoded_bytes();

        // Allocate dir_index in the parent before any per-kind branch
        // so all paths (subvol boundary, hardlink, regular entry)
        // reserve a slot in the same monotonic sequence.
        let dir_index = {
            let slot = dir_index_map.entry(parent_ino).or_insert(2);
            let v = *slot;
            *slot += 1;
            v
        };

        // Subvol boundary: emit DIR_ITEM + DIR_INDEX in parent
        // pointing at (subvol_id, ROOT_ITEM, 0), defer the actual
        // subvol creation + walk to the caller.
        let rel_to_tree =
            host_path.strip_prefix(tree_root).unwrap_or(&host_path);
        if meta.is_dir()
            && let Some(&(subvol_id, subvol_type)) =
                subvol_boundaries.get(rel_to_tree)
        {
            trans
                .link_subvol_entry(
                    fs,
                    root_objectid,
                    parent_ino,
                    subvol_id,
                    name_bytes,
                    dir_index,
                    now_ts,
                )
                .map_err(|e| {
                    anyhow::anyhow!(
                        "link_subvol_entry for '{}': {e}",
                        host_path.display()
                    )
                })?;
            deferred_subvols.push(DeferredSubvol {
                host_path: host_path.clone(),
                subvol_id,
                subvol_type,
                parent_root_id: root_objectid,
                parent_dirid: parent_ino,
                dir_index,
                name: name_bytes.to_vec(),
            });
            continue;
        }

        let host_dev_ino = (meta.dev(), meta.ino());
        let is_hardlink = meta.nlink() > 1
            && !meta.is_dir()
            && hardlink_map.contains_key(&host_dev_ino);

        let btrfs_ino = if is_hardlink {
            *hardlink_map.get(&host_dev_ino).unwrap()
        } else {
            let ino = next_ino;
            next_ino += 1;
            ino
        };

        let file_type = mode_to_btrfs_type(meta.mode());

        if is_hardlink {
            trans
                .link_dir_entry(
                    fs,
                    root_objectid,
                    parent_ino,
                    btrfs_ino,
                    name_bytes,
                    file_type,
                    dir_index,
                    now_ts,
                )
                .map_err(|e| {
                    anyhow::anyhow!(
                        "link_dir_entry (hardlink) for '{}': {e}",
                        host_path.display()
                    )
                })?;
            *nlink_count.entry(btrfs_ino).or_insert(1) += 1;
            continue;
        }

        if meta.nlink() > 1 && !meta.is_dir() {
            hardlink_map.insert(host_dev_ino, btrfs_ino);
            nlink_count.insert(btrfs_ino, 1);
        }

        let mode = meta.mode();
        let size = if meta.is_dir() { 0 } else { meta.size() };
        let rdev = if is_special_file(mode) {
            meta.rdev()
        } else {
            0
        };

        // --inode-flags lookup: paths in `inode_flags_map` are relative
        // to `rootdir`, matching how the user passes them. `nodatacow`
        // implies `nodatasum` for regular files (since COW is what
        // makes per-extent csums meaningful).
        let rel_path = host_path.strip_prefix(rootdir).unwrap_or(&host_path);
        let (nodatacow, nodatasum_arg) = inode_flags_map
            .get(rel_path)
            .copied()
            .unwrap_or((false, false));
        let nodatasum = nodatasum_arg || (nodatacow && meta.is_file());
        let mut iflags = InodeFlags::empty();
        if nodatacow {
            iflags |= InodeFlags::NODATACOW;
        }
        if nodatasum {
            iflags |= InodeFlags::NODATASUM;
        }

        let args = InodeArgs {
            generation: trans.transid,
            transid: trans.transid,
            size,
            nbytes: 0,
            nlink: 1,
            uid: meta.uid(),
            gid: meta.gid(),
            mode,
            rdev,
            flags: iflags,
            sequence: 0,
            atime: Timespec {
                sec: meta.atime() as u64,
                nsec: meta.atime_nsec() as u32,
            },
            ctime: Timespec {
                sec: meta.ctime() as u64,
                nsec: meta.ctime_nsec() as u32,
            },
            mtime: Timespec {
                sec: meta.mtime() as u64,
                nsec: meta.mtime_nsec() as u32,
            },
            otime: now_ts,
        };

        trans
            .create_inode(fs, root_objectid, btrfs_ino, &args)
            .map_err(|e| {
                anyhow::anyhow!(
                    "create_inode for '{}': {e}",
                    host_path.display()
                )
            })?;
        trans
            .link_dir_entry(
                fs,
                root_objectid,
                parent_ino,
                btrfs_ino,
                name_bytes,
                file_type,
                dir_index,
                now_ts,
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "link_dir_entry for '{}': {e}",
                    host_path.display()
                )
            })?;

        for (xname, xvalue) in read_xattrs(&host_path)? {
            trans
                .set_xattr(fs, root_objectid, btrfs_ino, &xname, &xvalue)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "set_xattr on '{}': {e}",
                        host_path.display()
                    )
                })?;
        }

        if meta.is_dir() {
            dir_index_map.insert(btrfs_ino, 2);
            let mut children = read_dir_sorted(&host_path)?;
            for child in children.drain(..).rev() {
                stack.push((child, btrfs_ino));
            }
        } else if meta.is_symlink() {
            let target = fs::read_link(&host_path).with_context(|| {
                format!("cannot readlink '{}'", host_path.display())
            })?;
            let target_bytes = target.as_os_str().as_encoded_bytes();
            trans
                .insert_inline_extent(
                    fs,
                    root_objectid,
                    btrfs_ino,
                    0,
                    target_bytes,
                    None,
                )
                .map_err(|e| {
                    anyhow::anyhow!(
                        "insert_inline_extent for symlink '{}': {e}",
                        host_path.display()
                    )
                })?;
        } else if meta.is_file() && size > 0 {
            if let Some(handles) = reflink_handles {
                // --reflink: skip the byte copy and FICLONERANGE the
                // source extents into each stripe's backing device.
                // Compression is incompatible with reflink (the extent
                // bytes on disk would be the source's uncompressed
                // bytes regardless of the recorded compression byte),
                // so we ignore the user's compress setting here and
                // store the raw bytes uncompressed.
                reflink_file_to_transaction(
                    fs,
                    trans,
                    &host_path,
                    size,
                    root_objectid,
                    btrfs_ino,
                    nodatasum,
                    handles,
                )?;
            } else {
                use btrfs_disk::items::CompressionType;
                let comp = match compress.algorithm {
                    CompressAlgorithm::No => None,
                    CompressAlgorithm::Zlib => Some(CompressionType::Zlib),
                    CompressAlgorithm::Lzo => Some(CompressionType::Lzo),
                    CompressAlgorithm::Zstd => Some(CompressionType::Zstd),
                };
                let mut data = Vec::with_capacity(size as usize);
                let mut f = fs::File::open(&host_path).with_context(|| {
                    format!("cannot open '{}'", host_path.display())
                })?;
                f.read_to_end(&mut data)?;
                trans
                    .write_file_data(
                        fs,
                        root_objectid,
                        btrfs_ino,
                        0,
                        &data,
                        nodatasum,
                        comp,
                    )
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "write_file_data for '{}': {e}",
                            host_path.display()
                        )
                    })?;
            }
        }
    }

    for (&ino, &nlink) in &nlink_count {
        if nlink > 1 {
            trans
                .set_inode_nlink(fs, root_objectid, ino, nlink)
                .map_err(|e| {
                    anyhow::anyhow!("set_inode_nlink({ino}, {nlink}): {e}")
                })?;
        }
    }

    Ok(deferred_subvols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_hash_known_values() {
        // Verify against C reference values.
        // btrfs_name_hash uses crc32c with seed 0xFFFFFFFE.
        let hash = btrfs_name_hash(b"..");
        assert_ne!(hash, 0);

        // The hash should be deterministic.
        assert_eq!(hash, btrfs_name_hash(b".."));

        // Different names produce different hashes (in general).
        assert_ne!(btrfs_name_hash(b"foo"), btrfs_name_hash(b"bar"));
    }

    #[test]
    fn mode_to_type_conversions() {
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFREG | 0o644),
            raw::BTRFS_FT_REG_FILE as u8
        );
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFDIR | 0o755),
            raw::BTRFS_FT_DIR as u8
        );
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFLNK | 0o777),
            raw::BTRFS_FT_SYMLINK as u8
        );
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFCHR),
            raw::BTRFS_FT_CHRDEV as u8
        );
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFBLK),
            raw::BTRFS_FT_BLKDEV as u8
        );
        assert_eq!(mode_to_btrfs_type(libc::S_IFIFO), raw::BTRFS_FT_FIFO as u8);
        assert_eq!(
            mode_to_btrfs_type(libc::S_IFSOCK),
            raw::BTRFS_FT_SOCK as u8
        );
    }

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }
}

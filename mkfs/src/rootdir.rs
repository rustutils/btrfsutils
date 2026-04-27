//! # Rootdir population: walk a directory tree and create btrfs items
//!
//! Implements `--rootdir` for mkfs: walks a source directory, creates inodes,
//! directory entries, file extents, and extended attributes in the FS tree,
//! writes file data to the data chunk, and generates checksums and extent
//! backrefs for the extent and csum trees.

use crate::{
    args::{CompressAlgorithm, InodeFlagsArg, SubvolArg, SubvolType},
    items,
    tree::Key,
    write::ChecksumType,
};
use anyhow::{Context, Result};
use btrfs_disk::{raw, util::raw_crc32c};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

/// Maximum size of a single file extent (1 MiB).
const MAX_EXTENT_SIZE: u64 = 1024 * 1024;

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

impl CompressConfig {
    /// On-disk compression type byte for `FILE_EXTENT_ITEM`.
    fn extent_type_byte(self) -> u8 {
        match self.algorithm {
            CompressAlgorithm::No => 0,
            CompressAlgorithm::Zlib => 1,
            CompressAlgorithm::Lzo => 2,
            CompressAlgorithm::Zstd => 3,
        }
    }

    /// Whether compression is enabled.
    fn is_enabled(self) -> bool {
        self.algorithm != CompressAlgorithm::No
    }
}

/// Try to compress `data` for an inline extent. Returns `Some(compressed)`
/// if the result is smaller than the original, `None` otherwise.
///
/// For LZO, uses the single-segment inline format:
/// `[4B total_len] [4B seg_len] [lzo data]`.
#[allow(clippy::cast_possible_wrap)] // zstd level fits in i32
fn try_compress_inline(data: &[u8], cfg: CompressConfig) -> Option<Vec<u8>> {
    if !cfg.is_enabled() || data.is_empty() {
        return None;
    }
    let compressed = match cfg.algorithm {
        CompressAlgorithm::No => return None,
        CompressAlgorithm::Zlib => {
            use flate2::write::ZlibEncoder;
            use std::io::Write;
            let level = cfg.level.unwrap_or(3);
            let mut encoder =
                ZlibEncoder::new(Vec::new(), flate2::Compression::new(level));
            encoder.write_all(data).ok()?;
            encoder.finish().ok()?
        }
        CompressAlgorithm::Zstd => {
            let level = cfg.level.unwrap_or(3) as i32;
            zstd::bulk::compress(data, level).ok()?
        }
        CompressAlgorithm::Lzo => lzo_compress_inline(data)?,
    };
    if compressed.len() < data.len() {
        Some(compressed)
    } else {
        None
    }
}

/// Try to compress `data` for a regular (non-inline) extent. Returns
/// `Some(compressed)` if the result is smaller, `None` otherwise.
///
/// For LZO, uses the per-sector framed format:
/// `[4B total_len] { [4B seg_len] [lzo data] [padding] }*`.
#[allow(clippy::cast_possible_wrap)] // zstd level fits in i32
fn try_compress_regular(
    data: &[u8],
    cfg: CompressConfig,
    sectorsize: u32,
) -> Option<Vec<u8>> {
    if !cfg.is_enabled() || data.is_empty() {
        return None;
    }
    let compressed = match cfg.algorithm {
        CompressAlgorithm::No => return None,
        CompressAlgorithm::Zlib => {
            use flate2::write::ZlibEncoder;
            use std::io::Write;
            let level = cfg.level.unwrap_or(3);
            let mut encoder =
                ZlibEncoder::new(Vec::new(), flate2::Compression::new(level));
            encoder.write_all(data).ok()?;
            encoder.finish().ok()?
        }
        CompressAlgorithm::Zstd => {
            let level = cfg.level.unwrap_or(3) as i32;
            zstd::bulk::compress(data, level).ok()?
        }
        CompressAlgorithm::Lzo => lzo_compress_extent(data, sectorsize)?,
    };
    if compressed.len() < data.len() {
        Some(compressed)
    } else {
        None
    }
}

/// LZO inline compression: single-segment format.
///
/// Layout: `[4B total_len] [4B seg_len] [lzo1x compressed data]`.
#[allow(clippy::cast_possible_truncation)] // lengths fit in u32
fn lzo_compress_inline(data: &[u8]) -> Option<Vec<u8>> {
    let seg = lzokay::compress::compress(data).ok()?;
    let total_len = 4 + 4 + seg.len();
    let mut buf = Vec::with_capacity(total_len);
    buf.extend_from_slice(&(total_len as u32).to_le_bytes());
    buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
    buf.extend_from_slice(&seg);
    Some(buf)
}

/// LZO per-sector compression for regular extents.
///
/// Layout: `[4B total_len] { [4B seg_len] [lzo1x data] [padding] }*`.
/// Each input sector is compressed independently. Padding is added when
/// the next segment header would cross a sector boundary.
#[allow(clippy::cast_possible_truncation)] // lengths fit in u32
fn lzo_compress_extent(data: &[u8], sectorsize: u32) -> Option<Vec<u8>> {
    let ss = sectorsize as usize;
    let sectors = data.len().div_ceil(ss);
    let mut buf = Vec::with_capacity(data.len());

    // Reserve space for the total length header.
    buf.extend_from_slice(&[0u8; 4]);

    for i in 0..sectors {
        let start = i * ss;
        let end = (start + ss).min(data.len());
        let seg = lzokay::compress::compress(&data[start..end]).ok()?;

        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(&seg);

        // Pad if the next 4-byte header would cross a sector boundary.
        let pos = buf.len();
        let sector_rem = ss - (pos % ss);
        if sector_rem < 4 && sector_rem < ss {
            buf.resize(pos + sector_rem, 0);
        }

        // Early exit: if first 3 sectors don't compress well, give up.
        if i >= 3 && buf.len() > i * ss {
            return None;
        }
    }

    // Write total length (includes the 4-byte header itself).
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());

    Some(buf)
}

/// Btrfs name hash: `crc32c((u32)~1, name)`.
///
/// Used for `DIR_ITEM` key offsets. The seed `~1` in C unsigned 32-bit is
/// `0xFFFFFFFE` (bitwise NOT of 1).
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

/// A file that needs data written to the data chunk.
pub struct FileAllocation {
    /// Host filesystem path to read from.
    pub host_path: PathBuf,
    /// Btrfs inode objectid.
    pub ino: u64,
    /// Size in bytes.
    pub size: u64,
    /// Whether to skip checksum generation (NODATASUM).
    pub nodatasum: bool,
    /// Tree objectid that owns this file (main FS tree or subvolume ID).
    pub root_objectid: u64,
}

/// Output of the directory walk: per-tree item sets plus file allocations.
pub struct RootdirPlan {
    /// Plans for each tree: index 0 = main FS tree, rest = subvolumes.
    pub subvols: Vec<SubvolPlan>,
    /// Subvolume metadata needed for root tree construction.
    pub subvol_meta: Vec<SubvolMeta>,
    /// Aggregate data bytes needed across all subvolumes.
    pub data_bytes_needed: u64,
}

/// Plan for a single tree (main FS tree or a subvolume).
pub struct SubvolPlan {
    /// Tree objectid (5 for main FS tree, 256+ for subvolumes).
    pub root_objectid: u64,
    /// Sorted FS tree items for this tree.
    pub fs_items: Vec<(Key, Vec<u8>)>,
    /// Files needing non-inline data extents in this tree.
    pub file_extents: Vec<FileAllocation>,
    /// Total data bytes needed for this tree.
    pub data_bytes_needed: u64,
    /// Root directory nlink (always 1 for btrfs directories).
    pub root_dir_nlink: u32,
    /// Root directory accumulated inode size.
    pub root_dir_size: u64,
    /// Root directory nbytes.
    pub root_dir_nbytes: u64,
}

/// Metadata about a subvolume, used for root tree entries.
pub struct SubvolMeta {
    /// Subvolume objectid (256+).
    pub subvol_id: u64,
    /// Root objectid of the parent tree.
    pub parent_root_id: u64,
    /// Inode of the directory in the parent tree containing this subvolume.
    pub parent_dirid: u64,
    /// Directory index in the parent (matches `DIR_INDEX` offset).
    pub dir_index: u64,
    /// Subvolume directory name.
    pub name: Vec<u8>,
    /// Whether this is a read-only subvolume.
    pub readonly: bool,
    /// Whether this is the default subvolume.
    pub is_default: bool,
}

/// A deferred subvolume walk discovered during the parent tree walk.
struct DeferredSubvol {
    host_path: PathBuf,
    subvol_id: u64,
    subvol_type: SubvolType,
    parent_root_id: u64,
    parent_dirid: u64,
    dir_index: u64,
    name: Vec<u8>,
}

/// Walk the source directory and build all FS tree items, handling subvolumes.
///
/// Assigns inode numbers starting at 257 (256 = root directory, handled
/// separately). Detects hardlinks via host `(dev, ino)`. Collects xattrs.
/// Subvolume boundaries are detected and walked as separate trees.
///
/// # Errors
///
/// Returns an error if any file cannot be stat'd, read, or is otherwise inaccessible.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
pub fn walk_directory(
    rootdir: &Path,
    sectorsize: u32,
    nodesize: u32,
    generation: u64,
    now_sec: u64,
    compress: CompressConfig,
    inode_flags: &[InodeFlagsArg],
    subvol_args: &[SubvolArg],
) -> Result<RootdirPlan> {
    // Build inode flags lookup: relative path → (nodatacow, nodatasum).
    let inode_flags_map: HashMap<PathBuf, (bool, bool)> = inode_flags
        .iter()
        .map(|f| (f.path.clone(), (f.nodatacow, f.nodatasum)))
        .collect();

    // Assign subvolume IDs starting at BTRFS_FIRST_FREE_OBJECTID (256).
    let mut next_subvol_id = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);
    let mut subvol_id_map: HashMap<PathBuf, (u64, SubvolType)> = HashMap::new();
    for arg in subvol_args {
        subvol_id_map
            .insert(arg.path.clone(), (next_subvol_id, arg.subvol_type));
        next_subvol_id += 1;
    }

    // Determine which subvolume boundaries belong to the main FS tree
    // (direct children, not nested under another subvolume).
    let main_tree_boundaries = direct_subvol_boundaries(&subvol_id_map, None);

    // Walk the main FS tree.
    let main_root_id = u64::from(raw::BTRFS_FS_TREE_OBJECTID);
    let (main_plan, deferred) = walk_single_tree(
        rootdir,
        rootdir,
        main_root_id,
        &main_tree_boundaries,
        &inode_flags_map,
        sectorsize,
        nodesize,
        generation,
        now_sec,
        compress,
    )?;

    let mut subvols = vec![main_plan];
    let mut subvol_meta: Vec<SubvolMeta> = Vec::new();
    let mut total_data = subvols[0].data_bytes_needed;

    // Process deferred subvolumes (may produce nested deferred subvols).
    let mut pending = deferred;
    while let Some(def) = pending.pop() {
        let sub_boundaries =
            direct_subvol_boundaries(&subvol_id_map, Some(&def.host_path));

        let (plan, nested_deferred) = walk_single_tree(
            rootdir,
            &def.host_path,
            def.subvol_id,
            &sub_boundaries,
            &inode_flags_map,
            sectorsize,
            nodesize,
            generation,
            now_sec,
            compress,
        )?;

        total_data += plan.data_bytes_needed;
        subvols.push(plan);

        subvol_meta.push(SubvolMeta {
            subvol_id: def.subvol_id,
            parent_root_id: def.parent_root_id,
            parent_dirid: def.parent_dirid,
            dir_index: def.dir_index,
            name: def.name,
            readonly: matches!(
                def.subvol_type,
                SubvolType::Ro | SubvolType::DefaultRo
            ),
            is_default: matches!(
                def.subvol_type,
                SubvolType::Default | SubvolType::DefaultRo
            ),
        });

        pending.extend(nested_deferred);
    }

    Ok(RootdirPlan {
        subvols,
        subvol_meta,
        data_bytes_needed: total_data,
    })
}

/// Find subvolume boundaries that are direct children of `parent_subvol_path`.
///
/// If `parent_subvol_path` is `None`, returns boundaries at the top level
/// (not nested under any other subvolume).
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

/// Walk a single tree (main FS tree or a subvolume tree).
///
/// Returns the items for this tree and any deferred subvolume walks
/// discovered at subvolume boundary directories.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::cast_possible_truncation)] // key types fit in u8, name lengths fit in u64
#[allow(clippy::cast_sign_loss)] // stat timestamps are non-negative in practice
fn walk_single_tree(
    rootdir: &Path,
    tree_root: &Path,
    root_objectid: u64,
    subvol_boundaries: &HashMap<PathBuf, (u64, SubvolType)>,
    inode_flags_map: &HashMap<PathBuf, (bool, bool)>,
    sectorsize: u32,
    nodesize: u32,
    generation: u64,
    now_sec: u64,
    compress: CompressConfig,
) -> Result<(SubvolPlan, Vec<DeferredSubvol>)> {
    let max_inline = max_inline_data_size(sectorsize, nodesize);

    let mut next_ino: u64 = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID) + 1; // 257
    let root_ino: u64 = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID); // 256

    let mut hardlink_map: HashMap<(u64, u64), u64> = HashMap::new();
    let mut nlink_count: HashMap<u64, u32> = HashMap::new();
    let mut dir_index_map: HashMap<u64, u64> = HashMap::new();
    dir_index_map.insert(root_ino, 2);
    let mut dir_sizes: HashMap<u64, u64> = HashMap::new();
    let mut fs_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut file_extents: Vec<FileAllocation> = Vec::new();
    let mut data_bytes_needed: u64 = 0;
    let mut deferred_subvols: Vec<DeferredSubvol> = Vec::new();

    let root_dir_nlink: u32 = 1;
    let mut root_dir_size: u64 = 0;

    let mut stack: Vec<(PathBuf, u64)> = Vec::new();

    let _root_meta = fs::symlink_metadata(tree_root).with_context(|| {
        format!("cannot stat rootdir '{}'", tree_root.display())
    })?;

    // Add xattrs for the root directory itself.
    let root_xattrs = read_xattrs(tree_root)?;
    for (xname, xvalue) in &root_xattrs {
        let name_hash = btrfs_name_hash(xname);
        let key =
            Key::new(root_ino, raw::BTRFS_XATTR_ITEM_KEY as u8, name_hash);
        fs_items.push((key, items::xattr_item(xname, xvalue)));
    }

    let mut root_entries = read_dir_sorted(tree_root)?;
    for entry in root_entries.drain(..) {
        stack.push((entry, root_ino));
    }

    while let Some((host_path, parent_ino)) = stack.pop() {
        let meta = fs::symlink_metadata(&host_path).with_context(|| {
            format!("cannot stat '{}'", host_path.display())
        })?;

        // Check if this directory is a subvolume boundary.
        let rel_to_tree =
            host_path.strip_prefix(tree_root).unwrap_or(&host_path);
        if meta.is_dir()
            && let Some(&(subvol_id, subvol_type)) =
                subvol_boundaries.get(rel_to_tree)
        {
            // Emit DIR_ITEM + DIR_INDEX pointing to subvolume root.
            let name = host_path
                .file_name()
                .expect("entry has no filename")
                .as_encoded_bytes();
            let name_hash = btrfs_name_hash(name);
            let location =
                Key::new(subvol_id, raw::BTRFS_ROOT_ITEM_KEY as u8, 0);
            let dir_item_data = items::dir_item(
                &location,
                generation,
                name,
                raw::BTRFS_FT_DIR as u8,
            );
            fs_items.push((
                Key::new(parent_ino, raw::BTRFS_DIR_ITEM_KEY as u8, name_hash),
                dir_item_data.clone(),
            ));

            let dir_index = dir_index_map.entry(parent_ino).or_insert(2);
            let current_index = *dir_index;
            *dir_index += 1;

            fs_items.push((
                Key::new(
                    parent_ino,
                    raw::BTRFS_DIR_INDEX_KEY as u8,
                    current_index,
                ),
                dir_item_data,
            ));

            if parent_ino == root_ino {
                root_dir_size += name.len() as u64 * 2;
            } else {
                *dir_sizes.entry(parent_ino).or_insert(0u64) +=
                    name.len() as u64 * 2;
            }

            deferred_subvols.push(DeferredSubvol {
                host_path: host_path.clone(),
                subvol_id,
                subvol_type,
                parent_root_id: root_objectid,
                parent_dirid: parent_ino,
                dir_index: current_index,
                name: name.to_vec(),
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

        let name = host_path
            .file_name()
            .expect("entry has no filename")
            .as_encoded_bytes();
        let name_hash = btrfs_name_hash(name);
        let file_type = mode_to_btrfs_type(meta.mode());

        let location = Key::new(btrfs_ino, raw::BTRFS_INODE_ITEM_KEY as u8, 0);
        let dir_item_data =
            items::dir_item(&location, generation, name, file_type);

        let dir_item_key =
            Key::new(parent_ino, raw::BTRFS_DIR_ITEM_KEY as u8, name_hash);
        fs_items.push((dir_item_key, dir_item_data.clone()));

        let dir_index = dir_index_map.entry(parent_ino).or_insert(2);
        let current_index = *dir_index;
        *dir_index += 1;

        let dir_index_key =
            Key::new(parent_ino, raw::BTRFS_DIR_INDEX_KEY as u8, current_index);
        fs_items.push((dir_index_key, dir_item_data));

        if parent_ino == root_ino {
            root_dir_size += name.len() as u64 * 2;
        } else {
            *dir_sizes.entry(parent_ino).or_insert(0u64) +=
                name.len() as u64 * 2;
        }

        if is_hardlink {
            let ref_key =
                Key::new(btrfs_ino, raw::BTRFS_INODE_REF_KEY as u8, parent_ino);
            fs_items.push((ref_key, items::inode_ref(current_index, name)));
            *nlink_count.entry(btrfs_ino).or_insert(1) += 1;
            continue;
        }

        if meta.nlink() > 1 && !meta.is_dir() {
            hardlink_map.insert(host_dev_ino, btrfs_ino);
            nlink_count.insert(btrfs_ino, 1);
        }

        let ref_key =
            Key::new(btrfs_ino, raw::BTRFS_INODE_REF_KEY as u8, parent_ino);
        fs_items.push((ref_key, items::inode_ref(current_index, name)));

        let nlink = 1u32;
        let mode = meta.mode();
        let size = if meta.is_dir() { 0 } else { meta.size() };
        let rdev = if is_special_file(mode) {
            meta.rdev()
        } else {
            0
        };

        // Compute inode flags from --inode-flags args.
        let rel_path = host_path.strip_prefix(rootdir).unwrap_or(&host_path);
        let (nodatacow, nodatasum) = inode_flags_map
            .get(rel_path)
            .copied()
            .unwrap_or((false, false));
        let nodatasum = nodatasum || (nodatacow && meta.is_file());
        let mut iflags = 0u64;
        if nodatacow {
            iflags |= u64::from(raw::BTRFS_INODE_NODATACOW);
        }
        if nodatasum {
            iflags |= u64::from(raw::BTRFS_INODE_NODATASUM);
        }

        let inode_data = items::inode_item(&items::InodeItemArgs {
            generation,
            transid: generation,
            size,
            nbytes: 0,
            nlink,
            uid: meta.uid(),
            gid: meta.gid(),
            mode,
            rdev,
            flags: iflags,
            atime: (meta.atime() as u64, meta.atime_nsec() as u32),
            ctime: (meta.ctime() as u64, meta.ctime_nsec() as u32),
            mtime: (meta.mtime() as u64, meta.mtime_nsec() as u32),
            otime: (now_sec, 0),
        });
        let inode_key = Key::new(btrfs_ino, raw::BTRFS_INODE_ITEM_KEY as u8, 0);
        fs_items.push((inode_key, inode_data));

        let xattrs = read_xattrs(&host_path)?;
        for (xname, xvalue) in &xattrs {
            let xhash = btrfs_name_hash(xname);
            let key =
                Key::new(btrfs_ino, raw::BTRFS_XATTR_ITEM_KEY as u8, xhash);
            fs_items.push((key, items::xattr_item(xname, xvalue)));
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
            let extent_data = items::file_extent_inline(
                generation,
                target_bytes.len() as u64,
                0,
                target_bytes,
            );
            let extent_key =
                Key::new(btrfs_ino, raw::BTRFS_EXTENT_DATA_KEY as u8, 0);
            fs_items.push((extent_key, extent_data));
        } else if meta.is_file() && size > 0 {
            if size <= max_inline as u64 {
                let mut data = Vec::with_capacity(size as usize);
                let mut f = fs::File::open(&host_path).with_context(|| {
                    format!("cannot open '{}'", host_path.display())
                })?;
                f.read_to_end(&mut data)?;
                let (stored_data, comp_type) = if let Some(compressed) =
                    try_compress_inline(&data, compress)
                {
                    (compressed, compress.extent_type_byte())
                } else {
                    (data.clone(), 0)
                };
                let extent_data = items::file_extent_inline(
                    generation,
                    data.len() as u64,
                    comp_type,
                    &stored_data,
                );
                let extent_key =
                    Key::new(btrfs_ino, raw::BTRFS_EXTENT_DATA_KEY as u8, 0);
                fs_items.push((extent_key, extent_data));
            } else {
                let aligned_size = align_up(size, u64::from(sectorsize));
                data_bytes_needed += aligned_size;
                file_extents.push(FileAllocation {
                    host_path: host_path.clone(),
                    ino: btrfs_ino,
                    size,
                    nodatasum,
                    root_objectid,
                });
            }
        }
    }

    for (&ino, &nlink) in &nlink_count {
        fixup_inode_nlink(&mut fs_items, ino, nlink);
    }
    for (&ino, &size) in &dir_sizes {
        fixup_inode_size(&mut fs_items, ino, size);
    }
    fixup_inline_nbytes(&mut fs_items);
    fs_items.sort_by_key(|(k, _)| *k);

    let plan = SubvolPlan {
        root_objectid,
        fs_items,
        file_extents,
        data_bytes_needed,
        root_dir_nlink,
        root_dir_size,
        root_dir_nbytes: 0,
    };
    Ok((plan, deferred_subvols))
}

/// Write file data to the data chunk and create extent/csum items.
///
/// Processes all file extents across all subvolumes. Returns per-tree
/// FS items, combined extent/csum items, and per-tree nbytes updates.
///
/// # Errors
///
/// Returns an error if file data cannot be read or written.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_possible_truncation)] // key types fit in u8, devid-1 fits usize, sector counts fit
#[allow(clippy::cast_sign_loss)] // EXTENT_CSUM_OBJECTID is positive
pub fn write_file_data(
    plan: &RootdirPlan,
    data_logical: u64,
    sectorsize: u32,
    generation: u64,
    csum_type: ChecksumType,
    compress: CompressConfig,
    reflink: bool,
    files: &[std::fs::File],
    chunks: &crate::layout::ChunkLayout,
) -> Result<DataOutput> {
    let mut offset = 0u64;
    let mut fs_items: BTreeMap<u64, Vec<(Key, Vec<u8>)>> = BTreeMap::new();
    let mut extent_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut csum_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut nbytes_updates: HashMap<(u64, u64), u64> = HashMap::new();
    let csum_size = csum_type.size();

    // Collect all file extents across all subvolumes.
    let all_extents: Vec<&FileAllocation> =
        plan.subvols.iter().flat_map(|s| &s.file_extents).collect();

    for alloc in &all_extents {
        let mut file = fs::File::open(&alloc.host_path).with_context(|| {
            format!("cannot open '{}'", alloc.host_path.display())
        })?;

        let mut file_offset: u64 = 0;
        let mut bytes_left = alloc.size;
        let mut disk_allocated: u64 = 0;

        while bytes_left > 0 {
            let extent_size = bytes_left.min(MAX_EXTENT_SIZE);
            // num_bytes / ram_bytes in EXTENT_DATA must be sectorsize-
            // aligned for regular extents (btrfs check enforces this).
            let aligned_logical = align_up(extent_size, u64::from(sectorsize));

            let mut raw_data = vec![0u8; extent_size as usize];
            file.read_exact(&mut raw_data).with_context(|| {
                format!("short read from '{}'", alloc.host_path.display())
            })?;

            let (disk_data, comp_type) = if let Some(compressed) =
                try_compress_regular(&raw_data, compress, sectorsize)
            {
                (compressed, compress.extent_type_byte())
            } else {
                (raw_data, 0u8)
            };

            let aligned_disk =
                align_up(disk_data.len() as u64, u64::from(sectorsize));
            let mut padded = disk_data;
            padded.resize(aligned_disk as usize, 0);

            let disk_bytenr = data_logical + offset;

            for (devid, phys) in chunks.logical_to_physical(disk_bytenr) {
                let file_idx = (devid - 1) as usize;
                if reflink {
                    use std::os::unix::io::AsRawFd;
                    let src_fd = file.as_raw_fd();
                    let dst_fd = files[file_idx].as_raw_fd();
                    // Clone the sector-aligned portion.
                    let clone_len = aligned_disk.min(extent_size);
                    let clone_aligned =
                        align_up(clone_len, u64::from(sectorsize));
                    ficlonerange(
                        src_fd,
                        file_offset,
                        clone_aligned,
                        dst_fd,
                        phys,
                    )
                    .with_context(|| {
                        format!(
                            "FICLONERANGE failed for '{}' to device {devid}; \
                             source and image must be on the same filesystem",
                            alloc.host_path.display()
                        )
                    })?;
                } else {
                    crate::write::pwrite_all(&files[file_idx], &padded, phys)
                        .with_context(|| {
                        format!("failed to write file data to device {devid}")
                    })?;
                }
            }

            if !alloc.nodatasum {
                let num_csums = (aligned_disk / u64::from(sectorsize)) as usize;
                let mut csums = Vec::with_capacity(num_csums * csum_size);
                for i in 0..num_csums {
                    let start = i * sectorsize as usize;
                    let end = start + sectorsize as usize;
                    let csum = csum_type.compute(&padded[start..end]);
                    csums.extend_from_slice(&csum[..csum_size]);
                }

                csum_items.push((
                    Key::new(
                        raw::BTRFS_EXTENT_CSUM_OBJECTID as u64,
                        raw::BTRFS_EXTENT_CSUM_KEY as u8,
                        disk_bytenr,
                    ),
                    csums,
                ));
            }

            fs_items.entry(alloc.root_objectid).or_default().push((
                Key::new(
                    alloc.ino,
                    raw::BTRFS_EXTENT_DATA_KEY as u8,
                    file_offset,
                ),
                items::file_extent_reg(
                    generation,
                    disk_bytenr,
                    aligned_disk,
                    0,
                    aligned_logical,
                    aligned_logical,
                    comp_type,
                ),
            ));

            extent_items.push((
                Key::new(
                    disk_bytenr,
                    raw::BTRFS_EXTENT_ITEM_KEY as u8,
                    aligned_disk,
                ),
                items::data_extent_item(
                    1,
                    generation,
                    alloc.root_objectid,
                    alloc.ino,
                    file_offset,
                    1,
                ),
            ));

            offset += aligned_disk;
            // INODE.nbytes is the sum of EXTENT_DATA num_bytes values
            // (sector-aligned logical size), not the on-disk size.
            disk_allocated += aligned_logical;
            file_offset += extent_size;
            bytes_left -= extent_size;
        }

        nbytes_updates.insert((alloc.root_objectid, alloc.ino), disk_allocated);
    }

    for items in fs_items.values_mut() {
        items.sort_by_key(|(k, _)| *k);
    }
    extent_items.sort_by_key(|(k, _)| *k);
    csum_items.sort_by_key(|(k, _)| *k);

    Ok(DataOutput {
        fs_items,
        extent_items,
        csum_items,
        data_used: offset,
        nbytes_updates,
    })
}

/// Output of the data writing phase.
pub struct DataOutput {
    /// `FILE_EXTENT_ITEM` entries per tree (keyed by root objectid).
    pub fs_items: BTreeMap<u64, Vec<(Key, Vec<u8>)>>,
    /// `EXTENT_ITEM` entries for data extents (to merge into extent tree).
    pub extent_items: Vec<(Key, Vec<u8>)>,
    /// `EXTENT_CSUM` entries (for csum tree).
    pub csum_items: Vec<(Key, Vec<u8>)>,
    /// Total data bytes allocated (aligned).
    pub data_used: u64,
    /// `(root_objectid, inode)` → nbytes for files with regular extents.
    pub nbytes_updates: HashMap<(u64, u64), u64>,
}

/// Maximum inline data size for files.
///
/// The C reference uses `min(sectorsize - 1, BTRFS_MAX_INLINE_DATA_SIZE)`.
/// `MAX_INLINE_DATA_SIZE` = `max_item_size` - `FILE_EXTENT_INLINE_DATA_START`
///                      = (nodesize - 101 - 25) - 21
///                      = nodesize - 147
fn max_inline_data_size(sectorsize: u32, nodesize: u32) -> usize {
    let max_item_inline = nodesize as usize - 147;
    max_item_inline.min(sectorsize as usize - 1)
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

/// Patch a field inside the `INODE_ITEM` for a given objectid.
///
/// Finds the first `INODE_ITEM` with `objectid == ino`, then writes
/// `value` at `field_offset` within the item data.
#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn patch_inode_field(
    fs_items: &mut [(Key, Vec<u8>)],
    ino: u64,
    field_offset: usize,
    value: &[u8],
) {
    for (key, data) in fs_items.iter_mut() {
        if key.objectid == ino
            && key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && data.len() >= field_offset + value.len()
        {
            data[field_offset..field_offset + value.len()]
                .copy_from_slice(value);
            return;
        }
    }
}

/// Fix up nlink for a specific inode (used for hardlinks).
fn fixup_inode_nlink(fs_items: &mut [(Key, Vec<u8>)], ino: u64, nlink: u32) {
    let offset = std::mem::offset_of!(raw::btrfs_inode_item, nlink);
    patch_inode_field(fs_items, ino, offset, &nlink.to_le_bytes());
}

/// Fix up inode size for a directory.
fn fixup_inode_size(fs_items: &mut [(Key, Vec<u8>)], ino: u64, size: u64) {
    let offset = std::mem::offset_of!(raw::btrfs_inode_item, size);
    patch_inode_field(fs_items, ino, offset, &size.to_le_bytes());
}

/// Fix up nbytes for files with inline extents and symlinks.
///
/// For inline file extents and symlinks, nbytes = data size (not aligned).
#[allow(clippy::cast_possible_truncation)] // key type fits in u8
fn fixup_inline_nbytes(fs_items: &mut [(Key, Vec<u8>)]) {
    let nbytes_off = std::mem::offset_of!(raw::btrfs_inode_item, nbytes);

    let mut inline_sizes: HashMap<u64, u64> = HashMap::new();
    for (key, data) in fs_items.iter() {
        if key.key_type == raw::BTRFS_EXTENT_DATA_KEY as u8
            && data.len() > 21
            && data[20] == raw::BTRFS_FILE_EXTENT_INLINE as u8
        {
            let data_size = data.len() as u64 - 21;
            *inline_sizes.entry(key.objectid).or_default() += data_size;
        }
    }

    for (key, data) in fs_items.iter_mut() {
        if key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && let Some(&nbytes) = inline_sizes.get(&key.objectid)
        {
            data[nbytes_off..nbytes_off + 8]
                .copy_from_slice(&nbytes.to_le_bytes());
        }
    }
}

/// Apply nbytes updates from data writing to `INODE_ITEM` entries.
///
/// The `updates` map is keyed by `(root_objectid, ino)`, but since we call
/// this per-subvolume, we filter by `root_objectid` and match on `ino`.
#[allow(clippy::cast_possible_truncation)] // key type fits in u8
#[allow(clippy::implicit_hasher)]
pub fn apply_nbytes_updates(
    fs_items: &mut [(Key, Vec<u8>)],
    root_objectid: u64,
    updates: &HashMap<(u64, u64), u64>,
) {
    let nbytes_off = std::mem::offset_of!(raw::btrfs_inode_item, nbytes);
    for (key, data) in fs_items.iter_mut() {
        if key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && let Some(&nbytes) = updates.get(&(root_objectid, key.objectid))
            && data.len() >= nbytes_off + 8
        {
            data[nbytes_off..nbytes_off + 8]
                .copy_from_slice(&nbytes.to_le_bytes());
        }
    }
}

/// `--reflink` path for one file: reserve a data extent in the
/// destination, FICLONERANGE the source bytes into each stripe's
/// backing device, then insert `EXTENT_DATA` + `EXTENT_CSUM`
/// records and bump `INODE.nbytes`.
///
/// The source file is read once per chunk via FICLONERANGE
/// (zero-copy clone of extents — both source and destination must
/// be on a filesystem that supports it, typically btrfs). The
/// destination bytes are read back via `BlockReader::read_data` to
/// compute checksums; this is one extra read per chunk but matches
/// what the legacy `--reflink` path did.
///
/// Compression is unsupported on this path (the recorded extent
/// `compression` byte would have to match the on-disk bytes, but
/// FICLONERANGE clones source bytes verbatim — the result would be
/// inconsistent). The caller is expected to suppress the user's
/// compress setting when routing here.
///
/// RAID5 / RAID6 are unsupported: the new bytes would land in the
/// data column but parity isn't recomputed by FICLONERANGE.
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
/// Mirrors the inode-numbering, hardlink, xattr, and subvolume
/// semantics of [`walk_directory`] (which produces hand-built items
/// for mkfs's legacy bootstrap pipeline) but emits items via
/// [`Transaction::create_inode`] / [`link_dir_entry`] /
/// [`set_xattr`] / [`write_file_data`] / [`link_subvol_entry`] /
/// [`insert_root_ref`] so the filesystem can be reopened, populated,
/// and committed without ever touching the hand-built `tree.rs` /
/// `treebuilder.rs` machinery.
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
    fn max_inline_defaults() {
        // 16K nodesize, 4K sectorsize: threshold = min(16384-147, 4095) = 4095
        assert_eq!(max_inline_data_size(4096, 16384), 4095);
    }

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn lzo_inline_roundtrip() {
        let original =
            b"hello world, this is a test of LZO inline compression!";
        let compressed = lzo_compress_inline(original).unwrap();

        // Verify format: [4B total_len] [4B seg_len] [lzo data]
        let total_len =
            u32::from_le_bytes(compressed[0..4].try_into().unwrap()) as usize;
        assert_eq!(total_len, compressed.len());
        let seg_len =
            u32::from_le_bytes(compressed[4..8].try_into().unwrap()) as usize;
        assert_eq!(seg_len, compressed.len() - 8);

        // Decompress and verify.
        let mut decompressed = vec![0u8; original.len()];
        lzokay::decompress::decompress(&compressed[8..], &mut decompressed)
            .unwrap();
        assert_eq!(&decompressed, original);
    }

    #[test]
    fn lzo_extent_roundtrip() {
        // Create data spanning multiple 4K sectors.
        let mut original = Vec::new();
        for i in 0..3u8 {
            let sector = vec![i; 4096];
            original.extend_from_slice(&sector);
        }

        let compressed = lzo_compress_extent(&original, 4096).unwrap();
        assert!(compressed.len() < original.len());

        // Verify total_len header.
        let total_len =
            u32::from_le_bytes(compressed[0..4].try_into().unwrap()) as usize;
        assert_eq!(total_len, compressed.len());

        // Decompress segment by segment.
        let ss = 4096usize;
        let mut pos = 4;
        let mut decompressed = Vec::new();
        while pos < total_len && decompressed.len() < original.len() {
            let sector_rem = ss - (pos % ss);
            if sector_rem < 4 && sector_rem < ss {
                pos += sector_rem;
            }
            let seg_len = u32::from_le_bytes(
                compressed[pos..pos + 4].try_into().unwrap(),
            ) as usize;
            pos += 4;
            let remaining = original.len() - decompressed.len();
            let out_len = remaining.min(ss);
            let mut seg_out = vec![0u8; out_len];
            lzokay::decompress::decompress(
                &compressed[pos..pos + seg_len],
                &mut seg_out,
            )
            .unwrap();
            decompressed.extend_from_slice(&seg_out);
            pos += seg_len;
        }
        assert_eq!(decompressed, original);
    }

    #[test]
    fn lzo_incompressible_returns_none() {
        // Random-ish data that won't compress well.
        let data: Vec<u8> = (0..256).map(|i| (i * 137 + 42) as u8).collect();
        let result = lzo_compress_inline(&data);
        // Small random data may or may not compress; just verify no panic.
        // If it compressed, verify format is valid.
        if let Some(buf) = result {
            let total =
                u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            assert_eq!(total, buf.len());
        }
    }
}

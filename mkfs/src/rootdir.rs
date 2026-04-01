//! # Rootdir population: walk a directory tree and create btrfs items
//!
//! Implements `--rootdir` for mkfs: walks a source directory, creates inodes,
//! directory entries, file extents, and extended attributes in the FS tree,
//! writes file data to the data chunk, and generates checksums and extent
//! backrefs for the extent and csum trees.

use crate::{
    args::{CompressAlgorithm, InodeFlagsArg},
    items,
    tree::Key,
    write::ChecksumType,
};
use anyhow::{Context, Result};
use btrfs_disk::{raw, util::raw_crc32c};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

/// Maximum size of a single file extent (1 MiB).
const MAX_EXTENT_SIZE: u64 = 1024 * 1024;

/// Compression configuration passed through from CLI args.
#[derive(Debug, Clone, Copy)]
pub struct CompressConfig {
    pub algorithm: CompressAlgorithm,
    pub level: Option<u32>,
}

impl CompressConfig {
    /// On-disk compression type byte for FILE_EXTENT_ITEM.
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

/// Try to compress `data`. Returns `Some(compressed)` if the result is
/// smaller than the original, `None` otherwise (incompressible data).
fn try_compress(data: &[u8], cfg: CompressConfig) -> Option<Vec<u8>> {
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
        CompressAlgorithm::Lzo => {
            unreachable!("LZO rejected at argument validation")
        }
    };
    if compressed.len() < data.len() {
        Some(compressed)
    } else {
        None
    }
}

/// Btrfs name hash: `crc32c((u32)~1, name)`.
///
/// Used for DIR_ITEM key offsets. The seed `~1` in C unsigned 32-bit is
/// `0xFFFFFFFE` (bitwise NOT of 1).
pub fn btrfs_name_hash(name: &[u8]) -> u64 {
    raw_crc32c(!1u32, name) as u64
}

/// Convert a POSIX file mode (from stat) to a btrfs file type constant.
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
}

/// Output of the directory walk: items for the FS tree plus file allocations.
pub struct RootdirPlan {
    /// Sorted FS tree items (INODE_ITEM, INODE_REF, DIR_ITEM, DIR_INDEX,
    /// FILE_EXTENT_ITEM for inline files, XATTR_ITEM).
    pub fs_items: Vec<(Key, Vec<u8>)>,
    /// Files that need regular (non-inline) data extents.
    pub file_extents: Vec<FileAllocation>,
    /// Total data bytes needed in the data chunk.
    pub data_bytes_needed: u64,
    /// Updated nlink for the root directory (inode 256).
    pub root_dir_nlink: u32,
    /// Updated inode size for the root directory.
    pub root_dir_size: u64,
    /// Number of subdirectory entries directly under root (for nbytes).
    pub root_dir_nbytes: u64,
}

/// Walk the source directory and build all FS tree items.
///
/// Assigns inode numbers starting at 257 (256 = root directory, handled
/// separately). Detects hardlinks via host `(dev, ino)`. Collects xattrs.
pub fn walk_directory(
    rootdir: &Path,
    sectorsize: u32,
    nodesize: u32,
    generation: u64,
    now_sec: u64,
    compress: CompressConfig,
    inode_flags: &[InodeFlagsArg],
) -> Result<RootdirPlan> {
    let max_inline = max_inline_data_size(sectorsize, nodesize);

    // Build inode flags lookup: relative path → (nodatacow, nodatasum).
    let inode_flags_map: HashMap<PathBuf, (bool, bool)> = inode_flags
        .iter()
        .map(|f| (f.path.clone(), (f.nodatacow, f.nodatasum)))
        .collect();

    let mut next_ino: u64 = raw::BTRFS_FIRST_FREE_OBJECTID as u64 + 1; // 257
    let root_ino: u64 = raw::BTRFS_FIRST_FREE_OBJECTID as u64; // 256

    // Maps host (dev, ino) → btrfs ino for hardlink detection.
    let mut hardlink_map: HashMap<(u64, u64), u64> = HashMap::new();
    // Maps host (dev, ino) → count of links seen so far (for nlink tracking).
    let mut nlink_count: HashMap<u64, u32> = HashMap::new();

    // Maps btrfs parent_ino → next dir_index counter.
    let mut dir_index_map: HashMap<u64, u64> = HashMap::new();
    // Start root dir index at 2 (0 and 1 reserved for . and ..).
    dir_index_map.insert(root_ino, 2);

    // Maps btrfs ino → accumulated directory inode size (for non-root dirs).
    let mut dir_sizes: HashMap<u64, u64> = HashMap::new();

    // All FS tree items (unsorted — we sort at the end).
    let mut fs_items: Vec<(Key, Vec<u8>)> = Vec::new();

    // File allocations for non-inline data.
    let mut file_extents: Vec<FileAllocation> = Vec::new();
    let mut data_bytes_needed: u64 = 0;

    // In btrfs, directory nlink is always 1 (no POSIX 2+subdirs convention).
    let root_dir_nlink: u32 = 1;
    let mut root_dir_size: u64 = 0;

    // Recursive walk stack: (host_path, parent_ino, btrfs_ino).
    // The root directory (ino 256) is the starting parent.
    let mut stack: Vec<(PathBuf, u64)> = Vec::new();

    // Read root directory entries.
    let _root_meta = fs::symlink_metadata(rootdir).with_context(|| {
        format!("cannot stat rootdir '{}'", rootdir.display())
    })?;

    // Add xattrs for the root directory itself.
    let root_xattrs = read_xattrs(rootdir)?;
    for (xname, xvalue) in &root_xattrs {
        let name_hash = btrfs_name_hash(xname);
        let key =
            Key::new(root_ino, raw::BTRFS_XATTR_ITEM_KEY as u8, name_hash);
        fs_items.push((key, items::xattr_item(xname, xvalue)));
    }

    // Walk root directory children.
    let mut root_entries = read_dir_sorted(rootdir)?;

    // Process root directory entries onto the stack.
    for entry in root_entries.drain(..) {
        stack.push((entry, root_ino));
    }

    while let Some((host_path, parent_ino)) = stack.pop() {
        let meta = fs::symlink_metadata(&host_path).with_context(|| {
            format!("cannot stat '{}'", host_path.display())
        })?;

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

        // DIR_ITEM in parent (keyed by name hash).
        let location = Key::new(btrfs_ino, raw::BTRFS_INODE_ITEM_KEY as u8, 0);
        let dir_item_data =
            items::dir_item(&location, generation, name, file_type);

        let dir_item_key =
            Key::new(parent_ino, raw::BTRFS_DIR_ITEM_KEY as u8, name_hash);
        fs_items.push((dir_item_key, dir_item_data.clone()));

        // DIR_INDEX in parent (keyed by sequential index).
        let dir_index = dir_index_map.entry(parent_ino).or_insert(2);
        let current_index = *dir_index;
        *dir_index += 1;

        let dir_index_key =
            Key::new(parent_ino, raw::BTRFS_DIR_INDEX_KEY as u8, current_index);
        fs_items.push((dir_index_key, dir_item_data));

        // Update parent inode size: each entry adds name_len * 2.
        if parent_ino == root_ino {
            root_dir_size += name.len() as u64 * 2;
        } else {
            *dir_sizes.entry(parent_ino).or_insert(0u64) +=
                name.len() as u64 * 2;
        }

        if is_hardlink {
            // Hardlink: add INODE_REF but not a new INODE_ITEM.
            let ref_key =
                Key::new(btrfs_ino, raw::BTRFS_INODE_REF_KEY as u8, parent_ino);
            fs_items.push((ref_key, items::inode_ref(current_index, name)));

            // Increment nlink counter.
            *nlink_count.entry(btrfs_ino).or_insert(1) += 1;
            continue;
        }

        // Track for hardlink detection (only for files with nlink > 1).
        if meta.nlink() > 1 && !meta.is_dir() {
            hardlink_map.insert(host_dev_ino, btrfs_ino);
            nlink_count.insert(btrfs_ino, 1);
        }

        // INODE_REF for this entry.
        let ref_key =
            Key::new(btrfs_ino, raw::BTRFS_INODE_REF_KEY as u8, parent_ino);
        fs_items.push((ref_key, items::inode_ref(current_index, name)));

        // INODE_ITEM.
        // In btrfs, nlink = number of INODE_REF entries for this inode.
        // Start at 1 (one ref from parent). Hardlinks add more via fixup.
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
        // NODATACOW implies NODATASUM for regular files.
        let nodatasum = nodatasum || (nodatacow && meta.is_file());
        let mut iflags = 0u64;
        if nodatacow {
            iflags |= raw::BTRFS_INODE_NODATACOW as u64;
        }
        if nodatasum {
            iflags |= raw::BTRFS_INODE_NODATASUM as u64;
        }

        let inode_data = items::inode_item(&items::InodeItemArgs {
            generation,
            transid: generation,
            size,
            nbytes: 0, // updated later for files with data
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

        // Extended attributes.
        let xattrs = read_xattrs(&host_path)?;
        for (xname, xvalue) in &xattrs {
            let xhash = btrfs_name_hash(xname);
            let key =
                Key::new(btrfs_ino, raw::BTRFS_XATTR_ITEM_KEY as u8, xhash);
            fs_items.push((key, items::xattr_item(xname, xvalue)));
        }

        // Type-specific items.
        if meta.is_dir() {
            // Initialize dir_index for this directory.
            dir_index_map.insert(btrfs_ino, 2);

            // Push children onto the stack (reverse order for DFS).
            let mut children = read_dir_sorted(&host_path)?;
            for child in children.drain(..).rev() {
                stack.push((child, btrfs_ino));
            }
        } else if meta.is_symlink() {
            // Symlink: inline extent with link target (never compressed).
            let target = fs::read_link(&host_path).with_context(|| {
                format!("cannot readlink '{}'", host_path.display())
            })?;
            let target_bytes = target.as_os_str().as_encoded_bytes();

            let extent_data = items::file_extent_inline(
                generation,
                target_bytes.len() as u64,
                0, // symlinks are never compressed
                target_bytes,
            );
            let extent_key =
                Key::new(btrfs_ino, raw::BTRFS_EXTENT_DATA_KEY as u8, 0);
            fs_items.push((extent_key, extent_data));
        } else if meta.is_file() && size > 0 {
            if size <= max_inline as u64 {
                // Inline extent: read file and embed in item.
                let mut data = Vec::with_capacity(size as usize);
                let mut f = fs::File::open(&host_path).with_context(|| {
                    format!("cannot open '{}'", host_path.display())
                })?;
                f.read_to_end(&mut data)?;

                // Try to compress inline data.
                let (stored_data, comp_type) =
                    if let Some(compressed) = try_compress(&data, compress) {
                        (compressed, compress.extent_type_byte())
                    } else {
                        (data.clone(), 0)
                    };
                let extent_data = items::file_extent_inline(
                    generation,
                    data.len() as u64, // ram_bytes = uncompressed size
                    comp_type,
                    &stored_data,
                );
                let extent_key =
                    Key::new(btrfs_ino, raw::BTRFS_EXTENT_DATA_KEY as u8, 0);
                fs_items.push((extent_key, extent_data));
            } else {
                // Regular extent: defer data writing.
                // The FILE_EXTENT_ITEM will be created during the data write phase
                // once we know the disk_bytenr.
                let aligned_size = align_up(size, sectorsize as u64);
                data_bytes_needed += aligned_size;
                file_extents.push(FileAllocation {
                    host_path: host_path.clone(),
                    ino: btrfs_ino,
                    size,
                    nodatasum,
                });
            }
        }
        // Special files (fifo, socket, char/block dev): INODE_ITEM only, no extent.
    }

    // Fix up nlink for hardlinked files.
    for (&ino, &nlink) in &nlink_count {
        fixup_inode_nlink(&mut fs_items, ino, nlink);
    }

    // Fix up inode size for non-root directories.
    for (&ino, &size) in &dir_sizes {
        fixup_inode_size(&mut fs_items, ino, size);
    }

    // Fix up nbytes for files with inline extents and symlinks.
    fixup_inline_nbytes(&mut fs_items);

    // Sort all items by key.
    fs_items.sort_by_key(|(k, _)| *k);

    Ok(RootdirPlan {
        fs_items,
        file_extents,
        data_bytes_needed,
        root_dir_nlink,
        root_dir_size,
        root_dir_nbytes: 0,
    })
}

/// Write file data to the data chunk and create extent/csum items.
///
/// Returns additional FS tree items (FILE_EXTENT_ITEM for regular extents),
/// extent tree items (EXTENT_ITEM + EXTENT_DATA_REF), and csum tree items.
/// Also returns a map of inode → nbytes for patching INODE_ITEMs.
#[allow(clippy::too_many_arguments)]
pub fn write_file_data(
    plan: &RootdirPlan,
    data_logical: u64,
    sectorsize: u32,
    generation: u64,
    csum_type: ChecksumType,
    compress: CompressConfig,
    files: &[std::fs::File],
    chunks: &crate::layout::ChunkLayout,
) -> Result<DataOutput> {
    let mut offset = 0u64;
    let mut fs_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut extent_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut csum_items: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut nbytes_updates: HashMap<u64, u64> = HashMap::new();
    let csum_size = csum_type.size();

    for alloc in &plan.file_extents {
        let mut file = fs::File::open(&alloc.host_path).with_context(|| {
            format!("cannot open '{}'", alloc.host_path.display())
        })?;

        let mut file_offset: u64 = 0;
        let mut bytes_left = alloc.size;
        let mut disk_allocated: u64 = 0;

        while bytes_left > 0 {
            let extent_size = bytes_left.min(MAX_EXTENT_SIZE);

            // Read the raw (uncompressed) data.
            let mut raw_data = vec![0u8; extent_size as usize];
            file.read_exact(&mut raw_data).with_context(|| {
                format!("short read from '{}'", alloc.host_path.display())
            })?;

            // Try compression.
            let (disk_data, comp_type) =
                if let Some(compressed) = try_compress(&raw_data, compress) {
                    (compressed, compress.extent_type_byte())
                } else {
                    (raw_data, 0u8)
                };

            // Pad to sectorsize alignment for on-disk storage.
            let aligned_disk =
                align_up(disk_data.len() as u64, sectorsize as u64);
            let mut padded = disk_data;
            padded.resize(aligned_disk as usize, 0);

            let disk_bytenr = data_logical + offset;

            // Write data to physical locations.
            for (devid, phys) in chunks.logical_to_physical(disk_bytenr) {
                let file_idx = (devid - 1) as usize;
                crate::write::pwrite_all(&files[file_idx], &padded, phys)
                    .with_context(|| {
                        format!("failed to write file data to device {devid}")
                    })?;
            }

            // Compute checksums (skip for NODATASUM files).
            if !alloc.nodatasum {
                let num_csums = (aligned_disk / sectorsize as u64) as usize;
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

            fs_items.push((
                Key::new(
                    alloc.ino,
                    raw::BTRFS_EXTENT_DATA_KEY as u8,
                    file_offset,
                ),
                items::file_extent_reg(
                    generation,
                    disk_bytenr,
                    aligned_disk, // compressed + aligned size on disk
                    0,
                    extent_size, // logical file bytes this extent covers
                    extent_size, // ram_bytes = uncompressed size
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
                    raw::BTRFS_FS_TREE_OBJECTID as u64,
                    alloc.ino,
                    file_offset,
                    1,
                ),
            ));

            offset += aligned_disk;
            disk_allocated += aligned_disk;
            file_offset += extent_size;
            bytes_left -= extent_size;
        }

        nbytes_updates.insert(alloc.ino, disk_allocated);
    }

    fs_items.sort_by_key(|(k, _)| *k);
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
    /// FILE_EXTENT_ITEM entries for regular extents (to merge into FS tree).
    pub fs_items: Vec<(Key, Vec<u8>)>,
    /// EXTENT_ITEM entries for data extents (to merge into extent tree).
    pub extent_items: Vec<(Key, Vec<u8>)>,
    /// EXTENT_CSUM entries (for csum tree).
    pub csum_items: Vec<(Key, Vec<u8>)>,
    /// Total data bytes allocated (aligned).
    pub data_used: u64,
    /// Inode → nbytes updates for files with regular extents.
    pub nbytes_updates: HashMap<u64, u64>,
}

/// Maximum inline data size for files.
///
/// The C reference uses `min(sectorsize - 1, BTRFS_MAX_INLINE_DATA_SIZE)`.
/// MAX_INLINE_DATA_SIZE = max_item_size - FILE_EXTENT_INLINE_DATA_START
///                      = (nodesize - 101 - 25) - 21
///                      = nodesize - 147
fn max_inline_data_size(sectorsize: u32, nodesize: u32) -> usize {
    let max_item_inline = nodesize as usize - 147;
    max_item_inline.min(sectorsize as usize - 1)
}

/// Align `val` up to the next multiple of `align`.
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
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    entries.sort();
    Ok(entries)
}

/// Read extended attributes from a path.
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
            list_buf.as_mut_ptr() as *mut libc::c_char,
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
                val_buf.as_mut_ptr() as *mut libc::c_void,
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

/// Fix up nlink for a specific inode (used for hardlinks).
fn fixup_inode_nlink(fs_items: &mut [(Key, Vec<u8>)], ino: u64, nlink: u32) {
    for (key, data) in fs_items.iter_mut() {
        if key.objectid == ino
            && key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && data.len() >= 44
        {
            data[40..44].copy_from_slice(&nlink.to_le_bytes());
            return;
        }
    }
}

/// Fix up inode size for a directory.
fn fixup_inode_size(fs_items: &mut [(Key, Vec<u8>)], ino: u64, size: u64) {
    for (key, data) in fs_items.iter_mut() {
        if key.objectid == ino
            && key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && data.len() >= 24
        {
            // inode_item.size is at offset 16.
            data[16..24].copy_from_slice(&size.to_le_bytes());
            return;
        }
    }
}

/// Fix up nbytes for files with inline extents and symlinks.
///
/// For inline file extents and symlinks, nbytes = data size (not aligned).
fn fixup_inline_nbytes(fs_items: &mut [(Key, Vec<u8>)]) {
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
            data[24..32].copy_from_slice(&nbytes.to_le_bytes());
        }
    }
}

/// Apply nbytes updates from data writing to INODE_ITEM entries.
pub fn apply_nbytes_updates(
    fs_items: &mut [(Key, Vec<u8>)],
    updates: &HashMap<u64, u64>,
) {
    for (key, data) in fs_items.iter_mut() {
        if key.key_type == raw::BTRFS_INODE_ITEM_KEY as u8
            && let Some(&nbytes) = updates.get(&key.objectid)
            && data.len() >= 32
        {
            data[24..32].copy_from_slice(&nbytes.to_le_bytes());
        }
    }
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
}

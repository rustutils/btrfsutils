use crate::{RunContext, Runnable, util::is_mounted};
use anyhow::{Context, Result, bail};
use btrfs_disk::{
    items::{
        CompressionType, DirItem, FileExtentBody, FileExtentItem,
        FileExtentType, FileType, InodeItem, RootItem,
    },
    raw, reader,
    superblock::SUPER_MIRROR_MAX,
    tree::{DiskKey, KeyType, TreeBlock},
};
use clap::Parser;
use regex_lite::Regex;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::symlink,
    path::{Path, PathBuf},
};

/// Try to restore files from a damaged filesystem (unmounted).
///
/// Attempt to recover files from a damaged or inaccessible btrfs filesystem
/// by scanning the raw filesystem structures. This command works on unmounted
/// devices and can recover files even when the filesystem cannot be mounted
/// normally. Recovery options allow selective restoration of files, metadata,
/// and extended attributes. Requires CAP_SYS_ADMIN.
#[derive(Parser, Debug)]
#[allow(clippy::doc_markdown, clippy::struct_excessive_bools)]
pub struct RestoreCommand {
    /// Block device containing the damaged filesystem
    device: PathBuf,

    /// Destination path for recovered files (not needed with --list-roots)
    path: Option<PathBuf>,

    /// Dry run (only list files that would be recovered)
    #[clap(short = 'D', long = "dry-run")]
    dry_run: bool,

    /// Ignore errors
    #[clap(short = 'i', long)]
    ignore_errors: bool,

    /// Overwrite existing files
    #[clap(short = 'o', long)]
    overwrite: bool,

    /// Restore owner, mode and times
    #[clap(short = 'm', long)]
    metadata: bool,

    /// Restore symbolic links
    #[clap(short = 'S', long)]
    symlink: bool,

    /// Get snapshots
    #[clap(short = 's', long)]
    snapshots: bool,

    /// Restore extended attributes
    #[clap(short = 'x', long)]
    xattr: bool,

    /// Restore only filenames matching regex
    #[clap(long)]
    path_regex: Option<String>,

    /// Ignore case (used with --path-regex)
    #[clap(short = 'c', long)]
    ignore_case: bool,

    /// Find dir
    #[clap(short = 'd', long)]
    find_dir: bool,

    /// List tree roots
    #[clap(short = 'l', long)]
    list_roots: bool,

    /// Verbose (use twice for extra detail)
    #[clap(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Filesystem location (bytenr)
    #[clap(short = 'f', long)]
    fs_location: Option<u64>,

    /// Root objectid
    #[clap(short = 'r', long)]
    root: Option<u64>,

    /// Tree location (bytenr)
    #[clap(short = 't', long)]
    tree_location: Option<u64>,

    /// Super mirror index (0, 1, or 2)
    #[clap(short = 'u', long = "super")]
    super_mirror: Option<u64>,
}

impl Runnable for RestoreCommand {
    fn supports_dry_run(&self) -> bool {
        true
    }

    #[allow(clippy::too_many_lines)]
    fn run(&self, _ctx: &RunContext) -> Result<()> {
        if let Some(m) = self.super_mirror
            && m >= u64::from(SUPER_MIRROR_MAX)
        {
            bail!(
                "super mirror index {m} is out of range (max {})",
                SUPER_MIRROR_MAX - 1
            );
        }

        if is_mounted(&self.device) {
            bail!(
                "'{}' is mounted, refusing to restore (unmount first)",
                self.device.display()
            );
        }

        let file = File::open(&self.device).with_context(|| {
            format!("cannot open '{}'", self.device.display())
        })?;

        // Open filesystem, trying mirror fallback if no specific mirror given.
        let mut open = if let Some(m) = self.super_mirror {
            #[allow(clippy::cast_possible_truncation)] // mirror index fits u32
            reader::filesystem_open_mirror(file, m as u32)
                .context("failed to open filesystem")?
        } else {
            let mut result = None;
            for mirror in 0..SUPER_MIRROR_MAX {
                match reader::filesystem_open_mirror(file.try_clone()?, mirror)
                {
                    Ok(o) => {
                        if mirror > 0 {
                            eprintln!(
                                "using superblock mirror {mirror} \
                                 (primary was damaged)"
                            );
                        }
                        result = Some(o);
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: superblock mirror {mirror} \
                             failed: {e}"
                        );
                    }
                }
            }
            result.context("all superblock mirrors failed")?
        };

        if self.list_roots {
            let root_bytenr =
                self.tree_location.unwrap_or(open.superblock.root);
            return list_roots(&mut open.reader, root_bytenr);
        }

        let output_path = self.path.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "destination path is required (unless --list-roots)"
            )
        })?;

        // Compile path regex if specified.
        let path_regex = self
            .path_regex
            .as_ref()
            .map(|pat| {
                let full = if self.ignore_case {
                    format!("(?i){pat}")
                } else {
                    pat.clone()
                };
                Regex::new(&full)
                    .with_context(|| format!("invalid regex '{pat}'"))
            })
            .transpose()?;

        // Determine which FS tree to restore from.
        let fs_tree_oid =
            self.root.unwrap_or(u64::from(raw::BTRFS_FS_TREE_OBJECTID));

        // Determine the FS tree root bytenr.
        let fs_root_bytenr = if let Some(loc) = self.fs_location {
            loc
        } else {
            open.tree_roots
                .get(&fs_tree_oid)
                .map(|(bytenr, _)| *bytenr)
                .with_context(|| {
                    format!("tree root for objectid {fs_tree_oid} not found")
                })?
        };

        let mut block_reader = open.reader;

        let opts = RestoreOpts {
            dry_run: self.dry_run,
            overwrite: self.overwrite,
            metadata: self.metadata,
            symlinks: self.symlink,
            snapshots: self.snapshots,
            xattr: self.xattr,
            ignore_errors: self.ignore_errors,
            verbose: self.verbose,
            path_regex: path_regex.as_ref(),
            tree_roots: &open.tree_roots,
        };

        let mut total_errors = 0;

        // Restore the primary FS tree.
        let items = collect_fs_tree_items(
            &mut block_reader,
            fs_root_bytenr,
            self.ignore_errors,
        )?;

        // Determine the starting objectid.
        let root_ino = if self.find_dir {
            let oid = find_first_dir(&items)?;
            println!("Using objectid {oid} for first dir");
            oid
        } else {
            u64::from(raw::BTRFS_FIRST_FREE_OBJECTID)
        };

        if !opts.dry_run {
            fs::create_dir_all(output_path).with_context(|| {
                format!(
                    "failed to create output directory '{}'",
                    output_path.display()
                )
            })?;
        }

        restore_dir(
            &mut block_reader,
            &items,
            root_ino,
            output_path,
            &opts,
            &mut total_errors,
            "",
        )?;

        // Restore snapshots if requested (handled inline via RootItem
        // entries during traversal above, but also restore any trees
        // that weren't reachable from the FS tree's directory structure).
        if self.snapshots {
            for (&oid, &(bytenr, _)) in &open.tree_roots {
                #[allow(clippy::cast_sign_loss)]
                let last_free = raw::BTRFS_LAST_FREE_OBJECTID as u64;
                if oid >= u64::from(raw::BTRFS_FIRST_FREE_OBJECTID)
                    && oid <= last_free
                    && oid != fs_tree_oid
                {
                    let snap_dest = output_path.join(format!("snapshot.{oid}"));
                    // Skip if already restored inline during directory walk.
                    if snap_dest.exists() {
                        continue;
                    }
                    let snap_items = collect_fs_tree_items(
                        &mut block_reader,
                        bytenr,
                        self.ignore_errors,
                    )?;
                    if !opts.dry_run {
                        fs::create_dir_all(&snap_dest).with_context(|| {
                            format!(
                                "failed to create snapshot directory '{}'",
                                snap_dest.display()
                            )
                        })?;
                    }
                    let snap_root = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);
                    restore_dir(
                        &mut block_reader,
                        &snap_items,
                        snap_root,
                        &snap_dest,
                        &opts,
                        &mut total_errors,
                        "",
                    )?;
                }
            }
        }

        if total_errors > 0 {
            eprintln!("warning: {total_errors} error(s) during restore");
        }

        Ok(())
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RestoreOpts<'a> {
    dry_run: bool,
    overwrite: bool,
    metadata: bool,
    symlinks: bool,
    snapshots: bool,
    xattr: bool,
    ignore_errors: bool,
    verbose: u8,
    path_regex: Option<&'a Regex>,
    tree_roots: &'a std::collections::BTreeMap<u64, (u64, u64)>,
}

/// Collected items from a single FS tree, grouped by objectid.
struct FsTreeItems {
    items: HashMap<u64, Vec<(DiskKey, Vec<u8>)>>,
}

impl FsTreeItems {
    /// Get all items for an objectid with a specific key type.
    fn get(&self, objectid: u64, key_type: KeyType) -> Vec<(&DiskKey, &[u8])> {
        self.items
            .get(&objectid)
            .map(|v| {
                v.iter()
                    .filter(|(k, _)| k.key_type == key_type)
                    .map(|(k, d)| (k, d.as_slice()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if any items exist with a given key type (any objectid).
    fn has_key_type(&self, key_type: KeyType) -> Option<u64> {
        for (oid, entries) in &self.items {
            if entries.iter().any(|(k, _)| k.key_type == key_type) {
                return Some(*oid);
            }
        }
        None
    }
}

/// Walk the FS tree once and collect all items grouped by objectid.
fn collect_fs_tree_items<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    root_bytenr: u64,
    ignore_errors: bool,
) -> Result<FsTreeItems> {
    let mut items: HashMap<u64, Vec<(DiskKey, Vec<u8>)>> = HashMap::new();
    let mut errors = 0u64;
    collect_items_dfs(
        reader,
        root_bytenr,
        &mut items,
        ignore_errors,
        &mut errors,
    )?;
    if errors > 0 {
        eprintln!(
            "warning: {errors} tree block(s) could not be read during scan"
        );
    }
    Ok(FsTreeItems { items })
}

fn collect_items_dfs<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    logical: u64,
    items: &mut HashMap<u64, Vec<(DiskKey, Vec<u8>)>>,
    ignore_errors: bool,
    errors: &mut u64,
) -> Result<()> {
    let block = match reader.read_tree_block(logical) {
        Ok(b) => b,
        Err(e) => {
            if ignore_errors {
                eprintln!(
                    "warning: skipping unreadable tree block at \
                     logical {logical}: {e}"
                );
                *errors += 1;
                return Ok(());
            }
            return Err(e).with_context(|| {
                format!("failed to read tree block at {logical}")
            });
        }
    };

    match &block {
        TreeBlock::Leaf {
            items: leaf_items,
            data,
            ..
        } => {
            let header_size = std::mem::size_of::<raw::btrfs_header>();
            for item in leaf_items {
                let start = header_size + item.offset as usize;
                let end = start + item.size as usize;
                if end <= data.len() {
                    items
                        .entry(item.key.objectid)
                        .or_default()
                        .push((item.key, data[start..end].to_vec()));
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                collect_items_dfs(
                    reader,
                    ptr.blockptr,
                    items,
                    ignore_errors,
                    errors,
                )?;
            }
        }
    }

    Ok(())
}

/// Find the first `DIR_INDEX` item in the tree, returning its objectid.
fn find_first_dir(items: &FsTreeItems) -> Result<u64> {
    items
        .has_key_type(KeyType::DirIndex)
        .context("no directory entry found in tree")
}

/// Recursively restore a directory and its contents.
#[allow(clippy::too_many_lines)]
fn restore_dir<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    items: &FsTreeItems,
    dir_ino: u64,
    output_path: &Path,
    opts: &RestoreOpts,
    errors: &mut u64,
    prefix: &str,
) -> Result<()> {
    // Get DIR_INDEX items for this directory (sorted by index = key.offset).
    let dir_entries = items.get(dir_ino, KeyType::DirIndex);

    for (_key, data) in &dir_entries {
        let parsed = DirItem::parse_all(data);
        for entry in parsed {
            let name = match std::str::from_utf8(&entry.name) {
                Ok(s) => s.to_string(),
                Err(_) => String::from_utf8_lossy(&entry.name).into_owned(),
            };
            let child_path = output_path.join(&name);
            let child_ino = entry.location.objectid;

            // Build the relative path for regex matching.
            let rel_path = if prefix.is_empty() {
                format!("/{name}")
            } else {
                format!("{prefix}/{name}")
            };

            // Check path regex filter (applies to both files and directories,
            // matching C reference behavior).
            if let Some(re) = opts.path_regex
                && !re.is_match(&rel_path)
            {
                continue;
            }

            // Subvolume/snapshot entries have location.key_type == RootItem.
            // Their data lives in a separate tree.
            if entry.location.key_type == KeyType::RootItem {
                if opts.snapshots {
                    // Restore the snapshot/subvolume inline.
                    let subvol_oid = entry.location.objectid;
                    if let Some(&(bytenr, _)) = opts.tree_roots.get(&subvol_oid)
                        && let Err(e) = restore_snapshot(
                            reader,
                            bytenr,
                            &child_path,
                            opts,
                            errors,
                            &rel_path,
                        )
                    {
                        if !opts.ignore_errors {
                            return Err(e);
                        }
                        eprintln!(
                            "warning: failed to restore snapshot '{}': {e}",
                            child_path.display()
                        );
                        *errors += 1;
                    }
                } else {
                    eprintln!("Skipping snapshot {name} (use -s to restore)");
                }
                continue;
            }

            match entry.file_type {
                FileType::Dir => {
                    if opts.dry_run {
                        println!("{}/", child_path.display());
                    } else {
                        if opts.verbose >= 1 {
                            eprintln!("Restoring {}/", child_path.display());
                        }
                        if let Err(e) = fs::create_dir_all(&child_path) {
                            if !opts.ignore_errors {
                                return Err(e).with_context(|| {
                                    format!(
                                        "failed to create directory '{}'",
                                        child_path.display()
                                    )
                                });
                            }
                            eprintln!(
                                "warning: failed to create '{}': {e}",
                                child_path.display()
                            );
                            *errors += 1;
                            continue;
                        }
                    }
                    restore_dir(
                        reader,
                        items,
                        child_ino,
                        &child_path,
                        opts,
                        errors,
                        &rel_path,
                    )?;
                    // Apply metadata after all children are written so
                    // timestamps are not clobbered by child writes.
                    if opts.metadata && !opts.dry_run {
                        apply_metadata(
                            items,
                            child_ino,
                            &child_path,
                            opts,
                            errors,
                        );
                    }
                }
                FileType::RegFile => {
                    if let Err(e) = restore_file(
                        reader,
                        items,
                        child_ino,
                        &child_path,
                        opts,
                        errors,
                    ) {
                        if !opts.ignore_errors {
                            return Err(e);
                        }
                        eprintln!(
                            "warning: failed to restore '{}': {e}",
                            child_path.display()
                        );
                        *errors += 1;
                    }
                }
                FileType::Symlink if opts.symlinks => {
                    if let Err(e) =
                        restore_symlink(items, child_ino, &child_path, opts)
                    {
                        if !opts.ignore_errors {
                            return Err(e);
                        }
                        eprintln!(
                            "warning: failed to restore symlink '{}': {e}",
                            child_path.display()
                        );
                        *errors += 1;
                    }
                    if opts.metadata && !opts.dry_run {
                        apply_metadata(
                            items,
                            child_ino,
                            &child_path,
                            opts,
                            errors,
                        );
                    }
                }
                _ => {}
            }

            // Restore xattrs if requested (works for files, dirs, and symlinks).
            if opts.xattr && !opts.dry_run {
                restore_xattrs(items, child_ino, &child_path, errors);
            }
        }
    }

    Ok(())
}

/// Restore a snapshot/subvolume by loading its separate tree.
fn restore_snapshot<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    bytenr: u64,
    output_path: &Path,
    opts: &RestoreOpts,
    errors: &mut u64,
    prefix: &str,
) -> Result<()> {
    let snap_items = collect_fs_tree_items(reader, bytenr, opts.ignore_errors)?;

    if !opts.dry_run {
        fs::create_dir_all(output_path).with_context(|| {
            format!(
                "failed to create snapshot directory '{}'",
                output_path.display()
            )
        })?;
    }

    let snap_root = u64::from(raw::BTRFS_FIRST_FREE_OBJECTID);
    restore_dir(
        reader,
        &snap_items,
        snap_root,
        output_path,
        opts,
        errors,
        prefix,
    )
}

/// Restore a regular file from its `EXTENT_DATA` items.
#[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
fn restore_file<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    items: &FsTreeItems,
    ino: u64,
    path: &Path,
    opts: &RestoreOpts,
    errors: &mut u64,
) -> Result<()> {
    if opts.dry_run {
        println!("{}", path.display());
        return Ok(());
    }

    if path.exists() && !opts.overwrite {
        return Ok(());
    }

    if opts.verbose >= 1 {
        eprintln!("Restoring {}", path.display());
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to create '{}'", path.display()))?;

    // Get inode size for final truncation.
    let inode_size = items
        .get(ino, KeyType::InodeItem)
        .first()
        .and_then(|(_, d)| InodeItem::parse(d))
        .map(|i| i.size);

    let extent_items = items.get(ino, KeyType::ExtentData);

    for (key, data) in &extent_items {
        let Some(extent) = FileExtentItem::parse(data) else {
            continue;
        };

        // Skip prealloc extents: they represent preallocated but
        // uninitialized blocks.
        if extent.extent_type == FileExtentType::Prealloc {
            continue;
        }

        let file_offset = key.offset;

        match &extent.body {
            FileExtentBody::Inline { inline_size } => {
                // Inline data is stored after the fixed-size header in the item.
                let header_len = data.len() - inline_size;
                let inline_data = &data[header_len..];

                let output = if extent.compression == CompressionType::None {
                    inline_data.to_vec()
                } else {
                    decompress(
                        inline_data,
                        extent.ram_bytes as usize,
                        extent.compression,
                    )
                    .with_context(|| {
                        format!(
                            "failed to decompress inline extent in '{}'",
                            path.display()
                        )
                    })?
                };

                file.seek(io::SeekFrom::Start(file_offset))?;
                file.write_all(&output).with_context(|| {
                    format!(
                        "failed to write inline extent to '{}'",
                        path.display()
                    )
                })?;
            }
            FileExtentBody::Regular {
                disk_bytenr,
                disk_num_bytes,
                offset,
                num_bytes,
            } => {
                if *disk_bytenr == 0 {
                    // Hole — seek past it.
                    continue;
                }

                // Validate extent offset bounds.
                if extent.compression == CompressionType::None
                    && *offset >= *disk_num_bytes
                {
                    eprintln!(
                        "warning: bogus extent offset {} >= disk_size {} \
                         in '{}'",
                        offset,
                        disk_num_bytes,
                        path.display()
                    );
                    *errors += 1;
                    continue;
                }
                if *offset > extent.ram_bytes {
                    eprintln!(
                        "warning: bogus extent offset {} > ram_bytes {} \
                         in '{}'",
                        offset,
                        extent.ram_bytes,
                        path.display()
                    );
                    *errors += 1;
                    continue;
                }

                if extent.compression == CompressionType::None {
                    // Uncompressed: read directly from disk at the right offset.
                    let data_buf = reader
                        .read_data(disk_bytenr + offset, *num_bytes as usize)
                        .with_context(|| {
                            format!(
                                "failed to read extent at logical {disk_bytenr}"
                            )
                        })?;

                    file.seek(io::SeekFrom::Start(file_offset))?;
                    file.write_all(&data_buf).with_context(|| {
                        format!(
                            "failed to write extent to '{}'",
                            path.display()
                        )
                    })?;
                } else {
                    // Read the full compressed extent from disk.
                    let compressed = reader
                        .read_data(*disk_bytenr, *disk_num_bytes as usize)
                        .with_context(|| {
                            format!(
                                "failed to read compressed extent at logical {disk_bytenr}"
                            )
                        })?;

                    let decompressed = decompress(
                        &compressed,
                        extent.ram_bytes as usize,
                        extent.compression,
                    )
                    .with_context(|| {
                        format!(
                            "failed to decompress extent in '{}'",
                            path.display()
                        )
                    })?;

                    // Extract the portion we need (offset..offset+num_bytes).
                    let start = *offset as usize;
                    let end = start + *num_bytes as usize;
                    let slice = if end <= decompressed.len() {
                        &decompressed[start..end]
                    } else {
                        &decompressed[start..]
                    };

                    file.seek(io::SeekFrom::Start(file_offset))?;
                    file.write_all(slice).with_context(|| {
                        format!(
                            "failed to write extent to '{}'",
                            path.display()
                        )
                    })?;
                }
            }
        }
    }

    // Truncate file to correct inode size (handles sparse files and
    // files where the last extent doesn't extend to EOF).
    if let Some(size) = inode_size {
        file.set_len(size)?;
    }

    if opts.metadata {
        // Drop the file handle first so metadata applies cleanly.
        drop(file);
        apply_metadata(items, ino, path, opts, errors);
    }

    Ok(())
}

/// Restore a symbolic link from its inline `EXTENT_DATA` item.
fn restore_symlink(
    items: &FsTreeItems,
    ino: u64,
    path: &Path,
    opts: &RestoreOpts,
) -> Result<()> {
    let extent_items = items.get(ino, KeyType::ExtentData);
    let (_, data) = extent_items
        .first()
        .context("symlink has no EXTENT_DATA item")?;

    let extent = FileExtentItem::parse(data)
        .context("failed to parse symlink extent")?;

    let target = match &extent.body {
        FileExtentBody::Inline { inline_size } => {
            let header_len = data.len() - inline_size;
            &data[header_len..]
        }
        FileExtentBody::Regular { .. } => bail!("symlink extent is not inline"),
    };

    let target_str = std::str::from_utf8(target)
        .context("symlink target is not valid UTF-8")?;

    if opts.dry_run {
        println!("{} -> {}", path.display(), target_str);
        return Ok(());
    }

    if path.exists() && !opts.overwrite {
        return Ok(());
    }

    if opts.verbose >= 2 {
        eprintln!("SYMLINK: '{}' => '{}'", path.display(), target_str);
    }

    // Remove existing entry if overwriting.
    if path.exists() {
        fs::remove_file(path).ok();
    }

    symlink(target_str, path).with_context(|| {
        format!("failed to create symlink '{}'", path.display())
    })?;

    Ok(())
}

/// Restore extended attributes for a file/directory/symlink.
fn restore_xattrs(
    items: &FsTreeItems,
    ino: u64,
    path: &Path,
    errors: &mut u64,
) {
    let xattr_items = items.get(ino, KeyType::XattrItem);
    for (_, data) in &xattr_items {
        let entries = DirItem::parse_all(data);
        for entry in entries {
            let Ok(name) = std::str::from_utf8(&entry.name) else {
                continue;
            };
            let Ok(c_path) =
                std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
            else {
                continue;
            };
            let Ok(c_name) = std::ffi::CString::new(name) else {
                continue;
            };
            // SAFETY: calling lsetxattr with valid C strings and data pointer.
            let ret = unsafe {
                libc::lsetxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    entry.data.as_ptr().cast(),
                    entry.data.len(),
                    0,
                )
            };
            if ret < 0 {
                let err = io::Error::last_os_error();
                eprintln!(
                    "warning: failed to set xattr '{name}' on '{}': {err}",
                    path.display()
                );
                *errors += 1;
            }
        }
    }
}

/// Apply inode metadata (uid, gid, mode, times) to a restored file.
fn apply_metadata(
    items: &FsTreeItems,
    ino: u64,
    path: &Path,
    opts: &RestoreOpts,
    errors: &mut u64,
) {
    let inode_items = items.get(ino, KeyType::InodeItem);
    let Some((_, data)) = inode_items.first() else {
        return;
    };
    let Some(inode) = InodeItem::parse(data) else {
        return;
    };

    let Ok(c_path) =
        std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
    else {
        return;
    };

    // SAFETY: calling POSIX functions with valid C string path.
    unsafe {
        if libc::lchown(c_path.as_ptr(), inode.uid, inode.gid) < 0 {
            let err = io::Error::last_os_error();
            eprintln!("warning: failed to chown '{}': {err}", path.display());
            *errors += 1;
            if !opts.ignore_errors {
                return;
            }
        }
        // Don't chmod symlinks.
        if !path.is_symlink()
            && libc::chmod(c_path.as_ptr(), inode.mode & 0o7777) < 0
        {
            let err = io::Error::last_os_error();
            eprintln!("warning: failed to chmod '{}': {err}", path.display());
            *errors += 1;
            if !opts.ignore_errors {
                return;
            }
        }

        #[allow(clippy::cast_possible_wrap)] // timestamps fit in i64
        let times = [
            libc::timespec {
                tv_sec: inode.atime.sec as i64,
                tv_nsec: i64::from(inode.atime.nsec),
            },
            libc::timespec {
                tv_sec: inode.mtime.sec as i64,
                tv_nsec: i64::from(inode.mtime.nsec),
            },
        ];
        if libc::utimensat(
            libc::AT_FDCWD,
            c_path.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        ) < 0
        {
            let err = io::Error::last_os_error();
            eprintln!(
                "warning: failed to set times on '{}': {err}",
                path.display()
            );
            *errors += 1;
        }
    }
}

/// Decompress extent data based on the compression type.
fn decompress(
    data: &[u8],
    output_len: usize,
    compression: CompressionType,
) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zlib => {
            let mut decoder = flate2::read::ZlibDecoder::new(data);
            let mut out = vec![0u8; output_len];
            decoder
                .read_exact(&mut out)
                .context("zlib decompression failed")?;
            Ok(out)
        }
        CompressionType::Zstd => zstd::bulk::decompress(data, output_len)
            .context("zstd decompression failed"),
        CompressionType::Lzo => decompress_lzo(data, output_len),
        CompressionType::Unknown(t) => {
            bail!("unsupported compression type {t}")
        }
    }
}

/// Decompress btrfs LZO format: sector-by-sector LZO1X compression.
///
/// Format: 4-byte LE total length, then per-sector: 4-byte LE segment
/// length + compressed data, padded to sector boundaries.
fn decompress_lzo(data: &[u8], output_len: usize) -> Result<Vec<u8>> {
    const SECTOR_SIZE: usize = 4096;

    if data.len() < 4 {
        bail!("LZO data too short for header");
    }
    let total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if total_len > data.len() {
        bail!(
            "LZO total length {total_len} exceeds data length {}",
            data.len()
        );
    }

    let mut out = Vec::with_capacity(output_len);
    let mut pos = 4;

    while pos < total_len && out.len() < output_len {
        let sector_remaining = SECTOR_SIZE - (pos % SECTOR_SIZE);
        if sector_remaining < 4 {
            if total_len - pos <= sector_remaining {
                break;
            }
            pos += sector_remaining;
        }

        if pos + 4 > total_len {
            bail!("LZO segment header truncated at offset {pos}");
        }
        let seg_len =
            u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + seg_len > data.len() {
            bail!(
                "LZO segment data truncated at offset {pos}, \
                 need {seg_len} bytes"
            );
        }

        let remaining = (output_len - out.len()).min(SECTOR_SIZE);
        let mut segment_out = vec![0u8; remaining];
        lzokay::decompress::decompress(
            &data[pos..pos + seg_len],
            &mut segment_out,
        )
        .map_err(|e| {
            anyhow::anyhow!("LZO decompression failed at offset {pos}: {e:?}")
        })?;
        out.extend_from_slice(&segment_out);

        pos += seg_len;
    }

    out.truncate(output_len);
    Ok(out)
}

/// List all tree roots found in the root tree.
fn list_roots<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    root_bytenr: u64,
) -> Result<()> {
    let mut entries: Vec<(DiskKey, RootItem)> = Vec::new();
    collect_root_items_for_listing(reader, root_bytenr, &mut entries)?;

    // Sort by objectid for deterministic output.
    entries.sort_by_key(|(k, _)| k.objectid);

    for (key, root_item) in &entries {
        println!(
            " tree key ({} ROOT_ITEM {}) {} level {}",
            key.objectid, key.offset, root_item.bytenr, root_item.level
        );
    }

    Ok(())
}

fn collect_root_items_for_listing<R: Read + Seek>(
    reader: &mut reader::BlockReader<R>,
    logical: u64,
    out: &mut Vec<(DiskKey, RootItem)>,
) -> Result<()> {
    let block = reader
        .read_tree_block(logical)
        .with_context(|| format!("failed to read tree block at {logical}"))?;

    match &block {
        TreeBlock::Leaf {
            items: leaf_items,
            data,
            ..
        } => {
            let header_size = std::mem::size_of::<raw::btrfs_header>();
            for item in leaf_items {
                if item.key.key_type != KeyType::RootItem {
                    continue;
                }
                let start = header_size + item.offset as usize;
                let end = start + item.size as usize;
                if end > data.len() {
                    continue;
                }
                if let Some(ri) = RootItem::parse(&data[start..end]) {
                    out.push((item.key, ri));
                }
            }
        }
        TreeBlock::Node { ptrs, .. } => {
            for ptr in ptrs {
                collect_root_items_for_listing(reader, ptr.blockptr, out)?;
            }
        }
    }

    Ok(())
}

//! Xattr enumeration and lookup.
//!
//! btrfs stores xattrs as `XATTR_ITEM` entries with the same `DirItem`
//! wire format as directory entries: key `(ino, XATTR_ITEM, crc32c(name))`.
//! The `DirItem.name` field is the xattr name and `DirItem.data` is the
//! xattr value. Multiple xattrs with the same hash bucket share one item
//! (packed `DirItem` list), so name lookup must scan all entries in the
//! bucket after finding the item.

use btrfs_disk::{
    items::DirItem,
    reader::{BlockReader, Traversal, tree_walk},
    tree::{KeyType, TreeBlock},
};
use std::{io, mem};

/// Return all xattr names for `oid`.
pub(crate) fn list_xattrs<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
) -> io::Result<Vec<Vec<u8>>> {
    let mut names: Vec<Vec<u8>> = Vec::new();
    tree_walk(reader, fs_tree_root, Traversal::Dfs, &mut |block| {
        let TreeBlock::Leaf { items, data, .. } = block else {
            return;
        };
        let hdr = mem::size_of::<btrfs_disk::raw::btrfs_header>();
        for item in items {
            if item.key.objectid != oid
                || item.key.key_type != KeyType::XattrItem
            {
                continue;
            }
            let start = hdr + item.offset as usize;
            let end = start + item.size as usize;
            if end > data.len() {
                continue;
            }
            for entry in DirItem::parse_all(&data[start..end]) {
                names.push(entry.name);
            }
        }
    })?;
    Ok(names)
}

/// Look up the value of a single xattr by exact name.
///
/// Returns `None` if the xattr does not exist. Multiple entries can share
/// the same `XATTR_ITEM` key (hash collision), so all packed entries are
/// scanned for an exact name match.
pub(crate) fn get_xattr<R: io::Read + io::Seek>(
    reader: &mut BlockReader<R>,
    fs_tree_root: u64,
    oid: u64,
    name: &[u8],
) -> io::Result<Option<Vec<u8>>> {
    let mut result = None;
    tree_walk(reader, fs_tree_root, Traversal::Dfs, &mut |block| {
        if result.is_some() {
            return;
        }
        let TreeBlock::Leaf { items, data, .. } = block else {
            return;
        };
        let hdr = mem::size_of::<btrfs_disk::raw::btrfs_header>();
        for item in items {
            if item.key.objectid != oid
                || item.key.key_type != KeyType::XattrItem
            {
                continue;
            }
            let start = hdr + item.offset as usize;
            let end = start + item.size as usize;
            if end > data.len() {
                continue;
            }
            for entry in DirItem::parse_all(&data[start..end]) {
                if entry.name == name {
                    result = Some(entry.data);
                    return;
                }
            }
        }
    })?;
    Ok(result)
}

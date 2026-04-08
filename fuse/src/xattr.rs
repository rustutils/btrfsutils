//! Xattr enumeration and lookup.
//!
//! Placeholder for milestone M5. btrfs stores xattrs as items keyed
//! `(ino, XATTR_ITEM, crc32c(name))` whose payload is parsed identically to
//! `DirItem` (`DirItem::parse_all` handles them — the `data` field carries
//! the xattr value).
//!
//! - `listxattr`: walk all `XATTR_ITEM` items for the inode and concatenate
//!   `name\0`.
//! - `getxattr`: hash the requested name with btrfs' crc32c seed, look up
//!   the item, then linear-scan the packed entries for an exact name match
//!   (hash collisions exist).

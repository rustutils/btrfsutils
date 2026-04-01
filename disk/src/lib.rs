//! # Parsing and serializing of btrfs on-disk data structures
//!
//! This crate reads and parses btrfs on-disk structures directly from block
//! devices or image files, without going through the kernel. Unlike
//! `btrfs-uapi` (which wraps Linux-only ioctls), this crate is
//! platform-independent: any system that can read raw bytes from a block
//! device or image file can use it.
//!
//! ## Filesystem layout
//!
//! A btrfs filesystem is organized as a forest of copy-on-write B-trees stored
//! on one or more block devices. Every tree block is `nodesize` bytes
//! (typically 16 KiB). The filesystem uses logical addresses internally; a
//! chunk tree maps logical address ranges to physical device offsets.
//!
//! **Superblock.** The entry point into the filesystem. A fixed-size 4 KiB
//! structure written at known byte offsets on each device (primary at 64 KiB,
//! mirrors at 64 MiB and 256 GiB). It stores the filesystem UUID, feature
//! flags, sizes, the checksum algorithm, and root pointers for bootstrapping
//! the tree hierarchy. It also embeds a copy of the system chunk array (enough
//! chunk mappings to locate the chunk tree itself). See [`superblock`].
//!
//! **Trees.** Each tree is a B-tree whose internal nodes contain key pointers
//! ([`tree::KeyPtr`]) to child blocks and whose leaves contain items
//! ([`tree::Item`]) with typed data payloads. Every item is addressed by a
//! three-part key: `(objectid, type, offset)` ([`tree::DiskKey`]). The major
//! trees are:
//!
//! - Root tree (objectid 1): contains [`items::RootItem`] entries that point
//!   to the root block of every other tree.
//! - Chunk tree (objectid 3): maps logical address ranges to physical device
//!   stripes ([`items::ChunkItem`]).
//! - FS tree (objectid 5, plus one per subvolume/snapshot): holds the actual
//!   filesystem content: inodes ([`items::InodeItem`]), directory entries
//!   ([`items::DirItem`]), file extents ([`items::FileExtentItem`]), and
//!   extended attributes.
//! - Extent tree (objectid 2): tracks space allocation and backreferences
//!   ([`items::ExtentItem`], [`items::BlockGroupItem`]).
//! - Device tree (objectid 4): per-device extent allocation
//!   ([`items::DeviceExtent`]).
//! - Checksum tree (objectid 7): per-block data checksums.
//! - Quota tree (objectid 8): quota group accounting
//!   ([`items::QgroupInfo`], [`items::QgroupLimit`]).
//! - UUID tree (objectid 9): fast subvolume UUID lookups.
//! - Free space tree (objectid 10): free space tracking
//!   ([`items::FreeSpaceInfo`]).
//! - Block group tree (objectid 11): block group items, separated from the
//!   extent tree for faster mount.
//!
//! ## Reading a filesystem
//!
//! Open a block device or image file and bootstrap the tree hierarchy with
//! [`reader::filesystem_open`]. This reads the superblock, seeds the chunk
//! cache from the `sys_chunk_array`, reads the full chunk tree, then walks the
//! root tree to discover all tree roots. The returned
//! [`reader::OpenFilesystem`] gives you a [`reader::BlockReader`] (for reading
//! tree blocks by logical address) and a map of tree roots.
//!
//! To walk a tree, use [`reader::tree_walk`] with a visitor callback, or read
//! individual blocks with [`reader::BlockReader::read_tree_block`] and match
//! on [`tree::TreeBlock`]. Parse leaf item payloads with
//! [`items::parse_item_payload`] or the individual struct `parse` methods.

// Note: missing_docs is not enabled as a warning because bitflags! generates
// undocumentable `const _ = !0` associated constants. All public items are
// documented — this was verified manually.

pub mod chunk;
pub mod items;
#[allow(missing_docs)]
pub mod raw;
pub mod reader;
pub mod superblock;
pub mod tree;
pub mod util;

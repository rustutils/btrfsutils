# Btrfs On-Disk Format Specification

This document describes the binary layout of btrfs on-disk structures, as
implemented by the parser in `disk/src/` and the serializer in `mkfs/src/`.
All multi-byte integer fields are little-endian. All byte offsets are
zero-based unless noted otherwise.

Kernel struct names appear in parentheses where helpful (for example
`btrfs_super_block`, `btrfs_header`). The authoritative source is the Linux
kernel UAPI headers `btrfs.h` and `btrfs_tree.h`.

**Conventions used in this document:**
- "LE u64" means a 64-bit unsigned integer stored in little-endian byte order.
- Byte offsets are from the start of the enclosing structure.
- Field sizes are in bytes unless noted otherwise.
- "Logical address" is an address in btrfs's virtual address space. It must
  be resolved to a physical device offset through the chunk tree.
- "Physical address" is a byte offset on a specific block device.


## Overview

Btrfs is a copy-on-write (COW) B-tree filesystem. All persistent data is
organized into B-trees. All B-trees share a single logical address space,
which a chunk and stripe layer maps to physical device locations.

### Architecture: trees within trees

The architecture is "trees within trees":

- The **superblock** (at fixed offsets on disk) bootstraps access to the
  chunk tree and root tree.
- The **chunk tree** maps logical addresses to physical device locations.
  A small subset of the chunk tree is embedded in the superblock to
  bootstrap access to the full tree.
- The **root tree** is the directory of all other trees: it contains a
  `ROOT_ITEM` for each tree, pointing to that tree's root block.
- Content trees (FS tree, extent tree, checksum tree, and so on) store the
  actual filesystem data and metadata.

### Copy-on-write semantics

Every modification creates new copies of the affected blocks (COW), from the
modified leaf up through the root of the tree. The final step atomically
updates the superblock to point to the new root tree root. This gives crash
consistency without a journal: at any point, the last successfully written
superblock points to a fully consistent tree hierarchy.

Tree blocks are never modified in place. Instead:

1. The leaf containing the modified item is written to a new location.
2. The parent node's key-pointer is updated to reference the new leaf,
   and the parent is written to a new location.
3. This propagates up to the tree root.
4. The root tree's ROOT_ITEM is updated with the new root block address.
5. The root tree itself is COWed up to its root.
6. The superblock is written with the new root tree root address.

The `generation` counter is incremented with each transaction. All blocks
written in a transaction share the same generation number.

### Shared format

All trees share the same block format (a header followed by items or
key-pointers) and the same key structure `(objectid, type, offset)`. The
block size (nodesize) is uniform across the filesystem, typically 16384
bytes. The sectorsize (typically 4096 bytes) is the minimum I/O unit for
data.

### Multi-device support

A single filesystem can span multiple devices. The chunk tree maps logical
addresses to physical offsets on specific devices. RAID profiles (SINGLE,
DUP, RAID0, RAID1, RAID5, RAID6, RAID10, RAID1C3, RAID1C4) determine how
chunks are distributed across devices.

### Bootstrap sequence

Reading a btrfs filesystem from a raw device takes these steps:

1. Read the superblock at offset 64 KiB. `restore` falls back to mirrors
   1 and 2 when the primary fails, and `check --super` selects a mirror
   explicitly.
2. Parse `sys_chunk_array` from the superblock to seed the chunk cache
   with system chunk mappings.
3. Resolve `chunk_root` through the chunk cache to a physical address.
4. Read the chunk tree root block and all chunk items to populate the
   full chunk cache.
5. Resolve `root` (root tree root) through the chunk cache.
6. Read the root tree to discover all other trees from their ROOT_ITEM
   entries.
7. Access any tree by resolving its root block address through the
   chunk cache.


## Superblock

The superblock (`btrfs_super_block`) is a 4096-byte structure stored at fixed
offsets on each device. It is the entry point for reading the filesystem.

### Mirror locations

The superblock has three copies (mirrors):

| Mirror | Offset         | Decimal         |
|--------|----------------|-----------------|
| 0      | 0x10000        | 65536 (64 KiB)  |
| 1      | 0x4000000      | 67108864 (64 MiB) |
| 2      | 0x4000000000   | 274877906944 (256 GiB) |

Mirror 0 is always present. Mirrors 1 and 2 are written only if the device
is large enough. The offsets are:

```
mirror 0:  64 KiB
mirror i:  16 KiB << (12 * i)    for i > 0
```

This project reads mirror 0 by default (`reader::filesystem_open`). Other
mirrors are read only on request or as a fallback (see the bootstrap
sequence above). `rescue super-recover` is the one command that compares
all mirrors: it treats the valid copies with the highest generation as
good and rewrites the rest from one of them.

### Binary layout

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `csum` | 0 | 32 | Checksum of bytes 32..4095 |
| `fsid` | 32 | 16 | Filesystem UUID (shared across devices) |
| `bytenr` | 48 | 8 | Physical offset of this superblock copy |
| `flags` | 56 | 8 | `BTRFS_SUPER_FLAG_*` flags |
| `magic` | 64 | 8 | `0x4D5F53665248425F` (`_BHRfS_M` LE) |
| `generation` | 72 | 8 | Transaction generation counter |
| `root` | 80 | 8 | Logical bytenr of root tree root |
| `chunk_root` | 88 | 8 | Logical bytenr of chunk tree root |
| `log_root` | 96 | 8 | Logical bytenr of log tree root (0 if none) |
| `__unused_log_root_transid` | 104 | 8 | Reserved, formerly log_root_transid |
| `total_bytes` | 112 | 8 | Total usable bytes across all devices |
| `bytes_used` | 120 | 8 | Total bytes used by data and metadata |
| `root_dir_objectid` | 128 | 8 | Objectid of root directory (always 6) |
| `num_devices` | 136 | 8 | Number of devices in this filesystem |
| `sectorsize` | 144 | 4 | Minimum I/O alignment (typically 4096) |
| `nodesize` | 148 | 4 | Tree block size in bytes (typically 16384) |
| `__unused_leafsize` | 152 | 4 | Legacy field, equal to nodesize |
| `stripesize` | 156 | 4 | Stripe size for RAID (typically 65536) |
| `sys_chunk_array_size` | 160 | 4 | Valid bytes in `sys_chunk_array` |
| `chunk_root_generation` | 164 | 8 | Generation of the chunk tree root |
| `compat_flags` | 172 | 8 | Compatible feature flags |
| `compat_ro_flags` | 180 | 8 | Compatible read-only feature flags |
| `incompat_flags` | 188 | 8 | Incompatible feature flags |
| `csum_type` | 196 | 2 | Checksum algorithm (0=CRC32C, 1=xxHash, 2=SHA256, 3=BLAKE2) |
| `root_level` | 198 | 1 | B-tree level of root tree root |
| `chunk_root_level` | 199 | 1 | B-tree level of chunk tree root |
| `log_root_level` | 200 | 1 | B-tree level of log tree root |
| `dev_item` | 201 | 98 | Embedded `btrfs_dev_item` for this device |
| `label` | 299 | 256 | Filesystem label (NUL-terminated, max 255 chars) |
| `cache_generation` | 555 | 8 | Generation of free space cache (v1) |
| `uuid_tree_generation` | 563 | 8 | Generation of UUID tree |
| `metadata_uuid` | 571 | 16 | Metadata UUID (when `METADATA_UUID` incompat set) |
| `nr_global_roots` | 587 | 8 | Number of global roots (extent-tree-v2) |
| `remap_root` | 595 | 8 | Logical bytenr of remap tree root |
| `remap_root_generation` | 603 | 8 | Generation of remap tree root |
| `remap_root_level` | 611 | 1 | B-tree level of remap tree root |
| `reserved` | 612 | 199 | Zero-filled |
| `sys_chunk_array` | 811 | 2048 | Bootstrap chunk items |
| `super_roots[4]` | 2859 | 672 | Four rotating backup root entries (168 bytes each) |
| (padding) | 3531 | 565 | Zero-filled to 4096 bytes |

Total: 4096 bytes (`BTRFS_SUPER_INFO_SIZE`).

### System chunk array bootstrap

The `sys_chunk_array` field embeds enough of the chunk tree to locate the
full chunk tree on disk. It contains a sequence of `(disk_key, chunk_item)`
pairs:

```
For each entry:
  17 bytes   btrfs_disk_key     (objectid, type, offset) -- offset = logical addr
  variable   btrfs_chunk        Chunk item
```

See [CHUNK_ITEM](#chunk_item-type-228) for the chunk item layout.

The array is parsed sequentially until `sys_chunk_array_size` bytes are
consumed. These entries typically hold the SYSTEM chunks that map the chunk
tree and root tree blocks.

### Backup roots

The `super_roots` array holds four rotating backup copies of critical tree
root pointers. The kernel updates one entry per transaction, cycling through
indices 0-3. Each backup root entry (`btrfs_root_backup`) is 168 bytes:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `tree_root` | 0 | 8 | Logical bytenr of root tree root |
| `tree_root_gen` | 8 | 8 | Generation of root tree root |
| `chunk_root` | 16 | 8 | Logical bytenr of chunk tree root |
| `chunk_root_gen` | 24 | 8 | Generation of chunk tree root |
| `extent_root` | 32 | 8 | Logical bytenr of extent tree root |
| `extent_root_gen` | 40 | 8 | Generation of extent tree root |
| `fs_root` | 48 | 8 | Logical bytenr of FS tree root |
| `fs_root_gen` | 56 | 8 | Generation of FS tree root |
| `dev_root` | 64 | 8 | Logical bytenr of device tree root |
| `dev_root_gen` | 72 | 8 | Generation of device tree root |
| `csum_root` | 80 | 8 | Logical bytenr of checksum tree root |
| `csum_root_gen` | 88 | 8 | Generation of checksum tree root |
| `total_bytes` | 96 | 8 | Total filesystem bytes at backup time |
| `bytes_used` | 104 | 8 | Bytes used at backup time |
| `num_devices` | 112 | 8 | Number of devices at backup time |
| (reserved) | 120 | 32 | Unused u64[4] |
| `tree_root_level` | 152 | 1 | B-tree level of root tree root |
| `chunk_root_level` | 153 | 1 | B-tree level of chunk tree root |
| `extent_root_level` | 154 | 1 | B-tree level of extent tree root |
| `fs_root_level` | 155 | 1 | B-tree level of FS tree root |
| `dev_root_level` | 156 | 1 | B-tree level of device tree root |
| `csum_root_level` | 157 | 1 | B-tree level of checksum tree root |
| (padding) | 158 | 10 | Unused bytes to 168 total |

### Superblock checksum

The checksum field (`csum`, bytes 0..31) covers bytes 32 through 4095
(inclusive). For CRC32C, the 4-byte result is stored little-endian at bytes
0..3, and bytes 4..31 are zeroed.

A valid superblock has the magic number `_BHRfS_M` (hex
`0x4D5F53665248425F`) at offset 64.

A superblock is valid when both the magic and the checksum match. See
[Mirror locations](#mirror-locations) for which mirror this project reads.


## Tree Block Format

Every B-tree block (node or leaf) is exactly `nodesize` bytes. It begins with
a 101-byte header (`btrfs_header`), followed by item descriptors (leaves) or
key-pointer entries (nodes).

### Header

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `csum` | 0 | 32 | Checksum of bytes 32..nodesize-1 |
| `fsid` | 32 | 16 | Filesystem UUID (must match superblock) |
| `bytenr` | 48 | 8 | Logical byte offset of this block |
| `flags` | 56 | 8 | Header flags (lower 56 bits) + backref rev (upper 8 bits) |
| `chunk_tree_uuid` | 64 | 16 | UUID of the chunk tree mapping this block |
| `generation` | 80 | 8 | Transaction generation when last written |
| `owner` | 88 | 8 | Objectid of the tree owning this block |
| `nritems` | 96 | 4 | Number of items (leaf) or key-pointers (node) |
| `level` | 100 | 1 | 0 = leaf, >0 = internal node |

Total header size: 101 bytes.

The `flags` field combines two values:
- Bits 0-55: block flags (`BTRFS_HEADER_FLAG_WRITTEN` = 1, `BTRFS_HEADER_FLAG_RELOC` = 2)
- Bits 56-63: backref revision (`BTRFS_MIXED_BACKREF_REV` = 1 for modern filesystems)

The header checksum covers bytes 32 through `nodesize - 1`. For CRC32C, the
result is stored as a 4-byte LE value at bytes 0..3, and bytes 4..31 are
zeroed.

### Leaf vs node distinction

The `level` field determines the block type:
- `level == 0`: leaf block, containing items
- `level > 0`: internal node, containing key-pointers to child blocks

The number of key-pointers that fit in a node bounds the tree depth. For a
16 KiB nodesize, a node holds up to:

```
max_ptrs = (nodesize - HEADER_SIZE) / KEY_PTR_SIZE
         = (16384 - 101) / 33
         = 493 key-pointers
```

With 493 children per node, a tree of depth 2 (a root node and leaves) can
hold `493 * 651 = ~320,000` items. A tree of depth 3 can hold
`493^2 * 651 = ~158 million` items. In practice, trees rarely exceed depth 3
or 4.


## Leaf Format

A leaf block (level 0) contains sorted item descriptors followed by a data
area. Item descriptors grow forward from the header. Item data grows
backward from the end of the block.

```
+-------------------------------------------+
| Header (101 bytes)                        |
+-------------------------------------------+
| Item descriptor 0  (25 bytes)             |
| Item descriptor 1  (25 bytes)             |
| ...                                       |
| Item descriptor N-1 (25 bytes)            |
+-------------------------------------------+
| (free space)                              |
+-------------------------------------------+
| Item data N-1                             |
| ...                                       |
| Item data 1                               |
| Item data 0                               |
+-------------------------------------------+
```

### Item descriptor

Each item descriptor (`btrfs_item`) is 25 bytes:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `objectid` | 0 | 8 | Key objectid (LE u64) |
| `type` | 8 | 1 | Key type byte (u8) |
| `offset` | 9 | 8 | Key offset (LE u64) |
| `data_offset` | 17 | 4 | Byte offset of item data from end of header (LE u32) |
| `data_size` | 21 | 4 | Size of item data in bytes (LE u32) |

The first 17 bytes form a `btrfs_disk_key`. The `data_offset` field is
relative to the start of the leaf data area, which begins right after the
header. To locate item data in the raw block buffer:

```
absolute_offset = HEADER_SIZE + data_offset
```

where `HEADER_SIZE` = 101 bytes.

### Data area layout

Item data is packed from the end of the block backward. The first item
pushed has its data at the highest offset. Each later item has its data at a
lower offset. So:

- Item descriptors grow forward: `HEADER_SIZE + i * 25`
- Item data grows backward: starting from `nodesize` and moving toward
  the descriptor area

The free space in a leaf is the gap between the end of the last descriptor
and the start of the lowest-offset item data.

### Offset bookkeeping

When building a leaf (as the mkfs `LeafBuilder` does), the bookkeeping works
like this:

```
Initial state:
  item_offset = HEADER_SIZE (101)    // next descriptor position
  data_end    = nodesize (16384)     // next data write position

For each item pushed (key, data[N bytes]):
  1. data_end -= N                   // reserve space for item data
  2. Write data at buf[data_end .. data_end + N]
  3. data_offset = data_end - HEADER_SIZE   // relative to header end
  4. Write descriptor at buf[item_offset]:
       key (17 bytes) + data_offset (LE u32) + data_size (LE u32)
  5. item_offset += 25               // advance to next descriptor slot
```

The space left for more items is:

```
space_left = data_end - (item_offset + ITEM_SIZE)
```

It must fit both the 25-byte descriptor and the item data.

### Key ordering invariant

Items within a leaf are sorted by key in lexicographic order: first by
`objectid`, then by `type`, then by `offset`. The B-tree insertion logic
maintains this invariant and `btrfs check` verifies it.

### Capacity

For a 16384-byte leaf, the maximum number of items depends on their data
sizes. With zero-length data items (such as `TREE_BLOCK_REF` or
`FREE_SPACE_EXTENT`), the theoretical maximum is:

```
max_items = (nodesize - HEADER_SIZE) / ITEM_SIZE
          = (16384 - 101) / 25
          = 651 items
```

In practice, most items have data payloads, which reduce this number
significantly.


## Node Format

An internal node (level > 0) contains sorted key-pointer entries
(`btrfs_key_ptr`). Each entry points to a child block and records the
lowest key in that child's subtree.

### Key-pointer entry

Each key-pointer (`btrfs_key_ptr`) is 33 bytes:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `objectid` | 0 | 8 | Key objectid (LE u64) |
| `type` | 8 | 1 | Key type byte (u8) |
| `offset` | 9 | 8 | Key offset (LE u64) |
| `blockptr` | 17 | 8 | Logical byte address of child block (LE u64) |
| `generation` | 25 | 8 | Generation of the child block (LE u64) |

The first 17 bytes form the `btrfs_disk_key` holding the lowest key in the
child subtree. The `generation` field is used for consistency checks: when
the child block is read, its header generation must match this value.

### Layout

```
+-------------------------------------------+
| Header (101 bytes)                        |
+-------------------------------------------+
| Key-pointer 0  (33 bytes)                 |
| Key-pointer 1  (33 bytes)                 |
| ...                                       |
| Key-pointer N-1 (33 bytes)                |
+-------------------------------------------+
| (unused space to nodesize)                |
+-------------------------------------------+
```

Key-pointers are sorted by key in the same lexicographic order as leaf
items. The child block referenced by key-pointer `i` contains all items with
keys >= key-pointer[i].key and < key-pointer[i+1].key (unbounded above for
the last pointer).


## Key Structure

Every item and key-pointer is addressed by a three-part key
(`btrfs_disk_key`):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `objectid` | 0 | 8 | LE u64 |
| `type` | 8 | 1 | u8 |
| `offset` | 9 | 8 | LE u64 |

Total: 17 bytes.

### Lexicographic ordering

Keys are compared as a tuple `(objectid, type, offset)`, in that order. The
`objectid` is compared first. On a tie, `type` is compared, and on a further
tie, `offset` decides. All comparisons are unsigned integer comparisons.

### Field semantics by tree

The meaning of the three key fields depends on the tree and item type.

**FS tree:**
- `objectid` = inode number (starting at 256 = `BTRFS_FIRST_FREE_OBJECTID`)
- `type` = item type (INODE_ITEM, DIR_ITEM, EXTENT_DATA, and so on)
- `offset` = type-dependent (0 for INODE_ITEM, name hash for DIR_ITEM,
  file byte offset for EXTENT_DATA, parent inode for INODE_REF, and so on)

**Root tree:**
- `objectid` = tree objectid (for example 5 for FS_TREE, 256+ for subvolumes)
- `type` = ROOT_ITEM, ROOT_REF, or ROOT_BACKREF
- `offset` = 0 for ROOT_ITEM, child or parent tree ID for refs

**Extent tree:**
- `objectid` = logical byte address of the extent
- `type` = EXTENT_ITEM, METADATA_ITEM, or backref type
- `offset` = extent length (EXTENT_ITEM), level (METADATA_ITEM), or
  backref-specific (root objectid, parent bytenr, hash)

**Chunk tree:**
- `objectid` = `BTRFS_FIRST_CHUNK_TREE_OBJECTID` (256) for CHUNK_ITEM
- `type` = CHUNK_ITEM
- `offset` = logical byte address of the chunk

The chunk tree also holds DEV_ITEMs: `objectid` =
`BTRFS_DEV_ITEMS_OBJECTID` (1), `type` = DEV_ITEM, `offset` = device ID.

**Device tree:**
- `objectid` = device ID for DEV_EXTENT, 0 for DEV_STATS and DEV_REPLACE
- `type` = DEV_EXTENT, PERSISTENT_ITEM (DEV_STATS), or DEV_REPLACE
- `offset` = physical offset for DEV_EXTENT, device ID for DEV_STATS,
  0 for DEV_REPLACE

**Checksum tree:**
- `objectid` = `BTRFS_EXTENT_CSUM_OBJECTID`
- `type` = EXTENT_CSUM
- `offset` = logical byte address of the first checksummed sector

**Free space tree:**
- `objectid` = block group logical offset (for FREE_SPACE_INFO) or
  extent start (for FREE_SPACE_EXTENT and FREE_SPACE_BITMAP)
- `type` = FREE_SPACE_INFO, FREE_SPACE_EXTENT, or FREE_SPACE_BITMAP
- `offset` = block group length (for INFO) or extent length (for EXTENT and BITMAP)

**UUID tree:**
- `objectid` = UUID bytes 0..7 interpreted as LE u64
- `type` = UUID_KEY_SUBVOL or UUID_KEY_RECEIVED_SUBVOL
- `offset` = UUID bytes 8..15 interpreted as LE u64

**Quota tree:**
- `objectid` = packed qgroupid `(level << 48) | subvolid`
- `type` = QGROUP_STATUS, QGROUP_INFO, QGROUP_LIMIT, QGROUP_RELATION
- `offset` = packed qgroupid for relations, 0 otherwise

### Key type values

| Value | Name | Description |
|-------|------|-------------|
| 1 | `INODE_ITEM_KEY` | Inode metadata (mode, size, timestamps, nlink) |
| 12 | `INODE_REF_KEY` | Link from inode to parent directory (name + index) |
| 13 | `INODE_EXTREF_KEY` | Extended inode ref for names exceeding `INODE_REF` capacity |
| 24 | `XATTR_ITEM_KEY` | Extended attribute (name + value, keyed by name hash) |
| 36 | `VERITY_DESC_ITEM_KEY` | fs-verity descriptor |
| 37 | `VERITY_MERKLE_ITEM_KEY` | fs-verity Merkle tree data |
| 48 | `ORPHAN_ITEM_KEY` | Orphan inode pending cleanup |
| 60 | `DIR_LOG_ITEM_KEY` | Directory log for fsync optimization |
| 72 | `DIR_LOG_INDEX_KEY` | Directory log index |
| 84 | `DIR_ITEM_KEY` | Directory entry keyed by `crc32c(name)` hash |
| 96 | `DIR_INDEX_KEY` | Directory entry keyed by sequential index |
| 108 | `EXTENT_DATA_KEY` | File extent (inline data or reference to disk extent) |
| 128 | `EXTENT_CSUM_KEY` | Data checksum covering one or more sectors |
| 132 | `ROOT_ITEM_KEY` | Tree root descriptor (bytenr, generation, UUID, timestamps) |
| 144 | `ROOT_BACKREF_KEY` | Backref from child subvolume to parent |
| 156 | `ROOT_REF_KEY` | Forward ref from parent subvolume to child |
| 168 | `EXTENT_ITEM_KEY` | Extent allocation with backrefs (non-skinny: offset = size) |
| 169 | `METADATA_ITEM_KEY` | Skinny metadata extent (offset = level, not size) |
| 172 | `EXTENT_OWNER_REF_KEY` | Simple quota owner backref |
| 176 | `TREE_BLOCK_REF_KEY` | Standalone backref: metadata extent → owning tree |
| 178 | `EXTENT_DATA_REF_KEY` | Standalone backref: data extent → (root, ino, offset) |
| 182 | `SHARED_BLOCK_REF_KEY` | Shared metadata backref (parent block address) |
| 184 | `SHARED_DATA_REF_KEY` | Shared data backref (parent block address + count) |
| 192 | `BLOCK_GROUP_ITEM_KEY` | Block group allocation info (used bytes, type, profile) |
| 198 | `FREE_SPACE_INFO_KEY` | Free space tree: per-block-group metadata |
| 199 | `FREE_SPACE_EXTENT_KEY` | Free space tree: free extent range |
| 200 | `FREE_SPACE_BITMAP_KEY` | Free space tree: bitmap of free sectors |
| 204 | `DEV_EXTENT_KEY` | Physical extent allocated to a chunk on a device |
| 216 | `DEV_ITEM_KEY` | Device descriptor (size, UUID, I/O parameters) |
| 228 | `CHUNK_ITEM_KEY` | Chunk mapping logical → physical with stripe info |
| 230 | `RAID_STRIPE_KEY` | RAID stripe tree entry (zoned devices) |
| 240 | `QGROUP_STATUS_KEY` | Quota group global status and generation |
| 242 | `QGROUP_INFO_KEY` | Per-qgroup usage counters (referenced, exclusive) |
| 244 | `QGROUP_LIMIT_KEY` | Per-qgroup size limits |
| 246 | `QGROUP_RELATION_KEY` | Parent-child relationship between qgroups |
| 248 | `TEMPORARY_ITEM_KEY` | Transient item, also used as `BALANCE_ITEM_KEY` |
| 249 | `PERSISTENT_ITEM_KEY` | Persistent metadata, also used as `DEV_STATS_KEY` |
| 250 | `DEV_REPLACE_KEY` | Device replace operation state |
| 251 | `UUID_KEY_SUBVOL` | UUID tree: maps subvolume UUID → subvolume ID |
| 252 | `UUID_KEY_RECEIVED_SUBVOL` | UUID tree: maps received UUID → subvolume ID |
| 253 | `STRING_ITEM_KEY` | Label or other string metadata |

### Well-known objectid values

| Value | Name | Notes |
|-------|------|-------|
| 1 | `ROOT_TREE_OBJECTID` | Root tree |
| 2 | `EXTENT_TREE_OBJECTID` | Extent tree |
| 3 | `CHUNK_TREE_OBJECTID` | Chunk tree |
| 4 | `DEV_TREE_OBJECTID` | Device tree |
| 5 | `FS_TREE_OBJECTID` | Default FS tree |
| 6 | `ROOT_TREE_DIR_OBJECTID` | Root tree directory |
| 7 | `CSUM_TREE_OBJECTID` | Checksum tree |
| 8 | `QUOTA_TREE_OBJECTID` | Quota tree |
| 9 | `UUID_TREE_OBJECTID` | UUID tree |
| 10 | `FREE_SPACE_TREE_OBJECTID` | Free space tree |
| 11 | `BLOCK_GROUP_TREE_OBJECTID` | Block group tree |
| 12 | `RAID_STRIPE_TREE_OBJECTID` | RAID stripe tree |
| 256 | `FIRST_FREE_OBJECTID` | First user inode / first subvolume ID |
| (u64)-4 | `BALANCE_OBJECTID` | Balance status |
| (u64)-5 | `ORPHAN_OBJECTID` | Orphan items |
| (u64)-6 | `TREE_LOG_OBJECTID` | Tree log |
| (u64)-7 | `TREE_LOG_FIXUP_OBJECTID` | Tree log fixup |
| (u64)-8 | `TREE_RELOC_OBJECTID` | Tree relocation |
| (u64)-9 | `DATA_RELOC_TREE_OBJECTID` | Data relocation tree |
| (u64)-10 | `EXTENT_CSUM_OBJECTID` | Extent checksums |
| (u64)-11 | `FREE_SPACE_OBJECTID` | Free space cache (v1) |
| (u64)-12 | `FREE_INO_OBJECTID` | Free inode number tracking |
| (u64)-255 | `MULTIPLE_OBJECTIDS` | Multiple-owner sentinel |

Negative objectids are stored in unsigned 64-bit two's complement form. For
example, `BALANCE_OBJECTID` = -4 is stored as `0xFFFFFFFF_FFFFFFFC`.


## Trees

Btrfs uses multiple B-trees, each identified by a well-known objectid. The
root tree stores a `ROOT_ITEM` for each tree that points to its root block.

### Root tree (objectid 1)

The directory of all other trees. Contains:
- `ROOT_ITEM` for each tree (objectid = tree ID, type = ROOT_ITEM, offset = 0)
- `ROOT_REF` for parent-to-child subvolume links
- `ROOT_BACKREF` for child-to-parent subvolume links
- `ROOT_TREE_DIR` directory entry linking to the default subvolume
- `TEMPORARY_ITEM` for balance status persistence

### Extent tree (objectid 2)

Tracks all allocated space (data extents and metadata tree blocks) with
reference counting and backreferences. Contains:
- `EXTENT_ITEM` for data and non-skinny metadata extents
- `METADATA_ITEM` for skinny metadata extents
- `TREE_BLOCK_REF` for direct metadata backrefs
- `SHARED_BLOCK_REF` for shared metadata backrefs (snapshots)
- `EXTENT_DATA_REF` for direct data backrefs
- `SHARED_DATA_REF` for shared data backrefs (snapshots)
- `BLOCK_GROUP_ITEM` for each block group (unless block_group_tree feature)

### Chunk tree (objectid 3)

Maps logical address ranges to physical device stripes. Contains:
- `CHUNK_ITEM` for each chunk (logical-to-physical mapping)
- `DEV_ITEM` for each device

The chunk tree is bootstrapped from the superblock's `sys_chunk_array`.

### Device tree (objectid 4)

Tracks per-device physical extent allocations. Contains:
- `DEV_EXTENT` for each allocated physical range on each device
- `PERSISTENT_ITEM` (DEV_STATS) for per-device error counters
- `DEV_REPLACE` for device replace status

### FS tree (objectid 5, 256+)

Holds the filesystem content for a subvolume. The default subvolume uses
objectid 5. Other subvolumes and snapshots use objectids starting at 256.
Contains:
- `INODE_ITEM` for each inode
- `INODE_REF` and `INODE_EXTREF` for hard links
- `DIR_ITEM` for directory entries (keyed by name hash)
- `DIR_INDEX` for directory entries (keyed by sequence number)
- `EXTENT_DATA` for file extent descriptors
- `XATTR_ITEM` for extended attributes
- `ORPHAN_ITEM` for unlinked but still open inodes

### Checksum tree (objectid 7)

Stores per-sector data checksums. Contains:
- `EXTENT_CSUM` items: each item covers a contiguous range of data
  sectors, storing an array of per-sector checksums

### Quota tree (objectid 8)

Tracks quota group accounting. Contains:
- `QGROUP_STATUS` (one per filesystem)
- `QGROUP_INFO` for each qgroup
- `QGROUP_LIMIT` for each qgroup with limits
- `QGROUP_RELATION` for parent-child qgroup relationships

### UUID tree (objectid 9)

Provides fast UUID-to-subvolume lookups for send and receive. Contains:
- `UUID_KEY_SUBVOL` mapping subvolume UUID to objectid
- `UUID_KEY_RECEIVED_SUBVOL` mapping received UUID to objectid

### Free space tree (objectid 10)

Tracks free space per block group, replacing the older free space cache
(v1). Contains:
- `FREE_SPACE_INFO` for each block group
- `FREE_SPACE_EXTENT` for free ranges
- `FREE_SPACE_BITMAP` for bitmap-tracked regions

Requires the `free_space_tree` compat_ro feature flag.

### Block group tree (objectid 11)

Keeps block group items out of the extent tree for faster mounts.
Contains:
- `BLOCK_GROUP_ITEM` for each block group

Requires the `block_group_tree` compat_ro feature flag. Without this tree,
block group items live in the extent tree.

### Data relocation tree (objectid (u64)-9)

A temporary FS tree used during balance to hold relocated data extents.
Uses the same item types as a regular FS tree.

### RAID stripe tree (objectid 12)

Maps logical extents to per-device physical stripe offsets. Contains:
- `RAID_STRIPE` items

Requires the `raid_stripe_tree` incompat feature flag.


## Item Types

This section gives the key format and payload layout of each major item
type.

### INODE_ITEM (type 1)

**Key:** `(inode_number, INODE_ITEM, 0)`

Exactly one per inode. Stores POSIX attributes, timestamps, and
btrfs-specific flags.

**Payload** (`btrfs_inode_item`, 160 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `generation` | 0 | 8 | Generation when created |
| `transid` | 8 | 8 | Transaction ID of last modification |
| `size` | 16 | 8 | Logical file size in bytes |
| `nbytes` | 24 | 8 | On-disk bytes used (all copies) |
| `block_group` | 32 | 8 | Block group hint for new allocations |
| `nlink` | 40 | 4 | Hard link count |
| `uid` | 44 | 4 | Owner user ID |
| `gid` | 48 | 4 | Owner group ID |
| `mode` | 52 | 4 | POSIX file mode (type + permissions) |
| `rdev` | 56 | 8 | Device number (char/block device inodes) |
| `flags` | 64 | 8 | Inode flags (see below) |
| `sequence` | 72 | 8 | NFS-compatible change sequence number |
| reserved | 80 | 32 | Reserved u64[4], must be zero |
| `atime` | 112 | 12 | Access time (`btrfs_timespec`) |
| `ctime` | 124 | 12 | Change time (`btrfs_timespec`) |
| `mtime` | 136 | 12 | Modification time (`btrfs_timespec`) |
| `otime` | 148 | 12 | Creation time (`btrfs_timespec`) |

Each `btrfs_timespec` is 12 bytes:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `sec` | 0 | 8 | Seconds since Unix epoch (LE u64) |
| `nsec` | 8 | 4 | Nanosecond component, 0..999999999 (LE u32) |

**Inode flags** (bitmask):

| Bit | Value | Name |
|-----|-------|------|
| 0 | `0x1` | `NODATASUM` |
| 1 | `0x2` | `NODATACOW` |
| 2 | `0x4` | `READONLY` |
| 3 | `0x8` | `NOCOMPRESS` |
| 4 | `0x10` | `PREALLOC` |
| 5 | `0x20` | `SYNC` |
| 6 | `0x40` | `IMMUTABLE` |
| 7 | `0x80` | `APPEND` |
| 8 | `0x100` | `NODUMP` |
| 9 | `0x200` | `NOATIME` |
| 10 | `0x400` | `DIRSYNC` |
| 11 | `0x800` | `COMPRESS` |
| 31 | `0x80000000` | `ROOT_ITEM_INIT` |

### INODE_REF (type 12)

**Key:** `(inode_number, INODE_REF, parent_dir_inode)`

Hard-link reference from an inode to a directory entry. Several refs can be
packed into one item when an inode has several hard links in the same
parent directory.

**Payload** (variable, packed sequence of entries):

For each ref:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `index` | 0 | 8 | `DIR_INDEX` sequence number (LE u64) |
| `name_len` | 8 | 2 | Length of name in bytes (LE u16) |
| `name` | 10 | name_len | Filename bytes (no NUL terminator) |

Multiple refs are concatenated without padding.

### INODE_EXTREF (type 13)

**Key:** `(inode_number, INODE_EXTREF, crc32c(parent_ino, name))`

Extended inode reference. Unlike `INODE_REF`, it stores the parent inode in
the struct, which allows references from different parent directories.
Requires the `extended_iref` incompat feature.

**Payload** (variable, packed sequence):

For each ref:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `parent` | 0 | 8 | Parent directory inode number (LE u64) |
| `index` | 8 | 8 | `DIR_INDEX` sequence number (LE u64) |
| `name_len` | 16 | 2 | Length of name (LE u16) |
| `name` | 18 | name_len | Filename bytes |

### DIR_ITEM (type 84) / DIR_INDEX (type 96)

**Key for DIR_ITEM:** `(dir_inode, DIR_ITEM, crc32c(name))`
**Key for DIR_INDEX:** `(dir_inode, DIR_INDEX, sequence)`

Both use the same on-disk format. `DIR_ITEM` entries are keyed by the CRC32C
hash of the filename (raw CRC32C, not standard). `DIR_INDEX` entries are
keyed by a monotonically increasing sequence number, for ordered directory
iteration.

Several entries can be packed into one `DIR_ITEM` when their names hash to
the same value (hash collision).

**Payload** (`btrfs_dir_item`, variable, packed sequence):

For each entry:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `location` | 0 | 17 | Target inode key (`btrfs_disk_key`) |
| `transid` | 17 | 8 | Transaction ID (LE u64) |
| `data_len` | 25 | 2 | Xattr value length, 0 for dirs (LE u16) |
| `name_len` | 27 | 2 | Filename length (LE u16) |
| `type` | 29 | 1 | File type (see below) |
| `name` | 30 | name_len | Filename bytes |
| `data` | 30+NL | data_len | Xattr value (for `XATTR_ITEM` only) |

The `location` field is a `btrfs_disk_key` pointing to the target. For
regular directory entries it is typically objectid = target inode,
type = INODE_ITEM, offset = 0. For subvolume entries, type = ROOT_ITEM and
objectid = the subvolume's tree objectid.

**File type values:**

| Value | Name |
|-------|------|
| 0 | `FT_UNKNOWN` |
| 1 | `FT_REG_FILE` |
| 2 | `FT_DIR` |
| 3 | `FT_CHRDEV` |
| 4 | `FT_BLKDEV` |
| 5 | `FT_FIFO` |
| 6 | `FT_SOCK` |
| 7 | `FT_SYMLINK` |
| 8 | `FT_XATTR` |

### FILE_EXTENT_ITEM (type 108)

**Key:** `(inode_number, EXTENT_DATA, file_byte_offset)`

Describes how a range of file bytes maps to on-disk storage. There are three
extent types: inline, regular, and preallocated.

**Common header** (21 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `generation` | 0 | 8 | Allocation generation (LE u64) |
| `ram_bytes` | 8 | 8 | Uncompressed size (LE u64) |
| `compression` | 16 | 1 | Compression type (0=none, 1=zlib, 2=lzo, 3=zstd) |
| `encryption` | 17 | 1 | Reserved (always 0) |
| `other_encoding` | 18 | 2 | Reserved (always 0) |
| `type` | 20 | 1 | Extent type (0=inline, 1=regular, 2=prealloc) |

**Inline extent** (type 0):

After the 21-byte header, the rest of the item is the file data. The data
length is `item_size - 21`. For compressed inline extents, the data is
compressed and `ram_bytes` gives the uncompressed size.

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| header | 0 | 21 | Common header (type = 0) |
| data | 21 | item_size-21 | Inline file data |

Total item size: 21 + data_length.

**Regular extent** (type 1) and **prealloc extent** (type 2):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| header | 0 | 21 | Common header (type = 1 or 2) |
| `disk_bytenr` | 21 | 8 | Logical address of extent on disk (LE u64) |
| `disk_num_bytes` | 29 | 8 | Size of extent on disk (LE u64) |
| `offset` | 37 | 8 | Byte offset into extent (LE u64) |
| `num_bytes` | 45 | 8 | Number of logical file bytes covered (LE u64) |

Total item size: 53 bytes.

A `disk_bytenr` of 0 indicates a hole (sparse region). For compressed
extents, `disk_num_bytes` is the compressed size on disk and `ram_bytes` is
the uncompressed size. The `offset` field allows referencing into the middle
of a shared extent (for example, after COW of part of a cloned extent).

Prealloc extents (type 2) are reserved but unwritten. Reads return zeroes.

### EXTENT_ITEM (type 168) / METADATA_ITEM (type 169)

**Key for EXTENT_ITEM:** `(logical_bytenr, EXTENT_ITEM, extent_length)`
**Key for METADATA_ITEM:** `(logical_bytenr, METADATA_ITEM, level)`

Tracks reference counts and backreferences for allocated space.
`METADATA_ITEM` is the "skinny" variant, used when the `skinny_metadata`
incompat flag is set. Its extent length is implicit (= nodesize), and the key
offset holds the tree block level instead.

**Base payload** (`btrfs_extent_item`, 24 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `refs` | 0 | 8 | Number of references (LE u64) |
| `generation` | 8 | 8 | Allocation generation (LE u64) |
| `flags` | 16 | 8 | Extent flags (LE u64) |

**Extent flags:**

| Bit | Value | Name |
|-----|-------|------|
| 0 | `0x1` | `EXTENT_FLAG_DATA` |
| 1 | `0x2` | `EXTENT_FLAG_TREE_BLOCK` |
| 8 | `0x100` | `BLOCK_FLAG_FULL_BACKREF` |

**Tree block info** (for non-skinny EXTENT_ITEM with TREE_BLOCK flag):

After the base extent item, non-skinny tree block extents have a
`btrfs_tree_block_info` (18 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `key` | 24 | 17 | First key in the tree block (`btrfs_disk_key`) |
| `level` | 41 | 1 | Tree block level (u8) |

Skinny metadata items (`METADATA_ITEM`) omit it, because the level is
encoded in the key offset.

**Inline backreferences:**

After the extent item header (and tree_block_info if present), zero or more
inline backreferences may follow. Each starts with a 1-byte type tag
followed by type-specific data:

| Type byte | Name | Data after type byte |
|-----------|------|----------------------|
| 176 (`0xB0`) | `TREE_BLOCK_REF` | 8 bytes: `root_objectid` (LE u64) |
| 182 (`0xB6`) | `SHARED_BLOCK_REF` | 8 bytes: `parent_bytenr` (LE u64) |
| 178 (`0xB2`) | `EXTENT_DATA_REF` | 28 bytes: `root`(8) + `objectid`(8) + `offset`(8) + `count`(4) |
| 184 (`0xB8`) | `SHARED_DATA_REF` | 12 bytes: `parent_bytenr`(8) + `count`(4) |
| 172 (`0xAC`) | `EXTENT_OWNER_REF` | 8 bytes: `root_objectid` (LE u64) |

For `EXTENT_DATA_REF`, the 8-byte offset field that normally follows the
type byte is absent. The struct fields begin right after the type byte:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `type` | 0 | 1 | 178 (`EXTENT_DATA_REF_KEY`) |
| `root` | 1 | 8 | Owning tree objectid (LE u64) |
| `objectid` | 9 | 8 | Referencing inode number (LE u64) |
| `offset` | 17 | 8 | File byte offset of reference (LE u64) |
| `count` | 25 | 4 | Number of references (LE u32) |

For other inline ref types, the format is:

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `type` | 0 | 1 | Type byte (176/182/184/172) |
| `offset` | 1 | 8 | Type-specific offset (LE u64) |

For `SHARED_DATA_REF`, 4 more bytes follow:

```
9       4     count       Number of references (LE u32)
```

### Standalone backreference items

When backreferences do not fit inline in the extent item, they are stored
as separate items in the extent tree:

**TREE_BLOCK_REF (type 176):**
Key: `(extent_bytenr, TREE_BLOCK_REF, root_objectid)`.
No data payload. The key offset encodes the owning root.

**SHARED_BLOCK_REF (type 182):**
Key: `(extent_bytenr, SHARED_BLOCK_REF, parent_bytenr)`.
No data payload. The key offset encodes the parent block.

**EXTENT_DATA_REF (type 178):**
Key: `(extent_bytenr, EXTENT_DATA_REF, hash)`.
The hash is computed from `(root, objectid, offset)` using two CRC32C
passes:

```
high_crc = raw_crc32c(0xFFFFFFFF, root_le_bytes)
low_crc  = raw_crc32c(0xFFFFFFFF, objectid_le_bytes)
low_crc  = raw_crc32c(low_crc,    offset_le_bytes)
hash     = (high_crc << 31) ^ low_crc
```

Payload (`btrfs_extent_data_ref`, 28 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `root` | 0 | 8 | Owning tree objectid (LE u64) |
| `objectid` | 8 | 8 | Referencing inode (LE u64) |
| `offset` | 16 | 8 | File byte offset (LE u64) |
| `count` | 24 | 4 | Reference count (LE u32) |

**SHARED_DATA_REF (type 184):**
Key: `(extent_bytenr, SHARED_DATA_REF, parent_bytenr)`.
Payload (4 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `count` | 0 | 4 | Reference count (LE u32) |

**EXTENT_OWNER_REF (type 172):**
Key: `(extent_bytenr, EXTENT_OWNER_REF, root_objectid)`.
No data payload. Used with the `simple_quota` feature.

### DEV_ITEM (type 216)

**Key:** `(DEV_ITEMS_OBJECTID [1], DEV_ITEM, devid)`

Stored in the chunk tree. Also embedded in the superblock at offset 201.

**Payload** (`btrfs_dev_item`, 98 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `devid` | 0 | 8 | Device ID (LE u64) |
| `total_bytes` | 8 | 8 | Total device size (LE u64) |
| `bytes_used` | 16 | 8 | Bytes allocated on device (LE u64) |
| `io_align` | 24 | 4 | I/O alignment (LE u32) |
| `io_width` | 28 | 4 | I/O width (LE u32) |
| `sector_size` | 32 | 4 | Device sector size (LE u32) |
| `type` | 36 | 8 | Device type (reserved, 0) (LE u64) |
| `generation` | 44 | 8 | Generation last updated (LE u64) |
| `start_offset` | 52 | 8 | Allocation start offset (LE u64) |
| `dev_group` | 60 | 4 | Device group (reserved, 0) (LE u32) |
| `seek_speed` | 64 | 1 | Seek speed hint (0 = unset) |
| `bandwidth` | 65 | 1 | Bandwidth hint (0 = unset) |
| `uuid` | 66 | 16 | Device UUID |
| `fsid` | 82 | 16 | Filesystem UUID |

### CHUNK_ITEM (type 228)

**Key:** `(FIRST_CHUNK_TREE_OBJECTID [256], CHUNK_ITEM, logical_offset)`

Maps a range of logical addresses to physical device locations. Stored in
the chunk tree and, for system chunks, in the superblock's
`sys_chunk_array`.

**Payload** (`btrfs_chunk` + stripes, variable):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `length` | 0 | 8 | Chunk size in bytes (LE u64) |
| `owner` | 8 | 8 | Owner objectid (LE u64) |
| `stripe_len` | 16 | 8 | Stripe length (typically 65536) (LE u64) |
| `type` | 24 | 8 | Chunk type + RAID profile flags (LE u64) |
| `io_align` | 32 | 4 | I/O alignment (LE u32) |
| `io_width` | 36 | 4 | I/O width (LE u32) |
| `sector_size` | 40 | 4 | Sector size (LE u32) |
| `num_stripes` | 44 | 2 | Number of stripes (LE u16) |
| `sub_stripes` | 46 | 2 | Sub-stripes for RAID10 (LE u16) |
| `stripes[]` | 48 | ... | Array of `num_stripes` stripe entries |

Each stripe entry (`btrfs_stripe`, 32 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `devid` | 0 | 8 | Device ID (LE u64) |
| `offset` | 8 | 8 | Physical byte offset on device (LE u64) |
| `dev_uuid` | 16 | 16 | Device UUID |

Total payload size: `48 + num_stripes * 32` bytes.

**Chunk type flags** (bitmask, same as block group flags):

| Bit | Value | Name |
|-----|-------|------|
| 0 | `0x1` | `DATA` |
| 1 | `0x2` | `SYSTEM` |
| 2 | `0x4` | `METADATA` |
| 3 | `0x8` | `RAID0` |
| 4 | `0x10` | `RAID1` |
| 5 | `0x20` | `DUP` |
| 6 | `0x40` | `RAID10` |
| 7 | `0x80` | `RAID5` |
| 8 | `0x100` | `RAID6` |
| 9 | `0x200` | `RAID1C3` |
| 10 | `0x400` | `RAID1C4` |

When no RAID profile bits are set, the chunk is SINGLE profile.

### DEV_EXTENT (type 204)

**Key:** `(devid, DEV_EXTENT, physical_offset)`

The inverse of a chunk stripe. It maps a physical range on a device back to
the owning chunk.

**Payload** (`btrfs_dev_extent`, 48 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `chunk_tree` | 0 | 8 | Chunk tree objectid (always 3) (LE u64) |
| `chunk_objectid` | 8 | 8 | Chunk objectid (LE u64) |
| `chunk_offset` | 16 | 8 | Logical offset of owning chunk (LE u64) |
| `length` | 24 | 8 | Length of this device extent (LE u64) |
| `chunk_tree_uuid` | 32 | 16 | Chunk tree UUID |

### BLOCK_GROUP_ITEM (type 192)

**Key:** `(logical_offset, BLOCK_GROUP_ITEM, length)`

Tracks space usage for a chunk. Stored in the extent tree, or in the block
group tree when the `block_group_tree` feature is enabled.

**Payload** (`btrfs_block_group_item`, 24 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `used` | 0 | 8 | Bytes used in this block group (LE u64) |
| `chunk_objectid` | 8 | 8 | Chunk objectid backing this group (LE u64) |
| `flags` | 16 | 8 | Type + RAID profile flags (LE u64) |

The `flags` field uses the same bitmask as the chunk type flags (see
CHUNK_ITEM above).

### ROOT_ITEM (type 132)

**Key:** `(tree_objectid, ROOT_ITEM, 0)`

Stored in the root tree. Describes a tree root: its block address,
generation, subvolume UUIDs, and timestamps.

**Payload** (`btrfs_root_item`, 439 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `inode` | 0 | 160 | Embedded `btrfs_inode_item` (root dir inode) |
| `generation` | 160 | 8 | Generation when last modified (LE u64) |
| `root_dirid` | 168 | 8 | Root directory inode objectid (LE u64) |
| `bytenr` | 176 | 8 | Logical bytenr of root block (LE u64) |
| `byte_limit` | 184 | 8 | Quota byte limit, 0=unlimited (LE u64) |
| `bytes_used` | 192 | 8 | Bytes used by this tree (LE u64) |
| `last_snapshot` | 200 | 8 | Generation of last snapshot (LE u64) |
| `flags` | 208 | 8 | Root flags (LE u64) |
| `refs` | 216 | 4 | Reference count (LE u32) |
| `drop_progress` | 220 | 17 | Drop operation progress key (`btrfs_disk_key`) |
| `drop_level` | 237 | 1 | Drop operation tree level (u8) |
| `level` | 238 | 1 | B-tree level of root block (u8) |
| `generation_v2` | 239 | 8 | Extended generation (v2) (LE u64) |
| `uuid` | 247 | 16 | Subvolume UUID |
| `parent_uuid` | 263 | 16 | Parent subvolume UUID (for snapshots) |
| `received_uuid` | 279 | 16 | Received UUID (for send/receive) |
| `ctransid` | 295 | 8 | Last change transaction (LE u64) |
| `otransid` | 303 | 8 | Creation transaction (LE u64) |
| `stransid` | 311 | 8 | Send transaction (LE u64) |
| `rtransid` | 319 | 8 | Receive transaction (LE u64) |
| `ctime` | 327 | 12 | Change timestamp (`btrfs_timespec`) |
| `otime` | 339 | 12 | Creation timestamp (`btrfs_timespec`) |
| `stime` | 351 | 12 | Send timestamp (`btrfs_timespec`) |
| `rtime` | 363 | 12 | Receive timestamp (`btrfs_timespec`) |
| reserved | 375 | 64 | Reserved u64[8] |

The embedded `inode_item` at the start describes the root directory
inode (objectid 256 = `BTRFS_FIRST_FREE_OBJECTID` for FS trees).

Older filesystems may store a shorter v1 root item without the UUID,
transaction, and timestamp fields. The parser accepts both formats.

**Root item flags:**

| Bit | Value | Name |
|-----|-------|------|
| 0 | `0x1` | `SUBVOL_RDONLY` (read-only snapshot) |

`SUBVOL_DEAD` (bit 48, value `0x1000000000000`) marks a deleted
subvolume pending cleanup.

### ROOT_REF (type 156) / ROOT_BACKREF (type 144)

**Key for ROOT_REF:** `(parent_tree_id, ROOT_REF, child_tree_id)`
**Key for ROOT_BACKREF:** `(child_tree_id, ROOT_BACKREF, parent_tree_id)`

Forward and backward references that link subvolumes to their parent
directories. Both use the same on-disk format.

**Payload** (`btrfs_root_ref`, 18 bytes + name):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `dirid` | 0 | 8 | Directory inode containing the subvol entry (LE u64) |
| `sequence` | 8 | 8 | `DIR_INDEX` sequence number (LE u64) |
| `name_len` | 16 | 2 | Length of name (LE u16) |
| `name` | 18 | name_len | Subvolume name bytes |

### FREE_SPACE_INFO (type 198)

**Key:** `(block_group_offset, FREE_SPACE_INFO, block_group_length)`

Metadata about free space tracking for a block group.

**Payload** (`btrfs_free_space_info`, 8 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `extent_count` | 0 | 4 | Number of free extents/bitmap entries (LE u32) |
| `flags` | 4 | 4 | Free space info flags (LE u32) |

**Flags:**

| Bit | Value | Name |
|-----|-------|------|
| 0 | `0x1` | `USING_BITMAPS` |

### FREE_SPACE_EXTENT (type 199)

**Key:** `(start, FREE_SPACE_EXTENT, length)`

A contiguous free range within a block group. The item has no data payload.
The key encodes the start address and length.

### FREE_SPACE_BITMAP (type 200)

**Key:** `(start, FREE_SPACE_BITMAP, length)`

A bitmap covering part of a block group's address range. The item data is
the raw bitmap, with one bit per sector. A set bit means free, a clear bit
means allocated.

### XATTR_ITEM (type 24)

**Key:** `(inode_number, XATTR_ITEM, crc32c(name))`

Extended attribute storage. Uses the same on-disk format as `DIR_ITEM` (see
DIR_ITEM above), but with:
- `location` = zeroed key
- `data_len` = length of the xattr value
- `type` = `FT_XATTR` (8)
- `name` = xattr name (e.g. `user.myattr`)
- `data` = xattr value

### EXTENT_CSUM (type 128)

**Key:** `(EXTENT_CSUM_OBJECTID, EXTENT_CSUM, logical_bytenr)`

Stores per-sector checksums for a contiguous range of data blocks. The item
data is a packed array of checksums, one per sector.

For CRC32C, each checksum is 4 bytes (LE u32), so the item covers
`item_size / 4` sectors. The logical byte range covered is:

```
start = key.offset
end   = key.offset + (item_size / csum_size) * sectorsize
```

### QGROUP_STATUS (type 240)

**Key:** `(0, QGROUP_STATUS, 0)`

One per filesystem. Tracks the overall state of quota accounting.

**Payload** (`btrfs_qgroup_status_item`, 32-40 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `version` | 0 | 8 | On-disk format version (LE u64) |
| `generation` | 8 | 8 | Last consistent generation (LE u64) |
| `flags` | 16 | 8 | Status flags (LE u64) |
| `scan` | 24 | 8 | Rescan progress objectid (LE u64) |
| `enable_gen` | 32 | 8 | Enable generation (kernel 6.8+, optional) (LE u64) |

### QGROUP_INFO (type 242)

**Key:** `(packed_qgroupid, QGROUP_INFO, 0)`

where `packed_qgroupid = (level << 48) | subvolid`.

**Payload** (`btrfs_qgroup_info_item`, 40 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `generation` | 0 | 8 | Last update generation (LE u64) |
| `referenced` | 8 | 8 | Total referenced bytes (LE u64) |
| `referenced_compressed` | 16 | 8 | Referenced bytes (compressed) (LE u64) |
| `exclusive` | 24 | 8 | Exclusive bytes (LE u64) |
| `exclusive_compressed` | 32 | 8 | Exclusive bytes (compressed) (LE u64) |

### QGROUP_LIMIT (type 244)

**Key:** `(packed_qgroupid, QGROUP_LIMIT, 0)`

**Payload** (`btrfs_qgroup_limit_item`, 40 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `flags` | 0 | 8 | Active limit bitmask (LE u64) |
| `max_referenced` | 8 | 8 | Max referenced bytes, 0=unlimited (LE u64) |
| `max_exclusive` | 16 | 8 | Max exclusive bytes, 0=unlimited (LE u64) |
| `rsv_referenced` | 24 | 8 | Reserved referenced bytes (LE u64) |
| `rsv_exclusive` | 32 | 8 | Reserved exclusive bytes (LE u64) |

### QGROUP_RELATION (type 246)

**Key:** `(child_qgroupid, QGROUP_RELATION, parent_qgroupid)`

Defines a parent-child relationship between qgroups. No data payload. The
key fully encodes the relationship.

### UUID_KEY_SUBVOL (type 251) / UUID_KEY_RECEIVED_SUBVOL (type 252)

**Key:** `(upper_half_uuid, UUID_KEY_SUBVOL, lower_half_uuid)`

Maps a UUID to one or more subvolume objectids. The UUID is split: bytes
0..7 are read as a LE u64 into the objectid field, and bytes 8..15 as a LE
u64 into the offset field.

**Payload** (variable, array of u64):

```
For each associated subvolume:
  8 bytes   subvolid   Subvolume tree objectid (LE u64)
```

### STRING_ITEM (type 253)

**Key:** `(BTRFS_FREE_SPACE_OBJECTID, STRING_ITEM, 0)`

Raw byte string. Typically stores the filesystem label in the root tree.

**Payload:** Raw bytes (length = item data size).

### TEMPORARY_ITEM (type 248) / BALANCE_ITEM

**Key:** `(BALANCE_OBJECTID, TEMPORARY_ITEM, 0)`

Persists in-progress balance state across reboots.

**Payload:** The first 8 bytes are balance flags (LE u64). The rest holds
`btrfs_balance_args` structures for the data, metadata, and system filters.

### PERSISTENT_ITEM (type 249) / DEV_STATS

**Key for device stats:** `(DEV_STATS_OBJECTID [0], PERSISTENT_ITEM, devid)`
**Key for device replace:** `(DEV_REPLACE_DEVID [0], DEV_REPLACE, 0)`

Both items live in the device tree.

**Device stats payload** (40 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `write_errs` | 0 | 8 | Write error count (LE u64) |
| `read_errs` | 8 | 8 | Read error count (LE u64) |
| `flush_errs` | 16 | 8 | Flush error count (LE u64) |
| `corruption_errs` | 24 | 8 | Corruption error count (LE u64) |
| `generation_errs` | 32 | 8 | Generation mismatch count (LE u64) |

**Device replace payload** (`btrfs_dev_replace_item`, 72+ bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `src_devid` | 0 | 8 | Source device ID (LE u64) |
| `cursor_left` | 8 | 8 | Left cursor position (LE u64) |
| `cursor_right` | 16 | 8 | Right cursor position (LE u64) |
| `replace_mode` | 24 | 8 | Replace mode (LE u64) |
| `replace_state` | 32 | 8 | Current state (LE u64) |
| `time_started` | 40 | 8 | Start timestamp (LE u64) |
| `time_stopped` | 48 | 8 | Stop timestamp (LE u64) |
| `num_write_errors` | 56 | 8 | Write errors (LE u64) |
| `num_uncorrectable_read_errors` | 64 | 8 | Uncorrectable reads (LE u64) |

### ORPHAN_ITEM (type 48)

**Key:** `(ORPHAN_OBJECTID, ORPHAN_ITEM, inode_number)`

Marks an inode that has been unlinked but is still open. The item has no
data payload. Orphan items are cleaned up on mount or by the kernel's orphan
cleanup thread.

### RAID_STRIPE (type 230)

**Key:** `(logical_offset, RAID_STRIPE, length)`

Maps logical extents to per-device physical stripe offsets. Requires the
`raid_stripe_tree` incompat feature.

**Payload** (variable):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `encoding` | 0 | 8 | RAID encoding type (LE u64) |
| `stripes[]` | 8 | ... | Array of stripe entries |

Each stripe entry (16 bytes):

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| `devid` | 0 | 8 | Device ID (LE u64) |
| `physical` | 8 | 8 | Physical byte offset (LE u64) |


## Checksums

Btrfs uses two CRC32C modes.

### Standard CRC32C (on-disk structures)

Used for all on-disk checksums: superblocks, tree block headers, and data
checksums (`EXTENT_CSUM` items).

This is the ISO 3309 (Castagnoli) CRC32C: seed = `0xFFFFFFFF`, and the result
is XORed with `0xFFFFFFFF`. It matches the standard `crc32c()` function in
most libraries.

```
checksum = crc32c(data)    // standard ISO 3309 CRC32C
```

The 4-byte LE result is stored in the checksum field. For superblocks and
tree blocks, the checksum covers everything from the end of the 32-byte csum
field to the end of the structure.

### Raw CRC32C (hash computations)

Used for internal hashes where the kernel calls `crc32c_le()` directly:
- Name hashes for `DIR_ITEM` keys (`crc32c(name)`)
- Name hashes for `XATTR_ITEM` keys
- Name hashes for `INODE_EXTREF` keys
- `extent_data_ref` key hash computation
- Send stream CRC32C

The raw CRC32C passes the seed through without inversion:

```
raw_crc32c(seed, data) = !crc32c_append(!seed, data)
```

This is NOT the standard ISO 3309 algorithm. The seed is typically
`0xFFFFFFFF` (`~0u32`), but unlike the standard algorithm, the output is not
inverted.

### Supported checksum algorithms

The `csum_type` field in the superblock selects the algorithm:

| Value | Name | Output size | Notes |
|-------|------|-------------|-------|
| 0 | CRC32C | 4 bytes | Default, by far the most common |
| 1 | xxHash64 | 8 bytes | Fast non-cryptographic hash |
| 2 | SHA-256 | 32 bytes | Cryptographic hash |
| 3 | BLAKE2b | 32 bytes | Cryptographic hash (BLAKE2b-256) |

The maximum checksum size is 32 bytes (`BTRFS_CSUM_SIZE`), which is also the
size of the checksum field in headers.


## Feature Flags

Feature flags are stored in three superblock fields. An implementation must
understand every set flag to operate correctly:

- `compat_flags`: backward-compatible features (no flags currently defined)
- `compat_ro_flags`: features compatible for read-only mounting
- `incompat_flags`: features that are fully incompatible

### Incompatible feature flags (`incompat_flags`)

| Bit | Value | Name | Notes |
|-----|-------|------|-------|
| 0 | `0x1` | `MIXED_BACKREF` | Mixed backref revision (always set on modern fs) |
| 1 | `0x2` | `DEFAULT_SUBVOL` | A non-default subvolume is the mount target |
| 2 | `0x4` | `MIXED_GROUPS` | Data and metadata may share block groups |
| 3 | `0x8` | `COMPRESS_LZO` | LZO compression used |
| 4 | `0x10` | `COMPRESS_ZSTD` | Zstandard compression used |
| 5 | `0x20` | `BIG_METADATA` | Metadata blocks > sectorsize (always set when nodesize > sectorsize) |
| 6 | `0x40` | `EXTENDED_IREF` | Extended inode references (`INODE_EXTREF` items) |
| 7 | `0x80` | `RAID56` | RAID5/6 profiles used |
| 8 | `0x100` | `SKINNY_METADATA` | Skinny metadata extent refs (`METADATA_ITEM` instead of `EXTENT_ITEM` for tree blocks) |
| 9 | `0x200` | `NO_HOLES` | File extents do not need explicit hole entries |
| 10 | `0x400` | `METADATA_UUID` | `metadata_uuid` differs from `fsid` |
| 11 | `0x800` | `RAID1C34` | RAID1C3/RAID1C4 profiles used |
| 12 | `0x1000` | `ZONED` | Zoned device support |
| 13 | `0x2000` | `EXTENT_TREE_V2` | Extent tree v2 (experimental) |
| 14 | `0x4000` | `RAID_STRIPE_TREE` | RAID stripe tree for stripe mappings |
| 16 | `0x10000` | `SIMPLE_QUOTA` | Simple quota (per-extent ownership tracking) |
| 17 | `0x20000` | `REMAP_TREE` | Remap tree (reserved for future use) |

**MIXED_BACKREF (bit 0):** The filesystem uses the mixed backref format
(revision 1). All modern filesystems set this. Old filesystems without it
use revision 0 backrefs.

**DEFAULT_SUBVOL (bit 1):** Set when a non-default subvolume has been made
the default mount target with `btrfs subvolume set-default`.

**MIXED_GROUPS (bit 2):** Allows data and metadata to share the same block
group. Unusual. Typically used only on very small filesystems.

**COMPRESS_LZO (bit 3):** Set when any file on the filesystem uses LZO
compression. Once set, it is never cleared.

**COMPRESS_ZSTD (bit 4):** Set when any file uses Zstandard compression.

**BIG_METADATA (bit 5):** Set when nodesize > sectorsize, so metadata blocks
span multiple sectors. Always set on modern filesystems with the typical
16384-byte nodesize and 4096-byte sectorsize.

**EXTENDED_IREF (bit 6):** Enables `INODE_EXTREF` items for inodes with hard
links from multiple parent directories. Without it, only `INODE_REF` is used
(keyed by a single parent inode, which limits hard links per parent
directory).

**SKINNY_METADATA (bit 8):** Uses `METADATA_ITEM` (type 169) instead of
`EXTENT_ITEM` (type 168) for tree block extent records. The tree block level
is encoded in the key offset. This removes the separate
`btrfs_tree_block_info` structure and saves 18 bytes per metadata extent
item.

**NO_HOLES (bit 9):** File extents do not need explicit hole entries.
Without this flag, holes in sparse files are `FILE_EXTENT_ITEM` entries with
`disk_bytenr = 0`. With it, holes are implicit (the gap needs no item).

**METADATA_UUID (bit 10):** The `metadata_uuid` field in the superblock
differs from `fsid`. This allows changing the user-visible filesystem UUID
without rewriting every tree block header.

### Compatible read-only feature flags (`compat_ro_flags`)

| Bit | Value | Name | Notes |
|-----|-------|------|-------|
| 0 | `0x1` | `FREE_SPACE_TREE` | Free space tree exists |
| 1 | `0x2` | `FREE_SPACE_TREE_VALID` | Free space tree is valid and should be used |
| 2 | `0x4` | `VERITY` | fs-verity support enabled |
| 3 | `0x8` | `BLOCK_GROUP_TREE` | Block group items in separate tree |

**FREE_SPACE_TREE (bit 0) and FREE_SPACE_TREE_VALID (bit 1):** When both are
set, the free space tree (objectid 10) is used instead of the legacy free
space cache (v1). Both bits must be set for the tree to be considered valid.

**VERITY (bit 2):** fs-verity has been enabled on at least one file, and the
filesystem contains `VERITY_DESC_ITEM` and `VERITY_MERKLE_ITEM` entries.

**BLOCK_GROUP_TREE (bit 3):** Block group items are stored in a dedicated
block group tree (objectid 11) instead of the extent tree. This speeds up
mounting, because finding block groups no longer needs a full extent tree
scan.


## Appendix A: Transaction Model

Btrfs uses a generation-based transaction model. Each transaction is
identified by a monotonically increasing `generation` counter stored in the
superblock.

### Transaction commit

A transaction commit works like this:

1. All modified tree blocks are written to new locations (COW). Each
   block's header records the current generation.
2. The superblock is updated with:
   - Incremented `generation`
   - New `root` (root tree root address)
   - New `chunk_root` (if chunk tree changed)
   - Updated `bytes_used` and `total_bytes`
   - Rotated `super_roots` backup entry
3. The superblock is written to all mirrors that fit on the device.

The superblock write is the atomic commit point. If the system crashes
before the superblock is fully written, the previous superblock (with the
previous generation) stays valid and the filesystem rolls back to that
state.

### Generation consistency

The generation appears in several places, which must all be consistent:

- Superblock `generation`: the current transaction counter
- Tree block header `generation`: must equal the generation when the
  block was last COWed
- Node key-pointer `generation`: must match the child block's header
  generation (used for read-time validation)
- `ROOT_ITEM.generation`: the generation when the tree was last modified
- Backup root `*_gen` fields: generation of each tree root at backup time

When reading a tree, the kernel checks that each block's generation matches
the expected generation from its parent's key-pointer. A mismatch indicates
corruption or a torn write.

### Superblock flag: CHANGING_FSID

The `BTRFS_SUPER_FLAG_CHANGING_FSID` flag (bit 35 of `flags`) is set during
an offline fsid rewrite. If the system crashes while the flag is set, the
rewrite must be completed or rolled back on the next access. This makes the
multi-block fsid change crash-safe.


## Appendix B: Size Constants

| Constant | Size | Notes |
|----------|------|-------|
| `BTRFS_SUPER_INFO_SIZE` | 4096 bytes | |
| `BTRFS_HEADER_SIZE` | 101 bytes | `sizeof(btrfs_header)` |
| `BTRFS_ITEM_SIZE` | 25 bytes | `sizeof(btrfs_item)` |
| `BTRFS_KEY_PTR_SIZE` | 33 bytes | `sizeof(btrfs_key_ptr)` |
| `BTRFS_DISK_KEY_SIZE` | 17 bytes | `sizeof(btrfs_disk_key)` |
| `BTRFS_CSUM_SIZE` | 32 bytes | Maximum checksum field width |
| `BTRFS_STRIPE_SIZE` | 32 bytes | `sizeof(btrfs_stripe)` |
| `BTRFS_INODE_ITEM_SIZE` | 160 bytes | `sizeof(btrfs_inode_item)` |
| `BTRFS_ROOT_ITEM_SIZE` | 439 bytes | `sizeof(btrfs_root_item)` |
| `BTRFS_DEV_ITEM_SIZE` | 98 bytes | `sizeof(btrfs_dev_item)` |
| `BTRFS_TIMESPEC_SIZE` | 12 bytes | `sizeof(btrfs_timespec)` |
| `BTRFS_BLOCK_GROUP_SIZE` | 24 bytes | `sizeof(btrfs_block_group_item)` |
| `BTRFS_EXTENT_ITEM_SIZE` | 24 bytes | `sizeof(btrfs_extent_item)` |
| `BTRFS_TREE_BLOCK_INFO_SIZE` | 18 bytes | `sizeof(btrfs_tree_block_info)` |
| `BTRFS_EXTENT_DATA_REF_SIZE` | 28 bytes | `sizeof(btrfs_extent_data_ref)` |
| `BTRFS_DEV_EXTENT_SIZE` | 48 bytes | `sizeof(btrfs_dev_extent)` |
| `BTRFS_FREE_SPACE_INFO_SIZE` | 8 bytes | `sizeof(btrfs_free_space_info)` |
| `BTRFS_ROOT_REF_SIZE` | 18 bytes | `sizeof(btrfs_root_ref)`, without name |
| `BTRFS_DIR_ITEM_SIZE` | 30 bytes | `sizeof(btrfs_dir_item)`, without name/data |
| `BTRFS_BACKUP_ROOT_SIZE` | 168 bytes | `sizeof(btrfs_root_backup)` |
| `SYS_CHUNK_ARRAY_SIZE` | 2048 bytes | |


## Appendix C: Logical-to-Physical Address Resolution

All tree block and extent addresses in btrfs are **logical** addresses. To
read one from disk, it must be resolved to a physical device offset through
the chunk tree.

Resolution works like this:

1. **Bootstrap:** Parse the superblock's `sys_chunk_array` to seed an
   initial chunk cache with system chunk mappings.

2. **Read the chunk tree:** Using the system chunk mappings, resolve
   `superblock.chunk_root` to a physical address and read the chunk
   tree. Add all `CHUNK_ITEM` entries to the cache.

3. **Resolve:** For any logical address, find the chunk whose range
   contains that address. The physical address is:

   ```
   physical = stripe.offset + (logical - chunk.logical)
   ```

   For SINGLE and DUP profiles, any stripe yields a valid copy. For
   RAID1, all stripes hold identical copies. For RAID0, RAID5, RAID6, and
   RAID10, the stripe index must be calculated.

4. **Read the root tree:** Using the full chunk cache, resolve
   `superblock.root` to a physical address and read the root tree.
   From there, all other trees can be located through their `ROOT_ITEM`
   entries.


## Appendix D: File Data Layout

A regular file's on-disk data is described by a sequence of
`FILE_EXTENT_ITEM` entries in the FS tree, keyed by `(inode, EXTENT_DATA,
file_offset)`.

**Inline extents:** Small files (typically < sectorsize) store their data
directly in the tree leaf. No separate disk allocation is needed.

**Regular extents:** Larger files reference data stored in data chunks. The
extent is described by `disk_bytenr` (logical address) and `disk_num_bytes`
(on-disk size). The `offset` field allows partial references into shared
extents (for example, after COW or clone operations).

**Compressed extents:** With compression, the `compression` field is
nonzero, `disk_num_bytes` is the compressed size, and `ram_bytes` is the
uncompressed size. Inline compressed extents store the compressed data
directly in the item.

**Sparse files:** With the `NO_HOLES` feature, gaps between extent items
are implicit holes. Without it, explicit hole entries with
`disk_bytenr = 0` fill the gaps.

The file size is stored in `INODE_ITEM.size`. It is authoritative even if
the extent items suggest a different range.

### Extent sharing and cloning

When a file extent is cloned (with `cp --reflink` or `BTRFS_IOC_CLONE`),
the source and destination inodes both reference the same on-disk extent
through their `FILE_EXTENT_ITEM` entries. The reference count in the extent
tree's `EXTENT_ITEM` is incremented.

The `offset` field in `FILE_EXTENT_ITEM` lets each reference start at a
different position within the shared extent:

```
File A:  [--- extent X (offset=0, num_bytes=4096) ---]
File B:  [--- extent X (offset=2048, num_bytes=2048) ---]
```

Both reference the same `disk_bytenr`, but File B starts reading 2048
bytes into the extent.

### Compression type encoding

The `compression` field in `FILE_EXTENT_ITEM` uses these values:

| Value | Name | Notes |
|-------|------|-------|
| 0 | none | No compression |
| 1 | zlib | Deflate compression |
| 2 | lzo | LZO compression (btrfs per-sector format) |
| 3 | zstd | Zstandard compression |

For compressed inline extents, the stored data is compressed and the inline
data size may differ from `ram_bytes`.


## Appendix E: Subvolume and Snapshot Model

### Subvolumes

Each subvolume is an independent FS tree with its own tree objectid (5 for
the default, 256+ for user-created subvolumes). The root tree stores:

- A `ROOT_ITEM` for each subvolume, recording the root block address,
  generation, UUIDs, and timestamps.
- `ROOT_REF` and `ROOT_BACKREF` pairs linking parent and child subvolumes.

### Snapshots

A snapshot is a subvolume created by COWing the root block of another
subvolume. At creation, the snapshot shares all tree blocks with the
source. As either one is modified, shared blocks are COWed on demand and
the two gradually diverge.

The `parent_uuid` field in `ROOT_ITEM` links a snapshot back to its source
subvolume. The `received_uuid` field tracks the source across send and
receive.

### Subvolume deletion

Deleted subvolumes are marked with the `SUBVOL_DEAD` flag in their
`ROOT_ITEM.flags`. The kernel cleans up the tree blocks asynchronously and
tracks progress in the `drop_progress` key and `drop_level` fields.

### Read-only snapshots

A subvolume is made read-only by setting the `SUBVOL_RDONLY` flag in
`ROOT_ITEM.flags`. Send requires this: the source subvolume must be
read-only.


## Appendix F: Name Hashing

Directory entries (`DIR_ITEM`) and extended attributes (`XATTR_ITEM`) are
keyed by a CRC32C hash of the name. The hash uses raw CRC32C (see
[Raw CRC32C](#raw-crc32c-hash-computations)) with seed `~1`
(`btrfs_name_hash` in `disk/src/util.rs`):

```
hash = raw_crc32c(0xFFFFFFFE, name_bytes)
```

This hash is the key offset of the `DIR_ITEM`. If two names hash to the
same value (collision), their `DIR_ITEM` entries are concatenated in a
single item.

`DIR_INDEX` entries use a monotonically increasing sequence number instead
of a hash. This gives a deterministic iteration order that does not depend
on name hashing.

For `INODE_EXTREF`, the key offset is a hash of the parent inode number and
the name. This project reads `INODE_EXTREF` items but never computes that
hash, so the exact formula is not covered here.


## Appendix G: Block Group and Chunk Relationship

Chunks, block groups, and device extents together form the space allocation
layer:

```
Chunk (chunk tree)
  |
  +-- maps logical range [L, L+length) to physical stripes
  |   on one or more devices
  |
  +-- Block Group (extent tree or block group tree)
  |     tracks used/free space within the logical range
  |     type flags must match the chunk type
  |
  +-- Device Extent(s) (device tree)
        one per stripe, maps physical range back to the chunk
```

**Allocation order:** mkfs computes the whole chunk layout up front
(`ChunkLayout` in `mkfs/src/layout.rs`), choosing a physical region on each
device and a logical range for each chunk. The bootstrap then writes the
`CHUNK_ITEM`s (chunk tree), `DEV_EXTENT`s (device tree), and
`BLOCK_GROUP_ITEM`s (extent tree). The post-bootstrap transaction moves the
block group items to the block group tree when that feature is on, and
seeds the free space tree with `FREE_SPACE_INFO` and `FREE_SPACE_EXTENT`
entries.

**Consistency invariant:** For every chunk, there must be:
- Exactly one `BLOCK_GROUP_ITEM` with matching logical offset and length
- One `DEV_EXTENT` per stripe, with `chunk_offset` pointing back to the chunk
- The block group `flags` must match the chunk `type` field

`btrfs check` verifies these cross-references.


## Appendix H: Default Feature Set

A modern btrfs filesystem created by `mkfs.btrfs` (or this project's
`btrfs-mkfs`) typically has these features enabled:

**Incompatible features:**
- `MIXED_BACKREF` (bit 0): always set
- `BIG_METADATA` (bit 5): set because nodesize (16384) > sectorsize (4096)
- `EXTENDED_IREF` (bit 6): enables extended inode references
- `SKINNY_METADATA` (bit 8): compact metadata extent records
- `NO_HOLES` (bit 9): implicit holes in sparse files

**Compatible read-only features:**
- `FREE_SPACE_TREE` (bit 0): free space tracking tree
- `FREE_SPACE_TREE_VALID` (bit 1): free space tree is valid
- `BLOCK_GROUP_TREE` (bit 3): block group items in their own tree

These are the `extref`, `skinny-metadata`, `no-holes`, `free-space-tree`,
and `block-group-tree` features referenced in mkfs output
(`MkfsConfig::default_incompat_flags` and `default_compat_ro_flags` in
`mkfs/src/mkfs.rs`).

**Default parameters:**
- `nodesize` = 16384 (16 KiB)
- `sectorsize` = 4096 (4 KiB), matching the device sector size
- `stripesize` = 65536 (64 KiB)
- `csum_type` = 0 (CRC32C)
- Metadata profile: DUP (two copies on the same device) for one device,
  RAID1 for several
- Data profile: SINGLE (no redundancy)
- System profile: SINGLE. mkfs writes one 4 MiB system chunk at 1 MiB on
  device 1 (`SYSTEM_GROUP_OFFSET` and `SYSTEM_GROUP_SIZE` in
  `mkfs/src/layout.rs`) and does not convert it.


## Appendix I: Extent Reference Counting

Btrfs tracks references to every allocated extent (data and metadata) in
the extent tree. The reference count in `EXTENT_ITEM.refs` (or
`METADATA_ITEM.refs`) records how many times the extent is referenced.

### Metadata extents

A metadata extent (tree block) is referenced by key-pointers in parent
nodes. A new snapshot initially shares all tree blocks with the source, so
each shared block has `refs >= 2`. When either tree COWs a shared block,
the old block's refcount is decremented and the new copy gets `refs = 1`.

Backreferences track which trees own each block:
- `TREE_BLOCK_REF` (inline or standalone): direct ownership by a tree root
- `SHARED_BLOCK_REF` (inline or standalone): ownership through a parent
  block that is itself shared between trees

### Data extents

A data extent is referenced by `FILE_EXTENT_ITEM` entries in FS trees.
Several files (or several positions in one file) can reference the same
data extent through reflink cloning.

Backreferences track which file inodes reference each extent:
- `EXTENT_DATA_REF` (inline or standalone): records `(root, inode, offset, count)`
- `SHARED_DATA_REF` (inline or standalone): records `(parent_block, count)`

### Reference count invariant

The `refs` field must equal the sum of all backreference counts for the
extent. `btrfs check` verifies this invariant by walking the extent tree
and cross-referencing with the FS trees.

When `refs` reaches 0, the extent is freed and its space is returned to the
block group's free space pool.

# Btrfs Transaction Infrastructure: On-Disk Format Specification

This document is the sole reference for implementing the `btrfs-transaction`
crate. It describes the on-disk format, invariants, and protocols needed to
safely modify a btrfs filesystem from userspace. 

## Tree block layout

A btrfs filesystem stores its metadata in a B-tree. Each tree block (also
called a node or extent buffer) is `nodesize` bytes (typically 16,384, but
can be 4,096 to 65,536). Tree blocks are identified by their logical byte
address (bytenr), which is translated to a physical device offset via the
chunk tree.

Every tree block begins with a 101-byte header, followed by either leaf
items (level 0) or internal node key pointers (level > 0).

### Header (101 bytes)

All multi-byte integers are little-endian on disk.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 32 | `csum` | Checksum of bytes 32..`nodesize` (header fields after csum + all payload). Algorithm determined by superblock `csum_type`. Zero-padded: for `CRC32C` only bytes 0..3 are meaningful. |
| 32 | 16 | `fsid` | Filesystem UUID. Must match superblock `fsid` (or `metadata_uuid` if `METADATA_UUID` incompat flag is set). |
| 48 | 8 | `bytenr` | Logical byte address of this block. Must match the address used to read/write it. |
| 56 | 8 | `flags` | Bits 0..55: header flags (currently unused by userspace). Bits 56..63: backref revision (1 = mixed backrefs, the modern format). |
| 64 | 16 | `chunk_tree_uuid` | UUID of the chunk tree that maps this block's logical address to physical. Typically the same for all blocks on a single-device fs. |
| 80 | 8 | `generation` | Transaction generation when this block was last written. Critical for COW: a block with `generation` == current transaction has already been COWed and can be modified in place. |
| 88 | 8 | `owner` | Tree ID that owns this block (e.g. 1 for root tree, 2 for extent tree, 5 for default fs tree). Used for backref accounting. |
| 96 | 4 | `nritems` | Number of items (leaf) or key pointers (node). |
| 100 | 1 | `level` | B-tree level. 0 = leaf, 1..7 = internal node. Maximum level is 7 (`BTRFS_MAX_LEVEL` = 8 levels total, 0-indexed). |

### Key (17 bytes)

Every item and pointer in the B-tree is identified by a three-part key.
On disk this is the `btrfs_disk_key` (little-endian):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `objectid` | Primary identifier (inode number, tree ID, extent bytenr, etc. depending on key type). |
| 8 | 1 | `type` | Key type discriminator (see section 7). |
| 9 | 8 | `offset` | Type-specific secondary value (file offset, extent size, parent ID, etc.). |

Keys are compared as a tuple `(objectid, type, offset)` in that order, all
as unsigned integers. This defines the sort order within every B-tree.

### Leaf layout (level 0)

A leaf contains item descriptors that grow forward from the header, and item
data payloads that grow backward from the end of the block. Free space is
the gap between them.

```
Byte 0..100:                    Header
Byte 101..101+nritems*25-1:     Item descriptors [item0, item1, ..., itemN-1]
                                (25 bytes each, sorted by key ascending)
  ...free space...
Byte X..nodesize-1:             Item data [dataN-1, ..., data1, data0]
                                (packed from the end of the block backward)
```

Each item descriptor is 25 bytes:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 17 | `key` | The item's key (`btrfs_disk_key`). |
| 17 | 4 | `offset` | Byte offset of this item's data payload, relative to the start of the data area (byte 101). To get the absolute position in the block: absolute = 101 + `offset`. |
| 21 | 4 | `size` | Size of the item's data payload in bytes. |

**Invariants:**

- Items are sorted by key in ascending order.
- Item data regions must not overlap.
- The last item's data starts at `101 + item[N-1].offset` and extends for
  `item[N-1].size` bytes. Items with lower indices have data at higher
  offsets (data grows backward).
- The first item's data ends at `101 + item[0].offset + item[0].size`,
  which must be <= `nodesize`.
- Free space = `(101 + item[N-1].offset)` - `(101 + nritems * 25)`.
  When this is < 25 + data_size for a new item, the leaf is full.

**Data offset convention:**

The `offset` field in `btrfs_item` counts from byte 101 (immediately after
the header), not from the start of the block. When constructing a new leaf:

1. Start `data_end` at `nodesize`.
2. For each item (in key order): `data_end -= data.len()`, write data at
   `data_end`, store `offset = data_end - 101` in the item descriptor.
3. Item descriptors are written at `101 + i * 25`.

### Internal node layout (level > 0)

An internal node contains key pointers that identify child subtrees.

```
Byte 0..100:                    Header
Byte 101..101+nritems*33-1:     Key pointers [ptr0, ptr1, ..., ptrN-1]
                                (33 bytes each, sorted by key ascending)
```

Each key pointer is 33 bytes:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 17 | `key` | Lowest key in the child subtree. |
| 17 | 8 | `blockptr` | Logical byte address of the child block. |
| 25 | 8 | `generation` | Generation of the child block (used for consistency checking during reads). |

**Invariants:**

- Key pointers are sorted by key in ascending order.
- `blockptr` must be a valid, allocated logical address.
- `generation` must match the generation in the child block's header.

### Maximum capacities

For a given `nodesize`:

- Leaf items per block: depends on item data size. The theoretical maximum
  number of zero-size items is `(nodesize - 101) / 25` = 651 for 16 KiB.
- Key pointers per node: `(nodesize - 101) / 33` = 493 for 16 KiB.
- Maximum tree depth: 8 levels (`BTRFS_MAX_LEVEL`). In practice, trees rarely
  exceed 3-4 levels.

## Superblock

The superblock is the entry point for reading a btrfs filesystem. It is a
4,096-byte structure stored at fixed offsets on every device:

- Mirror 0: byte 65,536 (64 KiB)
- Mirror 1: byte 67,108,864 (64 MiB)
- Mirror 2: byte 274,877,906,944 (256 GiB), only if device is large enough

### Superblock layout (4,096 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 32 | `csum` | Checksum of bytes 32..4095. |
| 32 | 16 | `fsid` | Filesystem UUID. |
| 48 | 8 | `bytenr` | Physical byte offset of this copy. |
| 56 | 8 | `flags` | `BTRFS_SUPER_FLAG_*` bits. |
| 64 | 8 | `magic` | 0x4D5F53665248425F ("_BHRfS_M" reversed). |
| 72 | 8 | `generation` | Current transaction generation. |
| 80 | 8 | `root` | Logical bytenr of root tree root block. |
| 88 | 8 | `chunk_root` | Logical bytenr of chunk tree root block. |
| 96 | 8 | `log_root` | Logical bytenr of log tree root (0 if none). |
| 104 | 8 | `__unused_log_root_transid` | Deprecated, always 0. |
| 112 | 8 | `total_bytes` | Total usable bytes across all devices. |
| 120 | 8 | `bytes_used` | Total bytes allocated to extents. |
| 128 | 8 | `root_dir_objectid` | Always 6 (`BTRFS_ROOT_TREE_DIR_OBJECTID`). |
| 136 | 8 | `num_devices` | Number of devices. |
| 144 | 4 | `sectorsize` | Minimum I/O unit (typically 4096). |
| 148 | 4 | `nodesize` | Tree block size (typically 16384). |
| 152 | 4 | `__unused_leafsize` | Legacy, always equal to `nodesize`. |
| 156 | 4 | `stripesize` | RAID stripe unit (typically 65536). |
| 160 | 4 | `sys_chunk_array_size` | Valid bytes in the `sys_chunk_array` field. |
| 164 | 8 | `chunk_root_generation` | Generation of the chunk tree root. |
| 172 | 8 | `compat_flags` | Compatible feature flags. |
| 180 | 8 | `compat_ro_flags` | Read-only compatible feature flags. |
| 188 | 8 | `incompat_flags` | Incompatible feature flags. |
| 196 | 2 | `csum_type` | Checksum algorithm (0=`CRC32C`, 1=xxhash, 2=SHA256, 3=BLAKE2). |
| 198 | 1 | `root_level` | B-tree level of root tree root. |
| 199 | 1 | `chunk_root_level` | B-tree level of chunk tree root. |
| 200 | 1 | `log_root_level` | B-tree level of log tree root. |
| 201 | 98 | `dev_item` | Embedded device item for this device (see section 6.4). |
| 299 | 256 | `label` | NUL-terminated filesystem label. |
| 555 | 8 | `cache_generation` | Free space cache v1 generation. |
| 563 | 8 | `uuid_tree_generation` | UUID tree last-updated generation. |
| 571 | 16 | `metadata_uuid` | Metadata UUID (if `METADATA_UUID` flag set). |
| 587 | 8 | `nr_global_roots` | Global root count (extent-tree-v2, rare). |
| 595 | 8 | `remap_root` | Remap tree bytenr. |
| 603 | 8 | `remap_root_generation` | Remap tree generation. |
| 611 | 1 | `remap_root_level` | Remap tree level. |
| 612 | 199 | `reserved` | Zero-filled. |
| 811 | 2048 | `sys_chunk_array` | Bootstrap chunk tree entries (key + chunk item pairs, packed sequentially). |
| 2859 | 668 | `super_roots` | 4 rotating backup root entries (167 bytes each). See section 2.3. |
| 3527 | 569 | `padding` | Zero-filled to 4096. |

### Fields updated on every transaction commit

When committing a transaction, the following superblock fields are updated:

1. `generation` — incremented by 1.
2. `root` — logical bytenr of the (possibly new) root tree root block.
3. `root_level` — level of the root tree root.
4. `chunk_root` — logical bytenr of the chunk tree root (if chunk tree
   was modified).
5. `chunk_root_generation` — generation of the chunk tree root.
6. `chunk_root_level` — level of the chunk tree root.
7. `bytes_used` — updated to reflect allocations/frees.
8. `log_root` — set to 0 after log replay, or updated if log is active.
9. `super_roots` — one of the 4 backup root slots is written (rotating).
10. `csum` — recomputed last, covering bytes 32..4095.

The commit writes the superblock to all mirrors. The superblock write is
the atomic commit point: if power is lost before the superblock is written,
the previous generation's state is intact because COW ensures old blocks
are never overwritten (see section 3).

### Backup roots (167 bytes each, 4 entries)

The superblock contains 4 rotating backup root entries. On each commit, one
slot is overwritten (cycling 0 → 1 → 2 → 3 → 0 → ...). These are used for
recovery when the primary root pointers are corrupt.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `tree_root` | Root tree root bytenr. |
| 8 | 8 | `tree_root_gen` | Root tree generation. |
| 16 | 8 | `chunk_root` | Chunk tree root bytenr. |
| 24 | 8 | `chunk_root_gen` | Chunk tree generation. |
| 32 | 8 | `extent_root` | Extent tree root bytenr. |
| 40 | 8 | `extent_root_gen` | Extent tree generation. |
| 48 | 8 | `fs_root` | Default FS tree root bytenr. |
| 56 | 8 | `fs_root_gen` | FS tree generation. |
| 64 | 8 | `dev_root` | Device tree root bytenr. |
| 72 | 8 | `dev_root_gen` | Device tree generation. |
| 80 | 8 | `csum_root` | Checksum tree root bytenr. |
| 88 | 8 | `csum_root_gen` | Checksum tree generation. |
| 96 | 8 | `total_bytes` | Total filesystem bytes at this point. |
| 104 | 8 | `bytes_used` | Bytes used at this point. |
| 112 | 8 | `num_devices` | Device count at this point. |
| 120 | 32 | `unused` | Reserved (zero). |
| 152 | 1 | `tree_root_level` | Root tree level. |
| 153 | 1 | `chunk_root_level` | Chunk tree level. |
| 154 | 1 | `extent_root_level` | Extent tree level. |
| 155 | 1 | `fs_root_level` | FS tree level. |
| 156 | 1 | `dev_root_level` | Device tree level. |
| 157 | 1 | `csum_root_level` | Checksum tree level. |
| 158 | 9 | `padding` | Padding to 167 bytes. |

### Superblock flags

| Bit | Name | Description |
|-----|------|-------------|
| 2 | `BTRFS_SUPER_FLAG_ERROR` | Filesystem has errors. |
| 32 | `BTRFS_SUPER_FLAG_SEEDING` | Seed device (read-only base for cloning). |
| 33 | `BTRFS_SUPER_FLAG_METADUMP` | Metadump image. |
| 34 | `BTRFS_SUPER_FLAG_METADUMP_V2` | Metadump v2 image. |
| 35 | `BTRFS_SUPER_FLAG_CHANGING_FSID` | FSID rewrite in progress. |
| 36 | `BTRFS_SUPER_FLAG_CHANGING_FSID_V2` | FSID rewrite v2 in progress. |
| 38 | `BTRFS_SUPER_FLAG_CHANGING_BG_TREE` | Block group tree migration. |
| 39 | `BTRFS_SUPER_FLAG_CHANGING_DATA_CSUM` | Data csum algorithm change. |
| 40 | `BTRFS_SUPER_FLAG_CHANGING_META_CSUM` | Metadata csum algorithm change. |

### Feature flags

**Incompatible (incompat_flags):**

| Bit | Name | Hex | Description |
|-----|------|-----|-------------|
| 0 | `MIXED_BACKREF` | 0x1 | Modern backreference format. |
| 1 | `DEFAULT_SUBVOL` | 0x2 | Non-default default subvolume set. |
| 2 | `MIXED_GROUPS` | 0x4 | Mixed data+metadata block groups. |
| 3 | `COMPRESS_LZO` | 0x8 | LZO compression used. |
| 4 | `COMPRESS_ZSTD` | 0x10 | ZSTD compression used. |
| 5 | `BIG_METADATA` | 0x20 | Metadata blocks > 4 KiB (always set with modern mkfs for `nodesize` > 4096). |
| 6 | `EXTENDED_IREF` | 0x40 | Extended inode references (`INODE_EXTREF`). |
| 7 | `RAID56` | 0x80 | RAID5/RAID6 profiles in use. |
| 8 | `SKINNY_METADATA` | 0x100 | Skinny metadata extent refs (see 5.1). |
| 9 | `NO_HOLES` | 0x200 | No explicit hole extent items. |
| 10 | `METADATA_UUID` | 0x400 | `metadata_uuid` field is in use. |
| 11 | `RAID1C34` | 0x800 | RAID1C3 or RAID1C4 profiles in use. |
| 12 | `ZONED` | 0x1000 | Zoned block device support. |
| 13 | `EXTENT_TREE_V2` | 0x2000 | Extent tree v2 (experimental). |
| 14 | `RAID_STRIPE_TREE` | 0x4000 | RAID stripe tree. |
| 16 | `SIMPLE_QUOTA` | 0x10000 | Simple quota accounting. |

**Read-only compatible (compat_ro_flags):**

| Bit | Name | Hex | Description |
|-----|------|-----|-------------|
| 0 | `FREE_SPACE_TREE` | 0x1 | Free space tree present. |
| 1 | `FREE_SPACE_TREE_VALID` | 0x2 | Free space tree is valid/consistent. |
| 2 | `VERITY` | 0x4 | fs-verity enabled files present. |
| 3 | `BLOCK_GROUP_TREE` | 0x8 | Separate block group tree. |

**Default features for modern mkfs:**
- `incompat_flags`: `MIXED_BACKREF | BIG_METADATA | EXTENDED_IREF | SKINNY_METADATA | NO_HOLES` = `0x361`
- `compat_ro_flags`: `FREE_SPACE_TREE | FREE_SPACE_TREE_VALID | BLOCK_GROUP_TREE` = `0xB`

### System chunk array

The `sys_chunk_array` (2,048 bytes at offset 811) contains bootstrap chunk
entries needed to read the chunk tree itself. Format: packed sequence of
`(btrfs_disk_key, btrfs_chunk)` pairs. The `sys_chunk_array_size` field says
how many bytes are valid. Parsing: read key (17 bytes), then chunk header
(48 bytes) + stripes (`num_stripes` * 32 bytes), repeat until consumed.

## Copy-on-write (COW) protocol

Btrfs never modifies tree blocks in place (except when a block was already
allocated in the current transaction). This is the fundamental mechanism
that provides crash consistency.

### COW a tree block

When a transaction needs to modify a tree block:

1. **Check generation.** If `block.generation == current_transaction_generation`,
   the block was already COWed in this transaction. Modify it in place.

2. **Allocate a new block.** Find free space in an appropriate metadata block
   group and allocate `nodesize` bytes at a new logical address.

3. **Copy.** Copy the entire block contents to the new address.

4. **Update parent pointer.** In the parent node, change the `blockptr` for
   the relevant slot to the new address, and set `generation` to the current
   transaction generation.

5. **Update the new block's header.** Set `bytenr` to the new logical
   address, `generation` to the current transaction generation.

6. **Queue old block for freeing.** The old block's extent reference is
   decremented. If its refcount reaches 0, the space is freed (but only
   after the transaction commits, to maintain crash consistency).

7. **COW cascades upward.** If the parent was not yet COWed, it must be
   COWed first (step 1 check), then updated. This cascades up to the root.

### COW and the root pointer

The root of each tree is stored in a `root_item` in the root tree (tree ID 1).
The root tree's own root pointer is stored in the superblock (`root` field).

When COW reaches the root of a non-root tree:
- Update the `root_item`'s `bytenr` and `level` fields in the root tree.
- This modification to the root tree triggers COW of the root tree itself.

When COW reaches the root tree's root:
- The new root block address is written to the superblock's `root` field
  at commit time.

### COW and the chunk tree

The chunk tree root is special: its pointer lives directly in the superblock
(`chunk_root` field), not in the root tree. If the chunk tree is modified,
its new root address updates `chunk_root` at commit time.

### Crash consistency

The commit point is the superblock write. Before the superblock is updated:
- All new tree blocks have been written to new locations.
- All old tree blocks are still intact at their original locations.
- The old superblock still points to the old root tree root, which points
  to the old state of all trees.

If power is lost before the superblock write completes, the filesystem
reverts to the previous generation. No fsck needed.

## Transaction lifecycle

A transaction groups multiple tree modifications into a single atomic commit.

### Start

1. Read the current superblock generation `G`.
2. Set the new transaction generation to `G + 1`.
3. Track all blocks modified during this transaction (the "dirty set").

### Modify

All tree modifications (insert, delete, update items) go through COW:
- `search_slot` descends the tree, COWing each block along the path.
- Item operations modify the COWed leaf.
- Reference counts are updated for allocated and freed extents.

### Commit

1. **Flush pending reference updates.** Process all queued extent reference
   changes (delayed refs, see section 5.3). This may modify the extent tree,
   which may COW more blocks and generate more ref updates. Repeat until
   stable (no more pending updates).

2. **Update root items.** For every tree whose root block changed, update
   its `root_item` in the root tree (fields: `bytenr`, `generation`,
   `level`). This may COW the root tree.

3. **Write dirty blocks.** Write all blocks in the dirty set to disk with
   correct checksums. Each block's checksum covers bytes 32..`nodesize`.

4. **Prepare superblock.** Update the superblock fields listed in section
   2.2. Write one backup root entry (rotating through slots 0-3).
   Recompute the superblock checksum.

5. **Write superblock.** Write the superblock to all mirrors. Issue fsync
   to ensure durability.

### Abort

Discard all dirty blocks. Do not write the superblock. The filesystem
remains at the previous generation.

## Extent tree and reference counting

The extent tree (tree ID 2) tracks which logical address ranges are
allocated and who references them. Every allocated extent (both data and
metadata) has an entry in the extent tree.

### Extent items

There are two key types for extent records:

**`EXTENT_ITEM` (type 168):** Used for data extents and (on older filesystems
without `SKINNY_METADATA`) for tree blocks.
- Key: `(logical_bytenr, EXTENT_ITEM=168, size_in_bytes)`
- Data: `extent_item` header (24 bytes), optionally `tree_block_info` (18 bytes),
  then inline backreferences.

**`METADATA_ITEM` (type 169):** Used for tree blocks when `SKINNY_METADATA`
incompat flag is set. This is the modern default.
- Key: `(logical_bytenr, METADATA_ITEM=169, tree_level)`
- Data: `extent_item` header (24 bytes), then inline backreferences.
  No `tree_block_info` (the level is in the key offset, and the first key
  is not stored).

**Extent item header (24 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `refs` | Total reference count for this extent. |
| 8 | 8 | `generation` | Transaction generation when allocated. |
| 16 | 8 | `flags` | `EXTENT_FLAG_DATA` (bit 0) for data extents, `EXTENT_FLAG_TREE_BLOCK` (bit 1) for metadata. `BLOCK_FLAG_FULL_BACKREF` (bit 8) indicates full backrefs (shared block refs use parent bytenr instead of root ID). |

**Tree block info (18 bytes, only for non-skinny `EXTENT_ITEM` with `TREE_BLOCK` flag):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 17 | `key` | First key in the tree block (`btrfs_disk_key`). |
| 17 | 1 | `level` | Level of the tree block. |

### Backreferences

Backreferences record who uses an extent. They come in two forms: inline
(packed inside the extent item's data) and standalone (separate items in
the extent tree).

**Inline backreferences** follow the extent item header (and tree_block_info
if present). Each inline ref has a 1-byte type followed by an 8-byte offset,
then type-specific data:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | `type` | One of the backref type codes below. |
| 1 | 8 | `offset` | Type-dependent (see below). |

The backref types:

| Type code | Name                  | Offset meaning        | Extra data    | Total inline size |
|-----------|-----------------------|-----------------------|---------------|-------------------|
| 176       | `TREE_BLOCK_REF`      | Root tree ID          | (none)        | 9 bytes           |
| 182       | `SHARED_BLOCK_REF`    | Parent block bytenr   | (none)        | 9 bytes           |
| 178       | `EXTENT_DATA_REF`     | (see below)           | 28 bytes      | 37 bytes          |
| 184       | `SHARED_DATA_REF`     | Parent block bytenr   | 4-byte count  | 13 bytes          |
| 172       | `EXTENT_OWNER_REF`    | Root tree ID          | (none)        | 9 bytes           |

**`TREE_BLOCK_REF` (type 176):** A tree block is referenced by a specific
tree (identified by root ID). The `offset` field IS the root objectid.
No additional data. Each such ref contributes 1 to the extent's refcount.

**`SHARED_BLOCK_REF` (type 182):** A tree block is referenced by another tree
block (identified by its bytenr) rather than by root ID. This happens
during snapshots. The `offset` field IS the parent block's bytenr. Each
such ref contributes 1 to the extent's refcount.

**`EXTENT_DATA_REF` (type 178):** A data extent is referenced by a file. The
inline form packs the following 28 bytes immediately after the type byte
(the 8-byte `offset` from the generic header is actually the first field
`root` of this struct — parse carefully):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `root` | Root tree ID containing the referencing inode. |
| 8 | 8 | `objectid` | Inode number. |
| 16 | 8 | `offset` | File offset where this extent is referenced. |
| 24 | 4 | `count` | Number of references (typically 1, >1 for reflinked files). |

Each `EXTENT_DATA_REF` contributes `count` to the extent's refcount.

**`SHARED_DATA_REF` (type 184):** A data extent is referenced through a shared
tree block (snapshot). The `offset` field is the parent block bytenr.
Additional 4 bytes:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | `count` | Reference count from this parent. |

Each `SHARED_DATA_REF` contributes `count` to the extent's refcount.

**Standalone backreferences:** When inline refs don't fit in the extent item
(rare, happens with many references), they overflow to standalone items:

- `TREE_BLOCK_REF_KEY` (176): key `(extent_bytenr, 176, root_id)`, no data.
- `SHARED_BLOCK_REF_KEY` (182): key `(extent_bytenr, 182, parent_bytenr)`, no data.
- `EXTENT_DATA_REF_KEY` (178): key `(extent_bytenr, 178, hash)`, 28-byte
  `btrfs_extent_data_ref` data. The hash is computed as:
  ```
  high_crc = crc32c(seed=0xFFFFFFFF, root.to_le_bytes())
  low_crc  = crc32c(seed=0xFFFFFFFF, objectid.to_le_bytes())
  low_crc  = crc32c(seed=low_crc,    offset.to_le_bytes())
  hash     = (high_crc as u64) << 31 ^ (low_crc as u64)
  ```
  Note: these are raw `CRC32C` (no final inversion), not the standard ISO
  3309 form.
- `SHARED_DATA_REF_KEY` (184): key `(extent_bytenr, 184, parent_bytenr)`,
  4-byte count.

### Delayed references

Modifying a tree generates many reference count updates (every COWed block
creates a new ref and removes an old ref). Processing each one immediately
would cause excessive extent tree modifications. Instead, reference updates
are queued and batched:

1. When a block is COWed, queue: `+1 ref at new_bytenr`, `-1 ref at
   old_bytenr`.
2. When a block is allocated for splitting, queue `+1 ref`.
3. When blocks are freed (e.g., after merging), queue `-1 ref`.

At commit time, process all queued refs:
- Merge updates to the same extent (e.g., `+1` and `-1` cancel out).
- For each remaining update, modify the extent item in the extent tree.
- If a refcount drops to 0, delete the extent item and free the space.
- Processing delayed refs modifies the extent tree, which may generate
  more delayed refs (from COWing extent tree blocks). Repeat until the
  queue is empty. This converges because each iteration processes more
  refs than it creates.

### Refcount invariant

The `refs` field in an extent item must always equal the sum of all its
backreferences:
- Each `TREE_BLOCK_REF` or `SHARED_BLOCK_REF` contributes 1.
- Each `EXTENT_DATA_REF` contributes its `count` field.
- Each `SHARED_DATA_REF` contributes its `count` field.

If `refs` reaches 0, the extent is freed.

## Block groups, chunks, and device extents

Btrfs organizes disk space into three layers: block groups (logical
allocation regions), chunks (logical-to-physical mapping), and device
extents (physical device reservations).

### Block group item (24 bytes)

Stored in the extent tree (or block group tree if `BLOCK_GROUP_TREE`
compat_ro flag is set).

Key: `(logical_offset, BLOCK_GROUP_ITEM=192, length)`

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `used` | Bytes currently allocated within this group. |
| 8 | 8 | `chunk_objectid` | Always 256 (`BTRFS_FIRST_CHUNK_TREE_OBJECTID`). |
| 16 | 8 | `flags` | Type + RAID profile (see 6.5). |

Block groups are the allocation units: when allocating an extent, the
allocator finds a block group of the right type (DATA, METADATA, or SYSTEM)
with enough free space.

### Chunk item (48 + num_stripes * 32 bytes)

Stored in the chunk tree (tree ID 3).

Key: `(256, CHUNK_ITEM=228, logical_offset)`

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `length` | Logical size of this chunk. |
| 8 | 8 | `owner` | Owner tree (always 2, extent tree). |
| 16 | 8 | `stripe_len` | Stripe unit for RAID (typically 65536). |
| 24 | 8 | `type` | Flags: same as block group flags. |
| 32 | 4 | `io_align` | I/O alignment (typically 65536 for non-system, `sectorsize` for system chunks). |
| 36 | 4 | `io_width` | I/O width (same as `io_align`). |
| 40 | 4 | `sector_size` | Device sector size (typically 4096). |
| 44 | 2 | `num_stripes` | Number of stripes. |
| 46 | 2 | `sub_stripes` | Sub-stripes for RAID10 (0 otherwise). |
| 48+ | 32*N | `stripes` | Array of stripe descriptors. |

Each stripe (32 bytes):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `devid` | Device ID. |
| 8 | 8 | `offset` | Physical byte offset on the device. |
| 16 | 16 | `dev_uuid` | Device UUID. |

**Chunk-to-physical resolution:** For a logical address `L` within a chunk
starting at `chunk_start` with a single stripe at device offset `phys`:
`physical = phys + (L - chunk_start)`. RAID profiles use more complex
mapping.

### Device extent (48 bytes)

Stored in the device tree (tree ID 4).

Key: `(devid, DEV_EXTENT=204, physical_offset)`

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `chunk_tree` | Always 3 (`BTRFS_CHUNK_TREE_OBJECTID`). |
| 8 | 8 | `chunk_objectid` | Always 256. |
| 16 | 8 | `chunk_offset` | Logical offset of the owning chunk. |
| 24 | 8 | `length` | Length of this device extent. |
| 32 | 16 | `chunk_tree_uuid` | Chunk tree UUID. |

For each stripe in a chunk, there is one device extent on the corresponding
device.

### Device item (98 bytes)

Stored in the chunk tree (and embedded in the superblock for the local
device).

Key: `(1, DEV_ITEM=216, devid)` (objectid 1 = `BTRFS_DEV_ITEMS_OBJECTID`)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `devid` | Device ID (1, 2, 3, ...). |
| 8 | 8 | `total_bytes` | Total device size. |
| 16 | 8 | `bytes_used` | Bytes allocated to chunks on this device. |
| 24 | 4 | `io_align` | I/O alignment. |
| 28 | 4 | `io_width` | I/O width. |
| 32 | 4 | `sector_size` | Sector size. |
| 36 | 8 | `type` | Reserved (0). |
| 44 | 8 | `generation` | Last transaction touching this device. |
| 52 | 8 | `start_offset` | Start offset for new allocations. |
| 60 | 4 | `dev_group` | Reserved (0). |
| 64 | 1 | `seek_speed` | Hint (0 = unset). |
| 65 | 1 | `bandwidth` | Hint (0 = unset). |
| 66 | 16 | `uuid` | Device UUID. |
| 82 | 16 | `fsid` | Filesystem UUID. |

### Block group type flags

| Bit | Name | Hex | Description |
|-----|------|-----|-------------|
| 0 | `DATA` | 0x1 | Data extents. |
| 1 | `SYSTEM` | 0x2 | System (chunk tree) metadata. |
| 2 | `METADATA` | 0x4 | Metadata extents. |
| 3 | `RAID0` | 0x8 | Striped. |
| 4 | `RAID1` | 0x10 | Mirrored (2 copies). |
| 5 | `DUP` | 0x20 | Duplicated on same device. |
| 6 | `RAID10` | 0x40 | Striped + mirrored. |
| 7 | `RAID5` | 0x80 | RAID5. |
| 8 | `RAID6` | 0x100 | RAID6. |
| 9 | `RAID1C3` | 0x200 | Mirrored (3 copies). |
| 10 | `RAID1C4` | 0x400 | Mirrored (4 copies). |

A block group's flags combine exactly one type (`DATA`, `SYSTEM`, `METADATA`) with
zero or one RAID profile. If no RAID profile bit is set, the block group is
`SINGLE` (no replication, but the virtual `SINGLE` bit 48 = 0x1000000000000
is used in some display contexts only).

### Relationships between structures

For each allocated region of logical space:

1. A **block group item** in the extent tree defines the logical range and
   tracks usage.
2. A **chunk item** in the chunk tree maps the same logical range to one or
   more physical stripes.
3. For each stripe, a **device extent** in the device tree reserves the
   physical space on that device.
4. The **device item** in the chunk tree tracks total and used bytes per
   device.

All four must be consistent. When allocating a new block group (rare in
rescue operations), all four structures must be created atomically within
one transaction.

## Tree types and key reference

### Tree IDs

| ID | Name | Stored in |
|----|------|-----------|
| 1 | Root tree | Superblock (`root` field) |
| 2 | Extent tree | Root tree (`ROOT_ITEM` objectid=2) |
| 3 | Chunk tree | Superblock (`chunk_root` field) |
| 4 | Device tree | Root tree (`ROOT_ITEM` objectid=4) |
| 5 | Default FS tree | Root tree (`ROOT_ITEM` objectid=5) |
| 6 | Root tree directory | (virtual, in root tree) |
| 7 | Checksum tree | Root tree (`ROOT_ITEM` objectid=7) |
| 8 | Quota tree | Root tree (`ROOT_ITEM` objectid=8) |
| 9 | UUID tree | Root tree (`ROOT_ITEM` objectid=9) |
| 10 | Free space tree | Root tree (`ROOT_ITEM` objectid=10) |
| 11 | Block group tree | Root tree (`ROOT_ITEM` objectid=11) |
| 12 | RAID stripe tree | Root tree (`ROOT_ITEM` objectid=12) |
| 256+ | User subvolume/snapshot trees | Root tree (`ROOT_ITEM` objectid=N) |

The root tree is the master index. It contains a ROOT_ITEM for every other
tree (except itself and the chunk tree, whose roots are in the superblock).

### Root item (439 bytes used, padded to 496 bytes)

Stored in root tree with key `(tree_id, ROOT_ITEM=132, 0)`.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 176 | `inode` | Embedded `btrfs_inode_item` (see 7.3). |
| 176 | 8 | `generation` | Transaction generation of this root. |
| 184 | 8 | `root_dirid` | Root directory objectid (typically 256). |
| 192 | 8 | `bytenr` | Logical bytenr of this tree's root block. |
| 200 | 8 | `byte_limit` | Deprecated (0). |
| 208 | 8 | `bytes_used` | Total bytes used by this tree's extents. |
| 216 | 8 | `last_snapshot` | Generation of last snapshot of this tree. |
| 224 | 8 | `flags` | Root flags (bit 0 = read-only subvolume). |
| 232 | 4 | `refs` | Reference count. |
| 236 | 17 | `drop_progress` | Key tracking in-progress drop operation. |
| 253 | 1 | `drop_level` | Level of drop progress. |
| 254 | 1 | `level` | Current B-tree height of this root. |
| 255 | 8 | `generation_v2` | Same as `generation` (marks v2 format). |
| 263 | 16 | `uuid` | Subvolume UUID. |
| 279 | 16 | `parent_uuid` | Parent subvolume UUID (for snapshots). |
| 295 | 16 | `received_uuid` | Source UUID (for received subvolumes). |
| 311 | 8 | `ctransid` | Transaction of last inode change. |
| 319 | 8 | `otransid` | Transaction when this root was created. |
| 327 | 8 | `stransid` | Transaction when sent. |
| 335 | 8 | `rtransid` | Transaction when received. |
| 343 | 12 | `ctime` | Change time (8-byte sec + 4-byte nsec). |
| 355 | 12 | `otime` | Creation time. |
| 367 | 12 | `stime` | Send time. |
| 379 | 12 | `rtime` | Receive time. |
| 391 | 64 | `reserved` | Zero-filled. |

Fields updated when a tree's root block changes (during commit):
- `bytenr` — new root block address.
- `generation` and `generation_v2` — current transaction generation.
- `level` — root block level.

### Inode item (176 bytes)

Embedded in root items and stored standalone in FS trees.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `generation` | NFS generation. |
| 8 | 8 | `transid` | Last modifying transaction. |
| 16 | 8 | `size` | File size. |
| 24 | 8 | `nbytes` | Disk bytes allocated. |
| 32 | 8 | `block_group` | Block group hint for allocation. |
| 40 | 4 | `nlink` | Hard link count. |
| 44 | 4 | `uid` | User ID. |
| 48 | 4 | `gid` | Group ID. |
| 52 | 4 | `mode` | File mode (permissions + type). |
| 56 | 8 | `rdev` | Device number (block/char devices). |
| 64 | 8 | `flags` | Inode flags. |
| 72 | 8 | `sequence` | NFS sequence number. |
| 80 | 32 | `reserved` | Zero-filled. |
| 112 | 12 | `atime` | Access time (8-byte sec + 4-byte nsec). |
| 124 | 12 | `ctime` | Change time. |
| 136 | 12 | `mtime` | Modification time. |
| 148 | 12 | `otime` | Creation time. |

### Key type reference

All key types with their numeric values:

| Value | Name | Primary tree | Key semantics |
|-------|------|--------------|---------------|
| 1 | `INODE_ITEM` | FS tree | (inode#, 1, 0) |
| 12 | `INODE_REF` | FS tree | (inode#, 12, parent_dir_inode#) |
| 13 | `INODE_EXTREF` | FS tree | (inode#, 13, hash) |
| 24 | `XATTR_ITEM` | FS tree | (inode#, 24, name_hash) |
| 36 | `VERITY_DESC_ITEM` | FS tree | (inode#, 36, 0) |
| 37 | `VERITY_MERKLE_ITEM` | FS tree | (inode#, 37, offset) |
| 48 | `ORPHAN_ITEM` | Root/FS tree | (objectid, 48, offset) |
| 60 | `DIR_LOG_ITEM` | Log tree | (dir_inode#, 60, hash) |
| 72 | `DIR_LOG_INDEX` | Log tree | (dir_inode#, 72, index) |
| 84 | `DIR_ITEM` | FS tree | (dir_inode#, 84, name_hash) |
| 96 | `DIR_INDEX` | FS tree | (dir_inode#, 96, index) |
| 108 | `EXTENT_DATA` | FS tree | (inode#, 108, file_offset) |
| 128 | `EXTENT_CSUM` | Csum tree | (-10, 128, logical_bytenr) |
| 132 | `ROOT_ITEM` | Root tree | (tree_id, 132, 0) |
| 144 | `ROOT_BACKREF` | Root tree | (child_id, 144, parent_id) |
| 156 | `ROOT_REF` | Root tree | (parent_id, 156, child_id) |
| 168 | `EXTENT_ITEM` | Extent tree | (bytenr, 168, size) |
| 169 | `METADATA_ITEM` | Extent tree | (bytenr, 169, level) |
| 172 | `EXTENT_OWNER_REF` | (inline only) | -- |
| 176 | `TREE_BLOCK_REF` | Extent tree | (bytenr, 176, root_id) |
| 178 | `EXTENT_DATA_REF` | Extent tree | (bytenr, 178, hash) |
| 182 | `SHARED_BLOCK_REF` | Extent tree | (bytenr, 182, parent_bytenr) |
| 184 | `SHARED_DATA_REF` | Extent tree | (bytenr, 184, parent_bytenr) |
| 192 | `BLOCK_GROUP_ITEM` | Extent tree* | (logical, 192, length) |
| 198 | `FREE_SPACE_INFO` | Free space tree | (bg_start, 198, bg_length) |
| 199 | `FREE_SPACE_EXTENT` | Free space tree | (start, 199, length) |
| 200 | `FREE_SPACE_BITMAP` | Free space tree | (start, 200, length) |
| 204 | `DEV_EXTENT` | Device tree | (devid, 204, phys_offset) |
| 216 | `DEV_ITEM` | Chunk tree | (1, 216, devid) |
| 228 | `CHUNK_ITEM` | Chunk tree | (256, 228, logical) |
| 230 | `RAID_STRIPE` | Stripe tree | (logical, 230, length) |
| 240 | `QGROUP_STATUS` | Quota tree | (0, 240, 0) |
| 242 | `QGROUP_INFO` | Quota tree | (qgroupid, 242, 0) |
| 244 | `QGROUP_LIMIT` | Quota tree | (qgroupid, 244, 0) |
| 246 | `QGROUP_RELATION` | Quota tree | (qgroupid, 246, other_qgroupid) |
| 248 | `TEMPORARY_ITEM` | Root tree | (objectid, 248, offset) |
| 249 | `PERSISTENT_ITEM` | Root tree | (objectid, 249, offset) |
| 250 | `DEV_REPLACE` | Root tree | (objectid, 250, 0) |

*`BLOCK_GROUP_ITEM` lives in the extent tree by default. With the
`BLOCK_GROUP_TREE` compat_ro flag, it moves to tree ID 11.

### Root ref and root backref (18+ bytes)

Forward and backward links between parent and child subvolumes.

`ROOT_REF` key: `(parent_tree_id, ROOT_REF=156, child_tree_id)`
`ROOT_BACKREF` key: `(child_tree_id, ROOT_BACKREF=144, parent_tree_id)`

Both use the same data format:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `dirid` | Directory objectid in the parent tree that contains this subvolume. |
| 8 | 8 | `sequence` | Index in the directory. |
| 16 | 2 | `name_len` | Length of the subvolume name. |
| 18 | N | `name` | Subvolume name (not NUL-terminated). |

## Checksum computation

### Tree block checksums

The checksum field (bytes 0..31 of the header) covers bytes 32..`nodesize`.
For `CRC32C` (type 0), the checksum is 4 bytes stored at offset 0, with
bytes 4..31 zero-padded.

Computation: standard ISO 3309 `CRC32C` (initial seed 0xFFFFFFFF, final XOR
with 0xFFFFFFFF) over the data region bytes 32..`nodesize`.

### Superblock checksums

Same as tree block checksums: bytes 0..31 are the checksum field, covering
bytes 32..4095.

### Data checksums (csum tree)

Data checksums are stored in the csum tree (tree ID 7) with key
`(EXTENT_CSUM_OBJECTID=-10, EXTENT_CSUM=128, logical_bytenr)`.

The item data is a packed array of checksums, one per sector. For `CRC32C`,
each checksum is 4 bytes. The number of sectors covered is
`item_size / csum_size_for_type`. Sectors are consecutive starting at the
key's offset (logical_bytenr).

### Extent data ref hash

The hash used in `EXTENT_DATA_REF_KEY`'s offset field:

```
high_crc = raw_crc32c(seed=0xFFFFFFFF, root.to_le_bytes())
low_crc  = raw_crc32c(seed=0xFFFFFFFF, objectid.to_le_bytes())
low_crc  = raw_crc32c(seed=low_crc,    offset.to_le_bytes())
hash     = (high_crc as u64) << 31 ^ (low_crc as u64)
```

Here `raw_crc32c` means NO final XOR — the raw CRC register value. This
can be recovered from the standard API: `raw = !standard_crc32c(data)`
when seed is `!0`, or equivalently `raw = crc32c_with_seed(!0, data)` if
the API exposes the seed.

## B-tree operations

This section describes the algorithms for searching, inserting, and
deleting items in a btrfs B-tree. These are standard B-tree algorithms
adapted for the btrfs leaf/node layout and COW model.

### Binary search within a block

Given a block and a target key, find the slot:

**In a leaf:** Binary search over items[0..nritems-1] comparing keys. If
found, return (true, slot). If not found, return (false, slot) where slot
is the insertion point (the index of the first item with key > target).

**In a node:** Binary search over ptrs[0..nritems-1] comparing keys. The
result is the slot of the child subtree that could contain the target key.
Specifically, find the largest slot where `ptrs[slot].key <= target`. If
the target is less than all keys, use slot 0.

### Search (search_slot)

`search_slot(trans, root, key, path, ins_len, cow)` descends from the root
to a leaf:

1. Start at the root block (level = root_level).
2. If `cow != 0` and the block hasn't been COWed in this transaction, COW it.
3. Binary search for the key within the block.
4. Store `(block, slot)` in `path.nodes[level]` and `path.slots[level]`.
5. If level > 0: read the child at `ptrs[slot].blockptr`, go to step 2
   with the child.
6. If level == 0: done. If the key was found, `path.slots[0]` points to
   it. If not found, `path.slots[0]` is the insertion point.

When `ins_len > 0` (insert operation), the search checks whether the target
leaf has enough free space. If not, it triggers a leaf split before
returning.

### Item insertion

Given a search path pointing to the insertion slot in a leaf:

1. If the leaf has enough free space (`>= 25 + data_size`):
   a. Shift items at slots [insert_slot..nritems-1] right by 25 bytes
      (one item descriptor).
   b. Shift all data belonging to items at [insert_slot..nritems-1]
      left by `data_size` bytes (making room at the end of the data area).
   c. Update the `offset` field of shifted items (subtract `data_size`
      from each).
   d. Write the new item descriptor at the insert slot.
   e. Write the new item data.
   f. Increment `nritems`.

2. If the leaf is full: split the leaf (section 9.5), then insert.

### Item deletion

Given a search path pointing to items to delete (slot, count):

1. If deleting items in the middle: shift items at
   [slot+count..`nritems`-1] left by `count * 25` bytes.
2. Shift data: move data belonging to remaining items to fill the gap
   left by deleted items' data. Update `offset` fields accordingly.
3. Decrement `nritems` by count.
4. If the leaf becomes empty: remove the key pointer from the parent node
   and free the leaf block. If the parent also becomes empty (or has only
   one child), rebalance upward.

### Leaf split

When a leaf is too full for an insertion:

1. Allocate a new leaf block.
2. Find the split point: aim for roughly half the data in each leaf.
   The split point should be at an item boundary (never split an item).
3. Copy items [split..`nritems`-1] and their data to the new leaf.
4. Update the original leaf's `nritems`.
5. Insert a new key pointer in the parent node pointing to the new leaf.
   The key is the first key of the new leaf.
6. If the parent node is full, split the parent (section 9.6).

### Node split

When an internal node is too full for a new key pointer:

1. Allocate a new node at the same level.
2. Move roughly half the key pointers to the new node.
3. Insert a new key pointer in the parent (one level up) for the new node.
   The key is the first key of the new node.
4. If the parent is also full, split it recursively.
5. If the root node splits, create a new root one level higher containing
   two key pointers (to the old and new nodes). Update the tree's root
   pointer. The tree grows taller by one level.

### Rebalancing (optional optimization)

Before splitting, try to redistribute items to a neighboring sibling:

- **Push left:** If the left sibling has free space, move items from the
  start of the full leaf to the end of the left sibling. Update the
  parent's key for the full leaf.
- **Push right:** If the right sibling has free space, move items from the
  end of the full leaf to the start of the right sibling. Update the
  parent's key for the right sibling.

This reduces tree height growth. It's an optimization, not required for
correctness. The same applies to nodes (push key pointers to siblings).

After deletion, if a leaf or node is less than ~25% full, consider merging
with a sibling. This is also optional for correctness but prevents
excessive tree bloat.

### Path advancement

`next_leaf(path)`: advance from the current leaf to the next one.

1. Walk up the path until finding a level where `slot < nritems - 1`.
2. Increment that slot.
3. Walk back down, always taking slot 0, until reaching a leaf.
4. Update the path at each level.

`prev_leaf(path)`: similar but in reverse (walk up until slot > 0,
decrement, walk down taking the last slot at each level).

## Free space management

To allocate extents, the transaction crate needs to know which logical
addresses are free within each block group.

### Extent tree scanning

The simplest approach: walk the extent tree within a block group's logical
range. Allocated extents are contiguous `EXTENT_ITEM`/`METADATA_ITEM` entries.
Gaps between them are free space. This is O(n) in the number of extents
but works without additional infrastructure.

### Free space tree (optional optimization)

If the `FREE_SPACE_TREE` compat_ro flag is set, the free space tree (tree ID
10) provides pre-computed free space information per block group.

For each block group, there is a `FREE_SPACE_INFO` item:
Key: `(block_group_start, FREE_SPACE_INFO=198, block_group_length)`

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | `extent_count` | Number of free extents. |
| 4 | 4 | `flags` | Bit 0: `USING_BITMAPS` (bitmap mode). |

If not using bitmaps, free extents are stored as:
Key: `(start, FREE_SPACE_EXTENT=199, length)` — no item data.

If using bitmaps:
Key: `(start, FREE_SPACE_BITMAP=200, length)` — item data is a bitmap
where each bit represents one sector (1 = free).

The free space tree must be kept in sync with the extent tree during
transactions. When allocating or freeing extents, update both.

### Allocation strategy

For metadata blocks:
- Find a block group with type `METADATA` (or `SYSTEM` for chunk tree blocks).
- Find a free region >= `nodesize`.
- Prefer the block group hinted by the tree's root item or the most
  recently used block group.

For data extents:
- Find a block group with type `DATA`.
- Find a free region >= requested size.

## Rescue command requirements

This section maps each rescue command to the specific tree operations needed.

### clear-uuid-tree

Delete all items from the UUID tree and remove its root item.

1. Start transaction.
2. Search for the first key in the UUID tree: `search_slot(uuid_root, min_key)`.
3. Delete items in batches (walk forward, delete, repeat until tree empty).
4. Delete the `ROOT_ITEM` for tree ID 9 from the root tree.
5. Free all tree blocks that belonged to the UUID tree (decrement refs).
6. Set `uuid_tree_generation` = 0 in the superblock (tells the kernel to
   rebuild the UUID tree on next mount).
7. Commit transaction.

### clear-ino-cache

Remove leftover inode cache items (from the deprecated v1 inode cache).

1. Start transaction.
2. For each FS tree (tree IDs 5, 256+): search for `INODE_ITEM` with
   objectid = `BTRFS_FREE_INO_OBJECTID` (-12). Delete the inode item and
   all associated `EXTENT_DATA` items.
3. Free any data extents referenced by the deleted extent data items.
4. Commit transaction.

### clear-space-cache

Two modes: v1 (free space inode cache) and v2 (free space tree).

**v1:** Similar to clear-ino-cache — delete free space cache inodes
(objectid = `BTRFS_FREE_SPACE_OBJECTID` = -11) from each block group.

**v2:** Delete the entire free space tree (tree ID 10) like clear-uuid-tree.
Clear the `FREE_SPACE_TREE_VALID` compat_ro flag so the kernel rebuilds it
on next mount.

### fix-device-size

Correct device and superblock size fields when they're inconsistent.

1. Start transaction.
2. Walk the device tree to find all `DEV_EXTENT` items for each device.
3. Sum the extent lengths to get the true `bytes_used` per device.
4. Update each `DEV_ITEM`'s `total_bytes` and `bytes_used`.
5. Update the superblock's embedded dev_item and `total_bytes`.
6. Commit transaction.

### fix-data-checksum

Verify and repair data checksums using mirror redundancy.

1. Start transaction.
2. Walk the csum tree (`EXTENT_CSUM` items).
3. For each checksummed range, read data from each available mirror.
4. Verify each mirror's data against the stored checksum.
5. If a checksum mismatch is found and a good mirror exists: optionally
   update the csum item to match the good mirror's data (or rewrite the
   data from the good mirror).
6. Commit transaction.

Requires: extent tree walking for backref resolution (to report which
files are affected), multi-device I/O for reading mirrors.

### chunk-recover

Rebuild the chunk tree by scanning device surfaces for tree blocks.

1. Scan all devices for valid tree block headers (check magic, csum).
2. From found tree blocks, reconstruct chunk items by cross-referencing
   block group items and device extents.
3. Rebuild the chunk tree with the recovered mappings.
4. Commit.

This is the most complex rescue operation and requires extensive device
scanning infrastructure beyond basic tree operations.

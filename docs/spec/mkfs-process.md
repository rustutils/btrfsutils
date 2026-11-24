# mkfs.btrfs: filesystem creation process

This document describes how `mkfs.btrfs` creates a new btrfs filesystem. It
covers both the empty filesystem (`make_btrfs`) and population from a
directory (`make_btrfs_with_rootdir`).

## Overview

`mkfs.btrfs` builds B-tree nodes as raw byte buffers and writes them directly
to a block device or image file with `pwrite`. It uses no kernel ioctls and
does not mount anything. The result is a valid, mountable btrfs filesystem.

The implementation spans several modules:

- `mkfs/src/mkfs.rs`: orchestration: `make_btrfs` and `make_btrfs_with_rootdir`
- `mkfs/src/layout.rs`: chunk layout computation and block address assignment
- `mkfs/src/tree.rs`: `LeafBuilder` and `NodeBuilder` for individual blocks
- `mkfs/src/treebuilder.rs`: `TreeBuilder` for multi-leaf trees
- `mkfs/src/items.rs`: serializers for all on-disk item types
- `mkfs/src/rootdir.rs`: directory walking, data writing, and compression
- `mkfs/src/write.rs`: checksum computation and pwrite I/O

## Part 1: empty filesystem creation (make_btrfs)

### Step 1: validation

Before any I/O, the configuration is validated:

- `sectorsize` must be a power of 2 and >= 4096.
- `nodesize` must be a power of 2, >= sectorsize, and <= 65536.
- If the `mixed-bg` incompat feature is set, nodesize must equal sectorsize.

### Step 2: chunk layout computation

`ChunkLayout::new` computes the physical placement of three block groups on
disk:

#### System block group

- Logical offset: 1 MiB (`SYSTEM_GROUP_OFFSET`).
- Size: 4 MiB (`SYSTEM_GROUP_SIZE`).
- Physical offset: same as logical (the system chunk is identity-mapped on device 1).
- Profile: always SINGLE (one stripe on device 1).
- Contains: the chunk tree block.

#### Metadata block group

- Logical offset: 5 MiB (`CHUNK_START` = system offset + system size).
- Size: `clamp(total_bytes / 10, 32 MiB, 256 MiB)`, rounded down to 64 KiB
  (`STRIPE_LEN`).
- Profile: DUP on a single device (two physical stripes on device 1, placed
  sequentially after the system group), or RAID1 on multiple devices (one
  stripe per device at `CHUNK_START`).
- Contains: all non-chunk tree blocks (root, extent, dev, FS, csum, free-space,
  data-reloc, and optionally block-group tree).

#### Data block group

- Logical offset: metadata logical + metadata size.
- Size: `clamp(total_bytes / 10, 64 MiB, 1 GiB)`, rounded down to `STRIPE_LEN`.
- Profile: SINGLE (one stripe on device 1, after the last metadata stripe).
- Contains: file data (empty for a freshly created filesystem).

The layout checks that every stripe fits on its device. If one does not,
`ChunkLayout::new` returns `None` and mkfs reports "device too small".

The minimum device size is approximately 133 MiB: 5 MiB (system) + 64 MiB
(2 x 32 MiB metadata DUP) + 64 MiB (data).

### Step 3: block address assignment

`BlockLayout` assigns a logical address to each tree block:

- **Chunk tree:** at `SYSTEM_GROUP_OFFSET` (1 MiB), in the system chunk.
- **Root, Extent, Dev, FS, Csum, FreeSpace, DataReloc trees:** sequential
  in the metadata chunk starting at `meta_logical`, spaced by `nodesize`.
- **Block-group tree** (if enabled): the 8th block in the metadata chunk.

For example, with nodesize = 16384 and meta_logical = 5 MiB:

| Tree | Logical address |
|------|----------------|
| Chunk | 0x100000 (1 MiB) |
| Root | 0x500000 (5 MiB) |
| Extent | 0x504000 |
| Dev | 0x508000 |
| FS | 0x50C000 |
| Csum | 0x510000 |
| FreeSpace | 0x514000 |
| DataReloc | 0x518000 |
| BlockGroup | 0x51C000 (optional) |

### Step 4: tree block construction

Each tree is built as a single leaf with `LeafBuilder`. Items must be pushed in
strictly ascending key order. The builder tracks offsets: item descriptors grow
forward from byte 101 (after the header), and item data grows backward from the
end of the block.

#### Tree block format

```
Bytes 0-31:    checksum (32 bytes, computed last)
Bytes 32-47:   fsid (16 bytes)
Bytes 48-55:   bytenr (logical address, 8 bytes LE)
Bytes 56-63:   flags (8 bytes LE)
Bytes 64-79:   chunk_tree_uuid (16 bytes)
Bytes 80-87:   generation (8 bytes LE)
Bytes 88-95:   owner tree objectid (8 bytes LE)
Bytes 96-99:   nritems (4 bytes LE)
Byte 100:      level (0 for leaf, >0 for internal node)
```

After the 101-byte header, item descriptors occupy 25 bytes each:

```
Bytes 0-16:    key (objectid:8 + type:1 + offset:8)
Bytes 17-20:   data_offset (relative to end of header, 4 bytes LE)
Bytes 21-24:   data_size (4 bytes LE)
```

Item data payloads fill the block backward from its end. The space between the
last descriptor and the first data payload is unused.

#### Root tree contents

The root tree contains a `ROOT_ITEM` (key type 132) for each tree that needs
one. The root tree itself and the chunk tree are excluded. The root tree cannot
reference itself, and the superblock's `chunk_root` pointer references the
chunk tree. In practice, a ROOT_ITEM is still written for the chunk tree
through the `ROOT_ITEM_TREES` list.

Trees receiving a ROOT_ITEM: Extent, Dev, FS, Csum, FreeSpace, DataReloc, and
optionally BlockGroup. Each ROOT_ITEM is 439 bytes and contains:

- An embedded `btrfs_inode_item` (160 bytes) for the root directory.
- Tree-specific fields: generation, root_dirid, bytenr (pointing to the tree's
  block), byte_limit, bytes_used, refs, level.

The FS tree's ROOT_ITEM gets extra initialization:
- A deterministic UUID (derived by XOR-flipping the filesystem UUID).
- `BTRFS_INODE_ROOT_ITEM_INIT` flag set in the embedded inode.
- `inode.size = 3`, `inode.nbytes = nodesize`.
- `ctime` and `otime` timestamps set to the creation time.

#### Extent tree contents

The extent tree contains one `METADATA_ITEM` (or `EXTENT_ITEM` if skinny
metadata is disabled) for each tree block. It also contains a
`BLOCK_GROUP_ITEM` for each block group, unless the block-group tree is
enabled, in which case block group items go there instead.

Each metadata extent item is 24 bytes of `btrfs_extent_item` (refs,
generation, flags) followed by a 9-byte inline `TREE_BLOCK_REF` (type byte and
root objectid). With skinny metadata, the key is
`(bytenr, METADATA_ITEM, level)`. Without skinny metadata, the key is
`(bytenr, EXTENT_ITEM, nodesize)` and an extra 18-byte
`btrfs_tree_block_info` is included.

Block group items (24 bytes each) are keyed as `(logical_addr,
BLOCK_GROUP_ITEM, chunk_size)` and contain the bytes used, chunk objectid, and
profile flags.

All items are collected, sorted by key, and then pushed to the leaf.

#### Chunk tree contents

The chunk tree contains:

1. **`DEV_ITEM`** entries for each device, keyed as `(DEV_ITEMS_OBJECTID,
   DEV_ITEM, devid)`. Each contains the device's total bytes, bytes used,
   sector size, and UUIDs.

2. **`CHUNK_ITEM`** entries for each block group:
   - System chunk: uses `sectorsize` for `io_align` and `io_width` (bootstrap
     convention). One stripe on device 1.
   - Metadata chunk: uses `STRIPE_LEN` (64 KiB) for `io_align` and
     `io_width`. Two stripes for DUP, one per device for RAID1.
   - Data chunk: uses `STRIPE_LEN` for `io_align` and `io_width`. One stripe
     for SINGLE.

#### Dev tree contents

The dev tree contains:

1. **`PERSISTENT_ITEM`** (DEV_STATS) for each device: all five counters
   zeroed (40 bytes).
2. **`DEV_EXTENT`** items for each physical allocation:
   - System chunk: device 1 at `SYSTEM_GROUP_OFFSET`.
   - Metadata stripes: one or two entries per device.
   - Data stripes: one entry per device.

Items are sorted by key `(devid, DEV_EXTENT, physical_offset)`.

#### FS tree contents

Contains two items for the root directory inode (objectid 256):

1. `INODE_ITEM`: directory mode 040755, nlink=1, nbytes=nodesize,
   generation=1, timestamps set to creation time.
2. `INODE_REF`: index=0, name=`..`, parent_ino=256 (the root directory
   references itself).

#### Csum tree

Empty leaf (no items). Populated later if files are written.

#### Free-space tree

If the free-space-tree feature is enabled, this tree contains
`FREE_SPACE_INFO` and `FREE_SPACE_EXTENT` items for each block group. Each
block group gets:
- One `FREE_SPACE_INFO` item with `extent_count=1`.
- One `FREE_SPACE_EXTENT` item covering the unused portion of the block group
  (from `used_bytes` to `group_size`).

If the free-space-tree feature is disabled, this is an empty leaf.

#### Data-reloc tree

Same structure as the FS tree: root directory inode (objectid 256) with
`INODE_ITEM` and `INODE_REF`.

#### Block-group tree (optional)

If the block-group-tree `compat_ro` feature is enabled, block group items go
here instead of the extent tree. The tree contains three `BLOCK_GROUP_ITEM`
entries (system, metadata, and data).

### Step 5: checksum computation

After each tree block is built, `btrfs_disk::util::csum_tree_block` computes
the checksum of bytes `CSUM_SIZE..nodesize` and writes it into the first bytes
of the block:

- **CRC32C:** 4 bytes (standard CRC32C via `crc32c::crc32c`).
- **xxHash64:** 8 bytes.
- **SHA-256:** 32 bytes.
- **BLAKE2b-256:** 32 bytes.

The remaining bytes of the 32-byte checksum field stay zero.

### Step 6: writing to disk

#### Tree blocks

Each tree block is written to its physical locations with `pwrite_all`.
`ChunkLayout::logical_to_physical` provides the logical-to-physical mapping:

- System chunk blocks: one write at the logical address (identity mapping) on
  device 1.
- Metadata chunk blocks: one write per stripe. For DUP: two writes on device 1
  at different offsets. For RAID1: one write per device.
- Data chunk blocks: one write per stripe (typically one for SINGLE).

#### Superblocks

The superblock is built with these fields:

- `magic`: `_BHRfS_M`
- `root`: logical address of the root tree block
- `chunk_root`: logical address of the chunk tree block
- `total_bytes`: sum across all devices
- `bytes_used`: system used + metadata used (an empty filesystem uses no data)
- `sectorsize`, `nodesize`, `leafsize` (= nodesize), `stripesize` (= sectorsize)
- `num_devices`: device count
- `incompat_flags`, `compat_ro_flags`: from configuration
- `csum_type`: checksum algorithm
- `cache_generation`: 0 if free-space-tree enabled, u64::MAX otherwise
- `sys_chunk_array`: embedded copy of the system chunk (disk_key and
  chunk_item bytes), so the kernel can bootstrap chunk mapping from the
  superblock alone

The `sys_chunk_array` is the bootstrap mechanism. It holds a serialized disk
key followed by the system chunk item data (including stripe info), in a fixed
2048-byte buffer within the superblock. The kernel reads this array first to
locate the chunk tree block, then reads the chunk tree to find all other
chunks.

Each device gets its own superblock with device-specific fields (devid,
dev_uuid, and that device's bytes_used). The superblock is written to every
valid mirror location (up to 3):

- Mirror 0: byte offset 65536 (64 KiB). Always written.
- Mirror 1: byte offset 67108864 (64 MiB). Written if device is large enough.
- Mirror 2: byte offset 274877906944 (256 GiB). Written if device is large enough.

After all writes, `fsync` is called on all device files.

## Part 2: rootdir population (make_btrfs_with_rootdir)

The `--rootdir` flag populates the new filesystem from a source directory on the
host. This is much more complex than the empty case because:

1. The FS tree may need multiple leaf blocks (and internal nodes).
2. File data must be written to the data chunk.
3. The extent tree must reference both metadata blocks and data extents.
4. The csum tree must contain checksums for all data.
5. The extent tree must contain entries for its own blocks, creating a circular
   dependency.

### Step 1: directory walk (walk_directory)

`rootdir::walk_directory` traverses the source directory depth-first. It builds
all FS tree items and identifies files that need data extents.

#### Inode assignment

Inode numbers are assigned sequentially starting at 257. Inode 256 is the root
directory and is handled separately: its `INODE_ITEM` and `INODE_REF` are added
during the merge phase.

#### Hardlink detection

For files with `nlink > 1`, the function tracks host `(dev, ino)` pairs in a
`HashMap`. When a later directory entry refers to the same host inode:

- The existing btrfs inode number is reused. No new one is assigned.
- An `INODE_REF` is added (an extra reference from the new parent).
- No new `INODE_ITEM` is created.
- The nlink counter for that btrfs inode is incremented.

After all entries are processed, `fixup_inode_nlink` patches the `nlink` field
in the `INODE_ITEM` of every hardlinked inode.

#### Per-entry processing

Each directory entry (file, directory, symlink, or special file) produces:

1. **DIR_ITEM** in the parent directory, keyed by name hash
   (`crc32c(0xFFFFFFFE, name)`).
2. **DIR_INDEX** in the parent directory, keyed by sequential index (starting
   at 2 for each directory).
3. **INODE_REF** for the new inode, pointing to the parent.
4. **INODE_ITEM** with metadata copied from the host filesystem (uid, gid, mode,
   timestamps, rdev for special files).
5. **XATTR_ITEM** entries for each extended attribute on the host file (read
   with `llistxattr` and `lgetxattr`).

Type-specific items:

- **Directories:** Push children onto the DFS stack in reverse, so they are
  visited in order. Initialize the dir_index counter for the new directory.
- **Symlinks:** Create an inline `FILE_EXTENT_ITEM` containing the link target
  (never compressed).
- **Regular files** with `size > 0`:
  - If `size <= max_inline_data_size`: read the file, optionally compress it,
    and create an inline `FILE_EXTENT_ITEM`.
  - If `size > max_inline_data_size`: defer to the data writing phase. Record a
    `FileAllocation` with the host path, btrfs inode, size, and NODATASUM flag.
- **Special files** (FIFO, socket, char or block device): `INODE_ITEM` only, no extent.

#### Inline extent threshold

The maximum inline data size is `min(sectorsize - 1, nodesize - 147)`. With
the defaults (sectorsize=4096, nodesize=16384), this is 4095 bytes. Files at
or below this size are stored directly in the tree leaf.

#### Inode flags

The `--inode-flags` argument sets `NODATACOW` and `NODATASUM` per path.
`NODATACOW` implies `NODATASUM` for regular files. The flags are stored in the
`INODE_ITEM` and control whether the data writing phase generates checksums.

#### Directory size fixup

After the walk, `fixup_inode_size` sets the size field in each non-root
directory's `INODE_ITEM` to the sum of `name_len * 2` over its `DIR_INDEX`
entries (the btrfs convention for directory sizes).

#### Inline nbytes fixup

`fixup_inline_nbytes` patches the `nbytes` field of the `INODE_ITEM` for files
with inline extents. For these, `nbytes` equals the inline data size (the
bytes actually stored, which may be compressed).

#### Output

`walk_directory` returns a `RootdirPlan` containing:
- `fs_items`: sorted list of all FS tree items (excluding root dir inode).
- `file_extents`: list of `FileAllocation` entries for files needing data extents.
- `data_bytes_needed`: total aligned data bytes needed in the data chunk.
- `root_dir_nlink`, `root_dir_size`: root directory metadata.

### Step 2: data writing (write_file_data)

For each file in `plan.file_extents`, the function reads the host file in 1 MiB
chunks (`MAX_EXTENT_SIZE`) and writes each chunk to the data block group.

#### Per-extent processing

1. Read up to 1 MiB of raw data from the host file.
2. Optionally try compression (zlib or zstd). If the compressed output is
   smaller than the input, use it. Otherwise store the data uncompressed.
3. Pad the (possibly compressed) data to sectorsize alignment.
4. Compute the logical disk address: `data_logical + current_offset`.
5. Write the padded data to all physical locations for this logical address.
6. Compute per-sector checksums (skipped for NODATASUM files):
   - For each sector in the padded data, compute the checksum using the
     configured algorithm.
   - Pack all checksums into a single `EXTENT_CSUM` item.
7. Create a `FILE_EXTENT_ITEM` (regular type) in the FS tree items:
   `disk_bytenr`, `disk_num_bytes` (aligned compressed size), `offset=0`,
   `num_bytes` (logical file extent size), `ram_bytes` (uncompressed size),
   compression type.
8. Create an `EXTENT_ITEM` with inline `EXTENT_DATA_REF` in the extent tree
   items: refs=1, generation=1, flags=DATA.

After all files are processed, `nbytes_updates` holds the total disk-allocated
bytes per inode. `apply_nbytes_updates` patches these into the matching
`INODE_ITEM` entries.

### Step 3: multi-leaf tree building (TreeBuilder)

When a tree has more items than fit in one leaf, `TreeBuilder` splits them
across multiple leaves and adds internal nodes to form a valid B-tree.

#### Leaf packing

Items are packed into leaves sequentially:
1. Start a new leaf.
2. For each item, check whether the leaf has room for the item descriptor
   (25 bytes) and the item data. If not, finalize the current leaf and start a
   new one.
3. Record the first key of each leaf for parent node entries.

#### Internal node construction

If more than one leaf is produced:
1. Create internal nodes at level 1, each pointing to up to
   `(nodesize - 101) / 33` child blocks (33 bytes per key-pointer entry:
   17 key + 8 blockptr + 8 generation).
2. If more than one level-1 node is needed, create level-2 nodes, and so on.
3. Repeat until a single root node remains.

Node balancing: if the last node at a level would hold fewer than 1/4 of the
maximum entries, the entries are split more evenly with the previous node to
avoid a tiny remainder.

#### Placeholder addresses

All blocks are first built with `bytenr = 0` in the header. After address
assignment, `TreeBuilder::assign_addresses` patches:
- The `bytenr` field in each block's header (offset 48).
- The `blockptr` field of each key-pointer entry in internal nodes (offset 17
  from the entry start).

### Step 4: the convergence loop

This step solves the bootstrapping problem.

#### The bootstrapping problem

The extent tree must contain a `METADATA_ITEM` (or `EXTENT_ITEM`) for every
tree block in the filesystem, including the extent tree's own blocks. But the
number of extent tree blocks depends on how many items it holds, including its
own self-referential entries. More extent tree blocks need more extent items,
which may need even more blocks.

#### Solution: iterate until stable

`converge_extent_tree_block_count` computes the extent tree block count
iteratively:

1. Start with `extent_tree_block_count = 1`.
2. Construct a trial set of all extent items:
   - One `METADATA_ITEM` per tree block (chunk tree, root tree,
     `extent_tree_block_count` extent tree blocks, dev tree, FS tree blocks,
     csum tree blocks, free-space tree block, data-reloc tree blocks,
     block-group tree block if applicable).
   - All data extent items from the data writing phase.
   - Block group items (if the block-group tree is not in use).
3. Sort all trial items by key.
4. Build the trial extent tree using `TreeBuilder::build` to determine how many
   blocks it needs.
5. If `trial.blocks.len() == extent_tree_block_count`, the count is stable.
   Stop.
6. Otherwise, set `extent_tree_block_count = trial.blocks.len()` and repeat.

In practice this converges in 1-3 iterations. The count never decreases
(adding self-referential items can only increase the block count), so
convergence is guaranteed.

### Step 5: address assignment

Once the extent tree block count is known, `BlockAllocator` assigns real
logical addresses in a fixed order:

1. **Chunk tree:** allocate from the system chunk (`alloc_system`).
2. **Root tree:** allocate from the metadata chunk (`alloc_metadata`).
3. **Extent tree blocks** (count from convergence loop): sequential metadata
   allocations.
4. **Dev tree:** one metadata allocation.
5. **FS tree blocks:** sequential metadata allocations.
6. **Csum tree blocks:** sequential metadata allocations.
7. **Free-space tree:** one metadata allocation (if enabled).
8. **Data-reloc tree blocks:** sequential metadata allocations.
9. **Block-group tree:** one metadata allocation (if enabled).

`BlockAllocator` keeps separate bump pointers for the system chunk
(`SYSTEM_GROUP_OFFSET` to `SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE`) and the
metadata chunk (`meta_logical` to `meta_logical + meta_size`). It returns an
error if either runs out of space.

### Step 6: building the real extent tree

With the real addresses known, the actual extent tree is built:

1. Create `METADATA_ITEM` entries for every tree block using their real
   addresses.
2. Include all data extent items from the data writing phase.
3. Include block group items (in the extent tree, or in the separate
   block-group tree).
4. Sort all items by key.
5. Build with `TreeBuilder::build`.
6. Assert that the block count matches the converged count. A mismatch means
   the convergence loop has a bug.
7. Assign addresses to extent tree blocks from the pre-allocated address list.

### Step 7: building remaining trees

With all addresses finalized:

1. **FS tree:** `TreeBuilder::assign_addresses` patches bytenr fields with the
   pre-allocated addresses.
2. **Csum tree:** same.
3. **Data-reloc tree:** same.
4. **Chunk tree:** rebuilt as a single leaf with final device bytes_used values.
5. **Dev tree:** rebuilt as a single leaf with final device extent information.
6. **Free-space tree:** rebuilt with final used-byte counts for each block group.
7. **Block-group tree:** rebuilt with final used-byte counts.
8. **Root tree:** rebuilt with final tree root addresses and levels for all trees.

The root tree is always a single leaf because it has few ROOT_ITEM entries
(6-8 trees). It is built last because it needs the root address and level of
every other tree.

### Step 8: writing to disk

All tree blocks are written in order:

1. Single-leaf trees (chunk, root, dev): compute the checksum and write to all
   physical locations.
2. Multi-block trees (extent, FS, csum, data-reloc): for each block, compute
   the checksum and write to all physical locations.
3. Optional single-leaf trees (free-space, block-group): compute the checksum
   and write.

The `write_rootdir_trees` helper drives this.

### Step 9: superblock

The superblock is built with:
- `root`: root tree address (from step 5).
- `chunk_root`: chunk tree address (from step 5).
- `bytes_used`: system_used + metadata_used + data_used.

It is written to every mirror location on every device.

### Step 10: shrink (optional)

If `--shrink` is specified and there is a single device:

1. Compute the physical end of the last chunk, across all metadata and data
   stripes.
2. Round up to sectorsize alignment.
3. Create a new config with `total_bytes` set to the shrunk size.
4. Rebuild the chunk tree and superblock with the reduced total_bytes
   (so `DEV_ITEM.total_bytes` and `superblock.total_bytes` reflect the
   actual image size).
5. After all writes, truncate the image file to the shrunk size with
   `set_len`.

This produces a minimal image file suitable for distribution or flashing.

## Item serialization (items.rs)

All item serializers return a `Vec<u8>` ready for `LeafBuilder::push`. They
use the `bytes::BufMut` trait for little-endian encoding and derive field
positions from `std::mem::offset_of!` and `std::mem::size_of` on the bindgen
structs.

Key serializers and their sizes:

| Function | Item type | Approximate size |
|----------|-----------|-----------------|
| `root_item` | ROOT_ITEM | 439 bytes |
| `extent_item` | EXTENT_ITEM/METADATA_ITEM | 33 bytes (skinny) or 51 bytes |
| `block_group_item` | BLOCK_GROUP_ITEM | 24 bytes |
| `dev_item` | DEV_ITEM | 98 bytes |
| `chunk_item` | CHUNK_ITEM | 48 + 32*num_stripes bytes |
| `dev_extent` | DEV_EXTENT | 48 bytes |
| `dev_stats_zeroed` | PERSISTENT_ITEM | 40 bytes |
| `free_space_info` | FREE_SPACE_INFO | 8 bytes |
| `inode_item_dir` | INODE_ITEM | 160 bytes |
| `inode_item` | INODE_ITEM | 160 bytes |
| `inode_ref` | INODE_REF | 10 + name_len bytes |
| `dir_item` | DIR_ITEM/DIR_INDEX | 30 + name_len bytes |
| `xattr_item` | XATTR_ITEM | 30 + name_len + value_len bytes |
| `file_extent_inline` | FILE_EXTENT_ITEM | 21 + data_len bytes |
| `file_extent_reg` | FILE_EXTENT_ITEM | 53 bytes |
| `data_extent_item` | EXTENT_ITEM | 53 bytes |

## Checksum computation (write.rs)

`ChecksumType` supports four algorithms. Each checksums the data portion
(bytes 32..end) of tree blocks and superblocks:

| Algorithm | On-disk type value | Output size | Implementation |
|-----------|-------------------|-------------|----------------|
| CRC32C | 0 | 4 bytes | `crc32c` crate |
| xxHash64 | 1 | 8 bytes | `xxhash-rust` crate |
| SHA-256 | 2 | 32 bytes | `sha2` crate |
| BLAKE2b-256 | 3 | 32 bytes | `blake2` crate |

`csum_tree_block` writes the computed hash into the first N bytes of the
block's checksum field (32 bytes total), zero-filling the remaining bytes.

Data block checksums (in the csum tree) use the same algorithm, computed per
sector.

## The bootstrapping problem in detail

The bootstrapping problem is central to mkfs.

### The circular dependency

Consider a minimal filesystem with 8 tree blocks. The extent tree must contain
8 `METADATA_ITEM` entries (one for each block, including itself). But what if
those 8 entries do not fit in a single leaf?

With skinny metadata (`METADATA_ITEM`, 33-byte payload), each item uses
25 (descriptor) + 33 (data) = 58 bytes. A 16 KiB leaf has 16384 - 101 = 16283
usable bytes, enough for 280 items. For an empty filesystem, the extent tree
easily fits in one block.

But with `--rootdir` populating thousands of files, the FS tree, csum tree, and
extent tree can each grow to many blocks. If the FS tree has 100 blocks and
there are 500 data extents, the extent tree might need several blocks itself,
and each additional extent tree block requires another `METADATA_ITEM` entry
in the extent tree.

### Why pre-computing works

The approach works because:

1. **Addresses are independent of content.** Tree block addresses are assigned
   by sequential bump allocation, so the address of each block depends only on
   how many blocks precede it, not on the content of any block.

2. **Block count is monotonically non-decreasing.** Adding self-referential
   entries can only increase or maintain the block count, never decrease it.

3. **The system is finite.** The metadata chunk can hold only so many blocks,
   which bounds the iteration.

4. **Content depends only on addresses and counts.** Once addresses are assigned,
   every tree block's content is fully determined. There are no further
   dependencies.

The convergence loop relies on properties (1) and (2). It guesses a block
count, computes trial content, and checks whether the trial needs the same
number of blocks. If not, it tries again with the new count. Property (2)
guarantees convergence: the count can only go up until it stabilizes.

### Implementation detail

Each trial uses placeholder addresses (sequential from `meta_logical`), not
the final addresses. This is fine because the
`TreeBuilder` only needs the item count and sizes to determine how many blocks
are needed. The actual address values do not affect block count. After
convergence, the real extent tree is built with the actual addresses from
`BlockAllocator`.

## Default features

The default incompat feature flags are:

- `MIXED_BACKREF`: mixed backreference format
- `BIG_METADATA`: larger metadata blocks
- `EXTENDED_IREF`: extended inode references (INODE_EXTREF)
- `SKINNY_METADATA`: skinny metadata extent refs (METADATA_ITEM key type)
- `NO_HOLES`: no explicit hole extent items

The default compat_ro feature flags are:

- `FREE_SPACE_TREE`: free-space tree (v2 free space tracking)
- `FREE_SPACE_TREE_VALID`: marks the free-space tree as valid
- `BLOCK_GROUP_TREE`: separate tree for block group items

Features can be enabled or disabled with `-O feature` or `-O ^feature`.

## Multi-device support

For multi-device filesystems, the chunk layout spreads stripes across devices:

- **RAID1 metadata:** one stripe per device at `CHUNK_START`.
- **SINGLE data:** one stripe on device 1.

Each device gets its own superblock with device-specific `devid`, `dev_uuid`,
and `bytes_used`. The chunk tree contains a `DEV_ITEM` per device, and the dev
tree contains `DEV_EXTENT` entries mapping physical allocations to chunks.

`logical_to_physical` determines where each block is written:

- System chunk blocks go to device 1 only.
- Metadata blocks go to every metadata stripe device.
- Data blocks go to every data stripe device.

## Limitations

Not yet implemented:
- `--rootdir` with LZO compression (rejected at argument validation).
- RAID0, RAID5, RAID6, and RAID10 profiles.
- Zoned device support.
- Mixed block group mode with `--rootdir`.

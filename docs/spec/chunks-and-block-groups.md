# Chunks and Block Groups

This document describes the btrfs chunk and block group system: how the
filesystem maps logical addresses to physical device locations, how space
is organized into typed block groups, and how these structures relate to
each other on disk.

All multi-byte integers in btrfs on-disk structures are little-endian.


## Address Spaces

Btrfs uses two distinct address spaces:

**Logical address space.** Every byte of allocated space in the filesystem
has a logical address. Tree node pointers, extent references, block group
descriptors, and file extent records all use logical addresses. The logical
address space is a flat 64-bit namespace shared across all devices in the
filesystem. There is no inherent relationship between a logical address and
any particular physical device.

**Physical address space.** Each device has its own independent physical
address space, starting at byte 0. Physical addresses identify actual byte
offsets on a block device.

The separation exists for several reasons:

1. **Multi-device support.** A single logical address can map to stripes on
   multiple physical devices (RAID1, DUP, RAID0, etc.) without the upper
   layers of the filesystem needing to know which devices are involved.

2. **Relocation.** The balance and resize operations can move data between
   physical locations while logical addresses remain stable. Since all
   internal pointers use logical addresses, no tree rewriting is needed
   when physical locations change.

3. **Redundancy profiles.** The same logical address range can have multiple
   physical copies (DUP, RAID1) or be striped across devices (RAID0) ---
   this is invisible to everything above the chunk layer.

The mapping between the two address spaces is maintained by three
cooperating data structures: **chunks** (logical to physical), **device
extents** (physical to logical), and **block groups** (space accounting).


## Chunks

A chunk maps a contiguous range of logical addresses to one or more
physical locations on devices. Chunks are the fundamental unit of the
logical-to-physical translation.

### CHUNK_ITEM On-Disk Structure

Chunks are stored in the chunk tree. Each chunk item has a key:

```
Key: (FIRST_CHUNK_TREE_OBJECTID, CHUNK_ITEM, logical_offset)
      objectid = 256                type = 228    offset = start of logical range
```

The item payload is a `btrfs_chunk` structure followed by an array of
`btrfs_stripe` structures:

`btrfs_chunk` (48 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `length` | 0 | 8 | Logical extent length in bytes |
| `owner` | 8 | 8 | Owner tree objectid (always `EXTENT_TREE_OBJECTID` = 2) |
| `stripe_len` | 16 | 8 | Stripe length for striped profiles (default 64 KiB) |
| `type` | 24 | 8 | Block group type + RAID profile flags |
| `io_align` | 32 | 4 | I/O alignment (`STRIPE_LEN` for normal chunks, sectorsize for bootstrap) |
| `io_width` | 36 | 4 | I/O width (same as `io_align`) |
| `sector_size` | 40 | 4 | Sector size of the underlying devices |
| `num_stripes` | 44 | 2 | Number of stripe entries following |
| `sub_stripes` | 46 | 2 | Sub-stripe count (nonzero only for RAID10) |

`btrfs_stripe` (32 bytes each, `num_stripes` entries):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `devid` | 0 | 8 | Device ID |
| `offset` | 8 | 8 | Physical byte offset on that device |
| `dev_uuid` | 16 | 16 | UUID of the device |

The total item size is `48 + num_stripes * 32` bytes.

### Logical-to-Physical Resolution

To resolve a logical address to a physical location:

1. Find the chunk whose logical range contains the address. The chunk tree
   is a B-tree keyed by `(256, CHUNK_ITEM, logical_offset)`, so a lookup
   finds the entry with the largest `logical_offset <= target`.

2. Verify the address falls within the chunk: `logical_offset <= target <
   logical_offset + length`.

3. Compute the offset within the chunk: `within = target - logical_offset`.

4. For simple profiles (SINGLE, DUP, RAID1): the physical address on
   stripe `i` is `stripe[i].offset + within`.

5. For striped profiles (RAID0, RAID10, RAID5, RAID6): the stripe index
   and offset within the stripe are computed from `within`, `stripe_len`,
   and `num_stripes`/`sub_stripes`.

The `ChunkTreeCache` in `disk/src/chunk.rs` implements this as a `BTreeMap`
keyed by logical start address, with `resolve()` returning the physical
offset on the first stripe (sufficient for SINGLE, DUP, and RAID1 reads).

### Chunk Ownership

The `owner` field in the chunk item is always `BTRFS_EXTENT_TREE_OBJECTID`
(2). This is a historical artifact --- it does not mean the extent tree
"owns" the chunk in any meaningful sense. The chunk tree is its own
independent tree (tree objectid 3) with its root pointer stored directly
in the superblock.


## Block Groups

A block group is the unit of space management in btrfs. Each block group
corresponds to exactly one chunk and tracks how much of that chunk's space
is used. Block groups carry type information that determines what kind of
data can be stored in them.

### BLOCK_GROUP_ITEM On-Disk Structure

Block group items are stored either in the extent tree (traditional
layout) or in the dedicated block-group tree (when the
`BLOCK_GROUP_TREE` compat_ro feature is enabled).

```
Key: (logical_offset, BLOCK_GROUP_ITEM, length)
      objectid = chunk start    type = 192    offset = chunk length
```

The item payload is a `btrfs_block_group_item` (24 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `used` | 0 | 8 | Bytes currently allocated within this block group |
| `chunk_objectid` | 8 | 8 | Always `FIRST_CHUNK_TREE_OBJECTID` (256) |
| `flags` | 16 | 8 | Type flags + RAID profile flags |

### Type Flags

The `flags` field is a bitfield combining a chunk type (what gets stored)
and a RAID profile (how it is stored):

**Chunk type bits (mutually exclusive in practice):**

| Flag       | Value  | Meaning                                      |
|------------|--------|----------------------------------------------|
| DATA       | 0x001  | File data extents                            |
| SYSTEM     | 0x002  | Chunk tree blocks (needed to bootstrap reads)|
| METADATA   | 0x004  | Tree node blocks (all trees except chunk)    |

The kernel also supports `DATA|METADATA` (0x005) for the `mixed-bg`
feature, where data and metadata share block groups.

**RAID profile bits:**

| Flag       | Value          | Meaning                                  |
|------------|----------------|------------------------------------------|
| (none)     | 0              | SINGLE --- one copy, one device          |
| RAID0      | 0x008          | Striped across N devices, no redundancy  |
| RAID1      | 0x010          | Mirrored on 2 devices                   |
| DUP        | 0x020          | Two copies on the same device            |
| RAID10     | 0x040          | Striped mirrors                          |
| RAID5      | 0x080          | Single parity                            |
| RAID6      | 0x100          | Double parity                            |
| RAID1C3    | 0x200          | Mirrored on 3 devices                   |
| RAID1C4    | 0x400          | Mirrored on 4 devices                   |

For example, a metadata block group using DUP has flags `0x024`
(`METADATA | DUP`). A system block group with no profile bits set is
SYSTEM|single (`0x002`).

The `BlockGroupFlags` type in `disk/src/items.rs` represents these
flags as a `bitflags` struct with methods `type_name()` (returns
"Data", "Metadata", "System", etc.) and `profile_name()` (returns
"RAID1", "DUP", "single", etc.).

### Block Group to Chunk Relationship

Every block group has a 1:1 correspondence with a chunk. The block
group's key `(logical_offset, BLOCK_GROUP_ITEM, length)` must match
a chunk item's `(256, CHUNK_ITEM, logical_offset)` with matching
`length`. The block group's `flags` must agree with the chunk item's
`type` field.

This invariant is verified by `btrfs check` (see section 8).


## Device Extents

Device extents are the inverse mapping of chunks: they record which
ranges of physical space on each device are allocated to which chunks.

### DEV_EXTENT On-Disk Structure

Device extents are stored in the device tree (tree objectid 4).

```
Key: (devid, DEV_EXTENT, physical_offset)
      objectid = device ID    type = 204    offset = start byte on device
```

The item payload is a `btrfs_dev_extent` (48 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `chunk_tree` | 0 | 8 | Chunk tree objectid (always 3) |
| `chunk_objectid` | 8 | 8 | `FIRST_CHUNK_TREE_OBJECTID` (256) |
| `chunk_offset` | 16 | 8 | Logical offset of the owning chunk |
| `length` | 24 | 8 | Physical extent length in bytes |
| `chunk_tree_uuid` | 32 | 16 | UUID of the chunk tree |

### Relationship to Chunks and Stripes

For each stripe in a chunk item, there is a corresponding device extent.
If a chunk at logical address L has `num_stripes` stripes, then:

- Stripe 0: `(stripe[0].devid, DEV_EXTENT, stripe[0].offset)` with
  `chunk_offset = L` and `length = chunk.length` (for SINGLE/DUP/RAID1).

- Stripe 1 (for DUP/RAID1): `(stripe[1].devid, DEV_EXTENT,
  stripe[1].offset)` with `chunk_offset = L` and `length = chunk.length`.

For a DUP metadata chunk on a single device, both stripes have the same
`devid` but different physical offsets, producing two device extents on
the same device.

### Device Items

Each device in the filesystem also has a `DEV_ITEM` in the chunk tree:

```
Key: (DEV_ITEMS_OBJECTID, DEV_ITEM, devid)
      objectid = 1              type = 216    offset = device ID
```

The item payload is a `btrfs_dev_item` (98 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `devid` | 0 | 8 | Unique device ID |
| `total_bytes` | 8 | 8 | Total device size |
| `bytes_used` | 16 | 8 | Bytes allocated to chunks on this device |
| `io_align` | 24 | 4 | I/O alignment |
| `io_width` | 28 | 4 | I/O width |
| `sector_size` | 32 | 4 | Sector size |
| `dev_type` | 36 | 8 | Reserved (0) |
| `generation` | 44 | 8 | Last-updated generation |
| `start_offset` | 52 | 8 | Start offset for allocations |
| `dev_group` | 60 | 4 | Reserved (0) |
| `seek_speed` | 64 | 1 | Seek speed hint (0) |
| `bandwidth` | 65 | 1 | Bandwidth hint (0) |
| `uuid` | 66 | 16 | Device UUID |
| `fsid` | 82 | 16 | Filesystem UUID |

The `bytes_used` field is the sum of the lengths of all device extents
on that device. A copy of the device item for device 1 is also embedded
in the superblock.


## The Bootstrap Problem

### Circular Dependency

To read any tree, you need to resolve logical addresses to physical
offsets, which requires the chunk tree. But the chunk tree is itself
stored at a logical address that needs resolution. This creates a
circular dependency.

### sys_chunk_array

Btrfs solves this with the `sys_chunk_array` --- a 2048-byte buffer
embedded directly in the superblock. This array contains a subset of the
chunk tree: specifically, the chunk items for SYSTEM-type block groups.

The SYSTEM block group contains the chunk tree's root block. By parsing
the sys_chunk_array, the filesystem driver can locate the chunk tree on
disk without needing a chunk tree to find it.

The array format is a packed sequence of `(btrfs_disk_key, btrfs_chunk)`
pairs:

```
sys_chunk_array[0..sys_chunk_array_size]:
  repeat {
    btrfs_disk_key (17 bytes):
      objectid: u64_le      (always FIRST_CHUNK_TREE_OBJECTID = 256)
      type:     u8           (always CHUNK_ITEM = 228)
      offset:   u64_le       (logical offset of the chunk)
    btrfs_chunk + stripes:
      (same format as the chunk item payload described in section 2.1)
  }
```

The `sys_chunk_array_size` field in the superblock records how many bytes
of the 2048-byte buffer are valid.

### Bootstrap Sequence

The full bootstrap sequence for reading a btrfs filesystem is:

1. **Read the superblock** at the primary offset (64 KiB). Verify the
   magic number, checksum, and fsid. The superblock provides:
   - `sys_chunk_array` + `sys_chunk_array_size`
   - `chunk_root` (logical address of the chunk tree root)
   - `root` (logical address of the root tree root)
   - `nodesize`, `sectorsize`, `csum_type`

2. **Parse the sys_chunk_array** to build an initial `ChunkTreeCache`.
   This cache contains only the SYSTEM chunk(s), which is enough to
   resolve the chunk tree root address.

3. **Read the chunk tree** starting from `chunk_root`. For each
   `CHUNK_ITEM` found, add the mapping to the `ChunkTreeCache`. After
   this step, the cache can resolve any logical address in the
   filesystem.

4. **Read the root tree** starting from `root`. This tree contains
   `ROOT_ITEM` entries for every other tree (extent, device, FS, csum,
   free-space, etc.), providing their root block logical addresses.

5. **Read any other tree** by looking up its `ROOT_ITEM` in the root
   tree and using the `ChunkTreeCache` to resolve addresses.

The `seed_from_sys_chunk_array()` function in `disk/src/chunk.rs`
implements step 2. The `BlockReader` in `disk/src/reader.rs` orchestrates
the full bootstrap sequence.


## RAID Profiles

The RAID profile determines how a chunk's logical space maps to physical
device locations. The profile affects `num_stripes`, `sub_stripes`, and
the interpretation of stripe entries.

### SINGLE

```
num_stripes = 1
sub_stripes = 0
```

One stripe, one device. Logical offset maps 1:1 to a physical offset on
a single device. No redundancy.

```
Logical:   [--------chunk------]
Physical:  [dev1: stripe 0     ]
```

### DUP

```
num_stripes = 2
sub_stripes = 0
```

Two stripes on the **same** device at different physical offsets. Both
stripes contain identical data. Provides protection against localized
media errors but not device failure.

```
Logical:   [--------chunk------]
Physical:  [dev1: stripe 0     ]
           [dev1: stripe 1     ]  (different offset, same data)
```

DUP is the default metadata profile for single-device filesystems.
The logical size of the chunk equals one stripe size. The physical space
consumed is `2 * stripe_size`.

In mkfs, DUP metadata stripes are laid out sequentially after the system
group:

```
Physical layout on device 1:
  [0..1M)          reserved (superblock at 64K)
  [1M..5M)         system chunk (4 MiB)
  [5M..5M+meta)    metadata stripe 0
  [5M+meta..5M+2*meta)  metadata stripe 1
  [5M+2*meta..)    data stripe 0
```

### RAID1

```
num_stripes = 2  (RAID1C3: 3, RAID1C4: 4)
sub_stripes = 0
```

One stripe per device, each containing identical data. RAID1 uses 2
devices, RAID1C3 uses 3, RAID1C4 uses 4.

```
Logical:   [--------chunk------]
Physical:  [dev1: stripe 0     ]
           [dev2: stripe 1     ]  (same data, different device)
```

For RAID1 metadata on a 2-device filesystem, mkfs places one stripe
on each device at the same physical offset (`CHUNK_START`):

```
Device 1: [system][meta stripe 0][data stripe 0]
Device 2:        [meta stripe 1]
```

### RAID0

```
num_stripes = N  (number of devices)
sub_stripes = 0
```

Data is striped across N devices in `stripe_len`-sized (64 KiB) units.
No redundancy. The logical chunk size equals `N * physical_stripe_size`.

```
Logical:   [--A--][--B--][--C--][--A--][--B--][--C--]
Physical:  dev1: [--A--]       [--A--]
           dev2:        [--B--]       [--B--]
           dev3:               [--C--]       [--C--]
```

To resolve a logical address within a RAID0 chunk:
1. `offset = logical - chunk_start`
2. `stripe_nr = offset / stripe_len`
3. `stripe_index = stripe_nr % num_stripes`
4. `stripe_offset = (stripe_nr / num_stripes) * stripe_len +
   (offset % stripe_len)`
5. Physical address = `stripes[stripe_index].offset + stripe_offset`

### RAID10

```
num_stripes = N  (must be even, >= 4)
sub_stripes = 2
```

Striped mirrors: data is striped across `N/2` mirror groups, each group
having `sub_stripes` (2) copies. Combines RAID0 throughput with RAID1
redundancy.

### RAID5 and RAID6

```
RAID5: num_stripes = N, sub_stripes = 0, one parity stripe
RAID6: num_stripes = N, sub_stripes = 0, two parity stripes
```

Data is striped with rotating parity. RAID5 tolerates one device failure;
RAID6 tolerates two.


## Allocation Sizing

When creating a new filesystem (`mkfs`), the initial chunk sizes are
computed from the total device size. The formulas, implemented in
`mkfs/src/layout.rs` (`ChunkLayout::new`), are:

### System Block Group

Fixed size and position:
- Offset: `SYSTEM_GROUP_OFFSET` = 1 MiB (0x100000)
- Size: `SYSTEM_GROUP_SIZE` = 4 MiB (0x400000)
- Profile: always SINGLE
- Contains: the chunk tree root block

The first 1 MiB of the device is reserved. The primary superblock sits
at offset 64 KiB within this reserved area.

### Metadata Block Group

```
meta_size = clamp(total_bytes / 10, 32 MiB, 256 MiB)
meta_size = round_down(meta_size, STRIPE_LEN)
```

where `STRIPE_LEN` = 64 KiB and `total_bytes` is the sum across all
devices.

The metadata chunk starts at logical offset `CHUNK_START` = 5 MiB
(`SYSTEM_GROUP_OFFSET + SYSTEM_GROUP_SIZE`). For DUP, two physical
stripes are placed sequentially on device 1. For RAID1, one stripe is
placed on each of the first two devices.

Examples:
- 256 MiB device: `clamp(25.6M, 32M, 256M)` = 32 MiB
- 1 GiB device: `clamp(102.4M, 32M, 256M)` = 102 MiB (rounded to 64K)
- 10 GiB device: `clamp(1G, 32M, 256M)` = 256 MiB

### Data Block Group

```
data_size = clamp(total_bytes / 10, 64 MiB, 1 GiB)
data_size = round_down(data_size, STRIPE_LEN)
```

The data chunk follows the metadata chunk in both logical and physical
address spaces. Logical offset = `CHUNK_START + meta_size`.

Examples:
- 256 MiB device: `clamp(25.6M, 64M, 1G)` = 64 MiB
- 1 GiB device: `clamp(102.4M, 64M, 1G)` = 102 MiB (rounded to 64K)
- 10 GiB device: `clamp(1G, 64M, 1G)` = 1 GiB

### Minimum Device Size

For a single-device filesystem with DUP metadata and SINGLE data, the
minimum physical space needed is:

```
1 MiB (reserved) + 4 MiB (system) + 2 * meta_size + data_size
```

With the minimum sizes (meta = 32 MiB, data = 64 MiB), this works out to
approximately 133 MiB. A 100 MiB device will fail with "device too small".

### Physical Layout Summary

For a single-device DUP-metadata SINGLE-data filesystem:

```
Physical byte offset:
  [0 .. 1M)                          Reserved (superblock at 64K)
  [1M .. 5M)                         System block group (4 MiB)
  [5M .. 5M + meta_size)             Metadata stripe 0
  [5M + meta_size .. 5M + 2*meta)    Metadata stripe 1 (DUP copy)
  [5M + 2*meta .. 5M + 2*meta + data)  Data

Logical address space:
  [1M .. 5M)                         System chunk
  [5M .. 5M + meta_size)             Metadata chunk
  [5M + meta_size .. 5M + meta + data)  Data chunk
```

Note the physical space for DUP metadata is `2 * meta_size`, but the
logical address range is only `meta_size`. Both physical stripes map to
the same logical range.


## Cross-Checks

The `btrfs check` command (implemented in `cli/src/check/chunks.rs`)
verifies the consistency of the chunk/block-group/device-extent triad.

### Chunk-to-Block-Group Check

For every chunk in the chunk tree, there must be a matching block group
item. The check walks the chunk tree cache and verifies that
`block_groups.contains_key(chunk.logical)` for each chunk.

If a chunk has no corresponding block group, `btrfs check` reports:

```
ChunkMissingBlockGroup { logical }
```

### Block-Group-to-Chunk Check

For every block group item (from the extent tree or block-group tree),
there must be a matching chunk. The check verifies that
`chunk_cache.lookup(bg_logical)` succeeds for each block group.

If a block group has no corresponding chunk, `btrfs check` reports:

```
BlockGroupMissingChunk { logical }
```

### Device Extent Overlap Check

All device extents for each device are collected from the device tree,
sorted by physical offset, and checked for overlaps. For consecutive
extents on the same device, the check verifies:

```
extent[i].offset >= extent[i-1].offset + extent[i-1].length
```

If two device extents overlap, `btrfs check` reports:

```
DeviceExtentOverlap { devid, offset }
```

### Block Group Source

When the `BLOCK_GROUP_TREE` compat_ro feature is enabled, block group
items are stored in a separate tree (tree objectid 10) rather than in the
extent tree. The check code handles both cases by selecting the
appropriate tree root:

```rust
let bg_root = block_group_tree_root.unwrap_or(extent_root);
```


## The Chunk Tree

The chunk tree (tree objectid 3) stores two kinds of items:

1. **DEV_ITEM** entries for each device in the filesystem:
   `(DEV_ITEMS_OBJECTID=1, DEV_ITEM=216, devid)`

2. **CHUNK_ITEM** entries for each chunk:
   `(FIRST_CHUNK_TREE_OBJECTID=256, CHUNK_ITEM=228, logical_offset)`

Items are sorted by key, so DEV_ITEMs (objectid 1) come before
CHUNK_ITEMs (objectid 256).

The chunk tree root pointer is stored directly in the superblock's
`chunk_root` field --- it does not go through the root tree like other
trees. This is because the chunk tree is needed to read the root tree
itself.

### mkfs Chunk Tree Construction

When `mkfs` builds the chunk tree (`build_chunk_tree` in
`mkfs/src/mkfs.rs`), it creates:

1. One `DEV_ITEM` per device, with `bytes_used` set to the sum of all
   chunk stripes on that device.

2. Three `CHUNK_ITEM` entries:
   - System chunk at `SYSTEM_GROUP_OFFSET` (1 MiB), size 4 MiB
   - Metadata chunk at `CHUNK_START` (5 MiB), with profile-dependent
     stripes
   - Data chunk after metadata, with profile-dependent stripes

The system chunk item uses `sectorsize` for `io_align` and `io_width`
(matching the kernel's bootstrap behavior), while the metadata and data
chunks use `STRIPE_LEN` (64 KiB).


## The Device Tree

The device tree (tree objectid 4) stores:

1. **DEV_STATS** (PERSISTENT_ITEM) for each device: per-device I/O error
   counters, initialized to zero by mkfs.

2. **DEV_EXTENT** entries for each physical stripe of each chunk.

Items are sorted by key: `(objectid=devid, type=DEV_EXTENT,
offset=physical_byte_offset)`.

### mkfs Device Tree Construction

When `mkfs` builds the device tree (`build_dev_tree` in
`mkfs/src/mkfs.rs`), it creates:

1. One `DEV_STATS` item per device (zeroed counters).

2. Device extents for each stripe:
   - System chunk: one DEV_EXTENT on device 1 at `SYSTEM_GROUP_OFFSET`
   - Metadata chunk: one DEV_EXTENT per stripe (two for DUP on device 1,
     or one per device for RAID1)
   - Data chunk: one DEV_EXTENT per stripe

All device tree items are collected, sorted by key, and written in order.
This is necessary because items span multiple device IDs and physical
offsets that are not naturally ordered by construction.


## Superblock Mirrors

The superblock is written at up to three fixed physical offsets on each
device:

| Mirror | Offset     | Size   |
|--------|------------|--------|
| 0      | 64 KiB     | 4 KiB  |
| 1      | 64 MiB     | 4 KiB  |
| 2      | 256 GiB    | 4 KiB  |

The formula is: mirror 0 at 65536 bytes; mirror N (N > 0) at
`16384 << (12 * N)` bytes. Mirrors are only written if the device is
large enough to contain them.

The superblock contains the `sys_chunk_array` bootstrap data, root
pointers for the chunk tree and root tree, the embedded device item for
device 1, and all filesystem-level metadata (UUID, label, feature flags,
generation counter, `bytes_used`, etc.).

All three mirrors contain identical data for a given generation. On mount,
the kernel reads all available mirrors and uses the one with the highest
valid generation, providing resilience against corruption of the primary
superblock.


## Tree Block Placement in mkfs

During filesystem creation, tree blocks must be placed at specific logical
addresses within the chunks. The `BlockLayout` struct in
`mkfs/src/layout.rs` assigns addresses:

**Chunk tree block:** placed at `SYSTEM_GROUP_OFFSET` (1 MiB) in the
system chunk. This is the only tree block in the system block group.

**All other tree blocks** (root, extent, device, FS, csum, free-space,
data-reloc, and optionally block-group): placed sequentially in the
metadata chunk starting at `meta_logical` = 5 MiB. With a 16 KiB
nodesize:

| Logical address | Tree |
|-----------------|------|
| `meta_logical + 0` | Root tree |
| `meta_logical + 16K` | Extent tree |
| `meta_logical + 32K` | Device tree |
| `meta_logical + 48K` | FS tree |
| `meta_logical + 64K` | Csum tree |
| `meta_logical + 80K` | Free-space tree |
| `meta_logical + 96K` | Data-reloc tree |
| `meta_logical + 112K` | Block-group tree (if enabled) |

For `--rootdir` mode, where trees may require multiple blocks, the
`BlockAllocator` hands out sequential addresses from the system and
metadata chunks, supporting trees of arbitrary size.

System chunk bytes used = nodesize (one chunk tree block).
Metadata chunk bytes used = `7 * nodesize` (or 8 with block-group tree).

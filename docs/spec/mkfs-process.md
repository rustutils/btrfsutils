# mkfs.btrfs: filesystem creation process

This document describes how `mkfs.btrfs` creates a new btrfs filesystem. It
covers the empty filesystem (`make_btrfs`) and population from a directory
(`make_btrfs_with_rootdir`).

## Overview

`mkfs.btrfs` uses no kernel ioctls (apart from `BLKGETSIZE64`, `BLKDISCARD`,
and `FICLONERANGE` for `--reflink`) and does not mount anything. Creation runs
in up to three phases:

1. **Bootstrap.** Hand-built tree blocks for the four always-present trees
   (root, extent, chunk, dev) and the superblock are written with `pwrite`.
   The result is the minimum on-disk state that the transaction crate's
   `Filesystem::open` accepts.
2. **Post-bootstrap.** The image is reopened with `btrfs_transaction`, and
   one transaction creates every other tree (block-group, FS, csum,
   data-reloc, quota, free-space, UUID) and commits.
3. **Rootdir population** (`--rootdir` only). The image is reopened again,
   and a second transaction walks the source directory and inserts its
   contents. Then it commits.

Phases 2 and 3 use the same `Transaction` pipeline as the rest of the
codebase. The extent tree's self-reference problem (extent items for the
extent tree's own blocks) is handled by the transaction commit's convergence
loop, not by mkfs.

The implementation spans several modules:

- `mkfs/src/run.rs`: argument handling, device checks, TRIM, and dispatch
- `mkfs/src/mkfs.rs`: `MkfsConfig`, `make_btrfs`, `make_btrfs_with_rootdir`,
  and the bootstrap tree builders
- `mkfs/src/layout.rs`: chunk layout and bootstrap block addresses
- `mkfs/src/tree.rs`: `LeafBuilder` for individual leaf blocks
- `mkfs/src/items.rs`: serializers for the bootstrap and quota items
- `mkfs/src/post_bootstrap.rs`: the post-bootstrap transaction
- `mkfs/src/rootdir.rs`: the `--rootdir` walker (`walk_to_transaction`)
- `mkfs/src/write.rs`: `pwrite_all` and the `ChecksumType` re-export

## Part 1: bootstrap (make_btrfs)

### Step 1: validation

`run.rs` checks the arguments before any I/O:

- `nodesize` defaults to 16384, `sectorsize` to 4096.
- `sectorsize` must be a power of 2 and >= 4096.
- `nodesize` must be a power of 2, >= sectorsize, and <= 65536.
- The label must be shorter than 256 bytes.
- The device count must meet the profile minimum (`Profile::min_devices`):
  1 for SINGLE and DUP, 2 for RAID0, RAID1, RAID5, and RAID10, 3 for RAID1C3
  and RAID6, 4 for RAID1C4.
- Each device size is rounded down to a multiple of sectorsize and must be at
  least `minimum_device_size` (29 MiB, see Step 2).
- A block device must not be mounted, and must not already hold a btrfs
  superblock unless `-f` is given.

Unless `--nodiscard` is given, each block device is fully discarded with
`BLKDISCARD`. A discard failure is only a warning.

`make_btrfs` repeats the sectorsize and nodesize checks, and also rejects:

- the `mixed-bg` incompat feature with nodesize != sectorsize
- quota and squota enabled together

### Step 2: chunk layout computation

`ChunkLayout::new` computes the placement of three block groups. Chunk sizes
are derived from `total_bytes`, the sum over all devices.

The default metadata profile is DUP on one device and RAID1 on several. The
default data profile is SINGLE.

#### System block group

- Logical offset: 1 MiB (`SYSTEM_GROUP_OFFSET`).
- Size: 4 MiB (`SYSTEM_GROUP_SIZE`).
- Physical offset: same as logical (identity mapping on device 1).
- Profile: always SINGLE (one stripe on device 1).
- Contains: the chunk tree block.

#### Metadata block group

- Logical offset: 5 MiB (`CHUNK_START` = system offset + system size).
- Per-stripe size: `clamp(total_bytes / 10, 8 MiB, 256 MiB)`, rounded down to
  64 KiB (`STRIPE_LEN`).
- Logical size: per-stripe size times the profile's data stripe count (1 for
  mirror profiles, all stripes for RAID0, half for RAID10, all minus parity for
  RAID5 and RAID6).
- Stripe placement by profile:
  - SINGLE: one stripe on device 1 at `CHUNK_START`.
  - DUP: two stripes on device 1, at `CHUNK_START` and
    `CHUNK_START + meta_size`.
  - RAID1, RAID1C3, RAID1C4: one stripe at `CHUNK_START` on each of the first
    2, 3, or 4 devices.
  - RAID0, RAID5, RAID6: one stripe at `CHUNK_START` on every device.
  - RAID10: one stripe at `CHUNK_START` on every device, with the count
    rounded down to an even number.
- Contains: the root, extent, and dev tree blocks after bootstrap, and every
  tree block the later transactions allocate.

#### Data block group

- Logical offset: metadata logical offset + metadata logical size.
- Per-stripe size: `clamp(total_bytes / 10, 8 MiB, 1 GiB)`, rounded down to
  `STRIPE_LEN`.
- Stripe placement by profile:
  - SINGLE: one stripe on device 1, after the last metadata stripe on that
    device.
  - DUP: two consecutive stripes on device 1, after the last metadata stripe.
  - All other profiles: one stripe on each of the first N devices (N from
    `Profile::num_stripes`), each after the last metadata stripe on that
    device, or at `CHUNK_START` if the device has none.
- Contains: file data (empty after phases 1 and 2).

The layout checks that every stripe fits on its device. If one does not,
`ChunkLayout::new` returns `None` and mkfs reports "device too small".

`minimum_device_size` is 29 MiB: 5 MiB (system) + 16 MiB (2 x 8 MiB metadata
DUP) + 8 MiB (data). It is a per-device precheck. Some layouts (for example
DUP data) need more, and `ChunkLayout::new` rejects those.

### Step 3: block address assignment

`BlockLayout` assigns a fixed logical address to each bootstrap tree block:

- **Chunk tree:** at `SYSTEM_GROUP_OFFSET` (1 MiB), in the system chunk.
- **Root, extent, dev trees:** sequential in the metadata chunk starting at
  `meta_logical`, spaced by `nodesize` (`NON_CHUNK_TREES`).

For example, with nodesize = 16384:

| Tree | Logical address |
|------|----------------|
| Chunk | 0x100000 (1 MiB) |
| Root | 0x500000 (5 MiB) |
| Extent | 0x504000 |
| Dev | 0x508000 |

Blocks created later by the transaction crate get addresses from its
allocator, not from `BlockLayout`.

### Step 4: tree block construction

Each bootstrap tree is a single leaf built with `LeafBuilder`. Items are
collected, sorted by key, and pushed in ascending order. Item descriptors grow
forward from byte 101 (after the header), and item data grows backward from
the end of the block. Every leaf has generation 1, and header flags
`WRITTEN` with backref revision `MIXED_BACKREF_REV`.

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

- A 439-byte `ROOT_ITEM` for the extent tree and for the dev tree
  (`ROOT_ITEM_TREES`). The root tree cannot reference itself, and the
  superblock's `chunk_root` points at the chunk tree. Each ROOT_ITEM has
  `bytenr` set to the tree's block, `root_dirid = 256`, `bytes_used =
  nodesize`, `refs = 1`, `level = 0`, and an embedded inode with nlink 1 and
  mode 040755.
- The root tree directory (objectid 6): an `INODE_ITEM` (directory, nlink 1,
  nbytes = nodesize), an `INODE_REF` named `..` pointing to itself, and a
  `DIR_ITEM` named `default` whose location is `(5, ROOT_ITEM, u64::MAX)`, the
  FS tree.

The post-bootstrap transaction inserts the other ROOT_ITEMs.

#### Extent tree contents

- One `METADATA_ITEM` per bootstrap tree block (`TreeId::ALL`: root, extent,
  chunk, dev), or an `EXTENT_ITEM` when skinny metadata is disabled. Each is
  24 bytes of `btrfs_extent_item` (refs = 1, generation = 1,
  flags = `TREE_BLOCK`) followed by a 9-byte inline `TREE_BLOCK_REF` (type
  byte and owner root objectid). With skinny metadata the key is
  `(bytenr, METADATA_ITEM, 0)`. Without it, the key is
  `(bytenr, EXTENT_ITEM, nodesize)` and a zeroed 18-byte
  `btrfs_tree_block_info` sits between the two parts.
- One 24-byte `BLOCK_GROUP_ITEM` per block group, keyed
  `(logical, BLOCK_GROUP_ITEM, logical_size)`. Used bytes: nodesize for
  system, 3 x nodesize for metadata, 0 for data. These always start in the
  extent tree. Post-bootstrap moves them when the block-group tree is
  enabled.

#### Chunk tree contents

1. **`DEV_ITEM`** per device, keyed `(DEV_ITEMS_OBJECTID, DEV_ITEM, devid)`,
   with total bytes, bytes used (sum of that device's stripes), sectorsize for
   `io_align`, `io_width`, and `sector_size`, the device UUID, and the fsid.
2. **`CHUNK_ITEM`** per block group, keyed
   `(FIRST_CHUNK_TREE_OBJECTID, CHUNK_ITEM, logical)`, owner = extent tree
   objectid, `stripe_len = STRIPE_LEN`:
   - System chunk: sectorsize for `io_align` and `io_width`, one stripe on
     device 1.
   - Metadata and data chunks: `STRIPE_LEN` for `io_align` and `io_width`,
     `sub_stripes` 2 for RAID10 and 1 otherwise, stripes from Step 2.

#### Dev tree contents

1. **`PERSISTENT_ITEM`** (dev stats) per device, keyed
   `(DEV_STATS_OBJECTID, PERSISTENT_ITEM, devid)`: five zeroed counters
   (40 bytes).
2. **`DEV_EXTENT`** per physical stripe, keyed
   `(devid, DEV_EXTENT, physical_offset)`: one for the system chunk on device
   1, and one per metadata and data stripe.

### Step 5: checksum computation

`btrfs_disk::util::csum_tree_block` computes the checksum of bytes
`32..nodesize` and writes it into the first bytes of the block. The rest of
the 32-byte checksum field is zeroed. See [Checksums](#checksum-computation-writers).

### Step 6: writing to disk

#### Tree blocks

Each block is written with `pwrite_all` to every location returned by
`ChunkLayout::logical_to_physical`:

- System chunk: device 1 at the logical address.
- Mirror profiles (SINGLE, DUP, RAID1, RAID1C3, RAID1C4): one write per
  stripe.
- RAID10: one write to each device of the mirror pair that owns the offset.
- RAID0, RAID5, RAID6: one write to the data stripe that owns the offset. The
  bootstrap writes no parity.

#### Superblocks

The bootstrap superblock has:

- `magic`: `_BHRfS_M`, `generation` and `chunk_root_generation`: 1
- `root`, `chunk_root`: the root and chunk tree block addresses, both level 0
- `total_bytes`: sum across all devices
- `bytes_used`: 4 x nodesize (the four bootstrap blocks)
- `sectorsize`, `nodesize`, `leafsize` (= nodesize), `stripesize`
  (= sectorsize)
- `num_devices`, `label`, `csum_type`
- `incompat_flags`, `compat_ro_flags`: the final configured flags, even
  though the FST and BGT trees do not exist yet
- `cache_generation`: 0 if the free-space tree is enabled, `u64::MAX`
  otherwise
- `sys_chunk_array`: the system chunk's disk key and `CHUNK_ITEM`, so the
  kernel can map the chunk tree from the superblock alone

Each device gets its own copy with its `dev_item` (devid, device UUID, total
and used bytes). It is written to each mirror that fits on the device:

- Mirror 0: 64 KiB.
- Mirror 1: 64 MiB.
- Mirror 2: 256 GiB.

All device files are then synced and closed, and `post_bootstrap::run` is
called.

## Part 2: post-bootstrap transaction (post_bootstrap.rs)

`post_bootstrap::run` reopens the devices with `Filesystem::open` (one
device) or `Filesystem::open_multi` (several, keyed by the devid in each
device's superblock). It starts one transaction, applies the steps below,
commits, and syncs. Each step first checks whether the tree already exists
and skips it if so.

1. **Block-group tree** (objectid 11), if `BLOCK_GROUP_TREE` is set.
   `convert::create_block_group_tree` creates the tree and moves every
   `BLOCK_GROUP_ITEM` out of the extent tree. This runs first so later block
   group updates in the same transaction go to the new tree.
2. **FS tree** (objectid 5), via `create_subvolume_shape`: inode 256 as a
   directory (mode 040755, nlink 1, nbytes = nodesize, all timestamps set to
   the creation time, flag `INODE_ROOT_ITEM_INIT`), a `..` `INODE_REF` to
   itself with index 0, and the ROOT_ITEM patched so `root_dirid = 256`, the
   embedded inode matches the standalone one, `ctime` and `otime` are the
   creation time, and `uuid` is the fsid with every byte XORed with 0xFF.
3. **Csum tree** (objectid 7): empty leaf.
4. **Data-reloc tree** (objectid -9): same shape as the FS tree, but with
   zero timestamps, no `INODE_ROOT_ITEM_INIT`, and a nil UUID.
5. **Quota tree** (objectid 8), if `-O quota` or `-O squota`. Items for
   qgroup 0/5:
   - `QGROUP_STATUS` at `(0, QGROUP_STATUS, 0)`: version 1, flags `ON` with
     `INCONSISTENT` (quota) or `SIMPLE_MODE` (squota). Squota also writes
     `enable_gen`, making the item 40 bytes instead of 32.
   - `QGROUP_INFO` at `(0, QGROUP_INFO, 5)`: zeroed for quota. For squota,
     referenced and exclusive are nodesize.
   - `QGROUP_LIMIT` at `(0, QGROUP_LIMIT, 5)`: zeroed.
6. **Free-space tree** (objectid 10), if `FREE_SPACE_TREE` is set. Created
   empty, then `convert::seed_free_space_tree` adds `FREE_SPACE_INFO` and
   `FREE_SPACE_EXTENT` items for every block group from the current extent
   tree. The commit's free-space update applies the allocations made later in
   the same transaction.
7. **UUID tree** (objectid 9): empty leaf.

The commit inserts extent items for every new tree block, rewrites the
superblock (generation 2), and writes all mirrors.

## Part 3: rootdir population (make_btrfs_with_rootdir)

`--rootdir` populates the new filesystem from a host directory.
`make_btrfs_with_rootdir` checks sectorsize and nodesize, runs `make_btrfs`
(phases 1 and 2), then reopens the devices, starts a new transaction, calls
`rootdir::walk_to_transaction`, and commits (generation 3).

`run.rs` validates the related options first:

- `--subvol` and `--reflink` require `--rootdir`.
- Every `--subvol` path must be a directory under the rootdir.
- At most one subvolume can be `default` or `default-ro`. If one is, the
  `DEFAULT_SUBVOL` incompat flag is set.
- `--reflink` and `--compress` cannot be combined.

### Directory walk

`walk_one_tree_to_transaction` walks one tree (the FS tree or one subvolume)
depth-first. Entries in each directory are sorted by name, so output is
deterministic. Xattrs of the tree's root directory are applied to inode 256.

#### Inode assignment

Inode numbers start at 257 in each tree. Inode 256 is the root directory that
phase 2 (or `create_subvolume_shape` for a subvolume) already created.

#### Directory indexes

Each directory's `DIR_INDEX` counter starts at 2. Every entry, including
hardlinks and subvolume entries, takes the next index in its parent.

#### Per-entry processing

For a new inode:

1. `create_inode` inserts the `INODE_ITEM`: uid, gid, mode, atime, ctime, and
   mtime from the host, otime = creation time, nlink 1, rdev for character
   and block devices, and the `--inode-flags` bits.
2. `link_dir_entry` inserts `DIR_ITEM` (keyed by `crc32c(0xFFFFFFFE, name)`),
   `DIR_INDEX`, and `INODE_REF`, and grows the parent directory's size by
   `2 x name_len`.
3. `set_xattr` inserts one `XATTR_ITEM` per host xattr (read with
   `llistxattr` and `lgetxattr`). `ENOTSUP` and `ENODATA` are skipped.

Then, by type:

- **Directories:** children are pushed onto the stack in reverse so they are
  visited in name order.
- **Symlinks:** `insert_inline_extent` with the link target, uncompressed.
- **Regular files with size > 0:** the whole file is read into memory and
  passed to `Transaction::write_file_data`, or to the reflink path below.
- **Empty files and special files** (FIFO, socket, character or block
  device): `INODE_ITEM` only.

#### Hardlinks

For non-directories with `nlink > 1`, the walker maps the host
`(dev, ino)` to the btrfs inode. A later entry for the same host inode gets
only a `link_dir_entry` to the existing inode. After the walk,
`set_inode_nlink` writes the counted link total for each such inode. The map
is per tree, so hardlinks across subvolumes become separate inodes.

#### Inode flags

`--inode-flags` sets `NODATACOW` and `NODATASUM` per path, relative to the
rootdir. `NODATACOW` implies `NODATASUM` for regular files. `NODATASUM`
suppresses data checksums.

### File data (write_file_data)

`Transaction::write_file_data` stores a file as:

- **Inline extent**, if the data is at most `max_inline_data_size`, which is
  `min(sectorsize - 1, nodesize - 147)` (4095 with the defaults). The
  compression attempt is kept only if it shrinks the data. No data extent or
  checksums.
- **Regular extents** otherwise, in chunks of up to 1 MiB. Each chunk is
  compressed if requested (zlib, zstd, or LZO in the per-sector framing) and
  kept compressed only if smaller. `alloc_data_extent` finds space in a data
  block group, writes the sector-padded bytes to all stripes, and queues the
  data extent ref. The `EXTENT_DATA` item records `disk_num_bytes` (the
  padded on-disk size), `num_bytes` and `ram_bytes` (the sector-aligned
  logical size), and the compression type. Unless `NODATASUM`, the on-disk
  bytes are read back and `insert_csums` adds per-sector checksums.
  `INODE_ITEM.nbytes` grows by the aligned logical size.

The commit creates the data `EXTENT_ITEM`s from the queued refs.

### Reflink (--reflink)

`reflink_file_to_transaction` replaces the byte copy. For each chunk of up to
1 MiB, it reserves a data extent with `reserve_data_extent`, plans the
physical placement, and calls `FICLONERANGE` from the source file into each
device image at the planned offset. Then it inserts checksums (unless
`NODATASUM`), an uncompressed `EXTENT_DATA`, and the `nbytes` update. The
source and the image must be on the same filesystem. RAID5 and RAID6 chunks
are rejected because parity would not be updated.

### Subvolumes (--subvol)

Subvolume IDs are assigned from 256 in `--subvol` argument order. When the
walk reaches a subvolume directory, it calls `link_subvol_entry` in the
parent (a `DIR_ITEM` and `DIR_INDEX` pointing to `(id, ROOT_ITEM, 0)`) and
defers the subvolume. For each deferred subvolume:

1. `create_subvolume_shape` creates the tree with inode 256 and a random v4
   UUID.
2. `insert_root_ref` adds `ROOT_REF` and `ROOT_BACKREF` in the root tree.
3. The subvolume's directory is walked into its own tree. Nested subvolumes
   found there are deferred in turn.
4. `ro` and `default-ro` subvolumes get `set_root_readonly`. `default` and
   `default-ro` get `set_default_subvol`, which rewrites the root tree's
   `default` `DIR_ITEM`.

### Shrink (--shrink)

Single device only. It is ignored with several devices.

1. `compute_shrunk_size` takes the physical end of the system, metadata, and
   data stripes on the device and rounds it up to sectorsize.
2. Before the rootdir commit, `set_device_total_bytes` updates the device's
   `DEV_ITEM`, and `superblock.total_bytes` is set to the same value.
3. After the commit and sync, the image file is truncated with `set_len`.

The size covers the whole data block group, not only the bytes used in it.

## Item serialization (items.rs)

The serializers return a `Vec<u8>` for `LeafBuilder::push` or for raw
inserts. They use `bytes::BufMut` for little-endian encoding and take field
positions from `offset_of!` and `size_of` on the bindgen structs. Phases 2
and 3 otherwise use the transaction crate's own item helpers.

| Function | Item type | Size |
|----------|-----------|------|
| `root_item` | ROOT_ITEM | 439 bytes |
| `extent_item` | EXTENT_ITEM or METADATA_ITEM | 33 bytes (skinny) or 51 bytes |
| `block_group_item` | BLOCK_GROUP_ITEM | 24 bytes |
| `dev_item` | DEV_ITEM | 98 bytes |
| `chunk_item` | CHUNK_ITEM | 48 + 32 x num_stripes bytes |
| `dev_extent` | DEV_EXTENT | 48 bytes |
| `dev_stats_zeroed` | PERSISTENT_ITEM | 40 bytes |
| `inode_item_dir` | INODE_ITEM | 160 bytes |
| `inode_ref` | INODE_REF | 10 + name_len bytes |
| `dir_item` | DIR_ITEM | 30 + name_len bytes |
| `disk_key` | (sys_chunk_array key) | 17 bytes |
| `qgroup_status` | QGROUP_STATUS | 32 bytes, or 40 with `enable_gen` |
| `qgroup_info`, `qgroup_info_zeroed` | QGROUP_INFO | 40 bytes |
| `qgroup_limit_zeroed` | QGROUP_LIMIT | 40 bytes |

`items.rs` also has serializers that the current pipeline does not call
(`inode_item`, `xattr_item`, `file_extent_inline`, `file_extent_reg`,
`data_extent_item`, `free_space_info`, `root_ref`, `chunk_tree_root_item`).

## Checksum computation (write.rs)

`ChecksumType` (from `btrfs_disk::superblock`) supports four algorithms. Each
checksums bytes 32 to the end of tree blocks and superblocks:

| Algorithm | On-disk type value | Output size | Implementation |
|-----------|-------------------|-------------|----------------|
| CRC32C | 0 | 4 bytes | `crc32c` crate |
| xxHash64 | 1 | 8 bytes | `xxhash-rust` crate |
| SHA-256 | 2 | 32 bytes | `sha2` crate |
| BLAKE2b-256 | 3 | 32 bytes | `blake2` crate |

The hash fills the first N bytes of the 32-byte checksum field, and the rest
is zeroed. Data checksums in the csum tree use the same algorithm, one per
sector.

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
Disabling `free-space-tree` also clears `block-group-tree`. RAID5 or RAID6 in
either profile sets the `RAID56` incompat flag. `-O squota` sets
`SIMPLE_QUOTA`. `-O zoned` and `-O raid-stripe-tree` are rejected.

## Multi-device support

All profiles work on several devices: SINGLE, DUP, RAID0, RAID1, RAID1C3,
RAID1C4, RAID10, RAID5, and RAID6, for metadata and data. Step 2 gives the
stripe placement for each.

Devid N is the Nth device on the command line. Device 1 holds the system
chunk. Each device gets its own superblock with its own `dev_item`. The chunk
tree has a `DEV_ITEM` per device, and the dev tree has a `DEV_EXTENT` per
stripe. Phases 2 and 3 open the devices with `Filesystem::open_multi`, whose
writes go through `ChunkTreeCache::plan_write` and compute RAID5 and RAID6
parity.

## Limitations

- Zoned device support is not implemented.
- The transaction crate does not allocate new chunks. Everything written in
  phases 2 and 3, including `--rootdir` content, must fit in the initial
  metadata and data block groups.
- `-M` (`--mixed`) is accepted but has no effect, and `-O mixed-bg` sets the
  incompat flag without creating mixed block groups.
- The `--compress` level is parsed but not passed to the compressor.
- `--reflink` does not support RAID5 or RAID6 data.

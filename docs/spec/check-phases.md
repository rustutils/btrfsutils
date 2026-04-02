# btrfs check: verification phases

This document describes the seven phases of `btrfs check`, as implemented in the
`cli/src/check/` module. The checker operates in read-only mode on an unmounted
filesystem, reading the raw on-disk image through `btrfs-disk`'s `BlockReader`
without requiring any kernel ioctls.

## Overview

The check command opens the filesystem image and bootstraps the chunk tree
(superblock -> sys_chunk_array -> chunk tree -> root tree), then runs seven
sequential verification phases:

1. Superblock mirror validation
2. Tree structure checks (all trees)
3. Extent tree cross-checks (reference counting and ownership)
4. Chunk / block group / device extent cross-checks
5. FS tree inode consistency
6. Checksum tree verification
7. ROOT_REF / ROOT_BACKREF consistency

Each phase accumulates errors into a `CheckResults` struct. Errors are printed
to stderr as they are found, and a summary is printed at the end. The process
exits with code 1 if any errors were detected.

### Orchestration (check.rs)

The main `CheckCommand::run` method:

1. Rejects unsupported flags (`--repair`, `--init-csum-tree`, `--init-extent-tree`,
   `--backup`, `--tree-root`, `--chunk-root`, `--qgroup-report`, `--subvol-extents`).
2. Checks mount status (skippable with `--force`).
3. Validates the superblock mirror index (0-2).
4. Opens the filesystem via `reader::filesystem_open_mirror`, which bootstraps
   chunk mapping and discovers all tree roots.
5. Runs phases 1-7 in order.
6. Prints summary and exits.

### Statistics tracking

Throughout all phases, `CheckResults` accumulates byte counts that are printed
in the final summary:

- `total_tree_bytes`: sum of nodesize for every tree block visited in phase 2.
- `total_fs_tree_bytes`: subset of the above for FS trees (objectid 5 and >= 256).
- `total_extent_tree_bytes`: subset of the above for the extent tree (objectid 2).
- `btree_space_waste`: for each leaf, nodesize minus actual bytes used (header +
  item descriptors + item data payloads).
- `data_bytes_allocated`: total length of data extents from extent items.
- `data_bytes_referenced`: total referenced bytes, accounting for shared extents
  via `ExtentDataRef` and `SharedDataRef` count fields.
- `total_csum_bytes`: total bytes of checksum data in the csum tree.

## Phase 1: Superblocks

**Source:** `cli/src/check/superblock.rs`

**Purpose:** Validate all three superblock mirror copies.

### What it checks

Btrfs stores up to three copies of the superblock at fixed byte offsets on the
device:

- Mirror 0: byte offset 65536 (64 KiB)
- Mirror 1: byte offset 67108864 (64 MiB)
- Mirror 2: byte offset 274877906944 (256 GiB)

For each mirror (0 through `SUPER_MIRROR_MAX - 1`):

1. Read 4096 bytes from the mirror offset using `read_superblock_bytes_at`.
2. Validate the superblock using `superblock_is_valid`, which checks:
   - The magic number matches `_BHRfS_M` (0x4D5F53665248425F).
   - The CRC32C checksum of bytes 32..4096 matches the stored checksum in
     bytes 0..4.

If a mirror cannot be read (I/O error), this is only reported as an error for
mirror 0. Mirrors 1 and 2 may legitimately be absent on small devices where the
device is shorter than the mirror offset.

### Generation consistency

The current implementation validates each mirror independently (magic + checksum).
The C reference additionally checks that the generation fields across valid mirrors
are consistent (the primary mirror should have the highest generation). This is not
yet implemented.

### Error variants produced

- **`SuperblockInvalid { mirror, detail }`** -- reported when:
  - A mirror has an invalid checksum or magic number.
  - Mirror 0 cannot be read at all (I/O error).

### Return value

Returns the count of valid mirrors found (0-3). This value is currently not used
by the caller but could be used for repair decisions in the future.

## Phase 2: Tree structure

**Source:** `cli/src/check/tree_structure.rs`

**Purpose:** Walk every tree in the filesystem and verify per-block structural
integrity. Collect a map of all tree block addresses and their owners for use
in phase 3.

### Trees checked

The phase checks:

1. **Root tree** -- directly from `superblock.root`.
2. **Chunk tree** -- directly from `superblock.chunk_root`.
3. **All trees discovered in the root tree** -- every `(tree_id, (bytenr, gen))`
   pair from `open.tree_roots`. This includes the extent tree, dev tree, FS tree,
   csum tree, free-space tree, data-reloc tree, block-group tree (if present),
   and all subvolume/snapshot trees.

Each tree is walked using `reader::tree_walk_tolerant`, which performs a depth-first
traversal through all internal nodes and leaves, calling the visitor callback for
each block. The `_tolerant` variant collects read errors instead of aborting,
allowing the checker to report all problems rather than stopping at the first.

### Per-block checks

For every tree block (leaf or internal node), the following checks are performed:

#### CRC32C checksum verification

The first 32 bytes of each block contain the checksum. The checker computes
`btrfs_csum_data(&raw[32..])` (standard CRC32C with ISO 3309 seed) and compares
it to the stored value in `raw[0..4]`. This check is only performed when the
superblock's `csum_type` is CRC32C; other checksum types emit a warning and
skip verification.

#### Fsid match

The block header's `fsid` field (16 bytes at offset 32) must match the
filesystem's effective fsid. The effective fsid is `metadata_uuid` if the
`METADATA_UUID` incompat flag is set, or `fsid` otherwise. This distinction
matters for filesystems that have had their metadata UUID changed via
`btrfs-tune -m`.

#### Generation bound

The block header's `generation` field must not exceed the superblock's
`generation`. A block with a higher generation than the superblock indicates
corruption (the block was written in a transaction that was never committed,
or the block has been corrupted).

#### Level consistency

- Leaf blocks (items present) must have `header.level == 0`.
- Internal nodes (key-pointer entries) must have `header.level > 0`.

A mismatch indicates structural corruption where a block's type disagrees with
its declared level.

#### Key ordering

Within each block, keys must be in strictly ascending order using the compound
key comparison `(objectid, type, offset)`:

- For leaves: consecutive items `items[i-1]` and `items[i]` must satisfy
  `key_less(prev, cur)`.
- For internal nodes: consecutive key-pointers `ptrs[i-1]` and `ptrs[i]` must
  satisfy `key_less(prev, cur)`.

Strictly ascending means no duplicates are allowed. The comparison function uses
the raw type byte for the type field (via `key_type.to_raw()`), comparing the
tuple `(objectid, type_raw, offset)` lexicographically.

### Byte attribution

Each visited block contributes `nodesize` bytes to the appropriate category:

- Extent tree blocks (objectid 2) -> `total_extent_tree_bytes`
- FS tree blocks (objectid 5 or >= 256) -> `total_fs_tree_bytes`
- All blocks -> `total_tree_bytes`

For leaf blocks, space waste is computed as:
```
waste = nodesize - (101 + nritems * 25 + sum(item.size for each item))
```
where 101 is the header size and 25 is the item descriptor size.

### Output

Returns a `HashMap<u64, u64>` mapping each tree block's logical address to the
objectid of the tree that owns it. This map is used by phase 3 for bidirectional
ownership verification.

### Tree name resolution

Tree names for error messages are derived from the objectid using `ObjectId`
formatting (e.g., objectid 1 = "ROOT_TREE", objectid 5 = "FS_TREE", objectid
256+ = the numeric subvolume ID). Names are leaked as `&'static str` since the
set of tree names is small and bounded.

### Error variants produced

- **`TreeBlockChecksumMismatch { tree, logical }`** -- CRC32C does not match.
- **`TreeBlockBadFsid { tree, logical }`** -- header fsid does not match the
  filesystem's effective fsid.
- **`TreeBlockBadBytenr { tree, logical, header_bytenr }`** -- the header's
  `bytenr` field does not match the logical address where the block was read.
  (Note: this check is performed by the block reader during parsing, not
  directly in this phase, but the error is reported here if it occurs.)
- **`TreeBlockBadGeneration { tree, logical, block_gen, super_gen }`** -- block
  generation exceeds superblock generation.
- **`TreeBlockBadLevel { tree, logical, detail }`** -- level/type mismatch
  (leaf with non-zero level, or node with zero level).
- **`KeyOrderViolation { tree, logical, index }`** -- key at `index` is not
  strictly greater than the key at `index - 1`.
- **`ReadError { logical, detail }`** -- I/O error reading a tree block.

## Phase 3: Extents

**Source:** `cli/src/check/extents.rs`

**Purpose:** Walk the extent tree to verify reference counts, detect overlapping
extents, and cross-check tree block ownership against extent tree backrefs in
both directions.

### How it works

The phase walks the extent tree leaf by leaf, processing items in key order.
It maintains an `ExtentCheckState` that tracks the "current" extent being
verified and accumulates statistics.

#### Item processing

Items are processed based on their key type:

**`EXTENT_ITEM` / `METADATA_ITEM`:** Start a new extent. The previous extent
(if any) is flushed first. For the new extent:

1. Record the bytenr in `extent_item_addrs` (for later ownership checks).
2. Determine the extent length:
   - `EXTENT_ITEM`: length = `key.offset`.
   - `METADATA_ITEM`: length = 0 (skinny refs use `key.offset` as level, not
     length, so overlap detection is skipped for metadata items).
3. Check for overlap: if `bytenr < prev_end` and `prev_end > 0`, report an
   overlapping extent error.
4. Parse the `ExtentItem` payload to extract:
   - The declared reference count (`refs`).
   - Inline backrefs and their count.
   - Whether this is a data extent (via `BTRFS_EXTENT_FLAG_DATA`).
   - For tree block extents: collect `TreeBlockBackref` inline refs into
     `extent_backref_owners[bytenr]`.
5. Initialize pending state: `pending_refs` = declared refs, `pending_counted` =
   inline ref count.
6. For data extents, add `length` to `data_bytes_allocated`.

**`TREE_BLOCK_REF`:** Standalone tree block backref. Increments `pending_counted`
by 1. Records `key.offset` (the root objectid) in `extent_backref_owners`.

**`SHARED_BLOCK_REF` / `EXTENT_OWNER_REF`:** Standalone backrefs. Each
increments `pending_counted` by 1.

**`EXTENT_DATA_REF`:** Standalone data backref. Parses the item to extract the
`count` field (number of references from this particular root/objectid/offset
combination). Increments `pending_counted` by `count`. Adds
`length * count` to `data_bytes_referenced`.

**`SHARED_DATA_REF`:** Same as `EXTENT_DATA_REF` but for shared (relocated)
data references.

**All other key types** (e.g., `BLOCK_GROUP_ITEM`): ignored.

#### Inline reference counting

The `count_inline_refs` function iterates over the `InlineRef` variants in an
`ExtentItem`:

- `TreeBlockBackref`, `SharedBlockBackref`, `ExtentOwnerRef`: count as 1 each.
- `ExtentDataBackref`, `SharedDataBackref`: count as their embedded `count` field
  (which may be > 1 for multiply-referenced data extents).

#### Flushing

When a new `EXTENT_ITEM`/`METADATA_ITEM` is encountered, or at the end of the
tree walk, `flush_pending` is called:

1. Skip if no extent is pending (`pending_bytenr == 0`).
2. For data extents where `data_bytes_referenced` is still 0 (only inline refs,
   no standalone `ExtentDataRef`), compute `data_bytes_referenced +=
   pending_length * pending_counted`.
3. Compare `pending_refs` (declared) to `pending_counted` (actual). If they
   differ, report an `ExtentRefMismatch` error.
4. Reset `pending_bytenr` to 0.

#### Bidirectional ownership cross-check

After the extent tree walk completes, two cross-checks are performed using the
`tree_block_owners` map from phase 2:

**Direction 1: tree block -> extent tree.** For every tree block address found
during phase 2 tree walks:

- If the address has no `EXTENT_ITEM` or `METADATA_ITEM` in the extent tree,
  report `MissingExtentItem`.
- If the address has extent tree entries but none of the claimed owner roots
  match the actual owner (the tree that contained this block during phase 2
  walks), report `BackrefOwnerMismatch`.

**Direction 2: extent tree -> tree block.** For every tree block address with
backrefs in the extent tree:

- For each claimed owner root, check if the actual owner (from the phase 2 map)
  matches. If the block was not found during phase 2 walks, or belongs to a
  different tree, report `BackrefOrphan`.

Both cross-checks sort addresses before iteration for deterministic error
ordering.

### Error variants produced

- **`ExtentRefMismatch { bytenr, expected, found }`** -- the declared reference
  count in the `ExtentItem` does not match the sum of inline and standalone
  backrefs.
- **`MissingExtentItem { bytenr }`** -- a tree block observed during phase 2
  has no corresponding `EXTENT_ITEM` or `METADATA_ITEM` in the extent tree.
- **`BackrefOwnerMismatch { bytenr, actual_owner, claimed_owners }`** -- the
  tree block's actual owner (from phase 2) does not appear in the extent tree's
  list of backref owners for that address.
- **`BackrefOrphan { bytenr, claimed_owner }`** -- the extent tree claims a
  backref for a tree that does not actually contain a block at that address.
- **`OverlappingExtent { bytenr, length, prev_end }`** -- two data extents
  overlap in logical address space (the start of one extent is before the end
  of the previous).
- **`ReadError { logical, detail }`** -- I/O error reading the extent tree.

## Phase 4: Chunks / block groups / device extents

**Source:** `cli/src/check/chunks.rs`

**Purpose:** Cross-check the chunk tree, block group items, and device extents
for mutual consistency.

### What it checks

This phase performs three categories of cross-checks:

#### Chunk <-> block group cross-check

Every chunk in the chunk tree's `ChunkTreeCache` (built during filesystem open)
should have a corresponding `BLOCK_GROUP_ITEM` in the extent tree (or block-group
tree, if the `BLOCK_GROUP_TREE` compat_ro feature is enabled). And vice versa:
every block group item should correspond to a chunk.

Block groups are collected by walking either:
- The **block-group tree** if `BTRFS_FEATURE_COMPAT_RO_BLOCK_GROUP_TREE` is set
  in the superblock's `compat_ro_flags`.
- The **extent tree** otherwise (block group items historically lived in the
  extent tree).

The walk collects all items with key type `BLOCK_GROUP_ITEM` into a `BTreeMap`
keyed by logical address.

Then:
1. For each chunk in the chunk cache: if no block group exists at that logical
   address, report `ChunkMissingBlockGroup`.
2. For each block group: if the chunk cache has no chunk at that logical address,
   report `BlockGroupMissingChunk`.

#### Device extent overlap detection

Device extents are collected from the device tree by walking all items with key
type `DEV_EXTENT`. Each extent is recorded as `(offset, length)` grouped by
device ID (`key.objectid`).

For each device, extents are sorted by physical offset. Then consecutive pairs
are checked: if `extents[i].offset < extents[i-1].offset + extents[i-1].length`,
the extents overlap and `DeviceExtentOverlap` is reported.

### Error variants produced

- **`ChunkMissingBlockGroup { logical }`** -- a chunk exists in the chunk tree
  but no block group item was found at the same logical address.
- **`BlockGroupMissingChunk { logical }`** -- a block group item exists but no
  chunk was found at the same logical address.
- **`DeviceExtentOverlap { devid, offset }`** -- two device extents on the same
  device overlap in physical address space.
- **`ReadError { logical, detail }`** -- I/O error reading the block-group tree,
  extent tree, or device tree.

## Phase 5: FS roots

**Source:** `cli/src/check/fs_roots.rs`

**Purpose:** Walk every filesystem tree (the default FS tree and all subvolume
trees) and verify inode-level consistency.

### Which trees are checked

From the `tree_roots` map (populated during filesystem open), the phase selects
trees whose objectid is either:
- `BTRFS_FS_TREE_OBJECTID` (5) -- the default filesystem tree.
- `>= BTRFS_FIRST_FREE_OBJECTID` (256) -- subvolume and snapshot trees.

### Item collection

For each FS tree, `collect_fs_items` walks all leaves and groups items by
objectid (inode number). Each item is stored as a `(KeyType, key_offset,
raw_data_bytes)` tuple. Items arrive in sorted key order due to the B-tree
traversal, which means within an objectid group, items are sorted by
`(key_type, offset)`.

### Per-inode checks

For each objectid group (inode), the following checks are performed:

#### INODE_ITEM presence

The checker notes whether the objectid has an `INODE_ITEM`. If directory entries
reference an objectid that has no `INODE_ITEM`, the entry is an orphan.

Parsed from `INODE_ITEM`: nlink, size (isize), nbytes, and mode.

#### Nlink consistency

The actual reference count is computed by counting entries across all
`INODE_REF` items (via `InodeRef::parse_all`) and `INODE_EXTREF` items (via
`InodeExtref::parse_all`) for this objectid. If the computed count differs from
`inode_item.nlink` and the inode has at least one reference, `NlinkMismatch` is
reported.

The root directory inode (objectid 256, `BTRFS_FIRST_FREE_OBJECTID`) is excluded
from this check because it has special nlink handling in btrfs.

#### File extent overlap detection

For regular files, all `EXTENT_DATA` items are processed to extract
`(file_offset, file_offset + length)` ranges:

- **Regular extents:** length = `num_bytes` from the `FileExtentBody::Regular`
  variant.
- **Inline extents:** length = `inline_size` from the `FileExtentBody::Inline`
  variant.

Since items are in key order and `EXTENT_DATA` keys use the file offset, ranges
are already sorted by start offset. Consecutive ranges are checked: if
`ranges[i].start < ranges[i-1].end`, a `FileExtentOverlap` is reported.

#### Directory inode size (isize) check

For directory inodes (mode & S_IFMT == S_IFDIR), the expected inode size is
computed by summing `name_len * 2` for every `DIR_INDEX` entry belonging to
this inode. The factor of 2 matches the btrfs convention where directory inode
size counts each entry's name length twice (once for `DIR_ITEM`, once for
`DIR_INDEX`).

If the inode's stored `size` field differs from this computed sum, `DirSizeWrong`
is reported.

#### File nbytes check

For regular files and symlinks (mode & S_IFMT == S_IFREG or S_IFLNK), the
expected `nbytes` is computed from extent items:

- **Inline extents:** `nbytes += data_len` (the inline payload size).
- **Regular extents:** `nbytes += disk_num_bytes`, but only for non-prealloc
  extents. Prealloc extents (preallocated but unwritten) and hole extents
  (disk_bytenr == 0) do not contribute.

If the inode's stored `nbytes` differs from the computed total, `NbytesWrong`
is reported.

#### Orphan directory entries

When processing `DIR_ITEM` and `DIR_INDEX` items, for each entry whose location
key type is `INODE_ITEM` and whose target objectid is >= `BTRFS_FIRST_FREE_OBJECTID`
(256): if the target inode has no `INODE_ITEM` anywhere in this tree,
`DirItemOrphan` is reported. Both `DIR_ITEM` and `DIR_INDEX` entries are
checked, so an orphan reference in either will be caught.

### Error variants produced

- **`InodeMissing { tree, ino }`** -- an objectid is referenced but has no
  `INODE_ITEM`. (Note: this is detected indirectly through `DirItemOrphan` in
  the current implementation.)
- **`NlinkMismatch { tree, ino, expected, found }`** -- the inode's stored
  nlink differs from the number of `INODE_REF` + `INODE_EXTREF` entries.
- **`FileExtentOverlap { tree, ino, offset }`** -- two file extent items for
  the same inode overlap in file offset space.
- **`DirItemOrphan { tree, parent_ino, name }`** -- a directory entry references
  an inode that has no `INODE_ITEM`.
- **`DirSizeWrong { tree, ino, expected, found }`** -- a directory inode's
  stored size does not match the computed sum of DIR_INDEX name lengths times 2.
- **`NbytesWrong { tree, ino, expected, found }`** -- a file inode's stored
  nbytes does not match the computed sum from extent items.
- **`ReadError { logical, detail }`** -- I/O error reading the FS tree.

## Phase 6: Checksums

**Source:** `cli/src/check/csums.rs`

**Purpose:** Walk the checksum tree and optionally verify data block checksums
against the actual on-disk data.

### Structure of the csum tree

The csum tree contains `EXTENT_CSUM` items (key type 128). Each item covers a
contiguous range of data sectors:

- Key objectid: `BTRFS_EXTENT_CSUM_OBJECTID` (fixed constant).
- Key offset: the logical byte address of the first sector covered.
- Item data: packed array of checksums, one per sector. With CRC32C (4 bytes per
  checksum) and 4K sectors, a single item can cover many sectors.

### What it checks

#### Phase 6a: tree walk and byte counting

Always performed. The phase walks the csum tree and for each `EXTENT_CSUM` item,
computes `num_csums = item_data_len / csum_size` and adds `item_data_len` to
`total_csum_bytes`. This total is reported in the final summary.

#### Phase 6b: data verification (optional)

Only performed when `--check-data-csum` is passed. Only supported for CRC32C
checksums; other checksum types emit a warning and skip verification.

For each csum item, the phase iterates over every sector:
1. Compute the logical address: `item.key.offset + i * sectorsize`.
2. Read `sectorsize` bytes from that logical address via `reader.read_data`.
3. Compute `btrfs_csum_data(&data)` (standard CRC32C).
4. Compare to the stored checksum (extracted from the item data at offset
   `i * csum_size`).
5. If they differ, or if the read fails, report `CsumMismatch`.

The `btrfs_csum_data` function uses the standard ISO 3309 CRC32C computation
(seed = 0xFFFFFFFF, final XOR), matching the kernel's checksum for tree blocks
and data. This is distinct from the raw CRC32C used in send streams.

### Error variants produced

- **`CsumMismatch { logical }`** -- the computed CRC32C of the data at the
  given logical address does not match the stored checksum, or the data could
  not be read.
- **`ReadError { logical, detail }`** -- I/O error reading the csum tree itself.

## Phase 7: Root refs

**Source:** `cli/src/check/root_refs.rs`

**Purpose:** Verify that `ROOT_REF` and `ROOT_BACKREF` items in the root tree
are consistent with each other.

### Background

In btrfs, subvolume parent-child relationships are recorded in the root tree
using two item types:

- **`ROOT_REF`** (key type 156): stored with `objectid = parent_root_id`,
  `offset = child_root_id`. Contains the directory ID, sequence number, and
  name of the directory entry that references the child subvolume.

- **`ROOT_BACKREF`** (key type 157): stored with `objectid = child_root_id`,
  `offset = parent_root_id`. Contains the same fields as the corresponding
  `ROOT_REF`.

These items form a bidirectional link. For every `ROOT_REF` there should be a
matching `ROOT_BACKREF`, and vice versa. The fields (dirid, sequence, name)
should be identical between the pair.

### What it checks

The phase walks the root tree and collects all `ROOT_REF` and `ROOT_BACKREF`
items into two maps, keyed by `(child_root_id, parent_root_id)`. Both item
types are parsed using `RootRef::parse` (the on-disk format is identical).

Then two passes are made:

#### Forward check: every ROOT_REF has a matching ROOT_BACKREF

For each `(child, parent)` pair in the forward refs map:
- If no entry exists in the back refs map, report `RootBackrefMissing`.
- If an entry exists, compare the three fields:
  - `dirid`: if they differ, report `RootRefMismatch` with "dirid mismatch".
  - `sequence`: if they differ, report `RootRefMismatch` with "sequence mismatch".
  - `name`: if they differ, report `RootRefMismatch` with "name mismatch".

Each field is checked independently, so a single pair can produce up to 3
mismatch errors.

#### Reverse check: every ROOT_BACKREF has a matching ROOT_REF

For each `(child, parent)` pair in the back refs map:
- If no entry exists in the forward refs map, report `RootRefMissing`.

Field comparison is not repeated in this direction because the forward check
already caught any field mismatches for pairs that exist in both maps.

### Error variants produced

- **`RootRefMissing { child, parent }`** -- a `ROOT_BACKREF` exists for this
  child/parent pair but no corresponding `ROOT_REF` was found.
- **`RootBackrefMissing { child, parent }`** -- a `ROOT_REF` exists for this
  child/parent pair but no corresponding `ROOT_BACKREF` was found.
- **`RootRefMismatch { child, parent, detail }`** -- both `ROOT_REF` and
  `ROOT_BACKREF` exist but one of their fields (dirid, sequence, or name)
  differs. The `detail` string describes which field mismatched and shows both
  values.
- **`ReadError { logical, detail }`** -- I/O error reading the root tree.

## Complete error type reference

All error variants are defined in `cli/src/check/errors.rs` as the `CheckError`
enum. Each variant implements `Display` for human-readable error messages.

### Phase 1 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `SuperblockInvalid` | `mirror: u32`, `detail: String` | Superblock mirror failed validation (bad magic, bad checksum, or read error) |

### Phase 2 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `TreeBlockChecksumMismatch` | `tree: &'static str`, `logical: u64` | CRC32C checksum does not match |
| `TreeBlockBadFsid` | `tree: &'static str`, `logical: u64` | Header fsid does not match filesystem |
| `TreeBlockBadBytenr` | `tree: &'static str`, `logical: u64`, `header_bytenr: u64` | Header bytenr disagrees with read address |
| `TreeBlockBadGeneration` | `tree: &'static str`, `logical: u64`, `block_gen: u64`, `super_gen: u64` | Block generation exceeds superblock generation |
| `TreeBlockBadLevel` | `tree: &'static str`, `logical: u64`, `detail: String` | Level/type mismatch (leaf with level>0 or node with level==0) |
| `KeyOrderViolation` | `tree: &'static str`, `logical: u64`, `index: usize` | Key at index is not strictly greater than previous key |

### Phase 3 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `ExtentRefMismatch` | `bytenr: u64`, `expected: u64`, `found: u64` | Declared refs != counted refs (inline + standalone) |
| `MissingExtentItem` | `bytenr: u64` | Tree block has no extent/metadata item in extent tree |
| `BackrefOwnerMismatch` | `bytenr: u64`, `actual_owner: u64`, `claimed_owners: Vec<u64>` | Actual tree block owner not in extent tree's backref list |
| `BackrefOrphan` | `bytenr: u64`, `claimed_owner: u64` | Extent tree claims a backref but no tree block found |
| `OverlappingExtent` | `bytenr: u64`, `length: u64`, `prev_end: u64` | Data extent overlaps with previous extent |

### Phase 4 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `ChunkMissingBlockGroup` | `logical: u64` | Chunk has no matching block group item |
| `BlockGroupMissingChunk` | `logical: u64` | Block group has no matching chunk |
| `DeviceExtentOverlap` | `devid: u64`, `offset: u64` | Two device extents overlap on the same device |

### Phase 5 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `InodeMissing` | `tree: u64`, `ino: u64` | Inode referenced but has no INODE_ITEM |
| `NlinkMismatch` | `tree: u64`, `ino: u64`, `expected: u32`, `found: u32` | Stored nlink differs from counted references |
| `FileExtentOverlap` | `tree: u64`, `ino: u64`, `offset: u64` | File extent items overlap in file offset space |
| `DirItemOrphan` | `tree: u64`, `parent_ino: u64`, `name: String` | Dir entry references non-existent inode |
| `DirSizeWrong` | `tree: u64`, `ino: u64`, `expected: u64`, `found: u64` | Directory inode size does not match DIR_INDEX name sum |
| `NbytesWrong` | `tree: u64`, `ino: u64`, `expected: u64`, `found: u64` | File inode nbytes does not match extent sum |

### Phase 6 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `CsumMismatch` | `logical: u64` | Data checksum does not match stored value |

### Phase 7 errors

| Variant | Fields | Description |
|---------|--------|-------------|
| `RootRefMissing` | `child: u64`, `parent: u64` | ROOT_BACKREF exists but no matching ROOT_REF |
| `RootBackrefMissing` | `child: u64`, `parent: u64` | ROOT_REF exists but no matching ROOT_BACKREF |
| `RootRefMismatch` | `child: u64`, `parent: u64`, `detail: String` | ROOT_REF and ROOT_BACKREF fields disagree |

### Cross-phase error

| Variant | Fields | Description |
|---------|--------|-------------|
| `ReadError` | `logical: u64`, `detail: String` | I/O error reading any tree block (used in phases 2-7) |

## Summary output

After all phases complete, `CheckResults::print_summary` writes to stdout:

```
found <bytes_used> bytes used, <error_count> error(s) found
total csum bytes: <total_csum_bytes>
total tree bytes: <total_tree_bytes>
total fs tree bytes: <total_fs_tree_bytes>
total extent tree bytes: <total_extent_tree_bytes>
btree space waste bytes: <btree_space_waste>
file data blocks allocated: <data_bytes_allocated>
 referenced <data_bytes_referenced>
```

If `error_count > 0`, the process exits with code 1.

## Limitations and future work

The following checks from the C reference implementation are not yet implemented:

- **`--mode lowmem`** differentiation (the current implementation uses the
  "original" mode approach of collecting all items then cross-checking).
- **Log tree checking** (the log tree is not walked in phase 2).
- **`--repair`** (all checking is read-only).
- **`--backup`** / `--tree-root` / `--chunk-root`** (alternate root selection).
- **`--init-csum-tree`** / `--init-extent-tree`** (destructive reconstruction).
- **`--qgroup-report`** (quota group consistency checking).
- **`--subvol-extents`** (per-subvolume extent sharing analysis).
- **Superblock generation cross-checking** between mirror copies.
- **Block group used-bytes verification** (comparing declared `used` in block
  group items against actual allocated extents).

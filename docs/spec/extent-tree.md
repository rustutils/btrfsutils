# Extent Tree and Backrefs

This document describes the btrfs extent tree: how every allocated byte on
disk is tracked, how reference counting works, and how backreferences link
extents to the trees and files that use them.

All multi-byte integers in btrfs on-disk structures are little-endian.


## Purpose

The extent tree is the central allocator of a btrfs filesystem. It records
every contiguous range of allocated disk space (data extents used by files
and metadata blocks used by trees) and tracks who references each extent.

It serves three purposes:

1. **Allocation tracking.** The set of extent items defines which logical
   byte ranges are in use. The free-space tree (or free-space cache) is
   derived from the gaps between extent items.

2. **Reference counting.** Each extent has a declared reference count.
   Snapshots and clones share extents by incrementing this count instead
   of copying data. When the count drops to zero, the extent can be freed.

3. **Backreferences.** Each extent stores references back to the trees,
   inodes, and file offsets that use it. This lets the filesystem find all
   users of an extent (for example, to relocate it during balance) and
   verify consistency (during `btrfs check`).

The extent tree is tree objectid 2 (`BTRFS_EXTENT_TREE_OBJECTID`). Its root
pointer is stored in the root tree as a `ROOT_ITEM`.


## EXTENT_ITEM vs METADATA_ITEM

Two key types record allocated extents.

### EXTENT_ITEM (type 168)

The original extent item format, used for both data and metadata extents.

```
Key: (bytenr, EXTENT_ITEM, length)
      objectid = logical start    type = 168    offset = size in bytes
```

For data extents, `length` is the extent's size on disk. For metadata
extents (tree blocks), `length` equals the filesystem's `nodesize`.

### METADATA_ITEM (type 169): Skinny Metadata

When the `SKINNY_METADATA` incompat feature is enabled (on by default in
this project's mkfs), metadata extents use a more compact key:

```
Key: (bytenr, METADATA_ITEM, level)
      objectid = logical start    type = 169    offset = tree level (0..7)
```

The extent's length is implicitly `nodesize` and is not stored in the key.
The key offset holds the B-tree level of the tree block, which allows
verification without reading the block itself.

Skinny metadata items are called "skinny refs" because they drop the
`btrfs_tree_block_info` structure that non-skinny `EXTENT_ITEM` entries for
tree blocks carry.

### Key Differences

| Aspect            | EXTENT_ITEM (non-skinny)      | METADATA_ITEM (skinny)      |
|-------------------|-------------------------------|-----------------------------|
| Key type          | 168                           | 169                         |
| Key offset        | nodesize                      | tree level (0..7)           |
| Item body         | extent_item + tree_block_info + inline refs | extent_item + inline refs |
| When used         | Always for data, and for metadata without skinny_metadata | Metadata only, with skinny_metadata |

The `SKINNY_METADATA` incompat flag controls the choice, both in the mkfs
bootstrap (`build_extent_tree` in `mkfs/src/mkfs.rs`) and in the
transaction crate (`create_metadata_extent` in
`transaction/src/transaction.rs`):

```rust
let (item_type, offset) = if skinny {
    (BTRFS_METADATA_ITEM_KEY, u64::from(level))
} else {
    (BTRFS_EXTENT_ITEM_KEY, u64::from(nodesize))
};
```


## The Extent Item Header

`EXTENT_ITEM` and `METADATA_ITEM` share the same header, `btrfs_extent_item`
(24 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `refs` | 0 | 8 | Total reference count |
| `generation` | 8 | 8 | Generation when allocated |
| `flags` | 16 | 8 | Extent type flags |

### Extent Flags

The `flags` field uses these bits:

| Flag           | Value | Meaning                                    |
|----------------|-------|--------------------------------------------|
| DATA           | 0x01  | Extent holds file data                     |
| TREE_BLOCK     | 0x02  | Extent holds a metadata tree block         |
| FULL_BACKREF   | 0x100 | Uses shared (parent-based) backrefs only   |

A data extent has `flags = DATA` (0x01). A metadata extent has
`flags = TREE_BLOCK` (0x02). `FULL_BACKREF` is set when the extent uses
shared backreferences (after a snapshot) instead of normal tree
backreferences.

The `ExtentFlags` type in `disk/src/items.rs` represents these flags as a
`bitflags` struct.

### Tree Block Info (Non-Skinny Only)

For non-skinny `EXTENT_ITEM` entries with the `TREE_BLOCK` flag, the header
is followed by `btrfs_tree_block_info` (18 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `key` | 0 | 17 | First key in the tree block (`btrfs_disk_key`) |
| `level` | 17 | 1 | B-tree level of the block |

`METADATA_ITEM` (skinny metadata) omits this structure, because the level is
stored in the key offset and the first key is not needed.

### Full Item Layout

For a skinny metadata extent item with one inline TREE_BLOCK_REF:

| Byte offset | Size | Content |
|-------------|------|---------|
| 0 | 8 | `refs` (u64_le) |
| 8 | 8 | `generation` (u64_le) |
| 16 | 8 | `flags` = `TREE_BLOCK` (u64_le) |
| 24 | 1 | inline ref type = `TREE_BLOCK_REF_KEY` (176) |
| 25 | 8 | root objectid (u64_le) |
| | | Total: 33 bytes |

For a data extent item with one inline EXTENT_DATA_REF:

| Byte offset | Size | Content |
|-------------|------|---------|
| 0 | 8 | `refs` (u64_le) |
| 8 | 8 | `generation` (u64_le) |
| 16 | 8 | `flags` = `DATA` (u64_le) |
| 24 | 1 | inline ref type = `EXTENT_DATA_REF_KEY` (178) |
| 25 | 8 | `root` (u64_le) |
| 33 | 8 | `objectid` (u64_le) -- inode number |
| 41 | 8 | `offset` (u64_le) -- file offset |
| 49 | 4 | `count` (u32_le) |
| | | Total: 53 bytes |


## Inline Backrefs

After the extent item header (and tree_block_info for non-skinny metadata),
zero or more inline backreferences are packed contiguously. Each inline ref
starts with a 1-byte type code followed by type-specific data.

Inline refs are the common case. They live inside the extent item and avoid
the overhead of separate B-tree items. When an extent item would grow too
large to fit in a leaf (because of many references), backrefs are stored as
standalone items instead.

### TREE_BLOCK_REF (type 176)

Direct backref from a metadata extent to the tree that owns it.

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `type` | 0 | 1 | 176 (`BTRFS_TREE_BLOCK_REF_KEY`) |
| root objectid | 1 | 8 | u64_le |

The `root` field identifies the tree that owns this metadata block. For
example, root = 5 is the FS tree and root = 2 is the extent tree itself.

Total size: 9 bytes.

### SHARED_BLOCK_REF (type 182)

Shared backref from a metadata extent to a parent tree block. It is used
when a tree block is shared between snapshots. The backref points to a
parent node instead of a root.

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `type` | 0 | 1 | 182 (`BTRFS_SHARED_BLOCK_REF_KEY`) |
| parent bytenr | 1 | 8 | u64_le |

The `parent` field is the logical byte address of the tree node that
contains a pointer to this extent.

Total size: 9 bytes.

### EXTENT_DATA_REF (type 178)

Backref from a data extent to a specific file inode. This is the most common
inline ref type for data extents.

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `type` | 0 | 1 | 178 (`BTRFS_EXTENT_DATA_REF_KEY`) |
| `root` | 1 | 8 | Tree objectid owning the inode (u64_le) |
| `objectid` | 9 | 8 | Inode number (u64_le) |
| `offset` | 17 | 8 | File byte offset (u64_le) |
| `count` | 25 | 4 | Number of references (u32_le) |

Unlike other inline ref types, `EXTENT_DATA_REF` does **not** have an 8-byte
offset field between the type byte and the struct body. The struct starts
right after the type byte. The parser in `disk/src/items.rs` handles this by
reinterpreting the speculatively read offset bytes as the `root` field:

```rust
raw::BTRFS_EXTENT_DATA_REF_KEY => {
    let root = ref_offset; // already read as u64_le
    let oid = buf.get_u64_le();
    let off = buf.get_u64_le();
    let count = buf.get_u32_le();
    // ...
}
```

The `count` field is the number of times this `(root, objectid, offset)`
triple references the extent. For a normal file with one reference,
count = 1. For a file cloned with `reflink`, each clone adds a new
`EXTENT_DATA_REF` with its own triple and count.

Total size: 29 bytes.

### SHARED_DATA_REF (type 184)

Shared data backref, used when data extents are shared between snapshots.

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `type` | 0 | 1 | 184 (`BTRFS_SHARED_DATA_REF_KEY`) |
| parent bytenr | 1 | 8 | u64_le |
| `count` | 9 | 4 | u32_le |

Total size: 13 bytes.

### EXTENT_OWNER_REF (type 172)

Simple ownership reference, used with the `simple_quota` feature. It records
which tree root owns the extent, without full backref details.

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `type` | 0 | 1 | 172 (`BTRFS_EXTENT_OWNER_REF_KEY`) |
| root objectid | 1 | 8 | u64_le |

Total size: 9 bytes.


## Standalone Backrefs

When inline backrefs do not fit inside the extent item (because the item
would exceed the available leaf space), they are stored as separate items in
the extent tree. Standalone backrefs use the same type codes as inline refs
but are encoded as independent key-value pairs.

### Standalone TREE_BLOCK_REF

```
Key: (bytenr, TREE_BLOCK_REF, root_objectid)
      objectid = extent start    type = 176    offset = owning tree
```

Item payload: empty (zero bytes). All backref information is in the key.

### Standalone SHARED_BLOCK_REF

```
Key: (bytenr, SHARED_BLOCK_REF, parent_bytenr)
      objectid = extent start    type = 182    offset = parent block
```

Item payload: empty.

### Standalone EXTENT_DATA_REF

```
Key: (bytenr, EXTENT_DATA_REF, hash)
      objectid = extent start    type = 178    offset = CRC32C hash
```

The key offset is a hash of `(root, objectid, offset)` computed by:

```rust
fn extent_data_ref_hash(root: u64, objectid: u64, offset: u64) -> u64 {
    let high_crc = raw_crc32c(!0u32, &root.to_le_bytes());
    let low_crc = raw_crc32c(!0u32, &objectid.to_le_bytes());
    let low_crc = raw_crc32c(low_crc, &offset.to_le_bytes());
    (u64::from(high_crc) << 31) ^ u64::from(low_crc)
}
```

This hash uses raw CRC32C (seed = `!0`, that is `0xFFFFFFFF`, without final
complement). It is applied separately to the root (high part) and to the
objectid and offset (low part), and the two are combined with a shift and
XOR.

Item payload (28 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `root` | 0 | 8 | u64_le |
| `objectid` | 8 | 8 | u64_le |
| `offset` | 16 | 8 | u64_le |
| `count` | 24 | 4 | u32_le |

### Standalone SHARED_DATA_REF

```
Key: (bytenr, SHARED_DATA_REF, parent_bytenr)
      objectid = extent start    type = 184    offset = parent block
```

Item payload (4 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `count` | 0 | 4 | u32_le |


## Reference Counting

### The refs Field

The `refs` field in `btrfs_extent_item` is the declared total reference count
for the extent. It equals the sum of all references from inline and
standalone backrefs.

Each `TREE_BLOCK_REF`, `SHARED_BLOCK_REF`, and `EXTENT_OWNER_REF` contributes
1 to the total. Each `EXTENT_DATA_REF` and `SHARED_DATA_REF` contributes its
`count` field.

### Counting Rules

The total reference count is computed as:

```
total = 0
for each inline ref:
    if EXTENT_DATA_REF:  total += count
    if SHARED_DATA_REF:  total += count
    otherwise:           total += 1
for each standalone ref:
    if EXTENT_DATA_REF:  total += count  (from item payload)
    if SHARED_DATA_REF:  total += count  (from item payload)
    otherwise:           total += 1
```

The declared `refs` in the extent item header must equal this total. A
mismatch indicates corruption.

### Example: Simple File

A newly created file with one 4 KiB extent in the FS tree (root 5):

```
Key: (bytenr, EXTENT_ITEM, 4096)
  refs = 1
  generation = 100
  flags = DATA
  inline EXTENT_DATA_REF:
    root = 5, objectid = 257, offset = 0, count = 1
```

Total refs: count(1) = 1. Matches declared refs.

### Example: Snapshot

After a snapshot of the FS tree, both the original and the snapshot
reference the same extent. The extent item becomes:

```
Key: (bytenr, EXTENT_ITEM, 4096)
  refs = 2
  generation = 100
  flags = DATA
  inline EXTENT_DATA_REF:
    root = 5, objectid = 257, offset = 0, count = 1
  inline EXTENT_DATA_REF:
    root = 260, objectid = 257, offset = 0, count = 1
```

Total refs: count(1) + count(1) = 2. Matches declared refs.

### Example: Reflink Clone

A reflink clone within the same tree adds another backref with a
different file offset:

```
Key: (bytenr, EXTENT_ITEM, 4096)
  refs = 2
  generation = 100
  flags = DATA
  inline EXTENT_DATA_REF:
    root = 5, objectid = 257, offset = 0, count = 1
  inline EXTENT_DATA_REF:
    root = 5, objectid = 258, offset = 0, count = 1
```

### Example: Metadata Block

A metadata block owned by the FS tree:

```
Key: (bytenr, METADATA_ITEM, 0)    // level 0 = leaf
  refs = 1
  generation = 100
  flags = TREE_BLOCK
  inline TREE_BLOCK_REF:
    root = 5
```


## Data Extent Backrefs in Detail

### The EXTENT_DATA_REF Triple

Each data extent backref identifies its user by a `(root, objectid, offset)`
triple:

- **root**: the objectid of the tree containing the referencing inode. For
  user files this is the FS tree (5) or a subvolume or snapshot tree ID.

- **objectid**: the inode number of the file that references the extent.
  Regular file inodes start at 257 (`BTRFS_FIRST_FREE_OBJECTID + 1`).

- **offset**: the byte offset within the file where this extent is
  referenced. This is the key offset of the `EXTENT_DATA` item in the
  FS tree.

### The count Field

The `count` field records how many times the exact same
`(root, objectid, offset)` triple references this extent. Normally
count = 1. It can be greater than 1 in some cases involving log replay or
certain reflink patterns.

### Hash Computation for Standalone Keys

When an `EXTENT_DATA_REF` is stored as a standalone item, the key offset is
not the file offset but a hash of the full triple. This lets several data
refs with different triples be stored as separate items under the same
extent bytenr.

The hash function in `disk/src/items.rs` computes:

```
high = CRC32C(seed=0xFFFFFFFF, root_le_bytes)
low  = CRC32C(seed=0xFFFFFFFF, objectid_le_bytes)
low  = CRC32C(seed=low,        offset_le_bytes)
hash = (high << 31) ^ low
```

This produces a 63-bit hash (the top bit is the MSB of the high CRC, shifted
to bit 62). The hash is deterministic, and the kernel and userspace tools use
the same function.


## Metadata Extent Backrefs in Detail

### TREE_BLOCK_REF

A `TREE_BLOCK_REF` links a metadata block to the tree that owns it.
The `root` field is the tree's objectid:

- 1 = root tree
- 2 = extent tree
- 3 = chunk tree
- 4 = device tree
- 5 = FS tree (default subvolume)
- 7 = csum tree
- 8 = quota tree
- 9 = UUID tree
- 10 = free-space tree
- 11 = block group tree
- >= 256 = subvolume and snapshot trees

### SHARED_BLOCK_REF

When a tree block is shared between a subvolume and its snapshot, the normal
`TREE_BLOCK_REF` is replaced with a `SHARED_BLOCK_REF` that points to the
parent node. This is because one physical block cannot be "owned" by two
trees at once.

The `parent` field is the logical bytenr of the tree node whose key pointer
array includes this block. To modify a shared block, the filesystem performs
copy-on-write: it allocates a new block, copies the data, and updates the
parent's pointer. This is why snapshot creation takes constant time: the
snapshot shares all blocks with the source subvolume.

### FULL_BACKREF Flag

The `FULL_BACKREF` flag in the extent item's flags field means this metadata
extent uses only shared backrefs (no direct tree backrefs). This typically
happens for tree blocks at levels > 0 after a snapshot, where ownership is
ambiguous until the block is CoW'd.

### Cross-Referencing with Tree Ownership

`btrfs check` builds a map of `(block_address -> owning_tree)` during its
tree walks. The owning tree for each block is the objectid of the tree whose
walk reached it, not the `owner` field in the block header
(`cli/src/check/tree_structure.rs`). The map is then cross-referenced against
the extent tree's `TREE_BLOCK_REF` entries in both directions.


## Block Group Items in the Extent Tree

Historically, `BLOCK_GROUP_ITEM` entries are stored in the extent tree
alongside extent items. With the `BLOCK_GROUP_TREE` compat_ro feature
(on by default in this project's mkfs), they move to a separate tree
(objectid 11).

### BLOCK_GROUP_ITEM Structure

```
Key: (logical_offset, BLOCK_GROUP_ITEM, length)
      objectid = group start    type = 192    offset = group size
```

Item payload (24 bytes):

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `used` | 0 | 8 | Bytes allocated within this block group |
| `chunk_objectid` | 8 | 8 | `FIRST_CHUNK_TREE_OBJECTID` (256) |
| `flags` | 16 | 8 | Type + profile flags (`DATA|DUP`, `METADATA|RAID1`, etc.) |

The `used` field is the number of bytes in the block group currently
allocated to extents. For a new filesystem:
- System block group: `used` = one nodesize (the chunk tree block)
- Metadata block group: `used` = N * nodesize (all non-chunk tree blocks)
- Data block group: `used` = 0 (no file data yet)

### Ordering in the Extent Tree

When block group items are in the extent tree, they sort among the extent
items by key. `BLOCK_GROUP_ITEM` has type 192, while `EXTENT_ITEM` has type
168 and `METADATA_ITEM` has type 169. Key comparison is
`(objectid, type, offset)` and 192 > 169, so a block group item sorts *after*
any extent item at the same address.

### mkfs Construction

The mkfs bootstrap (`build_extent_tree` in `mkfs/src/mkfs.rs`) writes
three block group items into the extent tree, one for each chunk: system
(SYSTEM flag), metadata (METADATA and the profile flag), and data (DATA and
the profile flag). At this stage the metadata block group's `used` covers
only the four bootstrap tree blocks.

When the `BLOCK_GROUP_TREE` feature is enabled, the post-bootstrap
transaction (`mkfs/src/post_bootstrap.rs`) creates the block group tree and
moves the items there with `convert::create_block_group_tree`. Later
allocations in that transaction update `used` through the transaction
crate.


## What btrfs check Verifies

The extent tree checker in `cli/src/check/extents.rs` performs several kinds
of verification.

### Reference Count Matching

For each extent item (EXTENT_ITEM or METADATA_ITEM) and its standalone
backrefs, the checker computes the total reference count from inline and
standalone refs and compares it to the declared `refs` field:

```rust
if state.pending_refs != state.pending_counted {
    results.report(CheckError::ExtentRefMismatch {
        bytenr, expected: state.pending_refs, found: state.pending_counted,
    });
}
```

The checker processes items in key order. On each new EXTENT_ITEM or
METADATA_ITEM, it "flushes" the previous extent (checking its ref count) and
starts accumulating refs for the new one. Standalone backref items
(TREE_BLOCK_REF, SHARED_BLOCK_REF, EXTENT_DATA_REF, SHARED_DATA_REF,
EXTENT_OWNER_REF) that follow an extent item with a matching objectid add to
the running count.

### Extent Overlap Detection

Extents in the extent tree are sorted by logical address. The checker tracks
the end address of the previous extent and reports an error if the next one
starts before it ends:

```rust
if length > 0 && bytenr < state.prev_end && state.prev_end > 0 {
    results.report(CheckError::OverlappingExtent {
        bytenr, length, prev_end: state.prev_end,
    });
}
```

METADATA_ITEM entries store the tree level, not the length, in the key
offset. The checker has no access to the nodesize at this point, so it uses
length = 0 for metadata items and skips overlap detection for them.

### Backref Owner Cross-Checks (Direction 1: Walk to Extent)

During tree walks in earlier check phases, the checker builds
`tree_block_owners: HashMap<u64, u64>`, which maps each tree block's logical
address to the objectid of the tree whose walk reached it. The block
header's `owner` field is not used.

After processing the extent tree, the checker verifies that every block seen
during the walks has an extent item:

```rust
if !state.extent_item_addrs.contains(&addr) {
    results.report(CheckError::MissingExtentItem { bytenr: addr });
}
```

It also verifies that the extent tree's backrefs agree with the actual owner:

```rust
if !claimed_owners.contains(&actual_owner) {
    results.report(CheckError::BackrefOwnerMismatch {
        bytenr: addr, actual_owner, claimed_owners,
    });
}
```

### Backref Owner Cross-Checks (Direction 2: Extent to Walk)

The checker also verifies the reverse. Every `TREE_BLOCK_REF` in the extent
tree (inline and standalone) must correspond to a tree block that was seen
during the walks and is owned by the claimed tree:

```rust
let actual = tree_block_owners.get(&addr).copied();
if actual != Some(claimed) {
    results.report(CheckError::BackrefOrphan {
        bytenr: addr, claimed_owner: claimed,
    });
}
```

This catches "orphan" backrefs that point to blocks that either do not exist
or are owned by a different tree than claimed.

### Data Byte Accounting

The checker accumulates two statistics from data extents:

- **data_bytes_allocated**: the sum of `length` over all data extent items.
  This is the total physical space reserved for data.

- **data_bytes_referenced**: the sum of `length * count` over all data
  extent references. When data is shared (through snapshots or reflinks),
  referenced bytes exceed allocated bytes.

For inline-only data refs (no standalone ExtentDataRef items), referenced
bytes are computed from the inline ref count. For standalone refs, each
`EXTENT_DATA_REF` and `SHARED_DATA_REF` item contributes `length * count`.


## Extent Item Construction in mkfs

mkfs builds extent items in two places. The bootstrap writes the items for
its own four tree blocks by hand. Everything after that goes through the
transaction crate.

### Bootstrap Metadata Extent Items

The bootstrap (`build_extent_tree` in `mkfs/src/mkfs.rs`) writes one
metadata extent item for each of the four bootstrap trees (root, extent,
chunk, device), each with one inline `TREE_BLOCK_REF`. The payload comes
from `items::extent_item` in `mkfs/src/items.rs`:

1. `btrfs_extent_item` header: refs=1, generation, flags=TREE_BLOCK
2. For non-skinny: zero-filled `btrfs_tree_block_info` (18 bytes)
3. Inline `TREE_BLOCK_REF`: type byte (176) and root objectid (8 bytes)

Total item size: 33 bytes (skinny) or 51 bytes (non-skinny).

### Transaction Metadata Extent Items

Every other tree block (the FS, csum, data-reloc, and UUID trees, the
optional free space, block group, and quota trees, and every block
allocated while populating `--rootdir`) is allocated by the transaction
crate. Allocation queues a delayed ref. At commit, `flush_delayed_refs`
calls `create_metadata_extent`, which writes the same layout through
`ExtentItem::to_bytes_skinny` or `ExtentItem::to_bytes_non_skinny`. The
non-skinny form fills `tree_block_info` with the block's first key and
level instead of zeros.

### Data Extent Items

In `--rootdir` mode, `rootdir::walk_to_transaction` writes file contents
with `Transaction::write_file_data`. Each regular extent queues a data ref,
and `flush_delayed_refs` calls `create_data_extent`. That writes an
`EXTENT_ITEM` with one inline `EXTENT_DATA_REF`, serialized by
`ExtentItem::to_bytes_data` in `disk/src/items.rs`:

```
btrfs_extent_item   refs, generation, flags = DATA         24 bytes
inline ref type     EXTENT_DATA_REF_KEY (178)               1 byte
extent_data_ref     root, objectid, offset, count          28 bytes
```

Total item size: 53 bytes (`ExtentItem::DATA_INLINE_SIZE`). The key is
`(extent_bytenr, EXTENT_ITEM, extent_length)`. Inline extents get no extent
item.

### Self-Referential Convergence

The extent tree must contain entries for its own tree blocks. Inserting an
extent item can COW or split an extent tree block, which allocates a new
block, which needs another extent item.

The transaction crate resolves this in its commit loop
(`Transaction::commit`): it runs `flush_delayed_refs`, `update_root_items`,
`snapshot_roots`, and `update_free_space_tree` repeatedly until no delayed
refs remain, no root changed, and the free space tree is stable. It gives
up with an error after 32 passes. The mkfs bootstrap has no such loop. Its
extent tree fits in one leaf, and every block it describes is placed at a
fixed address by `BlockLayout`.


## Extent Tree Key Ordering

Items in the extent tree are sorted by the standard btrfs key comparison
`(objectid, type, offset)`. The `objectid` is the extent's logical byte
address, so items are effectively sorted by logical address.

Within a single extent's address, the ordering is:

1. `EXTENT_ITEM` or `METADATA_ITEM` (type 168 or 169): the extent header
2. `EXTENT_OWNER_REF` (type 172): if simple quotas are enabled
3. `TREE_BLOCK_REF` (type 176): standalone metadata backrefs
4. `EXTENT_DATA_REF` (type 178): standalone data backrefs
5. `SHARED_BLOCK_REF` (type 182): standalone shared metadata backrefs
6. `SHARED_DATA_REF` (type 184): standalone shared data backrefs
7. `BLOCK_GROUP_ITEM` (type 192): if the block-group tree is not in use

This order follows from the type field values. It lets `btrfs check` process
all backrefs for an extent by reading items sequentially until the objectid
(bytenr) changes.


## Relationship to File Extents

The extent tree connects to file data through `EXTENT_DATA` items in FS
trees:

```
FS tree: (inode, EXTENT_DATA, file_offset)
  -> disk_bytenr, disk_num_bytes, offset, num_bytes

Extent tree: (disk_bytenr, EXTENT_ITEM, disk_num_bytes)
  -> refs, generation, flags=DATA
  -> inline EXTENT_DATA_REF(root, inode, file_offset, count)
```

The `disk_bytenr` in the file extent item is the logical address of the
data extent. The extent tree entry at that address records who references
the extent and how many times.

Inline file extents (small files whose data is embedded in the tree leaf)
have no extent tree entry, because the data does not occupy a separate
extent.

Hole (sparse) extents (`disk_bytenr = 0`) also have no extent tree entry.
The `no-holes` feature removes explicit hole extent items entirely.


## Summary of Key Formats

| Item type | Key | Payload |
|-----------|-----|---------|
| `EXTENT_ITEM` | `(bytenr, 168, length)` | `extent_item` + inline refs |
| `METADATA_ITEM` | `(bytenr, 169, level)` | `extent_item` + inline refs |
| `EXTENT_OWNER_REF` | `(bytenr, 172, root)` | (empty) |
| `TREE_BLOCK_REF` | `(bytenr, 176, root)` | (empty) |
| `EXTENT_DATA_REF` | `(bytenr, 178, hash)` | `extent_data_ref` (28 bytes) |
| `SHARED_BLOCK_REF` | `(bytenr, 182, parent)` | (empty) |
| `SHARED_DATA_REF` | `(bytenr, 184, parent)` | `shared_data_ref` (4 bytes) |
| `BLOCK_GROUP_ITEM` | `(logical, 192, length)` | `block_group_item` (24 bytes) |

All `bytenr` values are logical byte addresses. The extent tree gives the
complete picture of space allocation and ownership across the filesystem.

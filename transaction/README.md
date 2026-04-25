# btrfs-transaction

Atomic, COW-based modification of btrfs filesystem images.

> This is a pre-1.0, experimental crate. It is a clean-room
> reimplementation of btrfs's read-write tree machinery and may
> have edge cases that testing doesn't cover. Do not use it on
> filesystems you care about without taking a backup first.

This crate sits on top of `btrfs-disk` and provides the write-side
machinery that the parsing crate deliberately doesn't: copy-on-write
of tree blocks, leaf insert/delete/split/balance, a delayed reference
queue, free space tree maintenance, and a `Transaction` type that
groups all of those into an atomic commit. It works on raw image
files or block devices and does not call any ioctls — it only needs
read/write/seek access to the filesystem bytes.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

### Tree-block storage and COW

- **`Filesystem`**: open an image, resolve tree roots, read tree
  blocks via the chunk tree, write blocks back to disk on all
  mirrors with correct CRC32C checksums.
- **`ExtentBuffer`**: in-memory tree block with leaf/node helpers
  for items, key/data offsets, and slot manipulation. Tracks dirty
  state and pending writes.
- **`cow_block`**: copy-on-write a tree block, allocating a new
  bytenr, clearing the WRITTEN/RELOC flags, and queuing add/drop
  delayed refs against the old and new addresses.

### B-tree operations

- **`search_slot`**: search for a key in any tree, with optional
  COW of the path. Supports `ReadOnly`, `Insert(size)`, and
  `Delete` intents; the latter two pre-COW and pre-balance the
  path so the caller can mutate the leaf in place.
- **`next_leaf`**: cursor advance across leaf boundaries.
- **`items::insert_item` / `del_items` / `update_item` / `shrink_item`**:
  in-place leaf payload manipulation that maintains the descending
  data offset invariant.
- **Split / balance**: leaf and node split when an insert won't fit;
  push-left/push-right/merge to keep nodes within fill thresholds
  on delete.
- **`BtrfsPath`**: per-level slot+buffer trace from the root to a
  target leaf, owned and released explicitly so the caller controls
  borrow scope.

### Allocation

- **Block group scan**: load every `BLOCK_GROUP_ITEM` from the
  block group tree (id 11) when present, otherwise from the
  extent tree (id 2).
- **Per-kind bump allocator**: `Transaction::alloc_block(kind)`
  with separate cursors for `BlockGroupKind::Metadata`,
  `BlockGroupKind::System`, and `BlockGroupKind::Data`. Cursors
  are seeded by scanning the extent tree for free gaps inside
  block groups of the requested kind. Pinned blocks (freed
  earlier in the same transaction) are skipped to preserve
  crash consistency.
- **`alloc_tree_block(tree_id, level)`**: routes the chunk tree
  (id 3) to a SYSTEM block group and every other tree to a
  metadata block group, registers a delayed `add_ref`, and for
  SYSTEM allocations updates the superblock's `sys_chunk_array`
  bootstrap snippet so the next mount can resolve the new chunk
  root.

### Delayed reference queue

- **`DelayedRefQueue`**: batches reference count changes keyed by
  `DelayedRefKey::Metadata { bytenr, owner_root, level }` or
  `DelayedRefKey::Data { bytenr, owner_root, owner_ino, owner_offset }`.
  Add/drop pairs to the same key cancel.
- **Metadata flush**: creates `METADATA_ITEM` (skinny) or
  `EXTENT_ITEM` records with `TREE_BLOCK_REF` inline backrefs on
  positive deltas, deletes them on negative deltas, and threads
  byte deltas into per-block-group `bytes_used` accounting.
- **Data flush** (drop side): locates the matching
  `EXTENT_DATA_REF` either inline inside the parent `EXTENT_ITEM`
  or as a standalone item (with hash-collision walk forward),
  decrements both the inline `count` field and the parent
  `EXTENT_ITEM.refs`, and on hitting zero deletes the entire
  `EXTENT_ITEM` and trims overlapping csum tree entries.
- **Csum tree maintenance**: when a data extent is fully freed,
  walks the csum tree once to collect every overlapping
  `EXTENT_CSUM` item, then in a second pass deletes whole-coverage
  items and re-inserts trimmed head/tail fragments under new keys.
- **Data flush** (add side): on a positive data delta, inserts a
  `EXTENT_ITEM` (24-byte header + inline 29-byte `EXTENT_DATA_REF`
  = 53 bytes) and updates the per-block-group `bytes_used`
  delta plus the FST's allocated-range record.

### Data extent write path

- **`Transaction::alloc_data_extent(data, root, ino, file_offset)`**:
  finds free space in a `BlockGroupKind::Data` block group with
  sectorsize alignment, zero-pads `data` to sectorsize, writes it
  via `BlockReader::write_block` (fanning out to every stripe
  copy), queues a `+1` `EXTENT_DATA_REF` delayed ref, and returns
  the allocated logical address.
- **`Transaction::insert_file_extent(tree, ino, offset, payload)`**:
  inserts an `EXTENT_DATA` item at `(ino, EXTENT_DATA, offset)`.
  `payload` is a serialized `FileExtentItem` (use
  `to_bytes_regular` for non-inline extents or `to_bytes_inline`
  for inline-tail extents).
- **`Transaction::insert_csums(logical, on_disk_data)`**: computes
  per-sector standard CRC32C of the on-disk bytes and inserts
  `EXTENT_CSUM` items into the csum tree, splitting payloads that
  would exceed leaf capacity into multiple keyed items. Errors on
  non-CRC32C filesystems.
- **`Transaction::update_inode_nbytes(tree, ino, delta)`**: signed
  in-place patch of the inode's `nbytes` field at the fixed struct
  offset. Preserves all other fields (including `flags`, `rdev`,
  `sequence`) that round-tripping via `InodeItemArgs` would lose.
- **`Transaction::insert_inline_extent(tree, ino, file_offset,
  data)`**: embeds `data` directly in the FS tree leaf as an inline
  `EXTENT_DATA` item. No extent-tree entry, no csum entries.
  `INODE.nbytes` is bumped by the unaligned payload length.
- **`Transaction::write_file_data(tree, ino, file_offset, data,
  nodatasum, compression)`**: high-level helper that picks inline
  vs regular based on `data.len()` and the per-filesystem inline
  threshold (`max_inline_data_size`). For regular extents, splits
  `data` into ≤1 MiB chunks, optionally compresses each chunk
  (zlib, zstd, or LZO; per-chunk fallback to raw when compression
  doesn't shrink), allocates each, inserts the `EXTENT_DATA`
  item, computes csums (unless `nodatasum`), and bumps the
  inode's `nbytes` by the logical sector-aligned size.
- **`try_compress(data, algorithm)`**: free function that returns
  the inline-framed compressed bytes only when they shrink. For
  LZO this produces the inline framing format
  `[4B total_len LE] [4B seg_len LE] [lzo bytes]`; for zlib and
  zstd the raw compressor output is returned.

### Inode and directory entry helpers

- **`InodeArgs`** (`crate::inode`): full-fields counterpart to
  `btrfs_disk::items::InodeItemArgs` carrying every on-disk inode
  field (`flags`, `rdev`, `sequence`, four distinct timestamps).
  `InodeArgs::new(transid, mode)` provides sensible defaults;
  `with_uniform_time(ts)` sets all four stamps for tests.
- **`Transaction::create_inode(tree, ino, args)`**: insert an
  `INODE_ITEM` at `(ino, INODE_ITEM, 0)` from an `InodeArgs`.
- **`Transaction::link_dir_entry(tree, parent, child, name,
  file_type, dir_index, time)`**: insert the three records that
  make a directory entry visible — `INODE_REF`, `DIR_ITEM`,
  `DIR_INDEX` — bump the parent dir's `size` by `2 * name.len()`
  per the directory-isize convention, refresh `transid`, and
  update `ctime`/`mtime`. When `parent` is the canonical
  subvolume root dir (`BTRFS_FIRST_FREE_OBJECTID` = 256), also
  mirror the `size` update into the matching `ROOT_ITEM`'s
  embedded inode so `btrfs check`'s root-tree consistency check
  passes.
- **`Transaction::set_xattr(tree, ino, name, value)`**: insert an
  `XATTR_ITEM` at `(ino, XATTR_ITEM, name_hash(name))` carrying
  `(name, value)`. Same on-disk format as `DIR_ITEM` but with
  `FT_XATTR` and a non-empty value.
- **`try_compress_regular(data, algorithm, sectorsize)`**: variant
  for the regular-extent write path. For LZO produces the
  per-sector framing format
  `[4B total_len LE] { [4B seg_len LE] [lzo bytes] [zero pad] }*`
  with sector-boundary padding and an early-exit heuristic that
  abandons after 4 sectors if the framed buffer is already past
  3 sectors. For zlib and zstd delegates to `try_compress`.

### Free space tree

- **Incremental update**: `update_free_space_tree` consumes the
  per-block-group range deltas accumulated during the delayed-ref
  flush and applies them to the on-disk FST, deleting and
  re-inserting `FREE_SPACE_EXTENT` items and updating the
  `FREE_SPACE_INFO.extent_count`. Bitmap-layout block groups are
  refused with a clear error.
- **`FREE_SPACE_TREE_VALID` stays set**: the convergence loop in
  `Transaction::commit` runs `flush_delayed_refs →
  update_root_items → snapshot_roots → update_free_space_tree`
  in that order so the FST root change is captured by the next
  pass and the on-disk FST stays consistent with the extent tree.

### Transaction lifecycle

- **`Transaction::start`**: bumps the in-memory generation,
  snapshots the current root pointers, and seeds the metadata
  allocator cursor.
- **`Transaction::commit`**: force-COWs the root tree (so every
  commit advances `header.generation`), runs the convergence loop
  to drain delayed refs, root item updates, and FST updates until
  stable, flushes every dirty block to disk via the chunk tree
  (writing all DUP/RAID1 mirrors), updates superblock fields and
  the rotating backup roots, and writes the superblock to all
  mirrors.
- **`Transaction::abort`**: restores the in-memory root pointer
  snapshot so the next transaction reads consistent on-disk state.

## What's not yet implemented

- **RAID0 / RAID5 / RAID6 striped writes**: `BlockReader::write_block`
  fans out to every stripe's device for replication profiles
  (SINGLE / DUP / RAID1 / RAID1C3 / RAID1C4 / RAID10 mirror pairs)
  via `Filesystem::open_multi`, but for RAID0 / RAID5 / RAID6 a
  single buffer that spans more than one `stripe_len` would need
  per-stripe slicing. Tree blocks (always nodesize ≤ stripe_len)
  fit in one stripe slot; data extents larger than `stripe_len`
  do not. Defer until there's a concrete consumer.
- **New SYSTEM chunk allocation**: if no existing SYSTEM block
  group has free space, `ensure_in_sys_chunk_array` cannot carve
  out a new one. Bails cleanly.
- **Multi-device replication for chunk tree COW**: tested only
  on single-device filesystems.
- **Bitmap-layout free space tree**: refused with a clear error.

## Testing

Unit tests cover the leaf manipulation primitives, balance logic,
delayed ref merging, search/next_leaf, and the chunk array
bootstrap helpers (in `btrfs-disk`).

Integration tests build real filesystem images via `mkfs.btrfs`,
exercise the full COW pipeline through `Transaction::commit`, and
verify the result both by reopening the image with `Filesystem`
and by running `btrfs check --readonly` on it. The fixture image
under `cli/tests/commands/fixture.img.gz` is also used for
read-path and data-ref drop tests.

A regression suite hits historical bugs (cow pinning, sibling
generation, leaf data compaction, cascading splits, ...). A
proptest-based playground tree harness exercises insert/delete
sequences against an in-memory model and verifies that
`Transaction::commit` produces a structurally valid filesystem
on every step.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

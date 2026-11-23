# btrfs-transaction

Atomic, copy-on-write modification of btrfs filesystem images.

> This is a pre-1.0, experimental crate. It is a clean-room reimplementation
> of the btrfs read-write tree machinery and may have edge cases that tests
> don't cover. Take a backup before using it on filesystems you care about.

This crate sits on top of `btrfs-disk` and adds the write side that the
parsing crate leaves out:

- copy-on-write of tree blocks
- leaf insert, delete, split, and balance
- a delayed reference queue
- free space tree maintenance
- a `Transaction` type that groups all of these into an atomic commit

It works on raw image files or block devices and calls no ioctls. It only
needs read, write, and seek access to the filesystem bytes.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

### Tree-block storage and COW

- **`Filesystem`**: opens an image, resolves tree roots, reads tree blocks
  via the chunk tree, and writes blocks back to all mirrors with correct
  CRC32C checksums.
- **`ExtentBuffer`**: an in-memory tree block with leaf and node helpers for
  items, key and data offsets, and slot manipulation. Tracks dirty state and
  pending writes.
- **`cow_block`**: copies a tree block to a newly allocated bytenr, clears
  the WRITTEN and RELOC flags, and queues add and drop delayed refs against
  the new and old addresses.

### B-tree operations

- **`search_slot`**: searches for a key in any tree, optionally COWing the
  path. Supports `ReadOnly`, `Insert(size)`, and `Delete` intents. `Insert`
  and `Delete` pre-COW and pre-balance the path so the caller can mutate the
  leaf in place.
- **`next_leaf`**: advances the cursor across leaf boundaries.
- **`items::insert_item`, `del_items`, `update_item`, `shrink_item`**:
  in-place leaf payload manipulation that keeps data offsets descending.
- **Split and balance**: leaves and nodes split when an insert won't fit.
  On delete, push-left, push-right, and merge keep nodes within fill
  thresholds.
- **`BtrfsPath`**: the slot and buffer at each level from the root to a
  target leaf. The caller owns and releases it explicitly, which keeps the
  borrow scope under its control.

### Allocation

- **Block group scan**: loads every `BLOCK_GROUP_ITEM` from the block group
  tree (id 11) when present, otherwise from the extent tree (id 2).
- **Per-kind bump allocator**: `Transaction::alloc_block(kind)` keeps
  separate cursors for `BlockGroupKind::Metadata`, `BlockGroupKind::System`,
  and `BlockGroupKind::Data`. Cursors are seeded by scanning the extent tree
  for free gaps inside block groups of the requested kind. Pinned blocks
  (freed earlier in the same transaction) are skipped to preserve crash
  consistency.
- **`alloc_tree_block(tree_id, level)`**: places the chunk tree (id 3) in a
  SYSTEM block group and every other tree in a metadata block group, and
  registers a delayed `add_ref`. For SYSTEM allocations it also updates the
  superblock's `sys_chunk_array` bootstrap snippet so the next mount can
  resolve the new chunk root.

### Delayed reference queue

- **`DelayedRefQueue`**: batches reference count changes keyed by
  `DelayedRefKey::Metadata { bytenr, owner_root, level }` or
  `DelayedRefKey::Data { bytenr, owner_root, owner_ino, owner_offset }`.
  An add and a drop on the same key cancel out.
- **Metadata flush**: on positive deltas, creates `METADATA_ITEM` (skinny)
  or `EXTENT_ITEM` records with inline `TREE_BLOCK_REF` backrefs. On
  negative deltas, deletes them. Byte deltas feed the per-block-group
  `bytes_used` accounting.
- **Data flush, drop side**: finds the matching `EXTENT_DATA_REF`, either
  inline in the parent `EXTENT_ITEM` or as a standalone item (walking
  forward past hash collisions). It decrements both the inline `count` field
  and the parent `EXTENT_ITEM.refs`. When the count reaches zero, it deletes
  the whole `EXTENT_ITEM` and trims overlapping csum tree entries.
- **Csum tree maintenance**: when a data extent is fully freed, a first pass
  over the csum tree collects every overlapping `EXTENT_CSUM` item. A second
  pass deletes fully covered items and re-inserts the trimmed head and tail
  fragments under new keys.
- **Data flush, add side**: on a positive data delta, inserts an
  `EXTENT_ITEM` (24-byte header plus a 29-byte inline `EXTENT_DATA_REF`,
  53 bytes in total). It also updates the per-block-group `bytes_used` delta
  and the allocated-range record for the FST.

### Data extent write path

- **`Transaction::alloc_data_extent(data, root, ino, file_offset)`**: finds
  sectorsize-aligned free space in a `BlockGroupKind::Data` block group,
  zero-pads `data` to sectorsize, and writes it via
  `BlockReader::write_block`, which fans out to every stripe copy. It then
  queues a `+1` `EXTENT_DATA_REF` delayed ref and returns the allocated
  logical address.
- **`Transaction::insert_file_extent(tree, ino, offset, payload)`**: inserts
  an `EXTENT_DATA` item at `(ino, EXTENT_DATA, offset)`. `payload` is a
  serialized `FileExtentItem`. Use `to_bytes_regular` for non-inline extents
  and `to_bytes_inline` for inline tail extents.
- **`Transaction::insert_csums(logical, on_disk_data)`**: computes the
  standard CRC32C of each sector of the on-disk bytes and inserts
  `EXTENT_CSUM` items into the csum tree. Payloads that would exceed leaf
  capacity are split across multiple keyed items. Returns an error on
  non-CRC32C filesystems.
- **`Transaction::update_inode_nbytes(tree, ino, delta)`**: applies a signed
  in-place patch to the inode's `nbytes` field at its fixed struct offset.
  It preserves all other fields (including `flags`, `rdev`, and `sequence`),
  which a round trip through `InodeItemArgs` would lose.
- **`Transaction::insert_inline_extent(tree, ino, file_offset,
  data)`**: embeds `data` directly in the FS tree leaf as an inline
  `EXTENT_DATA` item. It creates no extent tree or csum entries.
  `INODE.nbytes` grows by the unaligned payload length.
- **`Transaction::write_file_data(tree, ino, file_offset, data,
  nodatasum, compression)`**: a high-level helper that chooses inline or
  regular based on `data.len()` and the filesystem's inline threshold
  (`max_inline_data_size`). For regular extents it:
  - splits `data` into chunks of at most 1 MiB
  - optionally compresses each chunk with zlib, zstd, or LZO, falling back
    to raw for chunks that don't shrink
  - allocates each chunk and inserts its `EXTENT_DATA` item
  - computes csums (unless `nodatasum` is set)
  - grows the inode's `nbytes` by the logical sector-aligned size
- **`try_compress(data, algorithm)`**: a free function that returns the
  inline-framed compressed bytes only when they are smaller. For LZO it
  produces the inline framing format
  `[4B total_len LE] [4B seg_len LE] [lzo bytes]`. For zlib and zstd it
  returns the raw compressor output.

### Inode and directory entry helpers

- **`InodeArgs`** (`crate::inode`): the full-field counterpart to
  `btrfs_disk::items::InodeItemArgs`. It carries every on-disk inode field
  (`flags`, `rdev`, `sequence`, and four distinct timestamps).
  `InodeArgs::new(transid, mode)` provides sensible defaults, and
  `with_uniform_time(ts)` sets all four timestamps for tests.
- **`Transaction::create_inode(tree, ino, args)`**: inserts an `INODE_ITEM`
  at `(ino, INODE_ITEM, 0)` from an `InodeArgs`.
- **`Transaction::link_dir_entry(tree, parent, child, name,
  file_type, dir_index, time)`**: inserts the three records that make a
  directory entry visible (`INODE_REF`, `DIR_ITEM`, `DIR_INDEX`). It grows
  the parent directory's `size` by `2 * name.len()` per the directory isize
  convention, refreshes `transid`, and updates `ctime` and `mtime`. When
  `parent` is the subvolume root directory (`BTRFS_FIRST_FREE_OBJECTID` =
  256), it also mirrors the `size` update into the embedded inode of the
  matching `ROOT_ITEM`, so the root tree consistency check in `btrfs check`
  passes.
- **`Transaction::link_subvol_entry(tree, parent, subvol_id,
  name, dir_index, time)`**: like `link_dir_entry`, but for a parent's entry
  pointing at a subvolume root. It inserts `DIR_ITEM` and `DIR_INDEX` with a
  `ROOT_ITEM` location and no `INODE_REF`, because subvolume parent linkage
  uses `ROOT_REF` and `ROOT_BACKREF` in the root tree instead. Pair it with
  `insert_root_ref`.
- **`Transaction::set_xattr(tree, ino, name, value)`**: inserts an
  `XATTR_ITEM` at `(ino, XATTR_ITEM, name_hash(name))` carrying
  `(name, value)`. The on-disk format matches `DIR_ITEM`, but with
  `FT_XATTR` and a non-empty value.
- **`Transaction::set_inode_nlink(tree, ino, nlink)`**: patches the inode's
  `nlink` field in place. mkfs uses it in its hardlink fixup pass.
- **`try_compress_regular(data, algorithm, sectorsize)`**: the variant for
  the regular extent write path. For LZO it produces the per-sector framing
  format
  `[4B total_len LE] { [4B seg_len LE] [lzo bytes] [zero pad] }*`
  with sector-boundary padding. An early-exit heuristic gives up after 4
  sectors if the framed buffer is already past 3 sectors. For zlib and zstd
  it delegates to `try_compress`.

### Whole-tree creation and conversion

- **`Transaction::create_empty_tree(tree_id)`**: allocates one metadata
  block, initialises it as an empty level-0 leaf with the inherited fsid and
  chunk_tree_uuid, registers the new `(tree_id -> bytenr)` mapping, and
  inserts a `ROOT_ITEM` into the root tree. This is the foundation for
  creating whole trees (FS, csum, FST, BGT, UUID, quota, user subvolumes).
  Refuses the bootstrap ids 0..=3.
- **`Transaction::insert_root_ref(parent_root, child_root,
  dirid, sequence, name)`**: inserts a paired `ROOT_REF` (parent → child)
  and `ROOT_BACKREF` (child → parent) for a subvolume link.
- **`Transaction::set_root_readonly(tree_id)`**: ORs `RootItemFlags::RDONLY`
  into a `ROOT_ITEM`'s flags. Used by `mkfs --rootdir --subvol ro:`.
- **`Transaction::set_default_subvol(subvol_id)`**: inserts or updates the
  `"default"` `DIR_ITEM` under `BTRFS_ROOT_TREE_DIR_OBJECTID` (id 6). Used
  by `mkfs --rootdir --subvol default:`.
- **`Transaction::set_device_total_bytes(devid, new_total)`**: patches a
  device's `DEV_ITEM.total_bytes` in the chunk tree and mirrors it into the
  superblock's embedded `dev_item`. Used by `mkfs --rootdir --shrink` and
  `rescue fix-device-size`.
- **`convert::convert_to_free_space_tree`** and
  **`convert::convert_to_block_group_tree`**: the top-level conversions
  used by `btrfs-tune`. The per-step helpers
  `convert::seed_free_space_tree` and `convert::create_block_group_tree`
  are also public, and mkfs's `post_bootstrap` uses them.

### Free space tree

- **Incremental update**: `update_free_space_tree` takes the
  per-block-group range deltas collected during the delayed-ref flush and
  applies them to the on-disk FST. It deletes and re-inserts
  `FREE_SPACE_EXTENT` items and updates `FREE_SPACE_INFO.extent_count`.
  Bitmap-layout block groups are refused with a clear error.
- **`FREE_SPACE_TREE_VALID` stays set**: the convergence loop in
  `Transaction::commit` runs `flush_delayed_refs →
  update_root_items → snapshot_roots → update_free_space_tree`
  in that order. The next pass then sees the FST root change, and the
  on-disk FST stays consistent with the extent tree.

### Transaction lifecycle

- **`Transaction::start`**: bumps the in-memory generation, snapshots the
  current root pointers, and seeds the metadata allocator cursor.
- **`Transaction::commit`**:
  - force-COWs the root tree, so every commit advances `header.generation`
  - runs the convergence loop to drain delayed refs, root item updates, and
    FST updates until stable
  - flushes every dirty block to disk via the chunk tree, writing all DUP
    and RAID1 mirrors
  - updates superblock fields and the rotating backup roots
  - writes the superblock to all mirrors
- **`Transaction::abort`**: restores the in-memory snapshot of the root
  pointers, so the next transaction reads consistent on-disk state.

## What's not yet implemented

- **Striped writes larger than `stripe_len` for RAID0, RAID5, and RAID6
  data extents**: `BlockReader::write_block` routes each stripe via the
  chunk cache's `plan_write` and handles every RAID profile correctly
  (parity-aware for RAID5 and RAID6, mirror pairs for RAID10). A single
  buffer that spans more than one row would need per-stripe slicing in the
  executor, though. Tree blocks (always nodesize ≤ stripe_len) fit in one
  row. Data extents larger than `stripe_len` do not. Deferred until there is
  a concrete consumer.
- **New SYSTEM chunk allocation**: if no existing SYSTEM block group has
  free space, `ensure_in_sys_chunk_array` cannot carve out a new one. It
  bails cleanly.
- **Bitmap-layout free space tree**: refused with a clear error.
- **`Filesystem::create`**: a from-scratch primitive to replace mkfs's
  hand-built bootstrap. It would let mkfs delete its remaining
  `LeafBuilder`, `TreeBuilder`, `BlockLayout`, and `build_*` machinery.

## Testing

Unit tests cover the leaf manipulation primitives, balance logic, delayed
ref merging, search and next_leaf, and the chunk array bootstrap helpers (in
`btrfs-disk`).

Integration tests build real filesystem images with `mkfs.btrfs`, run the
full COW pipeline through `Transaction::commit`, and check the result two
ways: by reopening the image with `Filesystem` and by running
`btrfs check --readonly` on it. Read-path and data-ref drop tests also use
the fixture image `cli/tests/commands/fixture.img.gz`.

A regression suite covers historical bugs (COW pinning, sibling generation,
leaf data compaction, cascading splits, and others). A proptest-based
playground tree harness runs insert and delete sequences against an
in-memory model and checks that `Transaction::commit` produces a
structurally valid filesystem at every step.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.

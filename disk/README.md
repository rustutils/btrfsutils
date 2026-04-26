# btrfs-disk

Platform-independent parsing and serialization of btrfs on-disk data structures.

This crate handles the raw on-disk format: reading superblocks, tree nodes,
and item payloads from block devices or image files, and writing them back.
It does not use ioctls or require a mounted filesystem, and works on any
platform that can read raw bytes.

The `btrfs-cli` crate uses the parsing side for commands like `dump-super`,
`dump-tree`, and `restore`. The `btrfs-mkfs` crate uses the serialization
side (raw constants, write helpers, struct sizes/offsets) to construct new
filesystems from scratch. The `btrfs-tune` crate uses both sides to read
and patch superblocks and tree block headers for offline UUID rewrites.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

### Parsing (read path)

- **Superblock**: full parsing including device items, backup roots, checksum
  type, feature flags, label
- **Tree nodes**: leaf and internal node parsing with key/item extraction
- **Item payloads**: typed structs for 30+ item types (InodeItem, DirItem,
  RootItem, FileExtentItem, ExtentItem, ChunkItem, DevItem, DevExtent,
  BlockGroupItem, QgroupStatus/Info/Limit, and more)
- **Chunk tree**: logical-to-physical address resolution via ChunkTreeCache,
  sys_chunk_array bootstrap parsing. `plan_write` and `plan_read` cover
  every RAID profile (SINGLE / DUP / RAID0 / RAID1* / RAID10 / RAID5 /
  RAID6); RAID5/6 writes use a parity-aware executor that prereads each
  data column slot, recomputes P (and Q for RAID6) over the assembled
  row, and writes the data + parity stripes to the rotating column
  positions.
- **RAID5/6 parity math**: `compute_p` (XOR) and `compute_p_q` (XOR +
  Reed-Solomon over GF(2^8) with reduction polynomial 0x1D) in the
  `raid56` module.
- **Block reader**: read tree blocks by logical address, full filesystem
  bootstrap (superblock -> chunk tree -> root tree)
- **Tree traversal**: BFS/DFS walk with visitor callbacks
- **Key types**: KeyType and ObjectId enums with Display formatting, format_key
  for human-readable output

### Serialization (write path)

- **Raw constants**: all `BTRFS_*` constants from kernel headers via bindgen
  (struct sizes, field offsets, magic numbers, item type codes, feature flags)
- **Write helpers**: random-access little-endian writers (`write_le_u64`,
  `write_le_u32`, `write_le_u16`, `write_uuid`) for patching on-disk
  structures in byte buffers. Sequential writes use `bytes::BufMut` directly.

### Shared

- **Checksum dispatch**: `ChecksumType::compute` and `csum_tree_block` /
  `csum_superblock` cover all four btrfs algorithms (CRC32C, xxhash64,
  SHA-256, BLAKE2b). The transaction crate uses these from its commit
  pipeline and mkfs uses them when sealing tree blocks and superblocks.
- **CRC32C**: also exposed as `raw_crc32c` for hash-only callers (e.g.
  the extent data ref hash in dump-tree, btrfs name hash).

## What's not yet implemented

- Incremental/streaming parsing of large trees

## Testing

Unit tests cover superblock parsing, tree node parsing, chunk cache operations,
key type round-trips, and write helpers.

```sh
cargo test -p btrfs-disk
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.


# btrfs-disk

Platform-independent parsing and serialization of btrfs on-disk data structures.

This crate handles the raw on-disk format: reading superblocks, tree nodes,
and item payloads from block devices or image files, and writing them back.
It does not use ioctls or require a mounted filesystem, and works on any
platform that can read raw bytes.

The `btrfs-cli` crate uses the parsing side for commands like `dump-super`
and `dump-tree`. The `btrfs-mkfs` crate uses the serialization side
(raw constants, write helpers, struct sizes/offsets) to construct new
filesystems from scratch.

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

## What's implemented

### Parsing (read path)

- **Superblock**: full parsing including device items, backup roots, checksum type, feature flags, label
- **Tree nodes**: leaf and internal node parsing with key/item extraction
- **Item payloads**: typed structs for 30+ item types (InodeItem, DirItem, RootItem, FileExtentItem, ExtentItem, ChunkItem, DevItem, DevExtent, BlockGroupItem, QgroupStatus/Info/Limit, and more)
- **Chunk tree**: logical-to-physical address resolution via ChunkTreeCache, sys_chunk_array bootstrap parsing
- **Block reader**: read tree blocks by logical address, full filesystem bootstrap (superblock -> chunk tree -> root tree)
- **Tree traversal**: BFS/DFS walk with visitor callbacks
- **Key types**: KeyType and ObjectId enums with Display formatting, format_key for human-readable output

### Serialization (write path)

- **Raw constants**: all `BTRFS_*` constants from kernel headers via bindgen (struct sizes, field offsets, magic numbers, item type codes, feature flags)
- **Write helpers**: little-endian writers (`write_le_u64`, `write_le_u32`, `write_le_u16`, `write_uuid`) for constructing on-disk structures in byte buffers

### Shared

- **Little-endian readers**: `read_le_u64`, `read_le_u32`, `read_le_u16`, `read_uuid`
- **CRC32C**: used for extent data ref hash computation

## What's not yet implemented

- Incremental/streaming parsing of large trees

## Testing

Unit tests cover superblock parsing, tree node parsing, chunk cache operations,
key type round-trips, and LE reader/writer helpers.

```sh
cargo test -p btrfs-disk
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.


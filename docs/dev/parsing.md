# Parsing

The `btrfs-disk` crate parses btrfs on-disk structures from raw byte buffers.
It is platform-independent — it works on any OS and can be used to inspect
filesystem images without a running kernel.

## Reading a filesystem

The typical entry point is `open_filesystem`, which bootstraps from the
superblock:

```
superblock → sys_chunk_array → chunk tree → root tree
```

From there, `walk_tree` traverses any tree in BFS or DFS order, calling a
visitor callback for each block:

```rust
let (reader, root_tree) = open_filesystem(&mut file)?;
walk_tree(&mut reader, root_tree, Traversal::Bfs, &mut |block| {
    // block: &TreeBlock — either a Node (internal) or Leaf
});
```

## Item payloads

Leaf blocks contain items, each with a `DiskKey` (objectid, type, offset) and a
raw payload. `parse_item_payload` dispatches to a typed parser based on the key
type:

```rust
let payload = parse_item_payload(key_type, data)?;
match payload {
    ItemPayload::Inode(inode) => { /* ... */ }
    ItemPayload::RootItem(root) => { /* ... */ }
    ItemPayload::FileExtent(extent) => { /* ... */ }
    // ...
}
```

## Reading on-disk fields safely

On-disk structs are packed and little-endian. Casting a `*const u8` pointer
directly to a packed struct is undefined behaviour due to potential misalignment.
Instead, use the LE reader helpers from `disk/src/util.rs`:

```rust
use btrfs_disk::util::{read_le_u64, read_le_u32};
use std::mem::offset_of;

let size = read_le_u64(data, offset_of!(raw::btrfs_inode_item, size));
let nlink = read_le_u32(data, offset_of!(raw::btrfs_inode_item, nlink));
```

Always use `std::mem::offset_of!` and `std::mem::size_of` to derive offsets and
sizes from the bindgen struct definitions — never hard-code numeric byte offsets.
The `field_size!(T, field)` macro (from `crate::util`) gives the size of an
individual field.

## Superblock mirrors

btrfs writes up to three superblock copies at fixed offsets.
`super_mirror_offset(n)` returns the byte offset for mirror `n` (0, 1, or 2).
`read_superblock` reads and validates a superblock — checking the magic number
and CRC — from any seekable reader.

## Display logic belongs in `cli/`

The `disk/` crate only produces typed structs. All formatting and human-readable
output lives in `cli/src/inspect/`. The `disk/` crate never calls `println!` or
constructs output strings.

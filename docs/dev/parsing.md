# Parsing

The `btrfs-disk` crate parses btrfs on-disk structures from raw byte buffers.
It is platform-independent — it works on any OS and can be used to inspect
filesystem images without a running kernel.

## Reading a filesystem

The typical entry point is `filesystem_open`, which bootstraps from the
superblock:

```
superblock → sys_chunk_array → chunk tree → root tree
```

The returned `OpenFilesystem` contains a `BlockReader` (for reading tree blocks
by logical address) and a map of tree root locations. From there, `tree_walk`
traverses any tree in BFS or DFS order, calling a visitor callback for each
block:

```rust
let open = filesystem_open(file)?;
let mut reader = open.reader;
tree_walk(&mut reader, root_bytenr, Traversal::Bfs, &mut |block| {
    // block: &TreeBlock — either a Node (internal) or Leaf
    Ok(())
})?;
```

## Item payloads

Leaf blocks contain items, each with a `DiskKey` (objectid, type, offset) and a
raw payload. `parse_item_payload` dispatches to a typed parser based on the key
type:

```rust
let payload = parse_item_payload(&key, data);
match payload {
    ItemPayload::InodeItem(inode) => { /* ... */ }
    ItemPayload::RootItem(root) => { /* ... */ }
    ItemPayload::FileExtentItem(extent) => { /* ... */ }
    // ...
}
```

## Reading on-disk fields safely

On-disk structs are packed and little-endian. Casting a `*const u8` pointer
directly to a packed struct is undefined behaviour due to potential misalignment.

### `btrfs-disk`: `bytes::Buf` / `bytes::BufMut`

The `disk` crate uses the `bytes` crate for all parsing and serialization. A
`&[u8]` implements `Buf`, so you can read fields sequentially with methods like
`get_u64_le()`, which advances the cursor automatically:

```rust
let mut buf = data;
let generation = buf.get_u64_le();
let size = buf.get_u64_le();
let mode = buf.get_u32_le();
```

For serialization, `BufMut` provides the inverse (`put_u64_le`, `put_slice`,
etc.). This approach avoids manual offset arithmetic and makes it impossible to
read past the end of the buffer (it panics instead of silently producing
garbage).

### `btrfs-uapi`: offset-based LE readers

The `uapi` crate parses tree search results returned by the kernel, which are
raw `&[u8]` buffers at known offsets. It uses explicit offset-based helpers from
`uapi/src/util.rs`:

```rust
use btrfs_uapi::util::read_le_u64;
use std::mem::offset_of;

let size = read_le_u64(data, offset_of!(raw::btrfs_inode_item, size));
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

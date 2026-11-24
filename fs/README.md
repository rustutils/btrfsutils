# btrfs-fs

High-level filesystem API on top of [`btrfs-disk`].

> This crate is **experimental**. It is read-only for now. Write support is
> planned via [`btrfs-transaction`].

`btrfs-fs` provides a `Filesystem` type with the operations a userspace driver
needs, without depending on any FUSE crate:

- `lookup`, `readdir`, `readdirplus`, `read`, `readlink`, `getattr`
- `xattr_get`, `xattr_list`, `statfs`, `seek_hole_data` (`SeekHoleData`)
- `tree_search`, `ino_lookup`, `ino_paths`, `resolve_subvol_path`
- `forget`, `send`, `list_subvolumes`, `get_subvol_info`, `dev_info`

The same API drives the `btrfs-fuse` mount, `btrfs send --offline`, and any
other embedder (offline tools, tests, alternate FUSE bindings).

The handle is `Clone` (a cheap `Arc` bump). All operations are `async fn` and
run sync I/O via `tokio::task::spawn_blocking`, so multiple tokio tasks can
drive the same filesystem concurrently.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

- Multi-subvolume traversal (`Inode = (SubvolId, ino)`). Subvolume crossings
  are followed automatically, and `..` on a subvolume root walks
  `ROOT_BACKREF`.
- Inline, regular, and prealloc extents (prealloc reads as zeros)
- Sparse hole reads, and `lseek SEEK_HOLE` and `SEEK_DATA` against the cached
  extent map
- `readdirplus`, which pairs each `Entry` with its `Stat` using
  cache-friendly reads
- zlib, zstd, and LZO decompression (btrfs per-sector LZO framing)
- xattr enumeration and lookup (with a hash-bucket scan for collisions)
- POSIX-style `Stat` with all timestamps, including btrfs `btime`
- Three-layer cache (tree blocks, inodes, extent maps) sized via
  `CacheConfig`. `forget(Inode)` evicts eagerly when an embedder signals that
  an inode is no longer referenced.
- `tree_search(filter, max_buf_size)` mirrors the semantics of the kernel's
  `BTRFS_IOC_TREE_SEARCH_V2` (compound-key range filter)
- Inode-to-path resolution: `ino_lookup(subvol, objectid)` returns a single
  path, and `ino_paths(subvol, objectid)` returns every hardlink.
- Full sends: `Filesystem::send(snapshot, output)` generates a v1 send stream
  describing the snapshot.

## What's not yet implemented

- Write path (planned: `Filesystem::transaction()` returning a handle
  backed by [`btrfs-transaction`])
- Incremental sends (`-p PARENT`) and clone sources
- v2 `EncodedWrite` passthrough for compressed extents in send
- True parallel I/O. A single internal mutex serialises reader access for
  now. A future `BlockReader` pool will lift this without changing the
  public API.

## Usage

```rust,ignore
use btrfs_fs::Filesystem;
use std::fs::File;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let file = File::open("image.img")?;
    let fs = Filesystem::open(file)?;

    let root = fs.root();
    let (ino, _) = fs.lookup(root, b"hello.txt").await?.unwrap();
    let data = fs.read(ino, 0, 1024).await?;
    println!("{}", String::from_utf8_lossy(&data));
    Ok(())
}
```

## License

Licensed under either the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or
the [MIT license](https://opensource.org/licenses/MIT), at your option.

[`btrfs-disk`]: https://docs.rs/btrfs-disk
[`btrfs-transaction`]: https://docs.rs/btrfs-transaction

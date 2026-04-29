# btrfs-fs

High-level filesystem API on top of [`btrfs-disk`].

> This crate is **experimental**. Read-only today. Write support is
> planned via [`btrfs-transaction`].

`btrfs-fs` provides a `Filesystem` type with the operations a userspace
driver needs — `lookup`, `readdir`, `readdirplus`, `read`, `readlink`,
`getattr`, `xattr_get`, `xattr_list`, `statfs`, `lseek`
(`SeekHoleData`), `tree_search`, `ino_lookup`, `ino_paths`,
`resolve_subvol_path`, `forget`, `send`, plus subvolume metadata
helpers — without depending on any FUSE crate. The same API drives the
`btrfs-fuse` mount, the `btrfs send --offline` CLI path, and any other
embedder (offline tools, tests, alternate FUSE bindings).

The handle is `Clone` (cheap `Arc` bump) and all operations are
`async fn` running sync I/O via `tokio::task::spawn_blocking`, so
multiple tokio tasks can drive the same filesystem concurrently.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

- Multi-subvolume traversal (`Inode = (SubvolId, ino)`); auto-follow
  on subvolume crossings; `..` walks `ROOT_BACKREF` for subvol roots
- Inline, regular, and prealloc extents (prealloc reads as zeros)
- Sparse hole reads + `lseek SEEK_HOLE` / `SEEK_DATA` against the
  cached extent map
- `readdirplus` (cache-friendly per-entry `InodeItem` reads, paired
  with each `Entry`)
- zlib, zstd, and LZO decompression (btrfs per-sector LZO framing)
- xattr enumeration and lookup (with hash-bucket scan for collisions)
- POSIX-style `Stat` with all timestamps including btrfs `btime`
- Three-layer cache (tree blocks, inodes, extent maps) sized via
  `CacheConfig`; `forget(Inode)` evicts eagerly when an embedder
  signals an inode is no longer referenced
- `tree_search(filter, max_buf_size)` mirrors the kernel's
  `BTRFS_IOC_TREE_SEARCH_V2` semantics (compound-key range filter)
- `ino_lookup(subvol, objectid)` (single path) and
  `ino_paths(subvol, objectid)` (every hardlink) for inode-to-path
  resolution
- Send tier 1: `Filesystem::send(snapshot, output)` generates a v1
  send stream describing the snapshot. Full sends only — incremental,
  clone sources, encoded-write passthrough are tier 2/3 (future)

## What's not yet implemented

- Write path (planned: `Filesystem::transaction()` returning a handle
  backed by [`btrfs-transaction`])
- Send tier 2 (incremental, `-p PARENT`)
- Send tier 3 (v2 `EncodedWrite` passthrough for compressed extents)
- True parallel I/O (currently a single internal mutex serialises
  reader access; future `BlockReader` pool will lift this without
  changing the public API)

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

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

[`btrfs-disk`]: https://docs.rs/btrfs-disk
[`btrfs-transaction`]: https://docs.rs/btrfs-transaction

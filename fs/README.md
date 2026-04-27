# btrfs-fs

High-level filesystem API on top of [`btrfs-disk`].

> This crate is **experimental**. Read-only today. Write support is
> planned via [`btrfs-transaction`].

`btrfs-fs` provides a `Filesystem` type with the operations a userspace
driver needs — `lookup`, `readdir`, `read`, `readlink`, `getattr`,
`xattr_get`, `xattr_list`, `statfs` — without depending on any FUSE
crate. The same API drives the `btrfs-fuse` mount and any other
embedder (offline tools, tests, alternate FUSE bindings).

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

- Default subvolume only
- Inline, regular, and prealloc extents (prealloc reads as zeros)
- Sparse hole reads
- zlib, zstd, and LZO decompression (btrfs per-sector LZO framing)
- xattr enumeration and lookup (with hash-bucket scan for collisions)
- POSIX-style `Stat` with all timestamps including btrfs `btime`

## What's not yet implemented

- Multi-subvolume / snapshot traversal
- Tree-block / inode / extent-map caching (every operation currently
  walks the FS tree)
- Any write operation (planned: `Filesystem::transaction()` returning a
  handle backed by [`btrfs-transaction`])

## Usage

```rust
use btrfs_fs::Filesystem;
use std::fs::File;

let file = File::open("image.img")?;
let mut fs = Filesystem::open(file)?;

let root = fs.root();
let (ino, _) = fs.lookup(root, b"hello.txt")?.unwrap();
let data = fs.read(ino, 0, 1024)?;
println!("{}", String::from_utf8_lossy(&data));
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

[`btrfs-disk`]: https://docs.rs/btrfs-disk
[`btrfs-transaction`]: https://docs.rs/btrfs-transaction

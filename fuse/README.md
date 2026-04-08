# btrfs-fuse

A userspace FUSE driver for btrfs, built on top of the `btrfs-disk` crate.

> This crate is **experimental** and should not be used for anything you
> care about. It should be functional, and being read-only it should
> not corrupt your data.

`btrfs-fuse` lets you mount a btrfs image file or block device read-only
without kernel btrfs support. All on-disk parsing, tree walks, and
decompression happen in Rust userspace; the kernel only sees a generic
FUSE mount.

It also doubles as the canonical integration test harness for the
`btrfs-disk` library — every filesystem operation exercises the parser
end-to-end, against images produced by real `mkfs.btrfs`.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

- Mount a single btrfs image or block device read-only
- Default FS tree (subvolume) only
- `lookup`, `getattr`, `readdir`, `readlink`, `read`, `listxattr`,
  `getxattr`, `statfs`
- Inline extents, regular extents, preallocated extents (read as zeros),
  sparse holes
- zlib, zstd, and lzo decompression (btrfs per-sector LZO framing)
- Symlinks, hardlinks, xattrs, regular files, directories

## What's not yet implemented

- Any write operation
- Multiple subvolumes / snapshot switching (`subvol=` / `subvolid=`)
- ioctl passthrough (`FS_INFO`, `TREE_SEARCH_V2`, `SEND`, ...)
- RAID1/10/5/6 redundancy handling on degraded devices
- Send stream generation
- `statfs` inode counts (reported as 0)

## Usage

### As a binary

```sh
# Install from source
cargo install --path fuse

# Mount an image or block device
btrfs-fuse /path/to/image.img /mnt/btrfs

# With debug logging
RUST_LOG=btrfs_fuse=debug btrfs-fuse -f /path/to/image.img /mnt/btrfs
```

The mount is always read-only. Pass `--allow-other` to let other users
on the system see the mount.

### As a library

The crate exposes `BtrfsFuse` and an operation layer of inherent methods
(`lookup_entry`, `get_attr`, `read_dir`, `read_symlink`, `read_data`,
`list_xattrs`, `get_xattr`, `stat_fs`) that return plain `io::Result`
values and do not depend on `fuser`. You can embed the driver in your
own code, drive it from tests, or swap in a different FUSE
implementation.

```rust
use btrfs_fuse::BtrfsFuse;
use std::fs::File;

let file = File::open("image.img")?;
let fs = BtrfsFuse::open(file)?;

let (ino, inode) = fs.lookup_entry(1, b"hello.txt")?.unwrap();
let data = fs.read_data(ino, 0, inode.size as u32)?;
println!("{}", String::from_utf8_lossy(&data));
```

## Testing

Integration tests build a fresh btrfs image per test run using
`mkfs.btrfs --rootdir` and drive the operation layer directly, so they
are unprivileged and run under plain `cargo test`:

```sh
cargo test -p btrfs-fuse
```

Requires `mkfs.btrfs` (from `btrfs-progs`) on `$PATH`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

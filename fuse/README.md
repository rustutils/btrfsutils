# btrfs-fuse

A userspace FUSE driver for btrfs.

> This crate is **experimental** and should not be used for anything you
> care about. It should be functional, and being read-only it should
> not corrupt your data.

`btrfs-fuse` lets you mount a btrfs image file or block device read-only
without kernel btrfs support. All filesystem semantics live in
[`btrfs-fs`]; this crate is the thin `fuser::Filesystem` adapter on top
(inode translation, `Stat` → `FileAttr` mapping, mount glue).

If you want to embed a btrfs reader in your own code without going
through FUSE, depend on [`btrfs-fs`] directly.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

[`btrfs-fs`]: https://docs.rs/btrfs-fs

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

`BtrfsFuse` is a `fuser::Filesystem` impl ready to hand to
`fuser::mount2`. It carries no inherent operation methods — those live
on [`btrfs_fs::Filesystem`][`btrfs-fs`], which is what you should
depend on for embedding.

## Testing

Read-path integration tests live in the [`btrfs-fs`] crate (the FUSE
adapter has no filesystem logic to test on its own). Run them with:

```sh
cargo test -p btrfs-fs
```

Requires `mkfs.btrfs` (from `btrfs-progs`) on `$PATH`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

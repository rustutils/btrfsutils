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
- Multi-subvolume traversal (auto-follow on subvolume crossings,
  `..` walks `ROOT_BACKREF` for subvol roots)
- `--subvol PATH` / `--subvolid ID` to mount a specific subvolume
  as the FUSE root
- `init` (negotiates `FUSE_DO_READDIRPLUS`, `FUSE_AUTO_INVAL_DATA`,
  splice flags), `forget` (eager cache eviction), `lookup`,
  `getattr`, `readdir`, `readdirplus`, `read`, `readlink`,
  `listxattr`, `getxattr`, `statfs`, `lseek` (`SEEK_HOLE` /
  `SEEK_DATA`), and `ioctl`
- Inline extents, regular extents, preallocated extents (read as
  zeros), sparse holes
- zlib, zstd, and lzo decompression (btrfs per-sector LZO framing)
- Symlinks, hardlinks, xattrs, regular files, directories
- 8 btrfs ioctls: `FS_INFO`, `GET_FEATURES`, `GET_SUBVOL_INFO`,
  `DEV_INFO`, `INO_LOOKUP`, `TREE_SEARCH` (v1, fixed 4 KiB),
  `GET_SUBVOL_ROOTREF`, and `TREE_SEARCH_V2` (returns `ENOPROTOOPT`
  so `btrfs-uapi::tree_search_auto` falls back to v1
  transparently — see the F6.4 design note in `fs/PLAN.md`)
- Tunable cache sizes via `--cache-tree-blocks N`,
  `--cache-inodes N`, `--cache-extent-maps N`
- Kernel `default_permissions` mount option enabled by default
  (matches kernel btrfs semantics); opt out with
  `--no-default-permissions` for image-inspection scenarios

## What's not yet implemented

- Any write operation
- Variable-size ioctls that need `FUSE_IOCTL_RETRY` (`INO_PATHS`,
  `LOGICAL_INO_V2`, `SPACE_INFO`, `ENCODED_READ`) — blocked at the
  kernel boundary; see `fs/PLAN.md` § F6.4
- `BTRFS_IOC_SEND` (kernel ioctl path); use `btrfs send --offline
  IMAGE` from `btrfs-cli` to generate send streams from images
- RAID1/10/5/6 redundancy handling on degraded devices
- `--foreground=false` daemonize path (`-f` foreground works)
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

# Mount a specific subvolume as the FUSE root
btrfs-fuse --subvol home/snapshots/2026-04-29 /path/to/image.img /mnt/btrfs

# Tune caches for a memory-constrained host
btrfs-fuse --cache-tree-blocks 256 --cache-inodes 256 \
           /path/to/image.img /mnt/btrfs

# Allow reading files whose stored UIDs don't match the local mounter
btrfs-fuse --no-default-permissions /path/to/image.img /mnt/btrfs
```

The mount is always read-only. Pass `--allow-other` to let other users
on the system see the mount.

### As a `btrfs` subcommand

When `btrfs-cli` is built with the opt-in `fuse` feature, the same
mount is available as `btrfs fuse`:

```sh
cargo install --path cli --features fuse
btrfs fuse /path/to/image.img /mnt/btrfs
```

Same flags, same behaviour. The standalone binary stays for
`mount.fuse.btrfs` / `/etc/fstab` integrations that expect a
dedicated executable.

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

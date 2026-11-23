# btrfs-fuse

A userspace FUSE driver for btrfs.

> This crate is **experimental**. Do not use it for anything you care about.
> It should work, and since it is read-only it should not corrupt your data.

`btrfs-fuse` mounts a btrfs image file or block device read-only, without
kernel btrfs support. All filesystem semantics live in [`btrfs-fs`]. This
crate is a thin `fuser::Filesystem` adapter on top of it (inode translation,
`Stat` → `FileAttr` mapping, mount glue).

To embed a btrfs reader in your own code without FUSE, depend on
[`btrfs-fs`] directly.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

[`btrfs-fs`]: https://docs.rs/btrfs-fs

## What's implemented

- Read-only mount of a single btrfs image or block device
- Multi-subvolume traversal. Subvolume crossings are followed automatically,
  and `..` on a subvolume root walks `ROOT_BACKREF`.
- `--subvol PATH` or `--subvolid ID` mounts a specific subvolume as the FUSE
  root
- FUSE operations:
  - `init` (negotiates `FUSE_DO_READDIRPLUS`, `FUSE_AUTO_INVAL_DATA`, and
    splice flags)
  - `forget` (eager cache eviction)
  - `lookup`, `getattr`, `readdir`, `readdirplus`, `read`, `readlink`
  - `listxattr`, `getxattr`, `statfs`
  - `lseek` (`SEEK_HOLE` and `SEEK_DATA`)
  - `ioctl`
- Inline, regular, and preallocated extents (read as zeros), and sparse holes
- zlib, zstd, and lzo decompression (btrfs per-sector LZO framing)
- Symlinks, hardlinks, xattrs, regular files, directories
- 8 btrfs ioctls: `FS_INFO`, `GET_FEATURES`, `GET_SUBVOL_INFO`,
  `DEV_INFO`, `INO_LOOKUP`, `TREE_SEARCH` (v1, fixed 4 KiB),
  `GET_SUBVOL_ROOTREF`, and `TREE_SEARCH_V2`. `TREE_SEARCH_V2` returns
  `ENOPROTOOPT`, so `btrfs-uapi::tree_search_auto` falls back to v1
  transparently.
- Tunable cache sizes via `--cache-tree-blocks N`, `--cache-inodes N`, and
  `--cache-extent-maps N`
- The kernel `default_permissions` mount option is on by default, matching
  kernel btrfs. Opt out with `--no-default-permissions` when inspecting
  images.

## What's not yet implemented

- Any write operation
- Variable-size ioctls that need `FUSE_IOCTL_RETRY` (`INO_PATHS`,
  `LOGICAL_INO_V2`, `SPACE_INFO`, `ENCODED_READ`). The kernel blocks these:
  Linux's `fuse_do_ioctl` only honours retry replies for ioctls issued with
  `FUSE_IOCTL_UNRESTRICTED`, which standard `ioctl(2)` callers never set.
- `BTRFS_IOC_SEND` (the kernel ioctl path). Use `btrfs send --offline IMAGE`
  from `btrfs-cli` to generate send streams from images.
- RAID1, RAID10, RAID5, and RAID6 redundancy handling on degraded devices
- The `--foreground=false` daemonize path (`-f` foreground works)
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

The mount is always read-only. Pass `--allow-other` to let other users on the
system see the mount.

### As a `btrfs` subcommand

When `btrfs-cli` is built with the opt-in `fuse` feature, the same mount is
available as `btrfs fuse`:

```sh
cargo install --path cli --features fuse
btrfs fuse /path/to/image.img /mnt/btrfs
```

It takes the same flags and behaves the same. The standalone binary remains
for `mount.fuse.btrfs` and `/etc/fstab` setups that expect a dedicated
executable.

### As a library

`BtrfsFuse` is a `fuser::Filesystem` impl ready to hand to `fuser::mount2`.
It has no operation methods of its own. Those live on
[`btrfs_fs::Filesystem`][`btrfs-fs`], which is what you should depend on for
embedding.

## Testing

Read-path integration tests live in the [`btrfs-fs`] crate, since the FUSE
adapter has no filesystem logic of its own. Run them with:

```sh
cargo test -p btrfs-fs
```

They require `mkfs.btrfs` (from `btrfs-progs`) on `$PATH`.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or
the [MIT license](LICENSE-MIT), at your option.

# btrfs-mkfs

A Rust implementation of `mkfs.btrfs`. It creates btrfs filesystems on block
devices or image files, with no mounted filesystem required. The only ioctls
it uses are the block device size and discard ioctls, and `FICLONERANGE` for
`--reflink`.

It works in three phases:

1. A hand-built bootstrap writes the four trees that always exist (root,
   extent, chunk, dev) and the superblock with raw `pwrite`.
2. `post_bootstrap` reopens the image with the
   [`btrfs-transaction`](https://gitlab.com/rustutils/btrfsutils/-/tree/master/transaction) crate and creates the other
   always-present trees (FS, csum, data-reloc, UUID, plus FST, BGT, and quota
   when those features are enabled).
3. With `--rootdir`, a third transaction walks the source directory and
   writes items through the transaction crate's high-level helpers.

## Usage

```
btrfs-mkfs [OPTIONS] <DEVICE>...
```

### Examples

```sh
# Create a filesystem on a single device (metadata DUP, data SINGLE)
btrfs-mkfs /dev/sda1

# Create on a file-backed image
truncate -s 1G test.img
btrfs-mkfs test.img

# Two-device RAID1 metadata
btrfs-mkfs /dev/sda1 /dev/sdb1

# Custom label, xxhash checksums, block-group-tree feature
btrfs-mkfs -L myfs --checksum xxhash -O block-group-tree /dev/sda1

# Force overwrite an existing btrfs filesystem
btrfs-mkfs -f /dev/sda1
```

### Options

| Flag | Description |
|------|-------------|
| `-d`, `--data <PROFILE>` | Data block group profile (single, dup, raid0, raid1, raid1c3, raid1c4, raid10, raid5, raid6) |
| `-m`, `--metadata <PROFILE>` | Metadata block group profile (default: DUP on a single device, RAID1 on multiple devices) |
| `-L`, `--label <LABEL>` | Filesystem label (max 255 bytes) |
| `-n`, `--nodesize <SIZE>` | B-tree node size (default 16 KiB, max 64 KiB) |
| `-s`, `--sectorsize <SIZE>` | Sector size (default 4 KiB) |
| `-b`, `--byte-count <SIZE>` | Limit filesystem size per device |
| `--checksum <TYPE>` | Checksum algorithm: crc32c (default), xxhash, sha256, blake2 |
| `-O`, `--features <LIST>` | Comma-separated feature flags (prefix `^` to disable) |
| `-U`, `--uuid <UUID>` | Set filesystem UUID |
| `-f`, `--force` | Force overwrite existing filesystem |
| `-K`, `--nodiscard` | Skip TRIM (discard) before writing |
| `-q`, `--quiet` | Suppress progress output |
| `--rootdir <DIR>` | Populate from an existing directory tree |
| `--compress <ALGO>` | Compress rootdir data (zlib, zstd, lzo) |
| `--subvol <[TYPE:]DIR>` | Create subdirectory as a subvolume (rw, ro, default, default-ro) |
| `--reflink` | Clone extents instead of copying bytes (requires same filesystem) |
| `--shrink` | Truncate image to minimal size after populating |
| `--inode-flags <FLAGS:PATH>` | Set NODATACOW or NODATASUM on specific paths |

## What's implemented

- Single and multi-device filesystems (up to N devices)
- All RAID profiles: SINGLE, DUP, RAID0, RAID1, RAID1C3, RAID1C4, RAID10, RAID5, RAID6
- All four checksum algorithms: CRC32C, xxhash64, SHA256, BLAKE2b
- Quota tree (`-O quota`) and simple quota tree (`-O squota`)
- Free-space-tree and block-group-tree feature flags
- Device validation: mounted check, existing filesystem detection, TRIM
- Minimum device size enforcement (29 MiB for the default layout)
- Default features: extref, skinny-metadata, no-holes, free-space-tree,
  block-group-tree
- `--rootdir` population: regular files (inline and regular extents up to
  1 MiB), directories, symlinks, hardlinks, xattrs, special files
- `--rootdir` compression: zlib, zstd, LZO (per-sector framed format)
- `--rootdir` subvolumes: separate FS trees, ROOT_REF and ROOT_BACKREF, rw,
  ro, and default types
- `--rootdir --reflink`: FICLONERANGE extent cloning
- `--rootdir --shrink`: truncate to the actual used size
- `--rootdir --inode-flags`: NODATACOW or NODATASUM per path
- Output passes `btrfs check` with zero errors

## What's not yet implemented

- Zoned device support
- Mixed data and metadata mode (`-M` is accepted but ignored)

## License

Licensed under the [GNU General Public License v2.0](LICENSE.md).

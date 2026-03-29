# btrfs-mkfs

A Rust implementation of `mkfs.btrfs` that creates btrfs filesystems by
constructing B-tree nodes as raw byte buffers and writing them directly to
block devices or image files with `pwrite`. No ioctls or mounted filesystem
required.

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
| `-d`, `--data <PROFILE>` | Data block group profile (single, dup, raid0, raid1, raid1c3, raid1c4) |
| `-m`, `--metadata <PROFILE>` | Metadata block group profile (default: DUP single-device, RAID1 multi-device) |
| `-L`, `--label <LABEL>` | Filesystem label (max 255 bytes) |
| `-n`, `--nodesize <SIZE>` | B-tree node size (default 16 KiB, max 64 KiB) |
| `-s`, `--sectorsize <SIZE>` | Sector size (default 4 KiB) |
| `-b`, `--byte-count <SIZE>` | Limit filesystem size per device |
| `--checksum <TYPE>` | Checksum algorithm: crc32c (default), xxhash, sha256, blake2 |
| `-O`, `--features <LIST>` | Comma-separated feature flags (prefix `^` to disable) |
| `-U`, `--uuid <UUID>` | Set filesystem UUID |
| `-f`, `--force` | Force overwrite existing filesystem |
| `-K`, `--nodiscard` | Skip TRIM/discard before writing |
| `-q`, `--quiet` | Suppress progress output |

## What's implemented

- Single and multi-device filesystems (up to N devices)
- Metadata profiles: SINGLE, DUP, RAID1, RAID1C3, RAID1C4
- Data profiles: SINGLE, DUP, RAID0, RAID1, RAID1C3, RAID1C4
- All four checksum algorithms: CRC32C, xxhash64, SHA256, BLAKE2b
- Free-space-tree and block-group-tree feature flags
- Device validation: mounted check, existing FS detection, TRIM
- Minimum device size enforcement (~133 MiB)
- Default features: extref, skinny-metadata, no-holes, free-space-tree
- Output passes `btrfs check` with zero errors

## What's not yet implemented

- `--rootdir` (populate filesystem from a directory tree)
- RAID0/5/6/10 profiles
- Zoned device support
- Mixed data+metadata mode (`-M`)

## Testing

Unit tests cover checksum algorithms, superblock construction, and leaf building.
Privileged integration tests create filesystem images, verify them with
`btrfs check`, then mount and exercise read/write operations.

```sh
# Run unit tests
cargo test -p btrfs-mkfs

# Run integration tests (requires root and btrfs-progs installed)
just test
```

## License

Licensed under the [GNU General Public License v2.0](LICENSE.md).

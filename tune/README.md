# btrfs-tune

A Rust implementation of `btrfstune` for modifying btrfs filesystem parameters
on unmounted devices. It writes directly to the on-disk superblock (and, for
full UUID rewrites, to every tree block on the filesystem).

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## Usage

```
btrfs-tune [OPTIONS] <DEVICE>
```

### Examples

```sh
# Enable the extref, skinny-metadata, and no-holes features
btrfs-tune -r -x -n /dev/sda1

# Mark a filesystem as a seed device
btrfs-tune -S 1 /dev/sda1

# Change the visible fsid (lightweight, superblock-only)
btrfs-tune -m /dev/sda1

# Rewrite the fsid in every tree block (full rewrite)
btrfs-tune -u /dev/sda1

# Set a specific UUID via the metadata_uuid mechanism
btrfs-tune -M 12345678-1234-1234-1234-123456789abc /dev/sda1
```

## What's implemented

- **Legacy feature flags** (`-r`, `-x`, `-n`): enable `extref`,
  `skinny-metadata`, or `no-holes` on older filesystems that lack them.
- **Seeding** (`-S 0`/`-S 1`): set or clear the seeding flag for sprouted
  filesystem workflows.
- **Metadata UUID** (`-m`, `-M UUID`): change the user-visible fsid via the
  lightweight metadata_uuid mechanism (superblock-only, no tree walk needed).
- **Full fsid rewrite** (`-u`, `-U UUID`): rewrite the fsid in every tree
  block header and device item on disk, with crash-safety via
  `BTRFS_SUPER_FLAG_CHANGING_FSID`.

## What's not yet implemented

- `--convert-to-free-space-tree` (requires transaction infrastructure)
- `--convert-to-block-group-tree` (requires transaction infrastructure)

## Testing

```sh
cargo test -p btrfs-tune
```

## License

Licensed under [GNU General Public License v2.0](../LICENSE.md).

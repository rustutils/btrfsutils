# btrfs-tune

> This is a pre-1.0 release. The conversion operations
> (`--convert-to-free-space-tree`, `--convert-to-block-group-tree`)
> are experimental: they go through the new `btrfs-transaction`
> crate, which is a clean-room reimplementation and may have edge
> cases that testing doesn't cover. Take a backup before running
> them on filesystems you care about. The other operations
> (feature flags, seeding, UUID changes) are stable.

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

# Convert a filesystem to use the v2 free space tree
btrfs-tune --convert-to-free-space-tree /dev/sda1

# Convert a filesystem to use the block group tree (requires FST first)
btrfs-tune --convert-to-block-group-tree /dev/sda1
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
- **Convert to free space tree** (`--convert-to-free-space-tree`): convert
  an unmounted filesystem to use the v2 free space tree
  (`FREE_SPACE_TREE` compat_ro feature). Built on top of the
  `btrfs-transaction` crate. Simple-case only: refuses if FST is already
  enabled, if a stale FST root is present, or if any v1 free-space-cache
  items remain in the root tree (clear them with
  `btrfs rescue clear-space-cache` first).
- **Convert to block group tree** (`--convert-to-block-group-tree`): convert
  an unmounted filesystem to use the block group tree
  (`BLOCK_GROUP_TREE` compat_ro feature). Requires FST to be enabled
  first (kernel invariant). Can be combined with
  `--convert-to-free-space-tree` in one invocation; both conversions
  then run in sequence.

## What's not yet implemented

All `btrfstune` operations supported by the C reference implementation
are now available.

## License

Licensed under [GNU General Public License v2.0](../LICENSE.md).

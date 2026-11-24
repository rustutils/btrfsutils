# btrfs-tune

> This is a pre-1.0 release. The conversion operations
> (`--convert-to-free-space-tree`, `--convert-to-block-group-tree`) are
> experimental. They run through the new `btrfs-transaction` crate, a
> clean-room reimplementation that may have edge cases tests don't cover.
> Take a backup before running them on filesystems you care about. The other
> operations (feature flags, seeding, UUID changes) are stable.

A Rust implementation of `btrfstune`, which changes btrfs filesystem
parameters on unmounted devices. It writes directly to the on-disk superblock
and, for full UUID rewrites, to every tree block on the filesystem.

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
- **Seeding** (`-S 0`, `-S 1`): set or clear the seeding flag for sprouted
  filesystem workflows.
- **Metadata UUID** (`-m`, `-M UUID`): change the user-visible fsid via the
  lightweight metadata_uuid mechanism. Only the superblock changes, so no
  tree walk is needed.
- **Full fsid rewrite** (`-u`, `-U UUID`): rewrite the fsid in every tree
  block header and device item on disk. `BTRFS_SUPER_FLAG_CHANGING_FSID`
  makes it crash-safe.
- **Convert to free space tree** (`--convert-to-free-space-tree`): convert
  an unmounted filesystem to the v2 free space tree (`FREE_SPACE_TREE`
  compat_ro feature). Built on the `btrfs-transaction` crate. Only the
  simple case is supported. It refuses if FST is already enabled, if a stale
  FST root is present, or if any v1 free-space-cache items remain in the
  root tree (clear them with `btrfs rescue clear-space-cache` first).
- **Convert to block group tree** (`--convert-to-block-group-tree`): convert
  an unmounted filesystem to the block group tree (`BLOCK_GROUP_TREE`
  compat_ro feature). FST must be enabled first (a kernel invariant). It can
  be combined with `--convert-to-free-space-tree` in one invocation, and the
  two conversions then run in sequence.

## What's not yet implemented

All `btrfstune` operations from the C reference implementation are
available.

## License

Licensed under the [GNU General Public License v2.0](https://gitlab.com/rustutils/btrfsutils/-/blob/master/LICENSE.md).

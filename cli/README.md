# btrfs-cli

Implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
command-line tool used to inspect and manage btrfs filesystems, written in
Rust.

This crate builds the `btrfs` binary. If you are a user who wants to manage
btrfs filesystems, install this. If you are a developer building tools on
top of btrfs, depend on `btrfs-uapi`, `btrfs-disk`, or `btrfs-stream`
instead.

```text
btrfs <command> [<subcommand>] [<args>]
```

Most commands require root privileges or `CAP_SYS_ADMIN`.

If you enable the `tune` or `mkfs` features, then the binary will embed
the `btrfs-tune` and `btrfs-mkfs` crates as subcommands.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

### Fully implemented commands

- **balance**: start, pause, cancel, resume, status (with filter string parsing)
- **device**: add, remove/delete, stats, scan, ready, usage
- **filesystem**: df, du, show, sync, defrag, resize, label, usage, mkswapfile, commit-stats
- **inspect-internal**: rootid, inode-resolve, logical-resolve, subvolid-resolve, min-dev-size, list-chunks, dump-super, dump-tree, tree-stats, map-swapfile
- **property**: get, set, list (ro, label, compression)
- **quota**: enable, disable, rescan, status
- **qgroup**: create, destroy, assign, remove, limit, show, clear-stale
- **replace**: start, status, cancel
- **scrub**: start, cancel, resume, status, limit
- **send**: full and incremental, multi-subvolume, protocol v1/v2, --compressed-data, --no-data
- **receive**: v1/v2/v3 streams, encoded write with decompression fallback, --dump, --chroot
- **rescue**: super-recover, zero-log, create-control-device
- **restore**: file recovery from damaged/unmounted filesystems with metadata, xattrs, snapshots, compression, path filtering
- **subvolume**: create, delete, snapshot, show, list, get-default, set-default, get-flags, set-flags, find-new, sync
- **check**: read-only filesystem verification with 7 phases (superblock, tree structure, extent refs, chunk/block group, FS tree inodes, checksum tree, root refs)

### Stubs (argument parsing only)

- **rescue**: fix-device-size, chunk-recover, clear-space-cache, clear-uuid-tree, clear-ino-cache, clear-free-space-tree

### Notable missing flags

- `--offline`: filesystem resize

## Testing

Tests are split into three layers:

- **Argument parsing snapshots**: verify clap parsing for every subcommand using `insta` snapshot tests. No privileges needed.
- **Help text snapshots**: auto-discovered for all subcommands, catch regressions in option descriptions and flag ordering. No privileges needed.
- **CLI integration tests**: exercise real commands against btrfs filesystems on loopback devices. Includes fixture-based tests (read-only against a pre-built image with known content) and live tests (create and mutate real filesystems). Require root.

```sh
# Run unit and snapshot tests (no privileges needed)
cargo test -p btrfs-cli

# Run integration tests (requires root and btrfs-progs installed)
just test
```

## License

Licensed under the [GNU General Public License v2.0](LICENSE.md).

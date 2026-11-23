# btrfs-cli

A Rust implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
command-line tool for inspecting and managing btrfs filesystems.

> This is a pre-1.0 release. Read-only commands are stable. The mutating
> commands built on the new `btrfs-transaction` crate are experimental and
> may have edge cases that tests don't cover. These are offline
> `filesystem resize`, the `rescue` subcommands, and the `tune` conversions
> (with the `tune` feature). Take a backup before running them on
> filesystems you care about.

This crate builds the `btrfs` binary. Install it to manage btrfs filesystems.
To build tools on top of btrfs, depend on `btrfs-uapi`, `btrfs-disk`, or
`btrfs-stream` instead.

```text
btrfs <command> [<subcommand>] [<args>]
```

Most commands require root privileges or `CAP_SYS_ADMIN`.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## Installation

Install the tools as separate binaries:

```sh
cargo install btrfs-cli btrfs-mkfs btrfs-tune
```

Or install a single `btrfs` binary with mkfs, tune, multicall, and fuse
support:

```sh
cargo install btrfs-cli --features mkfs,tune,multicall,fuse
```

The `mkfs`, `tune`, and `fuse` features add the `btrfs mkfs`, `btrfs tune`,
and `btrfs fuse` subcommands. `fuse` is not in the default set because the
FUSE driver is still experimental (see [`btrfs-fuse`](../fuse/README.md)).
The `multicall` feature makes the binary dispatch on its program name:
symlink or hardlink it to `mkfs.btrfs`, `btrfs-mkfs`, `btrfstune`, or
`btrfs-tune` and it runs that tool.

## What's implemented

### Fully implemented commands

- **balance**: start, pause, cancel, resume, status (with filter string parsing)
- **device**: add, remove (or delete), stats, scan, ready, usage
- **filesystem**: df, du, show, sync, defrag, resize, label, usage, mkswapfile, commit-stats
- **inspect-internal**: rootid, inode-resolve, logical-resolve, subvolid-resolve, min-dev-size, list-chunks, dump-super, dump-tree, tree-stats, map-swapfile
- **property**: get, set, list (ro, label, compression)
- **quota**: enable, disable, rescan, status
- **qgroup**: create, destroy, assign, remove, limit, show, clear-stale
- **replace**: start, status, cancel
- **scrub**: start, cancel, resume, status, limit
- **send**: full and incremental, multi-subvolume, protocol v1 and v2, --compressed-data, --no-data. `--offline IMAGE` generates a v1 send stream from an unmounted image with no privileges (full sends only).
- **receive**: v1, v2, and v3 streams, encoded write with decompression fallback, --dump, --chroot
- **reflink**: clone, a lightweight file copy via `FICLONERANGE`. Copies the whole file, or one or more ranges given as `-r SRCOFF:LENGTH:DESTOFF`.
- **rescue**: super-recover, zero-log, create-control-device, fix-device-size, fix-data-checksum, clear-uuid-tree, clear-space-cache (v1 and v2), clear-ino-cache, chunk-recover. chunk-recover only scans and reports by default. `--apply` writes the rebuilt chunk tree through the transaction crate.
- **restore**: file recovery from damaged or unmounted filesystems, with metadata, xattrs, snapshots, compression, and path filtering
- **subvolume**: create, delete, snapshot, show, list, get-default, set-default, get-flags, set-flags, find-new, sync
- **check**: read-only filesystem verification in 7 phases (superblock, tree structure, extent refs, chunks and block groups, FS tree inodes, checksum tree, root refs)
- **fuse** (opt-in `fuse` feature): mount a btrfs image or block device read-only via FUSE. Takes the same flags as the standalone `btrfs-fuse` binary.

### Notable missing flags

- `--offline`: filesystem resize

## Testing

Tests come in three layers:

- **Argument parsing snapshots**: check clap parsing for every subcommand with `insta` snapshot tests. No privileges needed.
- **Help text snapshots**: auto-discovered for all subcommands. They catch regressions in option descriptions and flag ordering. No privileges needed.
- **CLI integration tests**: run real commands against btrfs filesystems on loopback devices. Fixture tests run read-only against a pre-built image with known content. Live tests create and mutate real filesystems. Require root.

```sh
# Run unit and snapshot tests (no privileges needed)
cargo test -p btrfs-cli

# Run integration tests (requires root and btrfs-progs installed)
just test
```

## License

Licensed under the [GNU General Public License v2.0](LICENSE.md).

# btrfs-cli

Implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
command-line tool used to inspect and manage btrfs filesystems, written in
Rust.

> This is a pre-1.0 release. Read-only commands are stable. The
> mutating commands that go through the new `btrfs-transaction`
> crate (offline `filesystem resize`, the `rescue` subcommands,
> and the `tune` conversions when built with the `tune` feature)
> are experimental and may have edge cases that testing doesn't
> cover. Take a backup before running them on filesystems you
> care about.

This crate builds the `btrfs` binary. If you are a user who wants to manage
btrfs filesystems, install this. If you are a developer building tools on
top of btrfs, depend on `btrfs-uapi`, `btrfs-disk`, or `btrfs-stream`
instead.

```text
btrfs <command> [<subcommand>] [<args>]
```

Most commands require root privileges or `CAP_SYS_ADMIN`.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## Installation

Install the individual tools as separate binaries:

```sh
cargo install btrfs-cli btrfs-mkfs btrfs-tune
```

Or install a single `btrfs` binary with mkfs, tune, multicall, and
fuse support:

```sh
cargo install btrfs-cli --features mkfs,tune,multicall,fuse
```

With the `mkfs`, `tune`, and `fuse` features, `btrfs mkfs`,
`btrfs tune`, and `btrfs fuse` are available as subcommands. The
`fuse` feature is opt-in (and not in the default set) because the
FUSE driver is still experimental — see
[`btrfs-fuse`](../fuse/README.md). With the `multicall` feature,
the binary also dispatches by program name: symlink or hardlink it
to `mkfs.btrfs`, `btrfs-mkfs`, `btrfstune`, or `btrfs-tune` and it
will behave as that tool directly.

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
- **send**: full and incremental, multi-subvolume, protocol v1/v2, --compressed-data, --no-data, plus `--offline IMAGE` to generate a v1 send stream from an unmounted image with no privileges (tier 1: full sends only)
- **receive**: v1/v2/v3 streams, encoded write with decompression fallback, --dump, --chroot
- **reflink**: clone (lightweight file copy via `FICLONERANGE` — whole file or per-range, multiple `-r SRCOFF:LENGTH:DESTOFF` ranges in one invocation)
- **rescue**: super-recover, zero-log, create-control-device, fix-device-size, fix-data-checksum, clear-uuid-tree, clear-space-cache (v1 and v2), clear-ino-cache
- **restore**: file recovery from damaged/unmounted filesystems with metadata, xattrs, snapshots, compression, path filtering
- **subvolume**: create, delete, snapshot, show, list, get-default, set-default, get-flags, set-flags, find-new, sync
- **check**: read-only filesystem verification with 7 phases (superblock, tree structure, extent refs, chunk/block group, FS tree inodes, checksum tree, root refs)
- **fuse** (opt-in `fuse` feature): mount a btrfs image or block device read-only via FUSE; mirrors the standalone `btrfs-fuse` binary's flag set

### Stubs (argument parsing only)

- **rescue**: chunk-recover

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

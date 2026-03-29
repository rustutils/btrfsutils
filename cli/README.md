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

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

## What's implemented

### Fully implemented commands

- **balance**: start, pause, cancel, resume, status (with filter string parsing)
- **device**: add, remove/delete, stats, scan, ready, usage
- **filesystem**: df, du, show, sync, defrag, resize, label, usage, mkswapfile, commit-stats
- **inspect-internal**: rootid, inode-resolve, logical-resolve, subvolid-resolve, min-dev-size, list-chunks, dump-super, dump-tree
- **property**: get, set, list (ro, label, compression)
- **quota**: enable, disable, rescan, status
- **qgroup**: create, destroy, assign, remove, limit, show, clear-stale
- **replace**: start, status, cancel
- **scrub**: start, cancel, resume, status, limit
- **send**: full and incremental, multi-subvolume, protocol v1/v2, --compressed-data, --no-data
- **receive**: v1/v2/v3 streams, encoded write with decompression fallback, --dump, --chroot
- **subvolume**: create, delete, snapshot, show, list, get-default, set-default, get-flags, set-flags, find-new, sync

### Stubs (argument parsing only)

- **check**: full arg parsing scaffolded
- **restore**: full arg parsing scaffolded
- **rescue**: all 9 subcommands scaffolded
- **inspect-internal**: tree-stats, map-swapfile

### Notable missing flags

- `--format` (JSON output): device stats, filesystem df, qgroup show
- `--offline`: device stats, filesystem resize

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

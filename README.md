# btrfs-progrs

An alternative implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs) command-line tool, written in Rust.

## Status

This project is under active development. Most commands are fully implemented and
produce output matching the C original.

## Building

Requires a Rust toolchain (edition 2024) and Linux kernel headers.

```
cargo build --release
```

The resulting binary is `target/release/btrfs`.

## Usage

```
btrfs <command> [<args>]
```

The command structure mirrors the original `btrfs` tool:

```
btrfs filesystem show
btrfs subvolume list /mnt/data
btrfs device stats /mnt/data
btrfs scrub start /mnt/data
```

Most commands that talk to the kernel require root privileges or `CAP_SYS_ADMIN`.

## Folder structure

| Folder | Description |
|-------|-------------|
| `uapi` | Safe Rust wrappers around btrfs kernel ioctls, sysfs, and procfs. Linux-only. |
| `disk` | Platform-independent parsing of btrfs on-disk structures (superblocks, tree nodes, etc.) from block devices or image files. |
| `stream` | Send stream parser and receive operations. Platform-independent parser with optional Linux-only receive support. |
| `cli` | The command-line tool, built on top of `uapi`, `disk`, and `stream`. |
| `mkfs` | Filesystem creation tool (`mkfs.btrfs`). Constructs on-disk B-tree nodes and writes them directly to block devices or image files. |
| `mangen` | Man page generator. Uses `clap_mangen` to produce roff man pages for all commands. |
| `docs` | Documentation, rendered with mdBook. |

Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to see
what is available.

## Testing

Integration tests require root privileges and recent Linux kernel. They work by
creating file-backed btrfs filesystems, mounting them, and testing the operations
in there. To run them, use the justfile target (assuming you have `just` installed):

```
just test
```

You can generate a coverage report as well, assuming you have `cargo-llvm-cov`
installed.

```
just coverage
```

## License

This project is licensed under the GNU General Public License v2.0. See [LICENSE.md](LICENSE.md)
for the full text.

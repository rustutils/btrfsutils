# btrfs-progrs

A Rust reimplementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs) command-line tool.

## Status

This project is under active development. Many commands are fully implemented and
produce output matching the C original. See the [implementation status](#implemented-commands)
below for details.

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

## Crate structure

| Crate | Description |
|-------|-------------|
| `btrfs-uapi` | Safe Rust wrappers around btrfs kernel ioctls, sysfs, and procfs. Linux-only. |
| `btrfs-disk` | Platform-independent parsing of btrfs on-disk structures (superblocks, tree nodes, etc.) from block devices or image files. |
| `btrfs-cli` | The command-line tool, built on top of `uapi` and `disk`. |

Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to see
what is available.

## Testing

Integration tests require root privileges and a real btrfs filesystem (no mocks).

```
just test    # integration tests, builds as user, runs with sudo
```

## License

This project is licensed under the GNU General Public License v2.0. See [LICENSE.md](LICENSE.md)
for the full text.

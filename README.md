# btrfsutils

An implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
utilities for creating, managing and inspecting btrfs filesystems, written in
Rust.

This project contains low-level libraries for interacting with the btrfs kernel
driver (`btrfs-uapi`), parsing and writing on-disk data structures
(`btrfs-disk`), parsing and handling btrfs send streams (`btrfs-stream`).  The
goal for these is to be useful libraries that can be used in other projects to
interact with btrfs filesystems programmatically.

It also contains high-level CLI crates (`btrfs-cli` for the `btrfs` utility,
and `btrfs-mkfs` for the `mkfs.btrfs` utility) that are compatible with the
respective utilities from `btrfs-progs`. These aim to be drop-in replacements,
but may be missing some advanced features or have a simpler implementation.

## Status

This project is under active development. Most commands are fully implemented
and produce output matching the C original.

Currently, `btrfs check`, `btrfs restore` and `btrfs rescue` are not
implemented, and exist only as stubs.

## Building

Requires a Rust toolchain (edition 2024) and Linux kernel headers.

```
cargo build --release
```

The resulting binary is `target/release/btrfs`.

You can also build this project using Nix. The output includes the `btrfs`
binary, the `mkfs.btrfs` binary, and compressed man pages.

```
nix build
```

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
| `disk` | Platform-independent parsing and serialization of btrfs on-disk structures. Used by `cli` for dump-super/dump-tree and by `mkfs` for filesystem creation. |
| `stream` | Send stream parser and receive operations. Platform-independent parser with optional Linux-only receive support. |
| `cli` | The command-line tool, built on top of `uapi`, `disk`, and `stream`. |
| `mkfs` | Filesystem creation tool (`mkfs.btrfs`). Constructs on-disk B-tree nodes and writes them directly to block devices or image files. |
| `util/gen` | Man page and shell completion generator. Uses `clap_mangen` and `clap_complete` to produce roff man pages and bash/zsh/fish/elvish completions. |
| `docs` | Documentation, rendered with mdBook. |

## Testing

To run unit tests (this will not run any tests that require superuser
privileges):

```
cargo test
```

For the integration tests, due to the fact that they interact with the kernel
and will test privileged operations (many tests create and mount a file-backed
btrfs filesystem), they require superuser privileges. Because running `sudo cargo test` is generally a bad idea, this repository has a wrapper that will build
tests (as your user), and then run only integration tests with `sudo`. This
is the recommended way to run the entire test suite.

```
just test
```

You can generate a coverage report as well, assuming you have `cargo-llvm-cov`
installed. This uses the same functionality as the `just test`.

```
just coverage
```

## License

The library crates (`btrfs-uapi`, `btrfs-disk`, `btrfs-stream`) are original
work that implement parsers for on-disk data structures, shims around kernel
syscalls, and parsers for the send stream wire protocol. They are licensed
under either of [Apache License, Version 2.0](uapi/LICENSE-APACHE) or
[MIT license](uapi/LICENSE-MIT) at your option.

The application crates (`btrfs-cli`, `btrfs-mkfs`) are inspired by the
[btrfs-progs](https://github.com/kdave/btrfs-progs) C implementation and are
licensed under the [GNU General Public License v2.0](LICENSE.md).

Note: the kernel UAPI header files included in the repository for bindgen
code generation are licensed separately under GPL-2.0 with the Linux syscall
note exception, which permits their use by non-GPL userspace programs.

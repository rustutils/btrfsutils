# btrfsutils

An implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
utilities for creating, managing and inspecting btrfs filesystems, written in
Rust.

> Warning: some of the implemented functionality in this repository is
> considered experimental. Specifically, the `btrfs-transaction` is
> experimental, because it is based off a clean-room reimplementation, so it
> may have edge cases that testing doesn't cover. Features which rely on it
> (`btrfs-tune`, the `btrfs rescue` subcommands) are as well. 

This project contains low-level libraries for interacting with the btrfs kernel
driver (`btrfs-uapi`), parsing and writing on-disk data structures
(`btrfs-disk`), parsing and handling btrfs send streams (`btrfs-stream`), and
performing transactional read-write modifications to unmounted filesystems
(`btrfs-transaction`). The goal for these is to be useful libraries that can
be used in other projects to interact with btrfs filesystems programmatically.

It also contains high-level CLI crates (`btrfs-cli` for the `btrfs` utility,
and `btrfs-mkfs` for the `mkfs.btrfs` utility, `btrfs-tune` for the `btrfstune`
utility) that are compatible with the respective utilities from `btrfs-progs`.
These aim to be drop-in replacements, but may be missing some advanced features
or have a simpler implementation.

## Goals

- **Drop-in compatibility.** The goal is for the output of every command to
  match btrfs-progs exactly. Scripts, monitoring tools, and muscle memory all
  work unchanged.
- **Opt-in added features**. Pass `--format modern` or set `BTRFS_OUTPUT_FORMAT=modern`
  to opt in to cleaner-looking output, progress bars, adaptive column widths.
- **Reusable libraries.** Bring btrfs to Rust through the low-level crates
  (`btrfs-uapi`, `btrfs-disk`, `btrfs-stream`, `btrfs-transaction`) that you
  can use in your own code. Permissively licensed under MIT/Apache-2.0, and
  written from scratch to be ergonomic.

## Status

This project is under active development. Most commands are fully implemented
and produce output matching the C original.

`btrfs check` is fully implemented with 7-phase read-only filesystem
verification. `btrfs rescue` is implemented for every subcommand
except `chunk-recover`, which remains a stub.

The `btrfs-transaction` crate provides COW-correct read-write access
to unmounted filesystems, including delayed-ref bookkeeping, free
space tree updates, chunk tree COW, and full-tree conversions for
the v2 free space tree and the block group tree. It backs the
offline `btrfs filesystem resize`, the rescue commands, and the
`btrfs-tune --convert-to-free-space-tree` /
`--convert-to-block-group-tree` operations.

## Installation

Currently, we provide three executables: `btrfs` (the main btrfs CLI, use it to
configure and monitor btrfs filesystems), `btrfs-tune` (use it to modify feature flags, UUIDs, and seeding on unmounted filesystems),
and `btrfs-mkfs` (use it to format drives as btrfs filesystems, or bootrap
filesystem images from folder content).

You can install the individual tools as separate binaries:

```sh
cargo install btrfs-cli btrfs-mkfs btrfs-tune
```

You can also install just the `btrfs` CLI tool, but embed the other binaries as
subcommands, using the `mkfs` and `tune` features. The `multicall` feature
means that you can use them busybox-style: symlink the binary to `mkfs.btrfs`,
and it will run that tool by default.

```sh
cargo install btrfs-cli --features mkfs,tune,multicall
```

With the `mkfs` and `tune` features, `btrfs mkfs` and `btrfs tune` are
available as subcommands. With the `multicall` feature, the binary also
dispatches by program name: symlink or hardlink it to `mkfs.btrfs`,
`btrfs-mkfs`, `btrfstune`, or `btrfs-tune` and it will behave as that tool
directly.

This installation mode is useful in combination with building statically for
`x86_64-unknown-linux-musl`, if you want a single portable binary to deploy to
systems.

## Building

Requires a Rust toolchain (edition 2024) and Linux kernel headers.

```
cargo build --release
```

The resulting binaries are `target/release/btrfs`, `target/release/btrfs-mkfs`,
and `target/release/btrfs-tune`.

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
| `transaction` | Transactional read-write access to unmounted filesystems. COW-correct tree edits, delayed-ref bookkeeping, free space tree and chunk tree updates, full-tree conversions. Platform-independent. |
| `cli` | The command-line tool, built on top of `uapi`, `disk`, `stream`, and `transaction`. |
| `mkfs` | Filesystem creation tool (`mkfs.btrfs`). Constructs on-disk B-tree nodes and writes them directly to block devices or image files. |
| `tune` | Offline superblock tuning tool (`btrfstune`). Modifies feature flags, seeding, and filesystem UUIDs on unmounted devices. |
| `fuse` *(experimental)* | Userspace FUSE driver (`btrfs-fuse`) built on `btrfs-disk`. Read-only mount of images and block devices, no kernel btrfs required. |
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
btrfs filesystem), they require superuser privileges. Because running
`sudo cargo test` is generally a bad idea, this repository has a wrapper that
will build tests (as your user), and then run only integration tests with
`sudo`. This is the recommended way to run the entire test suite.

```
just test
```

You can generate a coverage report as well, assuming you have `cargo-llvm-cov`
installed. This uses the same functionality as the `just test`.

```
just coverage
```

For more details on the test layout (unit vs. integration vs. snapshot tests,
fixtures, the `cargo insta` workflow, and how the privileged harness is
wired up), see the
[testing guide](https://rustutils.gitlab.io/btrfsutils/dev/testing.html) in
the developer docs.

## License

The library crates (`btrfs-uapi`, `btrfs-disk`, `btrfs-stream`,
`btrfs-transaction`) are original work that implement parsers for on-disk
data structures, shims around kernel syscalls, parsers for the send stream
wire protocol, and a clean-room transactional read-write engine for
unmounted filesystems. They are licensed under either of
[Apache License, Version 2.0](uapi/LICENSE-APACHE) or
[MIT license](uapi/LICENSE-MIT) at your option. This license allows them
be used easily in other Rust crates.

The application crates (`btrfs-cli`, `btrfs-mkfs`, `btrfs-tune`) are inspired by the
[btrfs-progs](https://github.com/kdave/btrfs-progs) C implementation and are
licensed under the [GNU General Public License v2.0](LICENSE.md).

Note: the kernel UAPI header files included in the repository for bindgen
code generation are licensed separately under GPL-2.0 with the Linux syscall
note exception, which permits their use by non-GPL userspace programs.

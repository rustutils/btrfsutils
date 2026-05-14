# Installation

While these tools are still in their beta (pre-1.0 release) phase, you can
already install them and try them out.

## Pre-built packages

Each tagged release publishes statically-linked artifacts for `x86_64`,
`aarch64`, and `riscv64gc` Linux musl targets on the [GitLab releases
page](https://gitlab.com/rustutils/btrfsutils/-/releases) and mirrored to
[GitHub](https://github.com/rustutils/btrfsutils/releases). Each release
includes per-architecture:

- `btrfsutils_<ver>_<arch>.deb` — Debian/Ubuntu package
- `btrfsutils-<ver>-1.<arch>.rpm` — Fedora/RHEL/openSUSE package
- `btrfsutils-<arch>.tar.zst` — relocatable tarball (extracts into
  `btrfsutils-<arch>/{bin,README.md,LICENSE.md,CHANGELOG.md}`)
- `btrfs-<arch>.zst` — bare zstd-compressed multicall binary

The `.deb` and `.rpm` install a single `btrfs` multicall binary at
`/usr/bin/btrfs` plus `mkfs.btrfs` / `btrfstune` / `btrfs-mkfs` / `btrfs-tune`
symlinks that dispatch by `argv[0]`.

## Cargo

If you have cargo installed, you can install the utilities as separate
binaries:

```sh
cargo install btrfs-cli btrfs-mkfs btrfs-tune
```

Or install just `btrfs-cli` with `mkfs`, `tune`, `fuse`, and `multicall`
embedded — produces a single binary that dispatches by `argv[0]`:

```sh
cargo install btrfs-cli --features mkfs,tune,fuse,multicall
```

Then symlink `mkfs.btrfs`, `btrfstune`, etc. to the installed `btrfs`
binary.

## Nix

If you use Nix with flakes enabled, you can run the tool directly without
installing it:

```sh
nix run github:rustutils/btrfsutils -- filesystem show /mnt
```

Or install it into your profile:

```sh
nix profile install github:rustutils/btrfsutils
```

## From source

See [Building from Source](building.md) for instructions on compiling btrfsutils
yourself from the repository.

## Requirements

btrfsutils runs on Linux. Most commands that interact with a mounted filesystem
require `CAP_SYS_ADMIN` (i.e. root, or a process with that capability granted).
The exceptions are `btrfs inspect-internal dump-super` and `dump-tree`, which
only require read access to the block device or image file.

# Installation

The tools are in beta (pre-1.0), but you can install them and try them out.

## Pre-built packages

Each tagged release publishes statically linked artifacts for the `x86_64`,
`aarch64`, and `riscv64gc` Linux musl targets. They are on the [GitLab releases
page](https://gitlab.com/rustutils/btrfsutils/-/releases), mirrored to
[GitHub](https://github.com/rustutils/btrfsutils/releases). For each
architecture, a release includes:

- `btrfsutils_<ver>_<arch>.deb`: Debian and Ubuntu package
- `btrfsutils-<ver>-1.<arch>.rpm`: Fedora, RHEL, and openSUSE package
- `btrfsutils-<arch>.tar.zst`: relocatable tarball (extracts into
  `btrfsutils-<arch>/{bin,README.md,LICENSE.md,CHANGELOG.md}`)
- `btrfs-<arch>.zst`: bare zstd-compressed multicall binary

The `.deb` and `.rpm` install a single `btrfs` multicall binary at
`/usr/bin/btrfs`, plus `mkfs.btrfs`, `btrfstune`, `btrfs-mkfs`, and
`btrfs-tune` symlinks that dispatch on `argv[0]`. The packaged binary is
built with the `fuse` feature, so it also provides `btrfs fuse`.

## Cargo

Install the tools as separate binaries:

```sh
cargo install btrfs-cli btrfs-mkfs btrfs-tune btrfs-fuse
```

Or install only `btrfs-cli` with `mkfs`, `tune`, `fuse`, and `multicall`
embedded. This produces a single binary that dispatches on `argv[0]`:

```sh
cargo install btrfs-cli --features mkfs,tune,fuse,multicall
```

Then symlink `mkfs.btrfs`, `btrfstune`, etc. to the installed `btrfs`
binary.

## Nix

With flakes enabled, you can run the tool without installing it:

```sh
nix run github:rustutils/btrfsutils -- filesystem show /mnt
```

Or install it into your profile:

```sh
nix profile install github:rustutils/btrfsutils
```

## From source

See [Building from Source](building.md).

## Requirements

btrfsutils runs on Linux. Most commands that work on a mounted filesystem
require `CAP_SYS_ADMIN`: run them as root or grant the process that capability.
The exceptions are commands that read a block device or image file directly,
such as `btrfs inspect-internal dump-super`, `dump-tree`, and `tree-stats`.
They only need read access to the device or file.

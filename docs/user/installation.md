# Installation

While these tools are still in their beta (pre-1.0 release) phase, you can
already install them and try them out. Currently, the recommended way to
install them is using Cargo, there are no binary builds to download.

## Cargo

If you have cargo installed, you can install the utilities with it.

```sh
cargo install btrfs-cli
cargo install btrfs-tune
cargo install btrfs-mkfs
```

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

# Installation

## From source

See [Building from Source](building.md) for instructions on compiling btrfs-progrs
yourself.

## Nix

If you use Nix with flakes enabled, you can run the tool directly without
installing it:

```sh
nix run github:rustprojectprimer/btrfs-progrs -- filesystem show /mnt
```

Or install it into your profile:

```sh
nix profile install github:rustprojectprimer/btrfs-progrs
```

## Requirements

btrfs-progrs runs on Linux. Most commands that interact with a mounted filesystem
require `CAP_SYS_ADMIN` (i.e. root, or a process with that capability granted).
The exceptions are `btrfs inspect-internal dump-super` and `dump-tree`, which
only require read access to the block device or image file.

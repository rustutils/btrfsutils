# Introduction

btrfsutils is a Rust implementation of the btrfs filesystem utilities. It
provides three command-line tools: `btrfs`, for managing and inspecting
btrfs filesystems; `mkfs.btrfs`, for creating new ones; and `btrfstune`, for
offline superblock tuning. All three aim to be drop-in replacements for the
tools provided by [btrfs-progs](https://github.com/kdave/btrfs-progs).

Most commands are fully implemented and produce output matching the C reference.
`btrfs check` is not yet implemented. `btrfs rescue` has three working
subcommands (super-recover, zero-log, create-control-device) with six more
still stubbed.

The source is available at
[github.com/rustprojectprimer/btrfsutils](https://github.com/rustprojectprimer/btrfsutils).

# Introduction

btrfs-progrs is a Rust implementation of the btrfs filesystem utilities. It
provides two command-line tools: `btrfs`, for managing and inspecting mounted
btrfs filesystems, and `mkfs.btrfs`, for creating new ones. Both aim to be
drop-in replacements for the tools provided by
[btrfs-progs](https://github.com/kdave/btrfs-progs).

Most commands are fully implemented and produce output matching the C reference.
`btrfs check`, `btrfs restore`, and `btrfs rescue` are not yet implemented.

The source is available at
[github.com/rustprojectprimer/btrfs-progrs](https://github.com/rustprojectprimer/btrfs-progrs).

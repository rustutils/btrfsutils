# Architecture

## Crate structure

The project follows a strict layering: lower crates have no knowledge of the
layers above them.

<center>
 
![Architecture diagram](architecture.svg)

</center>

`btrfs-uapi` wraps kernel ioctls, sysfs reads, and procfs reads into safe Rust
APIs. It is Linux-only and the only crate that talks directly to the kernel.

`btrfs-disk` parses on-disk structures — superblocks, B-tree nodes, item payloads
— from raw byte buffers. It is platform-independent and does not depend on
`btrfs-uapi`, so it can be used to inspect filesystem images on any OS.

`btrfs-stream` parses the btrfs send stream wire format. The core parser is
platform-independent. The optional `receive` feature is Linux-only and applies a
parsed stream to a mounted filesystem via `btrfs-uapi`.

`btrfs-cli` is the `btrfs` tool. It handles argument parsing via clap, calls into
`btrfs-uapi` and `btrfs-disk` as needed, and formats all output.

`btrfs-mkfs` is the `mkfs.btrfs` tool. It constructs B-tree nodes as raw byte
buffers and writes them directly to a block device or image file using `pwrite`.
It does not use ioctls.

## The two-layer model

Every feature that involves kernel communication is split across two layers.
The `uapi/` layer provides a safe Rust function: it takes typed arguments, calls
the ioctl, and returns a typed result, with no `unsafe` in the public API and no
knowledge of CLI concerns. The `cli/` layer provides a clap subcommand that calls
into `uapi/` and formats the result for the user, with no ioctl calls or raw
kernel types.

This rule applies to all kernel interfaces — btrfs ioctls, standard VFS ioctls
like `FS_IOC_FIEMAP`, and block device ioctls like `BLKGETSIZE64` all live in
`uapi/`, never in `cli/`.

The same principle applies to `disk/`: it parses raw bytes into typed structs,
and `cli/` handles all display formatting. The `disk/` crate never calls
`println!`.

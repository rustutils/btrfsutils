# Architecture

## Crate structure

The project follows a strict layering: lower crates have no knowledge of the
layers above them.

<center>
 
![Architecture diagram](architecture.svg)

</center>

`btrfs-uapi` wraps kernel ioctls, sysfs reads, and procfs reads into safe
Rust APIs. It is Linux-only and the only crate that talks directly to the
kernel.

`btrfs-disk` parses on-disk structures — superblocks, B-tree nodes, item
payloads — from raw byte buffers. It is platform-independent and does not
depend on `btrfs-uapi`, so it can be used to inspect filesystem images on
any OS.

`btrfs-stream` parses the btrfs send stream wire format and encodes
`StreamCommand` values back into it. The core parser and encoder are
platform-independent. The optional `receive` feature is Linux-only and
applies a parsed stream to a mounted filesystem via `btrfs-uapi`.

`btrfs-transaction` provides COW-correct read-write access to unmounted
filesystems, on top of `btrfs-disk`. Includes delayed-ref bookkeeping,
free space tree updates, chunk tree COW, and full-tree conversions for
the v2 free space tree and the block group tree. Platform-independent.
Backs offline `btrfs filesystem resize`, the rescue commands, and the
`btrfs-tune` conversions.

`btrfs-fs` is a high-level read-only filesystem API on top of `btrfs-disk`.
Exposes a `Filesystem<R>` handle with POSIX-shaped operations (`lookup`,
`readdir`/`readdirplus`, `read`, `xattr_get`, `seek_hole_data`, ...) plus
btrfs-specific ones (`tree_search`, `ino_paths`, `list_subvolumes`,
`send`). Inodes are modelled as `(SubvolId, ino)` so multi-subvolume
traversal works. All ops are `async fn` running sync I/O via
`spawn_blocking`. FUSE-independent — drives the `btrfs-fuse` mount, the
offline `btrfs send --offline` path, and any other embedder.

`btrfs-mkfs` implements the `mkfs.btrfs` tool. It constructs B-tree nodes as
raw byte buffers and writes them directly to a block device or image file
using `pwrite`.  It does not use ioctls.

`btrfs-tune` implements the `btrfstune` tool. It modifies on-disk superblock
parameters (feature flags, seeding, filesystem UUIDs) on unmounted devices.
For lightweight UUID changes it only rewrites the superblock; for full fsid
rewrites it traverses every tree block on disk via `btrfs-disk`. The
`--convert-to-free-space-tree` and `--convert-to-block-group-tree`
conversions run through `btrfs-transaction`.

`btrfs-fuse` is a thin `fuser::Filesystem` adapter on top of `btrfs-fs` —
all filesystem semantics live in `btrfs-fs`; this crate adds the FUSE
protocol mapping (inode-number translation, `Stat` → `FileAttr`,
`Reply*` glue) and a tokio multi-thread runtime so concurrent FUSE
callbacks don't serialise. Available as the standalone `btrfs-fuse`
binary or as the `btrfs fuse` subcommand behind the opt-in `fuse` cargo
feature on `btrfs-cli`.

`btrfs-cli` implements the `btrfs` tool. It handles argument parsing via
clap, calls into `btrfs-uapi`, `btrfs-disk`, `btrfs-stream`, `btrfs-fs`,
and `btrfs-transaction` as needed, and formats all output. Optionally,
this tool can also embed the `btrfs-mkfs`, `btrfs-tune`, and `btrfs-fuse`
tools as subcommands, for easier single-file deployment.

## The two-layer model

Every feature that involves kernel communication is split across two layers.
The `uapi/` layer provides a safe Rust function: it takes typed arguments,
calls the ioctl, and returns a typed result, with no `unsafe` in the public
API and no knowledge of CLI concerns. The `cli/` layer provides a clap
subcommand that calls into `uapi/` and formats the result for the user, with
no ioctl calls or raw kernel types.

This rule applies to all kernel interfaces — btrfs ioctls, standard VFS
ioctls like `FS_IOC_FIEMAP`, and block device ioctls like `BLKGETSIZE64` all
live in `uapi/`, never in `cli/`.

The same principle applies to `disk/`: it parses raw bytes into typed
structs, and `cli/` handles all display formatting. The `disk/` crate never
calls `println!`.

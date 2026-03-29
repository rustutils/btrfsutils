# Send and Receive

`btrfs send` and `btrfs receive` transfer filesystem state between two btrfs
filesystems as a byte stream. This page explains how the mechanism works and how
to use the `btrfs-stream` and `btrfs-uapi` crates to implement receive in your
own application.

## How send works

`btrfs send` asks the kernel to generate a stream representing the contents of a
read-only subvolume. The kernel traverses the subvolume's B-trees and emits a
sequence of commands describing every file, directory, symlink, and extent. For
an incremental send (with `-p <parent>`), only the differences from the parent
subvolume are emitted.

The kernel is invoked via `BTRFS_IOC_SEND`, which writes the stream to a file
descriptor (typically the write end of a pipe). A reader thread on the other end
consumes the stream and writes it to a file or stdout.

## The stream format

The stream is a binary format consisting of a header followed by a sequence of
commands.

The stream header identifies the format version (v1, v2, or v3) and contains a
magic number (`btrfs-stream\0`). After the header, commands follow
back-to-back until an `END` command signals completion.

Each command has the following structure:

```
u32  total_length    (length of the entire command, including this header)
u16  command_type    (BTRFS_SEND_C_* constant)
u32  crc32c          (checksum of the command, with the crc field zeroed)
     attributes...   (variable-length TLV list)
```

Attributes are TLV-encoded:

```
u16  attribute_type  (BTRFS_SEND_A_* constant)
u16  length
     data...
```

The CRC32C used by btrfs is the *raw* variant (initial seed 0, no final XOR),
not the standard ISO 3309 variant (initial seed `0xFFFFFFFF`). When computing
or verifying a checksum, use:

```rust
let crc = !crc32c::crc32c_append(!0u32, data);
```

## Parsing a stream with `btrfs-stream`

The `btrfs-stream` crate provides `StreamReader`, which parses commands one at a
time from any `Read` source:

```rust
use btrfs_stream::{StreamReader, StreamCommand};

let mut reader = StreamReader::new(input)?; // reads and validates the header
while let Some(command) = reader.read_command()? {
    match command {
        StreamCommand::Subvol { path, uuid, ctransid } => { /* create subvolume */ }
        StreamCommand::MkFile { path } => { /* create file */ }
        StreamCommand::Write { path, offset, data } => { /* write data */ }
        StreamCommand::Rename { path, path_to } => { /* rename */ }
        StreamCommand::End => break,
        // ... all 22+ command types
    }
}
```

`StreamReader::new` reads the stream header and returns an error if the magic is
wrong or the version is unsupported. `read_command` returns `None` at EOF.

## Applying a stream with `btrfs-uapi`

To implement receive, you need to apply each command to a mounted btrfs
filesystem. The relevant operations are:

**Subvolume and snapshot creation** (`BTRFS_IOC_SUBVOL_CREATE`,
`BTRFS_IOC_SNAP_CREATE_V2`): for `Subvol` commands, create a new empty
subvolume. For `Snapshot` commands, look up the source subvolume by UUID using
`subvolume_search_by_received_uuid` or `subvolume_search_by_uuid`, then create a
writable snapshot.

**File operations**: standard POSIX calls — `open`/`create`, `unlink`, `mkdir`,
`rmdir`, `symlink`, `link`, `rename`. btrfs does not require any special ioctls
for these.

**Write** (`BTRFS_IOC_ENCODED_WRITE` or `pwrite`): v2 streams may send
pre-compressed data via `ENCODED_WRITE`. If the kernel supports it, this can be
passed directly; otherwise decompress and fall back to `pwrite`.

**Clone** (`BTRFS_IOC_CLONE_RANGE`): shares an extent between two files without
copying data. The source file is found by resolving its UUID via the UUID tree.

**Subvolume finalization**: once all commands for a subvolume have been processed,
call `BTRFS_IOC_SET_RECEIVED_SUBVOL` to record the UUID and ctransid, then set
the subvolume read-only with `BTRFS_IOC_SUBVOL_SETFLAGS`.

## Using `ReceiveContext`

If you want a complete, ready-to-use receive implementation rather than building
your own, the `receive` feature of `btrfs-stream` provides `ReceiveContext`:

```toml
btrfs-stream = { version = "0.5", features = ["receive"] }
```

```rust
use btrfs_stream::ReceiveContext;

let mut ctx = ReceiveContext::new(destination_dir)?;
ctx.receive(input_stream)?;
```

`ReceiveContext` handles all command types including v2 encoded writes (with
decompression fallback for zlib, zstd, and lzo) and v3 fs-verity. It uses an fd
cache to avoid reopening the same file for sequential writes, which is important
for performance when receiving large files.

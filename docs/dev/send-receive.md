# Send and Receive

`btrfs send` and `btrfs receive` transfer filesystem state between two btrfs
filesystems as a byte stream. This page explains how that works and how to use
the `btrfs-stream` and `btrfs-uapi` crates to implement receive in your own
application.

## How send works

`btrfs send` asks the kernel to generate a stream from a read-only subvolume.
The kernel walks the subvolume's B-trees and emits a sequence of commands that
describe every file, directory, symlink, and extent. An incremental send
(`-p <parent>`) emits only the differences from the parent subvolume.

The kernel call is `BTRFS_IOC_SEND`, which writes the stream to a file
descriptor, usually the write end of a pipe. A reader thread on the other end
copies the stream to a file or stdout.

`btrfs send --offline IMAGE` skips the kernel. `btrfs-fs` walks the subvolume
of an unmounted image and encodes a v1 stream with `btrfs-stream`'s
`StreamWriter`. It supports full sends only.

## The stream format

The stream is a header followed by a sequence of commands.

The header holds a magic number (`btrfs-stream\0`) and the format version (v1,
v2, or v3). Commands follow back-to-back until an `END` command.

Each command looks like this:

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

btrfs uses the *raw* CRC32C variant (initial seed 0, no final XOR), not the
standard ISO 3309 variant (initial seed `0xFFFFFFFF`). To compute or verify a
checksum, use:

```rust
let crc = !crc32c::crc32c_append(!0u32, data);
```

## Parsing a stream with `btrfs-stream`

The `btrfs-stream` crate provides `StreamReader`, which parses one command at a
time from any `Read` source:

```rust
use btrfs_stream::{StreamReader, StreamCommand};

let mut reader = StreamReader::new(input)?; // reads and validates the header
while let Some(command) = reader.next_command()? {
    match command {
        StreamCommand::Subvol { path, uuid, ctransid } => { /* create subvolume */ }
        StreamCommand::Mkfile { path } => { /* create file */ }
        StreamCommand::Write { path, offset, data } => { /* write data */ }
        StreamCommand::Rename { from, to } => { /* rename */ }
        StreamCommand::End => break,
        // ... the remaining v1, v2, and v3 command types
    }
}
```

`StreamReader::new` reads the stream header and returns an error if the magic
is wrong or the version is not 1, 2, or 3. `next_command` returns `None` at
EOF. `StreamWriter` is the matching encoder.

## Applying a stream with `btrfs-uapi`

To implement receive, apply each command to a mounted btrfs filesystem. The
relevant operations are:

**Subvolume and snapshot creation** (`BTRFS_IOC_SUBVOL_CREATE`,
`BTRFS_IOC_SNAP_CREATE_V2`): a `Subvol` command creates a new empty subvolume.
A `Snapshot` command looks up the source subvolume by UUID with
`subvolume_search_by_received_uuid` or `subvolume_search_by_uuid`, then
creates a writable snapshot.

**File operations**: standard POSIX calls (`open`, `create`, `unlink`,
`mkdir`, `rmdir`, `symlink`, `link`, `rename`). These need no btrfs-specific
ioctls.

**Write** (`BTRFS_IOC_ENCODED_WRITE` or `pwrite`): v2 streams may carry
pre-compressed data in `ENCODED_WRITE` commands. If the kernel supports encoded
writes, pass the data through as is. Otherwise decompress it and use `pwrite`.

**Clone** (`BTRFS_IOC_CLONE_RANGE`): shares an extent between two files without
copying data. The source file is found by resolving its UUID through the UUID
tree.

**Subvolume finalization**: after all commands for a subvolume are processed,
call `BTRFS_IOC_SET_RECEIVED_SUBVOL` to record the UUID and ctransid. Then make
the subvolume read-only with `BTRFS_IOC_SUBVOL_SETFLAGS`.

## Using `ReceiveContext`

For a complete receive implementation instead of building your own, enable the
`receive` feature of `btrfs-stream` and use `ReceiveContext`:

```toml
btrfs-stream = { version = "0.13", features = ["receive"] }
```

```rust
use btrfs_stream::{ReceiveContext, StreamCommand, StreamReader};

let mut ctx = ReceiveContext::new(&destination_dir)?;
let mut reader = StreamReader::new(input)?;
while let Some(command) = reader.next_command()? {
    if matches!(command, StreamCommand::End) {
        break;
    }
    ctx.process_command(&command)?;
}
ctx.finish_subvol()?;
```

`process_command` panics on `End`, so the caller handles it. `finish_subvol`
sets the received UUID and makes the subvolume read-only.

`ReceiveContext` handles all command types, including v2 encoded writes (with
decompression fallback for zlib, zstd, and lzo) and v3 fs-verity. It caches
file descriptors so sequential writes don't reopen the same file, which
matters for performance when receiving large files.

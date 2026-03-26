# btrfs-stream: btrfs send stream parser and receive operations

This crate handles the btrfs send stream format: a binary TLV protocol
used by `btrfs send` / `btrfs receive` to serialize and replay filesystem
changes between subvolume snapshots.

## Stream parsing (default, platform-independent)

The default feature set provides a zero-copy stream parser that works on
any platform:

- [`StreamReader`] reads a btrfs send stream from any `impl Read`,
  validates the stream header (magic, protocol version 1-3), and yields
  [`StreamCommand`] values with CRC32C integrity checks on every command.
- [`StreamCommand`] is an enum covering all v1, v2, and v3 command types
  (subvol, snapshot, write, clone, encoded write, fallocate, enable
  verity, and so on).
- [`Timespec`] represents timestamps carried in the stream.

```rust,no_run
use btrfs_stream::{StreamReader, StreamCommand};
use std::fs::File;

let file = File::open("stream.bin").unwrap();
let mut reader = StreamReader::new(file).unwrap();
while let Some(cmd) = reader.next_command().unwrap() {
    match cmd {
        StreamCommand::End => break,
        other => println!("{other:?}"),
    }
}
```

## Receive operations (feature `receive`, Linux-only)

Enable the `receive` feature to get [`ReceiveContext`], which applies a
parsed stream to a mounted btrfs filesystem:

```toml
[dependencies]
btrfs-stream = { version = "0.2", features = ["receive"] }
```

[`ReceiveContext`] creates subvolumes and snapshots, writes files, clones
extents, sets xattrs and permissions, and finalizes received subvolumes
with their received UUID. It handles v2 encoded writes with automatic
decompression fallback (zlib, zstd, lzo) and v3 fs-verity enablement.

This feature depends on [`btrfs-uapi`](../uapi) for ioctl access and
requires `CAP_SYS_ADMIN` on Linux.

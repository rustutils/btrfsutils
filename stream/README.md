# btrfs-stream

Btrfs send stream parser, encoder, and receive operations.

Use this crate to read, inspect, encode, or replay btrfs send streams.
The parser and encoder work on any platform. Enable the `receive`
feature to get `ReceiveContext`, which applies a stream to a mounted
btrfs filesystem on Linux.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's implemented

### Stream parser (default, platform-independent)

- Protocol versions 1, 2, and 3
- CRC32C validation on every command
- All 22 v1 command types: subvol, snapshot, mkfile, mkdir, mknod, mkfifo,
  mksock, symlink, rename, link, unlink, rmdir, set/remove xattr, write, clone,
  truncate, chmod, chown, utimes, update_extent, end
- v2 commands: encoded_write (compressed data), fallocate, fileattr
- v3 commands: enable_verity

### Stream encoder (default, platform-independent)

- `StreamWriter<W>`: mirror image of `StreamReader`. Writes the
  17-byte stream header on construction; `write_command(&cmd)`
  encodes any `StreamCommand` variant into the wire format with
  CRC32C, accepting any version (1/2/3).
- v2+ `BTRFS_SEND_A_DATA` quirk handled (no length field, extends
  to end of command payload), letting `Write` / `EncodedWrite`
  carry payloads beyond the v1 64 KiB cap.
- Round-trips with `StreamReader` for every variant — that's the
  primary correctness target (byte-for-byte parity with kernel
  send is impossible since command ordering inside a transaction
  has flexibility).

### Receive operations (`receive` feature, Linux-only)

- Subvolume and snapshot creation with UUID-based parent lookup
- Write operations via pwrite with fd caching
- Clone operations via `BTRFS_IOC_CLONE_RANGE` with UUID tree source resolution
- Encoded write with `BTRFS_IOC_ENCODED_WRITE` and decompression fallback (zlib, zstd, lzo)
- fs-verity enablement for v3 streams
- Subvolume finalization (received UUID set + read-only flag)
- Multi-stream support (continues after END command)
- Error counting with configurable max-errors threshold

## Testing

Unit tests cover stream header parsing, CRC validation, all command types,
and edge cases (truncated payloads, unknown commands, multi-command sequences).
No privileges needed.

```sh
cargo test -p btrfs-stream
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

# btrfs-stream

Btrfs send stream parser and receive operations.

Use this crate to read, inspect, or replay btrfs send streams. The stream
parser works on any platform. Enable the `receive` feature to get
`ReceiveContext`, which applies a stream to a mounted btrfs filesystem on
Linux.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## Usage

```toml
# Stream parsing only (any platform)
btrfs-stream = "0.5"

# Stream parsing + receive operations (Linux, requires CAP_SYS_ADMIN)
btrfs-stream = { version = "0.5", features = ["receive"] }
```

## What's implemented

### Stream parser (default, platform-independent)

- Protocol versions 1, 2, and 3
- CRC32C validation on every command
- All 22 v1 command types: subvol, snapshot, mkfile, mkdir, mknod, mkfifo, mksock, symlink, rename, link, unlink, rmdir, set/remove xattr, write, clone, truncate, chmod, chown, utimes, update_extent, end
- v2 commands: encoded_write (compressed data), fallocate, fileattr
- v3 commands: enable_verity

### Receive operations (`receive` feature, Linux-only)

- Subvolume and snapshot creation with UUID-based parent lookup
- Write operations via pwrite with fd caching
- Clone operations via BTRFS_IOC_CLONE_RANGE with UUID tree source resolution
- Encoded write with BTRFS_IOC_ENCODED_WRITE and decompression fallback (zlib, zstd, lzo)
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

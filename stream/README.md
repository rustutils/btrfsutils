# btrfs-stream

Btrfs send stream parser and receive operations.

Use this crate to read, inspect, or replay btrfs send streams. The stream
parser works on any platform. Enable the `receive` feature to get
`ReceiveContext`, which applies a stream to a mounted btrfs filesystem on
Linux.

```toml
# Stream parsing only (any platform)
btrfs-stream = "0.2"

# Stream parsing + receive operations (Linux, requires CAP_SYS_ADMIN)
btrfs-stream = { version = "0.2", features = ["receive"] }
```

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

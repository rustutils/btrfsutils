# btrfs-cli

An alternative implementation of the [btrfs-progs](https://github.com/kdave/btrfs-progs)
command-line tool, written in Rust.

This crate builds the `btrfs` binary. If you are a user who wants to manage
btrfs filesystems, install this. If you are a developer building tools on
top of btrfs, depend on `btrfs-uapi`, `btrfs-disk`, or `btrfs-stream`
instead.

```text
btrfs <command> [<subcommand>] [<args>]
```

Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to
see what is available. Most commands require root privileges or
`CAP_SYS_ADMIN`.

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

# btrfs-cli: a Rust reimplementation of the btrfs command-line tool

This crate provides the `btrfs` command-line binary, a Rust reimplementation
of [btrfs-progs](https://github.com/kdave/btrfs-progs). It is built on top
of [`btrfs-uapi`](../uapi) for kernel communication and
[`btrfs-disk`](../disk) for direct on-disk structure parsing.

Not all commands from btrfs-progs are implemented yet. Run `btrfs help` to
see what is available.

## Usage

```text
btrfs <command> [<subcommand>] [<args>]
```

Most commands require root privileges or `CAP_SYS_ADMIN`.

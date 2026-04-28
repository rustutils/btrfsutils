<img class="banner-light" alt="btrfsutils" src="branding/banner-light.svg">
<img class="banner-dark" alt="btrfsutils" src="branding/banner-dark.svg">

# Introduction

btrfsutils is a Rust implementation of the btrfs filesystem utilities. It
provides three command-line tools: `btrfs`, for managing and inspecting btrfs
filesystems; `btrfs-mkfs`, for creating new ones; and `btrfs-tune`, for offline
superblock tuning. All three aim to be drop-in replacements for the tools
provided by [btrfs-progs](https://github.com/kdave/btrfs-progs).

Most commands are fully implemented and produce output matching the C
reference. The explicit goal is to be drop-in compatible with the reference
implementation, with additional features. This is currently in a beta (pre-1.0)
version, so it should not be used in production, but the commands that are
implemented are thoroughly tested and can be assumed to be correctly
implemented.

It also provides library crates that can be used to access kernel APIs to
manage btrfs filesystems, decode and write on-disk structures and decode and
handle the `btrfs send` format.

## Source Code

The source is available on [github](https://github.com/rustutils/btrfsutils)
and [gitlab](https://gitlab.com/rustutils/btrfsutils).


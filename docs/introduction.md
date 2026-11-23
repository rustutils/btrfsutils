<img class="banner-light" alt="btrfsutils" src="branding/banner-light.svg">
<img class="banner-dark" alt="btrfsutils" src="branding/banner-dark.svg">

# Introduction

btrfsutils is a Rust implementation of the btrfs filesystem utilities. It
provides three command-line tools:

- `btrfs` manages and inspects btrfs filesystems.
- `btrfs-mkfs` creates new ones.
- `btrfs-tune` tunes the superblock of unmounted filesystems.

All three aim to be drop-in replacements for their
[btrfs-progs](https://github.com/kdave/btrfs-progs) counterparts, with some
extra features.

Most commands are fully implemented and match the output of the C reference.
The project is pre-1.0 and not yet meant for production, but the implemented
commands are thoroughly tested.

It also provides library crates for the btrfs kernel APIs, the on-disk format,
and the `btrfs send` stream format.

## Source Code

The source is available on [github](https://github.com/rustutils/btrfsutils)
and [gitlab](https://gitlab.com/rustutils/btrfsutils).


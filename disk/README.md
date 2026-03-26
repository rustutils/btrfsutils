# btrfs-disk

Platform-independent parsing of btrfs on-disk data structures.

Use this crate when you need to read btrfs superblocks or other on-disk
structures directly from block devices or image files, without going through
the kernel. Works on any platform that can read raw bytes.

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

# btrfs-uapi

Safe Rust wrappers around the btrfs kernel interface (ioctls and sysfs).

This is the low-level crate that talks to the kernel. If you are building
a tool that manages btrfs filesystems on Linux, this is the crate to depend
on. It provides typed, safe APIs for every btrfs ioctl and sysfs entry,
with no `unsafe` in the public API.

Requires Linux. Most operations require `CAP_SYS_ADMIN`.

Part of the [btrfs-progrs](https://github.com/rustutils/btrfs-progrs) project.

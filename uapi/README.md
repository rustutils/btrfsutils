# btrfs-uapi

Safe Rust wrappers around the btrfs kernel interface (ioctls and sysfs).

This is the low-level crate that talks to the kernel. Depend on it if you are
building a tool that manages btrfs filesystems on Linux. It provides typed,
safe APIs for every btrfs ioctl and sysfs entry, with no `unsafe` in the
public API.

Requires Linux. Most operations require `CAP_SYS_ADMIN`.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's wrapped

All non-superseded btrfs ioctls are wrapped, plus several standard VFS ioctls:

- **Balance**: start, pause, cancel, resume, progress
- **Device**: add, remove, scan, forget, ready, info, stats
- **Replace**: start, status, cancel
- **Scrub**: start, cancel, progress
- **Subvolume**: create, delete, snapshot, info, get and set flags, get and
  set default, list
- **Send and receive**: send stream, received subvol set, clone range,
  encoded read and write, UUID tree search
- **Filesystem**: fs info, space info, sync, start and wait sync, get and set
  label, resize
- **Quota**: enable, disable, rescan, rescan status and wait
- **Qgroup**: create, destroy, assign, remove, limit, list, clear stale
- **Inode resolution**: subvolume ID lookup, inode to paths, logical address
  to inodes, subvolume ID to path
- **Reflink**: clone range (FICLONERANGE)
- **Tree search**: generic v1 and v2 with a callback-based cursor, and an
  automatic v2-to-v1 fallback
- **Features**: get and set feature flags, get supported features
- **Defrag**: range defrag with compression options
- **Dedupe**: file extent same (out-of-band dedup)
- **Fiemap**: file extent mapping (FS_IOC_FIEMAP)
- **Verity**: fs-verity enablement (FS_IOC_ENABLE_VERITY)
- **Block device**: device size, discard of a range or the whole device
- **Sysfs**: filesystem info, commit stats, quota status, scrub speed limits,
  send stream version
- **Chunk and device tree**: chunk allocation walks, minimum device size
  calculation

## What's not wrapped

Only the 7 superseded v1 variants are unwrapped (balance, clone, defrag,
logical_ino, snap_create, snap_destroy, subvol_create). Their v2 versions are
wrapped.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.

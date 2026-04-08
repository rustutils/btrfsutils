# btrfs-uapi

Safe Rust wrappers around the btrfs kernel interface (ioctls and sysfs).

This is the low-level crate that talks to the kernel. If you are building
a tool that manages btrfs filesystems on Linux, this is the crate to depend
on. It provides typed, safe APIs for every btrfs ioctl and sysfs entry,
with no `unsafe` in the public API.

Requires Linux. Most operations require `CAP_SYS_ADMIN`.

Part of the [btrfsutils](https://github.com/rustutils/btrfsutils) project.

## What's wrapped

All non-superseded btrfs ioctls are wrapped, plus several standard VFS ioctls:

- **Balance**: start, pause, cancel, resume, progress
- **Device**: add, remove, scan, forget, ready, info, stats
- **Replace**: start, status, cancel
- **Scrub**: start, cancel, progress
- **Subvolume**: create, delete, snapshot, info, flags get/set, default
  get/set, list
- **Send / Receive**: send stream, received subvol set, clone range, encoded
  read/write, UUID tree search
- **Filesystem**: fs info, space info, sync, start/wait sync, label get/set,
  resize
- **Quota**: enable, disable, rescan, rescan status/wait
- **Qgroup**: create, destroy, assign, remove, limit, list, clear stale
- **Tree search**: generic v1 and v2 with callback-based cursor
- **Features**: get/set feature flags, get supported features
- **Defrag**: range defrag with compression options
- **Dedupe**: file extent same (out-of-band dedup)
- **Fiemap**: file extent mapping (FS_IOC_FIEMAP)
- **Verity**: fs-verity enablement (FS_IOC_ENABLE_VERITY)
- **Block device**: device size, discard range/whole
- **Sysfs**: filesystem info, commit stats, quota status, scrub speed limits,
  send stream version
- **Chunk/device tree**: chunk allocation walks, min device size calculation

## What's not wrapped

The only unwrapped ioctls are 7 superseded v1 variants (balance, clone,
defrag, logical_ino, snap_create, snap_destroy, subvol_create) where the
v2 is already wrapped.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
